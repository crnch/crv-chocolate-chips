from datetime import datetime, date, timedelta
import csv
import re
from functools import cache

import requests
from brownie import Contract, chain, web3, ZERO_ADDRESS

from .helpers import etherscan
from .helpers.coingecko import coin_price


def main(*args):
    pool, *_ = args
    contracts = initialize_contracts(pool)
    coin_addrs = set(get_coins(contracts[pool]))

    deployed_events = contracts["registry"].events.get_sequence(
        from_block=0, event_type="PlainPoolDeployed"
    )
    [deploy_event] = [
        event
        for event in deployed_events
        if coin_addrs == (set(event["args"]["coins"]) - set([ZERO_ADDRESS]))
    ]
    pool_deployed_block = deploy_event["blockNumber"]
    # assert pool_deployed_block == etherscan.creation_block(contracts[pool].address)

    def get_min_crv_apy(block):
        nonlocal contracts
        crv_price = coin_price(contracts["crv"].address, block)
        inflation_rate = contracts["crv"].rate(block_identifier=block) / 10**18
        relative_weight = (
            contracts["gauge_controller"].gauge_relative_weight(
                contracts[pool + "_gauge"], block_identifier=block
            )
            / 10**18
        )

        working_supply = (
            contracts[pool + "_gauge"].working_supply(block_identifier=block) / 10**18
        )
        asset_price = calc_asset_price(contracts[pool], block)
        virtual_price = (
            contracts[pool].get_virtual_price(block_identifier=block) / 10**18
        )

        # this prints the min APY; for max, multiply by 2.5
        return (
            (crv_price * inflation_rate * relative_weight * 12614400)
            / (working_supply * asset_price * virtual_price)
            * 100
        )

    with open(f"{pool}-crv-apy-history.csv", "w") as fp:
        writer = csv.writer(fp)
        writer.writerow(["Date", "CRV APY"])
        for block in yield_blocks_close_to_midnight(pool_deployed_block):
            current_date = date.fromtimestamp(chain[block].timestamp)
            try:
                data = [current_date, get_min_crv_apy(block)]
            except (ValueError, ZeroDivisionError) as e:
                print(f"{current_date}: No data")
                continue
            print(f"{data[0]}: {data[1]}%")
            writer.writerow(data)


def get_contract(addr):
    try:
        contract = Contract(addr)
    except ValueError:
        contract = Contract.from_explorer(addr)
    return contract


def get_pool_addr(name):
    url = "https://api.curve.fi/api/getFactoryAPYs?version=2"
    response = requests.get(url)
    pools = response.json()["data"]["poolDetails"]
    pool_symbol_pattern = re.compile(rf"^{name}-f$", re.IGNORECASE)
    [pool] = [pool for pool in pools if pool_symbol_pattern.match(pool["poolSymbol"])]
    return pool["poolAddress"]


def initialize_contracts(pool):
    provider = get_contract("0x0000000022D53366457F9d5E68Ec105046FC4383")
    provider.set_alias("provider")
    metapool_registry = get_contract("0xB9fC157394Af804a3578134A6585C0dc9cc990d4")
    metapool_registry.set_alias("registry")

    pool_addr = get_pool_addr(pool)
    pool_contracts = {pool: get_contract(pool_addr)}

    pool_gauge = get_contract(metapool_registry.get_gauge(pool_contracts[pool]))
    registry = get_contract(provider.get_registry())
    gauge_controller = get_contract(registry.gauge_controller())
    gauge_controller.set_alias("gauge_controller")
    crv = get_contract("0xD533a949740bb3306d119CC777fa900bA034cd52")
    crv.set_alias("crv")
    pool_contracts.update(
        registry=metapool_registry,
        gauge_controller=gauge_controller,
        crv=crv,
        **{f"{pool}_gauge": pool_gauge},
    )
    return pool_contracts


def yield_blocks_close_to_midnight(start_block, end_block=None):
    start_timestamp = chain[start_block]["timestamp"]
    dt = datetime.utcfromtimestamp(start_timestamp).replace(
        hour=0, minute=0, second=0
    ) + timedelta(days=1)
    current_block = start_block
    max_blockchain_height = end_block or int(web3.eth.block_number)

    while current_block < max_blockchain_height:
        dt_current_block = datetime.utcfromtimestamp(chain[current_block]["timestamp"])
        if dt_current_block > dt:
            dt += timedelta(days=1)
            print(f"Block #{current_block} mined at {dt_current_block}")
            yield current_block
        else:
            blocks_until_midnight_estimate = (
                (dt - dt_current_block).total_seconds() / 15.0 * 0.95
            )
            current_block += int(blocks_until_midnight_estimate) or 1


def calc_asset_price(pool, block=None):
    """
    Calculates the combined asset price of a pool
    """
    token_addrs = get_coins(pool)
    token_balances = [
        balance / 1e18 for balance in pool.get_balances(block_identifier=block)
    ]
    token_prices_usd = [coin_price(addr, block) for addr in token_addrs]

    return sum(
        balance * price for balance, price in zip(token_balances, token_prices_usd)
    ) / sum(token_balances)


@cache
def get_coins(pool):
    return Contract("registry").get_coins(pool)[:2]
