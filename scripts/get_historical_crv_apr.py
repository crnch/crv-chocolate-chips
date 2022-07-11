from datetime import datetime, timedelta, date
import csv
import re
import time
import sys
import os
from functools import cache
from itertools import groupby
from typing import Mapping, Iterator, Iterable, Any, Union, Dict, Tuple, List, Optional

import requests
from loguru import logger
from brownie import Contract, chain, web3, ZERO_ADDRESS

from scripts.helpers import persistence

from .helpers import etherscan
from .helpers.coingecko import CRVAdaptor
from .helpers.crv import (
    pool_data
)
from .helpers import persistence

logger.remove()
logger.add(sys.stderr, level="DEBUG")

def main(*pool_addrs: str):
    # initialize pool_data from curve.fi API
    if not pool_addrs:
        pool_addrs = tuple(pool_addr.lower() for pool_addr in pool_data.keys())
    else:
        pool_addrs = tuple(pool_addr.lower() for pool_addr in pool_addrs)
    price = CRVAdaptor(pool_data)

    # get implementation contracts
    implementation_addrs = set(
        impl_addr for addr in pool_addrs if (impl_addr := pool_data[addr].get("implementationAddress"))
    )
    logger.info("Fetching factory pool implementations")
    tick = time.time()
    implementations = {addr: get_contract(addr) for addr in implementation_addrs}
    # initialize implementation contracts where there is no info from the api
    logger.success(f"Finished fetching factory pool implementations ({time.time() - tick:.2f}s)")
    
    def registry_grouper(pool_datum: Dict[str, str]) -> str:
        return pool_datum["registry_name"]
    sorted_pool_data_wo_impl = sorted((pool_datum for pool_datum in pool_data.values() if pool_datum["registry_name"] in ("main", "crypto")), key=registry_grouper)

    logger.info("Fetching base/crypto pool implementations")
    tick = time.time()
    for registry_name, grouped_pool_data in groupby(sorted_pool_data_wo_impl, key=registry_grouper):
        implementation: Union[Contract, None] = None
        for i, pool_datum in enumerate(grouped_pool_data):
            assert not pool_datum.get("implementationAddress", False), "should have no `implementationAddress"
            if i < 1:
                implementation = get_contract(pool_datum["address"])
                implementations[pool_datum["address"]] = implementation

            if implementation:
                pool_datum["implementationAddress"] = implementation.address
    logger.success(f"Finished fetching base/crypto pool implementations ({time.time() - tick:.2f}s)")

    assert all(pool_datum.get("implementationAddress", False) for pool_datum in pool_data.values())

    # fill missing symbol
    pool_data_without_symbol = (pool_datum for pool_datum in pool_data.values() if not pool_datum.get("symbol", False))
    for pool_datum in pool_data_without_symbol:
        symbol = "-".join(coin["symbol"] for coin in pool_datum["coins"])
        pool_datum["symbol"] = symbol

    gauge_controller = get_contract("0x2F50D538606Fa9EDD2B11E2446BEb18C9D5846bB")
    # we only use working_supply so the gauge version does not matter
    gauge_template = get_contract(gauge_controller.gauges(0))

    logger.info("Fetching pool contracts")
    tick = time.time()
    pools = {
        addr: Contract.from_abi(
            pool_data[addr]["symbol"],
            addr,
            implementations[pool_data[addr]["implementationAddress"]].abi,
        )
        for addr in pool_addrs
    }
    logger.success(f"Finished fetching pool contracts ({time.time() - tick:.2f}s)")

    logger.info("Fetching liquidity gauges contracts")
    tick = time.time()
    gauges = {
        pool_addr: Contract.from_abi(
            f'{pool_data[pool_addr]["symbol"]}-gauge', gauge_addr, gauge_template.abi
        )
        for pool_addr, gauge_addr in get_gauges(pool_addrs, pool_data).items()
    }
    logger.success(f"Finished liquidity gauges contracts ({time.time() - tick:.2f}s)")

    logger.info("Determine deployment blocks for all pools with gauges")
    tick = time.time()
    pools_with_gauges = {pool_addr for pool_addr in pools.keys() if pool_addr in gauges}
    # token_addr to min(timestamp)
    price_data_requirements: Dict[str, int] = {}

    for pool_addr, block_deployed in get_block_deployed(
        pools_with_gauges, pool_data
    ).items():
        pool_data[pool_addr].update(block_deployed=block_deployed)
        for coin in pool_data[pool_addr]["coins"]:
            old_min_timestamp = price_data_requirements.get(coin["address"])
            timestamp = chain[block_deployed].timestamp
            price_data_requirements[coin["address"]] = min(old_min_timestamp, timestamp) if old_min_timestamp else timestamp
    logger.success(f"Finished determining deployment blocks for all pools with gauges ({time.time() - tick:.2f}s)")

    crv_token = get_contract("0xD533a949740bb3306d119CC777fa900bA034cd52")

    def get_min_crv_apr(pool_addr: str, block: int) -> float:
        crv_price = price.coin(crv_token.address, block)
        inflation_rate = get_crv_token_rate() / 10**18
        relative_weight = (
            gauge_controller.gauge_relative_weight(
                gauges[pool_addr], block_identifier=block
            )
            / 10**18
        )

        working_supply = (
            gauges[pool_addr].working_supply(block_identifier=block) / 10**18
        )
        asset_price = price.calc_asset(pools[pool_addr], block)
        virtual_price = (
            pools[pool_addr].get_virtual_price(block_identifier=block) / 10**18
        )

        # this prints the min APY; for max, multiply by 2.5
        return (
            (crv_price * inflation_rate * relative_weight * 12614400)
            / (working_supply * asset_price * virtual_price)
            * 100
        )

    max_datetime = None
    csv_path = "crv-apr-history.csv"
    if os.path.exists(csv_path):
        with open(csv_path) as fp:
            reader = csv.DictReader(fp)
            max_datetime = max(datetime.fromisoformat(row["date"]) for row in reader)


    with open(csv_path, "w") as fp:
        column_names = ["date"] + sorted(pool_data[addr]["symbol"] for addr in pools_with_gauges)
        writer = csv.DictWriter(fp, fieldnames=column_names)
        writer.writeheader()
        block_start = min(pool_data[pool_addr]["block_deployed"] for pool_addr in pools_with_gauges)
        if not max_datetime:
            timestamp_start = chain[block_start].timestamp
        else:
            timestamp_start = int(max_datetime.timestamp())

        price.fetch_history(timestamp_start)
        
        datetime_start = datetime.utcfromtimestamp(timestamp_start)
        logger.info(f"Begin pulling data from block {block_start} ({datetime_start})")
        tick = time.time()
        for block in yield_blocks_close_to_midnight(block_start):
            current_datetime = datetime.utcfromtimestamp(chain[block].timestamp)
            current_date = current_datetime.date()
            data: Dict[str, Union[str, float]] = {"date": current_date.isoformat()}
            deployed_pool_addrs = (addr for addr in pools_with_gauges if pool_data[addr]["block_deployed"] <= block)
            
            for addr in deployed_pool_addrs:
                try:
                    min_crv_apr = get_min_crv_apr(addr, block)
                    data.update({pool_data[addr]["symbol"]: min_crv_apr})
                    logger.info(f"[{current_date}] {pool_data[addr]['symbol']}: {min_crv_apr:.2f}%")
                except (ValueError, ZeroDivisionError) as e:
                    logger.error(f"[{current_date}] {pool_data[addr]['symbol']}: No data ({e})")
                    continue
            
            data.update({column_name: .0 for column_name in set(column_names) - data.keys()})

            writer.writerow(data)


def get_gauges(
    pool_addrs: Iterable[str], pool_data: Mapping[str, Mapping[str, Any]]
) -> Dict[str, str]:
    pool_addr_to_gauge_addrs: Dict[str, str] = {}

    main_registry = get_contract("0x90E00ACe148ca3b23Ac1bC8C240C2a7Dd9c2d7f5")
    factory_registry = get_contract("0xB9fC157394Af804a3578134A6585C0dc9cc990d4")

    for pool_addr in pool_addrs:
        if "factory" in pool_data[pool_addr]["id"]:
            gauge_addr = factory_registry.get_gauge(pool_addr)
        else:
            gauge_result = main_registry.get_gauges(pool_addr)
            gauge_addr = gauge_result[0][0]
        if gauge_addr != ZERO_ADDRESS:
            pool_addr_to_gauge_addrs[pool_addr] = gauge_addr

    return pool_addr_to_gauge_addrs

def get_block_deployed(
    pool_addrs: Iterable[str], pool_data: Mapping[str, Mapping[str, Any]]
) -> Dict[str, int]:
    """
    Parses all events since blockchain inception to get the block where pools were deployed
    TODO: make persistent
    """
    pool_addr_to_block_deployed = {}
    # better get this from the address provider?
    main_registry = get_contract("0x90E00ACe148ca3b23Ac1bC8C240C2a7Dd9c2d7f5")
    factory_registry = get_contract("0xB9fC157394Af804a3578134A6585C0dc9cc990d4")

    # TODO: make persistent?
    main_registry_events = main_registry.events.get_sequence(0, event_type="PoolAdded")
    pool_addr_to_block_deployed.update(
        {event["args"]["pool"].lower(): event["blockNumber"] for event in main_registry_events}
    )

    def normalize(coin_addrs: Iterable[str]) -> Tuple[str, ...]:
        """
        Return hashable, normalized tuple
        """
        return tuple(sorted(addr.lower() for addr in coin_addrs if addr != ZERO_ADDRESS))

    coins_to_block_deployed = {}
    factory_plain_pool_events = factory_registry.events.get_sequence(
        0, event_type="PlainPoolDeployed"
    )
    for event in factory_plain_pool_events:
        coins = normalize(event["args"]["coins"])
        coins_to_block_deployed[coins] = event["blockNumber"]

    factory_meta_pool_events = factory_registry.events.get_sequence(
        0, event_type="MetaPoolDeployed"
    )
    for event in factory_meta_pool_events:
        coins = normalize([event["args"]["coin"], event["args"]["base_pool"]])
        coins_to_block_deployed[coins] = event["blockNumber"]

    factory_pool_addrs = (addr for addr in pool_addrs if addr not in pool_addr_to_block_deployed)
    for pool_addr in factory_pool_addrs:
        coins = normalize(coin["address"] for coin in pool_data[pool_addr]["coins"])
        block_deployed = coins_to_block_deployed.get(coins)
        is_meta: bool = factory_registry.is_meta(pool_addr)
        if block_deployed:
            logger.debug(f"Successfully determined deployment block for pool {pool_addr} (meta: {is_meta})")
            pool_addr_to_block_deployed[pool_addr] = block_deployed
        else:
            logger.warning(f"Fallback etherscan web crawl for pool {pool_addr} (meta: {is_meta})")
            pool_addr_to_block_deployed[pool_addr] = etherscan.creation_block(pool_addr)

    assert all(pool_addr in pool_addr_to_block_deployed for pool_addr in pool_addrs), "Should have determined all pool deploy block numbers"

    return pool_addr_to_block_deployed


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


def yield_blocks_close_to_midnight(start_block: int, end_block: Union[int, None]=None) -> Iterator[int]:
    start_timestamp = chain[start_block]["timestamp"]
    dt = datetime.utcfromtimestamp(start_timestamp).replace(
        hour=0, minute=0, second=0
    ) + timedelta(days=1)
    current_block = start_block
    max_blockchain_height = end_block or int(web3.eth.block_number)

    i = 0
    while current_block < max_blockchain_height:
        dt_current_block = datetime.utcfromtimestamp(chain[current_block]["timestamp"])
        if dt_current_block > dt:
            dt += timedelta(days=1)
            logger.info(f"Block #{current_block} mined at {dt_current_block} ({i} iterations)")
            yield current_block
            i = 0
        else:
            blocks_until_midnight_estimate = (
                (dt - dt_current_block).total_seconds() / 15.0 * 0.95
            )
            current_block += int(blocks_until_midnight_estimate) or 1
            i += 1



def get_coins(pool: Contract) -> List[str]:
    return [coin["address"] for coin in pool_data[pool.address.lower()]["coins"]]

@cache
def get_decimals(token_addr: str) -> Optional[int]:
    decimals = None
    for pool in pool_data.values():
        for coin in pool["coins"]:
            if token_addr.lower() == coin["address"].lower():
                decimals = int(coin["decimals"])
                break
        if decimals:
            break    
    
    return decimals

@cache
def get_crv_token_rate(block: int) -> int:
    crv_token = get_contract("0xD533a949740bb3306d119CC777fa900bA034cd52")
    return crv_token.rate(block_identifier=block)

