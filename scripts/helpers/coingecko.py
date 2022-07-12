from functools import cache
import time
from datetime import datetime
import itertools
from typing import Optional, Mapping, Any, Dict, Sequence
from functools import wraps

from requests.exceptions import HTTPError
from pycoingecko import CoinGeckoAPI
from brownie import ZERO_ADDRESS, chain, Contract
import pandas as pd
from loguru import logger

from . import persistence

cg = CoinGeckoAPI()

def cool_down(fnc):
    @wraps(fnc)
    def wrapper(*args, **kwargs):
        try: 
            cooldown = kwargs.pop("_cooldown")
        except KeyError:
            cooldown = 1

        try:
            return fnc(*args, **kwargs)
        except HTTPError as e:
            logger.error(f"Something went wrong...sleeping {cooldown} seconds ({e})")
            time.sleep(cooldown)
            return wrapper(*args, _cooldown=cooldown*2, **kwargs)

    return wrapper


@cache
def get_timestamp(block: int) -> int:
    return chain[block].timestamp
class CRVAdaptor:
    currency_crv_to_cg_id = {"eth": "ethereum", "btc": "bitcoin"}

    def __init__(self, pool_data: Mapping[str, Mapping[str, Any]], pool_addrs: Optional[Sequence[str]]=None):
        if not pool_addrs:
            self.pool_data = pool_data
        else:
            self.pool_data = {pool_addr: pool_datum for pool_addr, pool_datum in pool_data.items() if pool_addr in pool_addrs}
        # add all addrs to coins
        pool_data_not_eth_btc_usd = (
            pool_datum
            for pool_datum in pool_data.values()
            if pool_datum["assetTypeName"] in {"other", "unknown"}
        )

        self.coin_addr_to_symbol: Dict[str, str] = {
            coin["address"]: coin["symbol"]
            for coin in itertools.chain.from_iterable(
                pool_datum["coins"] for pool_datum in pool_data_not_eth_btc_usd
            ) if coin["address"] != ZERO_ADDRESS
        }

        self.coin_addr_to_cg_id: Dict[str, str] = {
            coin_addr: coingecko_token_id
            for coin_addr in self.coin_addr_to_symbol.keys()
            if (coingecko_token_id := self.get_token_id(coin_addr))
        }
    
    @cool_down
    @persistence.cache
    def get_token_id(self, addr: str) -> Optional[str]:
        try:
            response = cg.get_coin_info_from_contract_address_by_id("ethereum", addr)
            token_id = response["id"]
            logger.info(f"Fetched CG id for {addr}: {token_id}")
            return token_id
        except ValueError as e:
            logger.error(f"Could not find coingecko data for {self.coin_addr_to_symbol[addr]} ({e})")
            return None

    @cool_down
    def _fetch_token_history(self, token_id: str, timestamp_start: int, timestamp_end: int) -> Dict[str, Any]:
        response = cg.get_coin_market_chart_range_by_id(token_id, "usd", timestamp_start, timestamp_end)
        return response
    
    def fetch_history(self, timestamp_start: int, timestamp_end: Optional[int]=None):
        coingecko_ids = set(["bitcoin", "ethereum"]).union(self.coin_addr_to_cg_id.values())
        timestamp_end = timestamp_end or int(datetime.now().timestamp())
        logger.info(f"Fetching price history from coingecko from {datetime.utcfromtimestamp(timestamp_start).date()} to {datetime.utcfromtimestamp(timestamp_end).date()}")
        price_data = {}
        for token_id in coingecko_ids:
            try:
                response = self._fetch_token_history(token_id, timestamp_start, timestamp_end)
            except ValueError as e:
                logger.error(f"Error fetching price data for {token_id} ({e})")
                continue
            logger.success(f"Successfully fetched price history for {token_id}")
            if response["prices"]:
                timestamps, prices_usd = zip(*response["prices"])
                price_data[token_id] = pd.Series(prices_usd, index=pd.to_datetime(timestamps, unit="ms"))
            else:
                continue
        logger.success("Finished fetching prices")
        self.price_data = pd.DataFrame(price_data)


    def calc_asset(self, pool: Contract, block:int) -> float:
        """
        Calculates the combined asset price of a pool
        """
        pool_datum = self.pool_data[pool.address.lower()]
        pool_type = pool_datum["assetTypeName"]
        timestamp = get_timestamp(block)
        date_utc = datetime.utcfromtimestamp(timestamp).date()
        date_str = date_utc.isoformat()

        match pool_type:
            case "usd":
                return 1
            case "eth":
                return self.price_data.loc[date_str, "ethereum"]
            case "btc": 
                return self.price_data.loc[date_str, "bitcoin"]

        decimals = (decimal for dec_str in pool_datum["decimals"] if (decimal := int(dec_str)))

        try: 
            balances = pool.get_balances(block_identifier=block)
        except AttributeError:
            n_coins = len(self.pool_data[pool.address.lower()]["coins"])
            balances = (pool.balances(i, block_identifier=block) for i in range(n_coins))

        token_balances = [
            balance / decimal for balance, decimal in zip(balances, decimals)
        ]

        token_prices_usd = [self.coin(coin["address"].lower(), block) for coin in pool_datum["coins"]]

        return sum(
            balance * price for balance, price in zip(token_balances, token_prices_usd)
        ) / sum(token_balances)

    @cache
    def coin(self, addr: str, block: int) -> float:
        """
        Return the price for a token
        """
        token_id = self.coin_addr_to_cg_id.get(addr)
        if not token_id:
            return 0
        
        timestamp = get_timestamp(block)
        date_utc = datetime.utcfromtimestamp(timestamp).date()
        return self.price_data.loc[date_utc.isoformat(), token_id]

