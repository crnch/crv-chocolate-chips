from functools import cache
import time
from datetime import datetime, date, timezone

from requests.exceptions import HTTPError
from pycoingecko import CoinGeckoAPI
from brownie import chain


cg = CoinGeckoAPI()

def cool_down(fnc):
    def wrapper(*args, **kwargs):
        try:
            return fnc(*args, **kwargs)
        except HTTPError:
            print("Something went wrong...sleeping 5 seconds")
            time.sleep(5)
            return wrapper(*args, **kwargs)

    return wrapper
            
@cache
def get_token_id(addr):
    return cg.get_coin_info_from_contract_address_by_id("ethereum", addr)["id"]

@cool_down
def coin_price(addr, block=None):
    cg_token_id = get_token_id(addr)
    if not block:
        response = cg.get_price(ids=cg_token_id, vs_currencies='usd')
        return response[cg_token_id]['usd']
    else:
        timestamp = chain[block]["timestamp"]
        current_dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        current_date = date(*current_dt.timetuple()[:3])
        response = cg.get_coin_history_by_id(cg_token_id, current_date.strftime("%d-%m-%Y")) 
        return response["market_data"]["current_price"]["usd"]
