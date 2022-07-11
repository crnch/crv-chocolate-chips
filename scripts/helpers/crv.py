from typing import Iterator, Dict, Any

import requests

CRV_API_BASE_URL = "https://api.curve.fi/api"

def yield_all_pool_data() -> Iterator[Dict[str, Dict[str, Any]]]:
    for registry_name in ["main", "crypto", "factory", "crypto-factory"]:
        yield get_pool_data(registry_name)


def get_pool_data(registry_name) -> Dict[str, Dict[str, Any]]:
    uri = f"/getPools/ethereum/{registry_name}"
    response = requests.get(CRV_API_BASE_URL + uri)
    if (
        response.status_code == 200
        and (response_data := response.json())
        and response_data["success"]
    ):
        data = {pool["address"].lower(): dict(**pool, registry_name=registry_name) for pool in response_data["data"]["poolData"]}
        return data
    else:
        return {}

pool_data: dict[str, dict[str, Any]] = {}
for registry_pool_data in yield_all_pool_data():
    pool_data.update(registry_pool_data)
