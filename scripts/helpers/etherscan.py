import re
import lxml.html.soupparser
from lxml.etree import XPath
import requests
from brownie import web3

LINKS_IN_CREATION_DIV = XPath("//div[@id='ContentPlaceHolder1_trContract']//a")
TX_HREF_PATTERN = re.compile(r"^/tx/\w+$")

def creation_tx_hash(addr: str) -> str:
    url = f"https://etherscan.io/address/{addr}"
    response = requests.get(url, headers={'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.53 Safari/537.36'})
    tree = lxml.html.soupparser.fromstring(response.text)
    link_els_in_creation_div = LINKS_IN_CREATION_DIV(tree)
    [link_el_with_tx_hash] = [el for el in link_els_in_creation_div if TX_HREF_PATTERN.match(el.get("href", ""))]
    return link_el_with_tx_hash.text

def creation_block(addr: str) -> int:
    tx_hash = creation_tx_hash(addr)
    transaction = web3.eth.get_transaction(tx_hash)
    return transaction["blockNumber"]
