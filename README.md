# CRV Factory Pools Historical APR

This is a [`brownie`](https://github.com/eth-brownie/brownie) script to pull historical data about CRV APR for CRV Factory Pools.
I was inspired by the excellent [brownie video tutorial series](https://www.youtube.com/playlist?list=PLVOHzVzbg7bFUaOGwN0NOgkTItUAVyBBQ), in particular #19[[Github](https://github.com/curvefi/brownie-tutorial/tree/main/lesson-19-applications-iii)|[YT](https://www.youtube.com/watch?v=usZ8SXMY6iA)] and #30[[Github](https://github.com/curvefi/brownie-tutorial/tree/main/lesson-30-chain-history)|[YT](https://www.youtube.com/watch?v=B1OMuIr7fCI)].

## Installation

If you installed `brownie` via `pipx`, you can inject the additional dependencies via:

```sh
pipx runpip eth-brownie install -r requirements.txt
```

else, after setting up a dedicated venv and activating it, install the depencies as usual

```sh
pip install -r requirements.txt
```

## Data collection

The script to collect the full APR history for a pool needs the poolname (case insensitive) - which is usually the concatenated names of the assets, sometimes with some dividing char between them.

```sh
brownie run get_historical_crv_apr main rethwsteth
```
