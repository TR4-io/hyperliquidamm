# Hyperliquid BTC perpetual market-making research

The simulator evaluates BTC quotes for a 10,000 USDC paper account using official Hyperliquid L2 books, taker trades and funding history. It cancels quotes and flattens at a 10% drawdown from peak executable equity.

The first five-day fit submitted zero quotes in the January 4–5, 2026 development replay. The year-long validation and final evaluation remain pending. [Research methods and commands](research/btc/README.md)

## Download the development snapshot

The data branch includes the strategy source, fitted development models, 168 hourly BTC book/trade packets from December 30 through January 5 UTC, daily BTC context samples at one-minute intervals, and 7,200 hourly funding rates covering training and validation. Each packet has source and coverage metadata. The final test stays sealed. [Browse the development data](https://github.com/TR4-io/hyperliquidamm/tree/btc-development-data)

To download the verified snapshot and run the offline model and account checks:

```sh
git clone --depth 1 --single-branch --branch btc-development-data https://github.com/TR4-io/hyperliquidamm.git
cd hyperliquidamm
sha256sum --quiet -c SHA256SUMS
node research/btc/btc-quote-model.cjs self-test
node research/btc/btc-account-replay.cjs self-test
```
