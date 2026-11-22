# Hyperliquid BTC perpetual market-making research

The simulator evaluates BTC quotes for a 10,000 USDC paper account using official Hyperliquid L2 books, taker trades and funding history. It cancels quotes and flattens at a 10% drawdown from peak executable equity.

The model trained on the first five days submitted zero quotes on January 4–9, 2026. On January 6–9, 9,640 hypothetical trade-throughs averaged −3.03 bps at the 30-second mark after one maker fee. The three active benchmarks carried inventory into a book gap, leaving their four-day P&L unknown. [Four-day development replay](research/btc/evidence/replay-20260106-20260109.json) The year-long validation and final evaluation remain pending. [Research methods and commands](research/btc/README.md)

## Download the development snapshot

The data branch includes the strategy source, fitted development models, hourly BTC book/trade packets starting December 30, 2025 UTC, daily BTC context samples at one-minute intervals, and 7,200 hourly funding rates covering training and validation. Each packet has source and coverage metadata. The final test stays sealed. [Browse the development data](https://github.com/TR4-io/hyperliquidamm/tree/btc-development-data)

To download the verified snapshot and run the offline model and account checks:

```sh
git clone --depth 1 --single-branch --branch btc-development-data https://github.com/TR4-io/hyperliquidamm.git
cd hyperliquidamm
sha256sum --quiet -c SHA256SUMS
node research/btc/btc-quote-model.cjs self-test
node research/btc/btc-account-replay.cjs self-test
```
