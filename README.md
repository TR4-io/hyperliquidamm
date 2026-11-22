# Hyperliquid BTC perpetual market-making research

The simulator evaluates BTC quotes for a 10,000 USDC paper account using official Hyperliquid L2 books, taker trades and funding history. It cancels quotes and flattens at a 10% drawdown from peak executable equity.

The model trained on the first five days submitted zero quotes on January 4–9, 2026. On January 6–9, 9,640 hypothetical trade-throughs averaged −3.03 bps at the 30-second mark after one maker fee. The three active benchmarks carried inventory into a book gap, leaving their four-day P&L unknown. [Four-day development replay](research/btc/evidence/replay-20260106-20260109.json) The year-long validation and final evaluation remain pending. [Research methods and commands](research/btc/README.md)

The rolling development fit through January 11 declines to quote in all 36 flat-inventory states. The January 11 decision cutoff allows outstanding 30-second markout labels to finish just after midnight UTC. [Fitted model checkpoint](research/btc/evidence/development-model-through-2026-01-11.json)

## Download the development snapshot

The data branch includes the strategy source, fitted development models, 192 hourly BTC book/trade packets from December 30, 2025 through January 6, 2026 UTC, minute-level BTC context samples for the first seven days, hourly context samples on January 6, and 7,200 hourly funding rates covering training and validation. The branch also includes verified funding cash evidence for the first 168 hours. Each packet has source and coverage metadata. The final test stays sealed. [Browse the development data](https://github.com/TR4-io/hyperliquidamm/tree/btc-development-data)

To download the verified snapshot and run the offline model and account checks:

```sh
git clone --depth 1 --single-branch --branch btc-development-data https://github.com/TR4-io/hyperliquidamm.git
cd hyperliquidamm
sha256sum --quiet -c SHA256SUMS
node research/btc/btc-quote-model.cjs self-test
node research/btc/btc-account-replay.cjs self-test
```

## Additional development packets

January 7–9, 2026 UTC is available as a separate shallow clone. The batch includes 72 hourly packets, BTC asset contexts, source manifests and 153 file checksums. [Browse January 7–9 on GitHub](https://github.com/TR4-io/hyperliquidamm/tree/data-btc-training-20260107-20260109)

```sh
git clone --depth 1 --single-branch --branch data-btc-training-20260107-20260109 https://github.com/TR4-io/hyperliquidamm.git btc-jan07-09
(cd btc-jan07-09 && sha256sum --quiet -c SHA256SUMS)
```

January 10–11, 2026 UTC adds 48 hourly packets, 48 hourly BTC context samples, and 102 file checksums. [Browse January 10–11 on GitHub](https://github.com/TR4-io/hyperliquidamm/tree/data-btc-training-20260110-20260111)

New date-bounded batches can be found in the [repository releases](https://github.com/TR4-io/hyperliquidamm/releases) or [data branches](https://github.com/TR4-io/hyperliquidamm/branches/all?query=data-btc).
