# Hyperliquid BTC perpetual research snapshot

This archive contains 1,057,917 sampled L2 snapshots and 1,907,696 per-hour deduplicated BTC taker trade records from December 30, 2025 through January 5, 2026 UTC. Hourly packet manifests retain the source keys, raw sizes and hashes, and clock diagnostics. Some intervals have gaps. Trade-through is a simulated execution proxy.

The training snapshot extends through January 6, 2026 at 23:59:59 UTC. All 24 January 6 hourly packets and their source manifests are in `data/btc/annual/training/20260106/`. They contain 151,403 book snapshots and 432,505 per-hour deduplicated BTC taker trades. The same folder contains 24 hourly BTC asset context samples, including sampled oracle prices, with the official source key and checksum.

The training and validation funding pages contain 7,200 official hourly settlements through October 25, 2026 UTC. The BTC context CSVs contain 10,080 samples filtered from the seven official daily asset context objects. Sampled oracle and mark prices are development proxies.

The first-week folder contains the training-only fitted models and chronological development diagnostics. The five-day policy submitted zero quotes on January 4–5. The fitted models include their training dates. The sealed final period is excluded.

Extract into the strategy repository to reuse the data/btc layout. The first-week model predates later code changes, so its code hash describes the original fit. Run the current training runner to generate a current-version fit. SHA256SUMS records the uncompressed archive members' file hashes, including the exact compressed bytes for individual .gz packets.
