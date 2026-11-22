"""Join official minute-sampled oracle and mark prices for a development sensitivity."""

import argparse
import csv
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import lz4.frame
from archive import atomic_json, sha
from funding import END, HOUR, START


def join(rates_path, contexts, output):
    rates = json.loads(rates_path.read_bytes())
    if [r["time"] // HOUR for r in rates] != list(range(START // HOUR, END // HOUR)):
        raise ValueError("Expected all 168 UTC funding hours")
    samples = {}
    for path in contexts:
        key = "asset_ctxs/" + path.name
        digest = sha(path)
        with lz4.frame.open(path, "rt") as raw:
            for row in csv.DictReader(raw):
                if row["coin"] != "BTC":
                    continue
                t = int(
                    datetime.fromisoformat(row["time"]).astimezone(UTC).timestamp()
                    * 1000
                )
                if t % HOUR or not START <= t < END:
                    continue
                if t in samples:
                    raise ValueError(f"Duplicate BTC context sample at {t}")
                samples[t] = (row, key, digest)
    result = []
    for rate in rates:
        t = rate["time"]
        sample_time = t // HOUR * HOUR
        row, key, digest = samples[sample_time]
        result.append(
            {
                "time": t,
                "sampleTime": sample_time,
                "oraclePx": row["oracle_px"],
                "markPx": row["mark_px"],
                "sourceBucket": "hyperliquid-archive",
                "sourceKey": key,
                "sourceSha256": digest,
                "sourceField": "oracle_px",
                "priceEvidence": "minute-sampled context proxy",
                "snapshotFundingMatchesSettledRate": Decimal(row["funding"])
                == Decimal(rate["fundingRate"]),
            }
        )
    atomic_json(output, result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("rates", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("context_objects", type=Path, nargs="+")
    args = parser.parse_args()
    join(args.rates, args.context_objects, args.output)
