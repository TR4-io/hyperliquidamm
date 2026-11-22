"""Retain the complete public BTC time series in an official daily asset-context CSV."""

import csv
import itertools
import math
import re
from datetime import UTC, datetime, timedelta

import lz4.frame

PUBLIC_NUMBERS = {
    "funding",
    "open_interest",
    "prev_day_px",
    "day_ntl_vlm",
    "premium",
    "oracle_px",
    "mark_px",
    "mid_px",
    "impact_bid_px",
    "impact_ask_px",
}
REQUIRED_NUMBERS = {"funding", "oracle_px", "mark_px"}
NUMBER = re.compile(r"[-+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][-+]?[0-9]+)?")
SOURCE_CLOCK = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:[.,][0-9]{1,3}0*)?(?:Z|[+-][0-9]{2}:[0-9]{2})"
)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
MILLISECOND = timedelta(milliseconds=1)
DAY_MS = 86_400_000


def read_context(raw, start):
    samples = []
    times = []
    with lz4.frame.open(raw, "rt") as stream:
        reader = csv.DictReader(stream, strict=True)
        header = reader.fieldnames or []
        if len(header) != len(set(header)) or not {
            "time",
            "coin",
            *REQUIRED_NUMBERS,
        }.issubset(header):
            raise ValueError("Missing or duplicate asset context columns")
        for row in reader:
            if row["coin"] != "BTC":
                continue
            if None in row or None in row.values():
                raise ValueError("Malformed BTC asset context row")
            if not SOURCE_CLOCK.fullmatch(row["time"]):
                raise ValueError(
                    "Expected a timezone-aware millisecond BTC context clock"
                )
            utc = datetime.fromisoformat(row["time"])
            t = (utc - EPOCH) // MILLISECOND
            if not start <= t < start + DAY_MS or (times and t <= times[-1]):
                raise ValueError(
                    "BTC context timestamps outside the UTC day, duplicated or unordered"
                )
            # DictReader preserves the official column ordering, including time.
            clean = {
                k: v
                for k, v in row.items()
                if k in PUBLIC_NUMBERS or k in ("coin", "time")
            }
            for key in PUBLIC_NUMBERS.intersection(clean):
                value = clean[key]
                if value == "" and key not in REQUIRED_NUMBERS:
                    continue
                if not NUMBER.fullmatch(value) or not math.isfinite(float(value)):
                    raise ValueError(f"Invalid numeric BTC context field {key}")
            clean["time"] = t
            samples.append(clean)
            times.append(t)
    return samples, {
        "first_ms": times[0] if times else None,
        "last_ms": times[-1] if times else None,
        "max_gap_ms": max((b - a for a, b in itertools.pairwise(times)), default=0),
        "observed_minutes": len({t // 60_000 for t in times}),
        "expected_minutes": 1440,
    }
