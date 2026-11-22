"""Aggregate observed BTC fee charges from an already downloaded development hour."""

import argparse
import hashlib
import json
import os
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import lz4.frame
from archive import atomic_json

FIRST = int(datetime(2025, 12, 30, tzinfo=UTC).timestamp() * 1000)
FINAL = int(datetime(2026, 10, 26, tzinfo=UTC).timestamp() * 1000)
HOUR = 3_600_000
BASE = {"maker": Decimal("0.00015"), "taker": Decimal("0.00045")}
MICRO_USDC = Decimal("0.000001")


def scheduled(hour):
    if not FIRST <= hour < FINAL:
        return False
    dt = datetime.fromtimestamp(hour / 1000, UTC)
    return dt.weekday() == 0 and hour % (24 * HOUR) == 0


def audit(raw, receipt, mode):
    if mode not in ("weekly", "opportunistic"):
        raise ValueError("Unknown fee evidence sampling method")
    source = receipt["source"]
    hour = source["hour"]
    if not FIRST <= hour < FINAL or hour % HOUR:
        raise ValueError("Requires a development hour")
    if mode == "weekly" and not scheduled(hour):
        raise ValueError("Outside the preassigned weekly sample")
    dt = datetime.fromtimestamp(hour / 1000, UTC)
    expected = f"node_fills_by_block/hourly/{dt:%Y%m%d}/{dt.hour}.lz4"
    if (
        source["kind"] != "fills"
        or source["bucket"] != "hl-mainnet-node-data"
        or source["key"] != expected
    ):
        raise ValueError("Unexpected fill source")
    counts = Counter()
    size = {role: Counter() for role in BASE}
    frequencies = {role: Counter() for role in BASE}
    seen = {}
    with raw.open("rb") as f:
        if (
            os.fstat(f.fileno()).st_size != source["bytes"]
            or hashlib.file_digest(f, "sha256").hexdigest() != receipt["sha256"]
        ):
            raise ValueError("Raw fee sample failed verification")
        f.seek(0)
        with lz4.frame.open(f, "rb") as stream:
            for line in stream:
                for _, r in json.loads(line)["events"]:
                    if r["coin"] != "BTC":
                        continue
                    counts["btc_account_fill_rows"] += 1
                    if not hour <= r["time"] < hour + HOUR:
                        counts["outside_exchange_hour"] += 1
                        continue
                    if (
                        r.get("feeToken") != "USDC"
                        or type(r.get("crossed")) is not bool
                        or "fee" not in r
                    ):
                        counts["missing_role_or_usdc_fee"] += 1
                        continue
                    if (
                        r.get("liquidation")
                        or "liquidat" in str(r.get("dir", "")).lower()
                    ):
                        counts["liquidation_rows"] += 1
                        continue
                    if any(
                        Decimal(str(r[k]))
                        for k in ("deployerFee", "priorityFee", "protocolFee")
                        if r.get(k) is not None
                    ):
                        counts["other_fee_component"] += 1
                        continue
                    role = "taker" if r["crossed"] else "maker"
                    px, sz, gross = (Decimal(str(r[k])) for k in ("px", "sz", "fee"))
                    builder = Decimal(str(r.get("builderFee", "0")))
                    amount = px * sz
                    if not (
                        px.is_finite()
                        and sz.is_finite()
                        and gross.is_finite()
                        and builder.is_finite()
                        and px > 0
                        and sz > 0
                        and builder >= 0
                    ):
                        raise ValueError("Invalid numeric fee sample")
                    key = (role, r["tid"], r["oid"])
                    identity = (r["time"], px, sz, gross, builder)
                    if key in seen:
                        if seen[key] != identity:
                            raise ValueError("Conflicting fill in fee sample")
                        counts["duplicate"] += 1
                        continue
                    # Count a role/order/trade once. Keep participant identifiers out of outputs.
                    seen[key] = identity
                    if amount < 10:
                        size[role]["excluded_under_10_usdc"] += 1
                        continue
                    charge = gross - builder
                    difference = charge - amount * BASE[role]
                    if builder:
                        size[role]["builder_fee_removed"] += 1
                    size[role]["analyzed"] += 1
                    if abs(difference) <= MICRO_USDC:
                        size[role]["within_base_tolerance"] += 1
                    elif difference > 0:
                        size[role]["above_base"] += 1
                    else:
                        size[role]["below_base"] += 1
                    bps = (charge / amount * 10000).quantize(Decimal("0.01"))
                    frequencies[role][str(bps)] += 1
    return {
        "sampling": mode,
        "hour": hour,
        "bucket": source["bucket"],
        "key": expected,
        "sourceBytes": source["bytes"],
        "sourceSha256": receipt["sha256"],
        "baseBps": {"maker": 1.5, "taker": 4.5},
        "toleranceUsdc": "0.000001",
        "minimumNotionalUsdc": "10",
        "counts": dict(counts),
        "byRole": {role: dict(row) for role, row in size.items()},
        "mostFrequentEffectiveBps": {
            role: dict(sorted(freq.items(), key=lambda item: (-item[1], item[0]))[:8])
            for role, freq in frequencies.items()
        },
        "interpretation": "Observed fees include participant-specific tiers, rebates and discounts. Rate matches do not identify the account tier or establish an uninterrupted historical schedule.",
    }


def save(raw, receipt_path, output, mode):
    result = audit(raw, json.loads(receipt_path.read_bytes()), mode)
    atomic_json(output, result)
    return result


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("raw", type=Path)
    ap.add_argument("receipt", type=Path)
    ap.add_argument("output", type=Path)
    args = ap.parse_args()
    save(args.raw, args.receipt, args.output, "opportunistic")
