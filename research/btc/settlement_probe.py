"""Derive aggregate BTC cash-rate bounds from an official LZ4 settlement prefix.

This is an offline diagnostic. With reported cash quantized to one micro-USDC,
each account constrains the common hourly charge per long BTC. We allow one
full quantum of unknown rounding per ledger entry and verify the intersection.
The returned bounds do not require separately identifying the exact spot oracle
or unrounded funding rate. Account identifiers never leave this computation.
"""

import argparse
import hashlib
import json
import re
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import lz4.frame
from archive import atomic_json, block_ms
from funding import HOUR

CASH_QUANTUM = Decimal("0.000001")
MARKER = '"Funding":{"deltas":['


def extract(data, rate, context):
    # The first Funding event routinely exceeds a 4 MiB range. Parse only
    # complete delta objects. JSON is not allowed to cross a gap between files.
    block = lz4.frame.LZ4FrameDecompressor().decompress(data).decode("utf8")
    begin = block.index(MARKER)
    line_start = block.rfind("\n", 0, begin) + 1
    header = json.loads(block[line_start : block.index(',"events":', line_start)] + "}")
    if (
        block_ms(header["block_time"]) != rate["time"]
        or context["time"] != rate["time"]
    ):
        raise ValueError("Block, settled rate and sampled price event times disagree")
    j = begin + len(MARKER)
    decoder = json.JSONDecoder()
    raw_rate = Decimal(rate["fundingRate"])
    if rate["coin"] != "BTC" or not raw_rate.is_finite():
        raise ValueError("Invalid BTC rate")
    seen = set()
    signs = Counter()
    lower = Decimal("-Infinity")
    upper = Decimal("Infinity")
    while j < len(block) and block[j] == "{":
        try:
            record, j = decoder.raw_decode(block, j)
        except json.JSONDecodeError:
            break  # Prefix ended during this object.
        if record["coin"] == "BTC":
            position = Decimal(record["szi"])
            cash = Decimal(record["funding_amount"])
            if (
                not position.is_finite()
                or not cash.is_finite()
                or cash != cash.quantize(CASH_QUANTUM)
                or Decimal(record["funding_rate"]) != raw_rate
                or record["user"] in seen
            ):
                raise ValueError("Inconsistent BTC funding delta")
            seen.add(record["user"])
            if position == 0:
                if cash != 0:
                    raise ValueError("Funding cash flow on zero BTC position")
            else:
                signs["long" if position > 0 else "short"] += 1
                implied = -cash / position
                width = CASH_QUANTUM / abs(position)
                lower = max(lower, implied - width)
                upper = min(upper, implied + width)
        if j >= len(block) or block[j] != ",":
            break
        j += 1
    if not (signs["long"] and signs["short"] and lower <= upper):
        raise ValueError("Insufficient independent positions or inconsistent bounds")
    equiv = sorted((lower / raw_rate, upper / raw_rate)) if raw_rate else []
    oracle = Decimal(context["oraclePx"])
    return {
        "time": rate["time"],
        "blockNumber": header["block_number"],
        "settledRate": str(raw_rate),
        "distinctBtcPositionsInPrefix": len(seen),
        "longCount": signs["long"],
        "shortCount": signs["short"],
        "chargePerLongBtcUsdcLower": str(lower),
        "chargePerLongBtcUsdcUpper": str(upper),
        "cashIntervalWidthAtPositionCapUsdc": str((upper - lower) * Decimal("0.01")),
        "reportedRateEquivalentOracleBounds": list(map(str, equiv)),
        "contextSampledOracle": str(oracle),
        "sampleInEquivalentOracleBounds": (equiv[0] <= oracle <= equiv[1])
        if equiv
        else None,
    }


def audit(prefixes, rates_path, contexts_path, output):
    rates = {r["time"] // HOUR: r for r in json.loads(rates_path.read_bytes())}
    contexts = {r["time"]: r for r in json.loads(contexts_path.read_bytes())}
    rows = []
    for path in prefixes:
        src = json.loads(path.with_suffix(".json").read_bytes())
        data = path.read_bytes()
        match = re.fullmatch(
            r"misc_events_by_block/hourly/(\d{8})/(\d+)\.lz4", src["key"]
        )
        if (
            src["bucket"] != "hl-mainnet-node-data"
            or src["sha256"] != hashlib.sha256(data).hexdigest()
            or src["range_prefix_bytes"] != len(data)
            or not match
        ):
            raise ValueError("Funding prefix provenance mismatch")
        day = datetime.strptime(match[1], "%Y%m%d").replace(tzinfo=UTC)
        h = int(day.timestamp()) // 3600 + int(match[2])
        r = rates[h]
        rows.append({**extract(data, r, contexts[r["time"]]), "source": src})
    atomic_json(
        output,
        {
            "cashRoundingAllowancePerPositionUsdc": str(CASH_QUANTUM),
            "scope": "Training-only sampled BTC event prefixes. Conditional bounds on realized charge per BTC; exact oracle not independently identified.",
            "rateSource": "https://api.hyperliquid.xyz/info",
            "ratePageSha256": hashlib.sha256(rates_path.read_bytes()).hexdigest(),
            "contextPricesSha256": hashlib.sha256(
                contexts_path.read_bytes()
            ).hexdigest(),
            "events": rows,
        },
    )


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("rates", type=Path)
    cli.add_argument("context_prices", type=Path)
    cli.add_argument("output", type=Path)
    cli.add_argument("prefixes", type=Path, nargs="+")
    args = cli.parse_args()
    audit(args.prefixes, args.rates, args.context_prices, args.output)
