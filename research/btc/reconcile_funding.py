"""Reconcile funding cash on an existing development replay's simulated fills."""

import argparse
import hashlib
import json
from decimal import Decimal
from pathlib import Path

from archive import atomic_json


def reconcile(diagnostic, rates_path, contexts_path, certificate):
    raw = diagnostic.read_bytes()
    report = json.loads(raw)
    cert = json.loads(certificate.read_bytes())
    rates_raw = rates_path.read_bytes()
    contexts_raw = contexts_path.read_bytes()
    if (
        cert["ratePageSha256"] != hashlib.sha256(rates_raw).hexdigest()
        or cert["contextPricesSha256"] != hashlib.sha256(contexts_raw).hexdigest()
    ):
        raise ValueError("Settlement evidence and replay funding sources differ")
    rates = json.loads(rates_raw)
    proxies = {r["time"]: Decimal(r["oraclePx"]) for r in json.loads(contexts_raw)}
    observed = {r["time"]: r for r in cert["events"]}
    quantum = Decimal(cert["cashRoundingAllowancePerPositionUsdc"])
    clip = Decimal(str(report["model"]["config"]["clip"]))
    results = {}
    for policy, r in report["results"].items():
        start, end = r["start"], r["end"]
        if (
            r["haltTime"] is not None
            or cert["start"] > start
            or end > cert["endExclusive"]
        ):
            raise ValueError(
                "Expected the unhalted first-week development interval with flat initial inventory"
            )
        events = [rr for rr in rates if start <= rr["time"] < end]
        missing = [rr["time"] for rr in events if rr["time"] not in observed]
        if missing:
            raise ValueError(f"Missing {len(missing)} diagnostic settlement bounds")
        expected_proxy = Decimal(0)
        lower, upper = Decimal(0), Decimal(0)
        active = 0
        for rr in events:
            t = rr["time"]
            # The simulator applies funding before executions at a tied timestamp.
            pos = sum(f["side"] for f in r["fills"] if f["time"] < t) * clip
            expected_proxy -= pos * proxies[t] * Decimal(rr["fundingRate"])
            if pos:
                event = observed[t]
                a, b = sorted(
                    -pos * Decimal(event[k])
                    for k in ("chargePerLongBtcUsdcLower", "chargePerLongBtcUsdcUpper")
                )
                lower += a - quantum
                upper += b + quantum
                active += 1
        reported = Decimal(str(r["fundingPnl"]))
        if abs(expected_proxy - reported) > Decimal("1e-10"):
            raise ValueError(
                f"Could not reproduce {policy} proxy cash flow from position history"
            )
        net = Decimal(str(r["net"]))
        results[policy] = {
            "settlements": len(events),
            "nonzeroPositionPayments": active,
            "reportedProxyFundingUsdc": str(reported),
            "observedFundingBoundsUsdc": list(map(str, (lower, upper))),
            "sameFillPathNetBoundsUsdc": list(
                map(str, (net - reported + lower, net - reported + upper))
            ),
        }
    return {
        "replaySha256": hashlib.sha256(raw).hexdigest(),
        "cashEvidenceSha256": hashlib.sha256(certificate.read_bytes()).hexdigest(),
        "scope": "Fixed simulated fill path and execution fees; per-payment allowance of one micro-USDC for rounding on nonzero balances.",
        "results": results,
    }


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("week", type=Path)
    cli.add_argument("output", type=Path)
    args = cli.parse_args()
    base = args.week
    atomic_json(
        args.output,
        reconcile(
            base / "diagnostic-replay.json",
            base / "fundingHistory.raw.json",
            base / "context-prices.json",
            base / "settlement-cash/manifest.json",
        ),
    )
