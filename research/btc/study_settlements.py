"""Collect aggregate hourly BTC funding cash evidence as official contexts arrive."""

import argparse
import fcntl
import gzip
import hashlib
import json
import sys
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from archive import atomic_json, estimated_cost, sha, sources
from bulk_archive import (
    AFTER_WEEK,
    AUXILIARY_START,
    FIRST,
    LAST,
    ArchiveGetUnavailable,
    ArchiveHeadUnavailable,
    clients,
    day_dir,
    error_details,
    head,
    partition,
    retryable,
)
from funding import HOUR, URL
from settlement_probe import CASH_QUANTUM
from settlement_week import (
    BUCKET,
    MAX_PREFIX,
    STEP,
    aggregate_prefix,
    fetch_range,
    result_source,
    source,
)
from study_funding import atomic, join_samples, timestamp, validate

DAY = HOUR * 24


def digest(data):
    return hashlib.sha256(data).hexdigest()


def compact_day_complete(data, day):
    folder = (
        data / "first-week"
        if FIRST <= day < AFTER_WEEK
        else day_dir(data / "annual", day)
    )
    start = timestamp(day)
    for t in range(start, start + DAY, HOUR):
        meta, packet = folder / f"{t}.manifest.json", folder / f"{t}.json.gz"
        if not meta.exists() or not packet.exists():
            return False
        m = json.loads(meta.read_bytes())
        if (
            m["hour"] != t
            or [(row["bucket"], row["key"]) for row in m["sources"]] != sources(t)
            or m["packet_bytes"] != packet.stat().st_size
            or sha(packet) != m["packet_sha256"]
        ):
            raise ValueError(
                f"Compact market packet failed integrity verification at {t}"
            )
    return True


def inputs(data, folder, day):
    start = timestamp(day)
    meta_path = folder / "funding.manifest.json"
    if not meta_path.exists():
        return None
    meta = json.loads(meta_path.read_bytes())
    rates_body = (folder / "fundingHistory.raw.json").read_bytes()
    if (
        meta["source"] != URL
        or meta["startUtcMs"] != start
        or meta["dailyRowsSha256"] != digest(rates_body)
        or Path(meta["rawPage"]).name != meta["rawPage"]
    ):
        raise ValueError("BTC daily funding metadata mismatch")
    validate(rates_body, start, start + DAY)
    rates = json.loads(rates_body)
    page = (data / "funding" / partition(day) / meta["rawPage"]).read_bytes()
    source_request = meta["rawPageRequest"]
    if (
        digest(page) != meta["rawPageSha256"]
        or source_request["type"] != "fundingHistory"
        or source_request["coin"] != "BTC"
        or [r for r in json.loads(page) if start <= r["time"] < start + DAY] != rates
    ):
        raise ValueError("BTC daily rates differ from official raw page")
    if FIRST <= day < AFTER_WEEK:
        # Week collector independently verifies these dates. Annual code never requests them again.
        path = data / "first-week" / "context-prices.json"
        body = path.read_bytes()
        context = [r for r in json.loads(body) if start <= r["time"] < start + DAY]
        context_sha = digest(body)
    else:
        cm_path = folder / "context.manifest.json"
        if not cm_path.exists():
            return None
        cm = json.loads(cm_path.read_bytes())
        compressed = (folder / "context-samples.json.gz").read_bytes()
        context_sha = digest(compressed)
        raw_source = cm["sources"][0]
        if (
            cm["hour"] != start
            or cm["packet_sha256"] != context_sha
            or cm["packet_bytes"] != len(compressed)
            or raw_source["bucket"] != "hyperliquid-archive"
            or raw_source["key"] != f"asset_ctxs/{day:%Y%m%d}.csv.lz4"
        ):
            raise ValueError("BTC context source or checksum mismatch")
        context = join_samples(
            rates, json.loads(gzip.decompress(compressed)), raw_source
        )
    return (
        rates,
        {r["time"]: r for r in context},
        {
            "source": URL,
            "dailyRatesSha256": digest(rates_body),
            "sourcePageSha256": digest(page),
            "sourceRequest": source_request,
            "contextSha256": context_sha,
        },
    )


def save_event_file(path, records, anchor, required, gaps):
    rows = [records[t] for t in sorted(records)]
    atomic_json(
        path,
        {
            "schema": 1,
            "inputs": anchor,
            "requiredHours": required,
            "verifiedHours": len(rows),
            "coveredPositions": "observed signed BTC funding cash per unit of position",
            "cashRoundingAllowancePerPositionUsdc": str(CASH_QUANTUM),
            "settlementOracleIndependentlyVerified": False,
            "settlementMarkIndependentlyVerified": False,
            "eventsSha256": digest(
                json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()
            ),
            "events": rows,
            "coverageGaps": {str(t): v for t, v in sorted(gaps.items())},
        },
    )


def collect_prefix(s3, ledger, row, rate, context, cache):
    # Individual account records exist only in a private, temporary range cache.
    # A detached proof and its source hashes are the only retained per-hour artifacts.
    cache.mkdir(parents=True, exist_ok=True, mode=0o700)
    signature = digest(row["etag"].encode())[:16]
    raw = bytearray()
    cached = []
    for begin in range(0, min(MAX_PREFIX, row["bytes"]), STEP):
        end = min(begin + STEP, row["bytes"])
        path = cache / f"{row['hour']}.{signature}.{begin}.lz4"
        receipt = path.with_suffix(".receipt.json")
        if receipt.exists():
            part = path.read_bytes()
            if (
                digest(part) != json.loads(receipt.read_bytes())["sha256"]
                or len(part) != end - begin
            ):
                raise ValueError("Private funding range cache checksum mismatch")
        else:
            try:
                part = fetch_range(s3[BUCKET], ledger, row, begin, end)
            except Exception as exc:
                if retryable(exc):
                    raise ArchiveGetUnavailable(row, error_details(exc)) from exc
                raise
            atomic(path, part)
            atomic_json(receipt, {"sha256": digest(part)})
        cached.extend((receipt, path))
        raw.extend(part)
        try:
            event = aggregate_prefix(raw, rate, context)
        except ValueError as exc:
            return (
                None,
                {
                    "state": "unverified",
                    "reason": str(exc),
                    "source": result_source(row, raw),
                },
                cached,
            )
        if event is not None:
            return {**event, "source": result_source(row, raw)}, None, cached
    return (
        None,
        {
            "state": "unverified",
            "reason": "No consistent BTC cash proof within bounded prefix",
            "source": result_source(row, raw),
        },
        cached,
    )


def collect_settlement_days(data, days, ledger, s3):
    """Run from the existing acquisition supervisor. Reuse its authenticated clients and shared ledger."""
    totals = {
        "days": 0,
        "requiredHours": 0,
        "verifiedHours": 0,
        "waitingForInputsDays": 0,
        "sourceGaps": 0,
    }
    for day in sorted(set(days)):
        if day < AUXILIARY_START or day > LAST or day >= datetime.now(UTC).date():
            raise ValueError(
                "Use elapsed complete UTC days within the acquisition window"
            )
        folder = day_dir(data / "annual", day)
        bundle = inputs(data, folder, day) if compact_day_complete(data, day) else None
        if bundle is None or len(bundle[1]) != 24:
            totals["waitingForInputsDays"] += 1
            continue
        rates, contexts, anchor = bundle
        by_hour = {r["time"] // HOUR * HOUR: r for r in rates}
        folder.mkdir(exist_ok=True, parents=True)
        with (folder / ".settlement.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            output = folder / "settlement-cash.json"
            records = {}
            gaps = {
                t: {
                    "state": "missing sampled context"
                    if r["time"] not in contexts
                    else "pending"
                }
                for t, r in by_hour.items()
            }
            if output.exists():
                old = json.loads(output.read_bytes())
                if (
                    old["schema"] != 1
                    or old["inputs"]["dailyRatesSha256"] != anchor["dailyRatesSha256"]
                    or old["inputs"]["contextSha256"] != anchor["contextSha256"]
                    or old["eventsSha256"]
                    != digest(
                        json.dumps(
                            old["events"], sort_keys=True, separators=(",", ":")
                        ).encode()
                    )
                ):
                    raise ValueError(
                        "Cached aggregate settlement packet differs from verified inputs"
                    )
                for event in old["events"]:
                    t = event["time"] // HOUR * HOUR
                    if (
                        t in records
                        or t not in by_hour
                        or by_hour[t]["time"] != event["time"]
                        or event["source"]["key"] != source(t)["key"]
                        or event["source"]["bucket"] != BUCKET
                    ):
                        raise ValueError("Cached settlement event identity mismatch")
                    records[t] = event
                    del gaps[t]
                for key, reason in old["coverageGaps"].items():
                    if int(key) in gaps and (
                        reason["state"] == "unverified"
                        or (
                            reason["state"] == "missing source"
                            and datetime.now(UTC)
                            - datetime.fromisoformat(reason["checkedUtc"])
                            < timedelta(days=1)
                        )
                    ):
                        gaps[int(key)] = reason
            pending = [t for t, reason in gaps.items() if reason["state"] == "pending"]
            if FIRST <= day < AFTER_WEEK:
                # Atomically published hour aggregates can be reused before all 168 have arrived.
                for t in pending:
                    p = data / "first-week" / "settlement-cash" / f"{t}.json"
                    if not p.exists():
                        gaps[t] = {"state": "awaiting first-week acquisition"}
                        continue
                    e = json.loads(p.read_bytes())
                    if (
                        e["time"] != by_hour[t]["time"]
                        or e["source"]["key"] != source(t)["key"]
                        or e["source"]["bucket"] != BUCKET
                        or e["settledRate"] != str(Decimal(by_hour[t]["fundingRate"]))
                        or e["contextSampledOracle"] != contexts[e["time"]]["oraclePx"]
                    ):
                        raise ValueError("First-week settlement source mismatch")
                    records[t] = e
                    del gaps[t]
            elif pending:
                plan_file = folder / "settlement-source-plan.json"
                plan = json.loads(plan_file.read_bytes()) if plan_file.exists() else {}
                for t in pending:
                    key = str(t)
                    if key not in plan or "etag" not in plan[key]:
                        row = head(source(t), ledger, s3)
                        if row.get("status") == "unresolved":
                            raise ArchiveHeadUnavailable(day, row, 1)
                        plan[key] = row
                        atomic_json(plan_file, plan)
                    row = plan[key]
                    if any(row[k] != v for k, v in source(t).items()):
                        raise ValueError("Funding source plan identity mismatch")
                    if row.get("status") == "missing":
                        gaps[t] = {
                            "state": "missing source",
                            "key": row["key"],
                            "checkedUtc": datetime.now(UTC).isoformat(),
                        }
                remaining = [t for t in pending if "etag" in plan[str(t)]]
                ledger.require(
                    sum(
                        estimated_cost(BUCKET, min(MAX_PREFIX, plan[str(t)]["bytes"]))
                        + 3 * estimated_cost(BUCKET, 0)
                        for t in remaining
                    )
                )
                for t in remaining:
                    row = plan[str(t)]
                    event, reason, transient_files = collect_prefix(
                        s3,
                        ledger,
                        row,
                        by_hour[t],
                        contexts[by_hour[t]["time"]],
                        data
                        / "private-settlement-prefixes"
                        / partition(day)
                        / folder.name,
                    )
                    if event is None:
                        gaps[t] = reason
                    else:
                        records[t] = event
                        del gaps[t]
                    save_event_file(output, records, anchor, 24, gaps)
                    for p in transient_files:
                        p.unlink(missing_ok=True)
            save_event_file(output, records, anchor, 24, gaps)
            totals["days"] += 1
            totals["requiredHours"] += 24
            totals["verifiedHours"] += len(records)
            totals["sourceGaps"] += len(gaps)
    return totals


def check_credit_balance(account):
    import boto3

    # GetCredits has no continuation token in the AWS Billing API model.
    month_start = datetime.now(UTC).replace(
        day=1, hour=0, minute=0, second=0, microsecond=0
    )
    result = boto3.client("billing", region_name="us-east-1").get_credits(
        accountId=account, startDate=month_start
    )
    required = {"Amazon Simple Storage Service", "AWS Data Transfer"}
    return sum(
        (
            Decimal(c["estimatedAmount"]["currencyAmount"])
            for c in result["credits"]
            if c["creditStatus"] == "ENABLED"
            and c["estimatedAmount"]["currencyCode"] == "USD"
            and required <= set(c["applicableProductNames"])
        ),
        Decimal(0),
    )


def range_plan(data, days):
    ready, waiting = [], []
    remaining = 0
    for day in days:
        folder = day_dir(data / "annual", day)
        bundle = inputs(data, folder, day) if compact_day_complete(data, day) else None
        if bundle is None or len(bundle[1]) != 24:
            waiting.append(day)
            continue
        ready.append(day)
        if not FIRST <= day < AFTER_WEEK:
            path = folder / "settlement-cash.json"
            count = (
                json.loads(path.read_bytes())["verifiedHours"] if path.exists() else 0
            )
            if not 0 <= count <= 24:
                raise ValueError("Invalid verified settlement count")
            remaining += 24 - count
    return ready, {
        "readyDays": len(ready),
        "skippedIncompleteDays": [d.isoformat() for d in waiting],
        "remainingPossiblePaidHours": remaining,
        "estimatedGrossUsdOneRange": remaining
        * (estimated_cost(BUCKET, STEP) + estimated_cost(BUCKET, 0)),
        "estimatedGrossUsdFourRanges": remaining
        * (4 * estimated_cost(BUCKET, STEP) + estimated_cost(BUCKET, 0)),
    }


def run(data, start, through, estimate_only=False):
    root = data / "annual"
    if (
        not AUXILIARY_START
        <= start
        <= through
        <= min(LAST, datetime.now(UTC).date() - timedelta(days=1))
    ):
        raise ValueError(
            "Select complete elapsed UTC dates within the acquisition window"
        )
    days = [start + timedelta(days=n) for n in range((through - start).days + 1)]
    if estimate_only:
        print(json.dumps(range_plan(data, days)[1]), flush=True)
        return
    # A standalone follow-on waits for market acquisition to release its
    # supervisor lock. Both locks match the ordering used by resume_archive.py.
    with (
        (root / "supervisor.lock").open("a") as supervisor,
        (root / "collect.lock").open("a") as collector,
    ):
        fcntl.flock(supervisor, fcntl.LOCK_EX)
        fcntl.flock(collector, fcntl.LOCK_EX)
        ready, plan = range_plan(data, days)
        status_path = root / "settlement-transfer-status.json"

        def status(stage, **updates):
            value = {
                "state": stage,
                "updatedUtc": datetime.now(UTC).isoformat(),
                "start": start.isoformat(),
                "through": through.isoformat(),
                **updates,
            }
            atomic_json(status_path, value)
            print(
                json.dumps(
                    {
                        k: v
                        for k, v in value.items()
                        if k not in ("skippedIncompleteDays",)
                    }
                ),
                flush=True,
            )

        status("checked local inputs", **plan)
        if not ready:
            status("no complete days ready", **plan)
            return
        budget_path = data / "archive-budget.json"
        ledger, s3 = clients(budget_path)
        with ledger.file:
            credits = check_credit_balance(ledger.account)
            gross = ledger.require(0)
            status(
                "verified billing and estimated transfer",
                verifiedAccount=ledger.account,
                estimatedApplicableCreditsUsd=str(credits),
                grossReservedIncludingPriorUsd=gross,
                **plan,
            )
            totals = {"readyDays": len(ready), "verifiedHours": 0, "requiredHours": 0}
            for day in ready:
                try:
                    result = collect_settlement_days(data, [day], ledger, s3)
                except Exception as exc:
                    status(
                        "interrupted",
                        day=day.isoformat(),
                        transport=getattr(exc, "details", None) or error_details(exc),
                        retryable=isinstance(exc, ArchiveHeadUnavailable)
                        or retryable(exc),
                        **totals,
                    )
                    raise
                totals["verifiedHours"] += result["verifiedHours"]
                totals["requiredHours"] += result["requiredHours"]
                status("acquiring settlement evidence", day=day.isoformat(), **totals)
            status(
                "available-day pass complete",
                **totals,
                skippedIncompleteDays=plan["skippedIncompleteDays"],
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data", type=Path)
    parser.add_argument("--start", type=date.fromisoformat, default=FIRST)
    parser.add_argument("--through", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--estimate-only",
        action="store_true",
        help="Inspect local readiness and cost ranges without AWS",
    )
    args = parser.parse_args()
    try:
        run(args.data.resolve(), args.start, args.through, args.estimate_only)
    except Exception as exc:
        if isinstance(exc, ArchiveHeadUnavailable) or retryable(exc):
            print(
                json.dumps(
                    {
                        "state": "temporary transport failure",
                        "details": getattr(exc, "details", None) or error_details(exc),
                    }
                ),
                file=sys.stderr,
            )
            raise SystemExit(75) from exc
        raise
