"""Budget-limited official funding cash-flow evidence for the first training week."""

import argparse
import fcntl
import hashlib
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from archive import atomic_json, estimated_cost
from botocore.exceptions import BotoCoreError, ClientError
from bulk_archive import clients, error_details, head, retryable
from funding import END, HOUR, START
from settlement_probe import CASH_QUANTUM, extract

BUCKET = "hl-mainnet-node-data"
MAX_PREFIX = 4 * 1024 * 1024
STEP = 1024 * 1024
CAP = 0.12


def source(ms):
    dt = datetime.fromtimestamp(ms / 1000, UTC)
    return {
        "hour": ms,
        "bucket": BUCKET,
        "key": f"misc_events_by_block/hourly/{dt:%Y%m%d}/{dt.hour}.lz4",
    }


class WeekBudget:
    """One process lock owns task spending; global ledger still enforces all other tasks' reservations."""

    def __init__(self, ledger, budget_file, allowed):
        self.ledger = ledger
        self.lock = threading.Lock()
        with (
            ledger.locked(),
            (budget_file.parent / "archive-reservations.jsonl").open() as f,
        ):
            self.gross = sum(
                row["reserved_usd"]
                for line in f
                if (row := json.loads(line))["key"] in allowed
                and row["bucket"] == BUCKET
            )

    def require(self, amount):
        if self.gross + amount > CAP:
            raise ValueError("First-week funding reservation cap would be exceeded")
        return self.ledger.require(amount)

    def reserve(self, row, verb):
        charge = estimated_cost(BUCKET, row["bytes"] if verb == "GET" else 0)
        with self.lock:
            if self.gross + charge > CAP:
                raise ValueError("First-week funding reservation cap reached")
            self.ledger.reserve(row, verb)
            self.gross += charge


def fetch_range(client, journal, row, start, end):
    # Inclusive lower and exclusive upper byte range. Journal each transport attempt.
    for attempt in range(2):
        journal.reserve({**row, "bytes": end - start}, "GET")
        try:
            res = client.get_object(
                Bucket=BUCKET,
                Key=row["key"],
                IfMatch=row["etag"],
                Range=f"bytes={start}-{end - 1}",
                RequestPayer="requester",
            )
            with closing(res["Body"]) as body:
                raw = body.read()
            if (
                res["ETag"] != row["etag"]
                or len(raw) != end - start
                or res["ContentRange"] != f"bytes {start}-{end - 1}/{row['bytes']}"
            ):
                raise ValueError("Funding range failed transport verification")
            return raw
        except Exception as exc:
            if attempt == 1 or not retryable(exc):
                raise
            time.sleep(2**attempt)


def collect(data, pilot, workers=2):
    budget_path = data / "archive-budget.json"
    folder = data / "first-week" / "settlement-cash"
    folder.mkdir(exist_ok=True)
    rates_body = (data / "first-week/fundingHistory.raw.json").read_bytes()
    rates_by_hour = {r["time"] // HOUR * HOUR: r for r in json.loads(rates_body)}
    context_body = (data / "first-week/context-prices.json").read_bytes()
    contexts = {r["time"]: r for r in json.loads(context_body)}
    expected = list(range(START, END, HOUR))
    if list(rates_by_hour) != expected:
        raise ValueError("Missing or duplicate training settlement hours")
    pilot_data = json.loads(pilot.read_bytes()) if pilot else {"events": []}
    rate_sha = hashlib.sha256(rates_body).hexdigest()
    context_sha = hashlib.sha256(context_body).hexdigest()
    if pilot and (
        pilot_data["ratePageSha256"] != rate_sha
        or pilot_data["contextPricesSha256"] != context_sha
    ):
        raise ValueError("Pilot input source mismatch")
    reuse = {r["source"]["key"]: r for r in pilot_data["events"]}
    with (folder / "collect.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        ledger, clients_by_bucket = clients(budget_path)
        with ledger.file:
            journal = WeekBudget(
                ledger, budget_path, {source(ms)["key"] for ms in expected}
            )
            plan_path = folder / "plan.json"
            cached = (
                json.loads(plan_path.read_bytes())
                if plan_path.exists()
                else {"objects": []}
            )
            if cached.get("account", ledger.account) != ledger.account:
                raise ValueError("Funding plan account mismatch")
            cached_keys = {r["key"]: r for r in cached["objects"] if "etag" in r}

            def metadata(ms):
                row = source(ms)
                saved = cached_keys.get(row["key"])
                if saved:
                    if any(saved[k] != v for k, v in row.items()):
                        raise ValueError("Funding plan metadata mismatch")
                    return saved
                if row["key"] in reuse:
                    old = reuse[row["key"]]["source"]
                    return {**row, "bytes": old["bytes"], "etag": old["etag"]}
                return head(row, journal, clients_by_bucket)

            def one(row):
                ms = row["hour"]
                r = rates_by_hour[ms]
                raw = bytearray()
                for end in range(STEP, MAX_PREFIX + STEP, STEP):
                    stop = min(end, row["bytes"])
                    if len(raw) == stop:
                        break
                    raw.extend(
                        fetch_range(
                            clients_by_bucket[BUCKET], journal, row, len(raw), stop
                        )
                    )
                    try:
                        event = extract(raw, r, contexts[r["time"]])
                    except ValueError as exc:
                        # A partial LZ4 frame or a block spilling across the hour
                        # can be inconclusive. Any inconsistent observations stay flagged.
                        if str(exc) not in (
                            "substring not found",
                            "Insufficient independent positions or inconsistent bounds",
                        ):
                            return ms, None, str(exc)
                        continue
                    if event["distinctBtcPositionsInPrefix"] < 10:
                        continue
                    event["source"] = {
                        **{k: row[k] for k in ("bucket", "key", "bytes", "etag")},
                        "range_prefix_bytes": len(raw),
                        "sha256": hashlib.sha256(raw).hexdigest(),
                    }
                    atomic_json(folder / f"{ms}.json", event)
                    return ms, event, None
                return (
                    ms,
                    None,
                    "No consistent cash evidence within bounded object prefix",
                )

            known = {r["hour"]: r for r in cached["objects"]}
            gaps = {t: "awaiting metadata" for t in expected}
            complete = {}

            def checkpoint():
                plans = [known[t] for t in expected if t in known]
                atomic_json(plan_path, {"account": ledger.account, "objects": plans})
                events = [complete[t] for t in sorted(complete)]
                result = {
                    "start": START,
                    "endExclusive": END,
                    "rateSource": "https://api.hyperliquid.xyz/info",
                    "ratePageSha256": rate_sha,
                    "contextPricesSha256": context_sha,
                    "cashRoundingAllowancePerPositionUsdc": str(CASH_QUANTUM),
                    "observedHours": len(events),
                    "requiredHours": len(expected),
                    "coverageGaps": gaps,
                    "settlementOracleIndependentlyVerified": False,
                    "settlementMarkIndependentlyVerified": False,
                    "scope": "Observed Funding cash per signed BTC by block. Implied price at the published rate is diagnostic only.",
                    "positionCapBtc": "0.01",
                    "maxCashIntervalWidthAtPositionCapUsdc": str(
                        max(
                            (
                                Decimal(e["cashIntervalWidthAtPositionCapUsdc"])
                                for e in events
                            ),
                            default=Decimal(0),
                        )
                    ),
                    "grossFundingReservationsUsd": journal.gross,
                    "events": events,
                }
                atomic_json(folder / "manifest.json", result)

            for t in expected:
                row = known.get(t)
                if not row or "etag" not in row:
                    if row:
                        gaps[t] = row.get("status", "awaiting metadata")
                    continue
                if any(row[k] != v for k, v in source(t).items()):
                    raise ValueError("Funding plan source mismatch")
                path = folder / f"{t}.json"
                if row["key"] in reuse:
                    event = reuse[row["key"]]
                elif path.exists():
                    event = json.loads(path.read_bytes())
                else:
                    gaps[t] = "metadata ready, prefix pending"
                    continue
                if (
                    event["source"]["etag"] != row["etag"]
                    or event["time"] != rates_by_hour[t]["time"]
                    or event["source"]["key"] != row["key"]
                    or event["source"]["bucket"] != BUCKET
                ):
                    raise ValueError("Cached settlement provenance mismatch")
                if not path.exists():
                    atomic_json(path, event)
                complete[t] = event
                del gaps[t]
            checkpoint()
            pending = sorted(gaps, key=lambda t: ("etag" not in known.get(t, {}), t))
            with ThreadPoolExecutor(max_workers=workers) as pool:
                for i in range(0, len(pending), 8):
                    batch = pending[i : i + 8]
                    measured = list(pool.map(metadata, batch))
                    for row in measured:
                        t = row["hour"]
                        known[t] = row
                        gaps[t] = row.get("status", "metadata ready, prefix pending")
                    todo = [r for r in measured if "etag" in r]
                    if all(r.get("status") == "unresolved" for r in measured):
                        checkpoint()
                        raise RuntimeError(
                            "Funding HEAD batch unavailable; retained progress"
                        )
                    high = sum(
                        estimated_cost(BUCKET, min(MAX_PREFIX, r["bytes"]))
                        + 3 * estimated_cost(BUCKET, 0)
                        for r in todo
                    )
                    used = journal.require(high)
                    print(
                        json.dumps(
                            {
                                "batchHours": len(batch),
                                "available": len(todo),
                                "maxBatchGrossBeforeRetries": high,
                                "globalGrossIncludingPriorAndReservations": used,
                                "fundingReservations": journal.gross,
                                "verifiedHours": len(complete),
                            }
                        ),
                        flush=True,
                    )
                    jobs = [(r, pool.submit(one, r)) for r in todo]
                    errors = []
                    for row, job in jobs:
                        try:
                            t, event, reason = job.result()
                            if event:
                                complete[t] = event
                                del gaps[t]
                            else:
                                gaps[t] = reason
                        except (BotoCoreError, ClientError, OSError) as exc:
                            gaps[row["hour"]] = {
                                "state": "prefix request failed",
                                **error_details(exc),
                            }
                            errors.append(exc)
                    checkpoint()
                    if errors:
                        raise errors[0]
            print(
                "Verified",
                len(complete),
                "of",
                len(expected),
                "first-week hours",
                flush=True,
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data", type=Path)
    parser.add_argument("--reuse", type=Path)
    parser.add_argument("--workers", type=int, choices=(1, 2, 3, 4), default=2)
    args = parser.parse_args()
    collect(args.data, args.reuse, args.workers)
