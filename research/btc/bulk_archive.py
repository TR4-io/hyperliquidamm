"""Budgeted daily acquisition for the BTC study, with isolated final-test storage."""

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from archive import RATES, atomic_json, estimated_cost, sources
from funding import HOUR

FIRST = date(2025, 12, 30)
AFTER_WEEK = date(2026, 1, 6)
VALIDATION = date(2026, 8, 27)
FINAL = date(2026, 10, 26)
LAST = date(2026, 12, 29)


def partition(day):
    return (
        "training"
        if day < VALIDATION
        else "validation"
        if day < FINAL
        else "sealed-final"
    )


def day_dir(root, day):
    return root / partition(day) / day.strftime("%Y%m%d")


def items(day):
    start = int(datetime.combine(day, datetime.min.time(), UTC).timestamp() * 1000)
    for h in range(24):
        if day == date(2026, 10, 25) and h < 6:
            continue  # the six pilot hours are held for local reuse
        t = start + h * HOUR
        for kind, (b, k) in zip(("book", "fills"), sources(t)):
            yield {"kind": kind, "hour": t, "bucket": b, "key": k}
    yield {
        "kind": "context",
        "hour": start,
        "bucket": "hyperliquid-archive",
        "key": f"asset_ctxs/{day:%Y%m%d}.csv.lz4",
    }


class Ledger:
    """Serialize journal reservations across threads, processes and the week collector."""

    def __init__(self, budget, account):
        import math

        cfg = json.loads(budget.read_text())
        prior = cfg["prior_archive_charges_usd"]
        if (
            cfg["account"] != account
            or cfg["cap_usd"] != 100
            or not math.isfinite(prior)
            or prior < 0
            or cfg.get("remaining_applicable_credits_usd") is None
        ):
            raise ValueError(
                "Budget identity, gross cap, prior charges or credits missing"
            )
        self.account = account
        self.file = (budget.parent / "archive-reservations.jsonl").open("a+")
        self.offset = 0
        self.used = prior
        self.lock = threading.Lock()

    @contextlib.contextmanager
    def locked(self):
        with self.lock:
            fcntl.flock(self.file, fcntl.LOCK_EX)
            try:
                self.file.seek(self.offset)
                self.used += sum(json.loads(line)["reserved_usd"] for line in self.file)
                self.offset = self.file.tell()
                yield
            finally:
                fcntl.flock(self.file, fcntl.LOCK_UN)

    def require(self, additional):
        with self.locked():
            if self.used + additional > 100:
                raise ValueError(f"Gross budget exceeded: {self.used} + {additional}")
            return self.used

    def reserve(self, row, verb):
        amount = row["bytes"] if verb == "GET" else 0
        charge = estimated_cost(row["bucket"], amount)
        with self.locked():
            if self.used + charge > 100:
                raise ValueError("Gross budget exhausted")
            self.file.write(
                json.dumps(
                    {
                        "utc": datetime.now(UTC).isoformat(),
                        "account": self.account,
                        "bucket": row["bucket"],
                        "key": row["key"],
                        "verb": verb,
                        "bytes": amount,
                        "reserved_usd": charge,
                    }
                )
                + "\n"
            )
            self.file.flush()
            os.fsync(self.file.fileno())
            self.used += charge
            self.offset = self.file.tell()


def clients(budget):
    import boto3
    from botocore.config import Config

    cfg = Config(
        retries={"total_max_attempts": 1},
        max_pool_connections=64,
        connect_timeout=12,
        read_timeout=75,
    )
    account = boto3.client(
        "sts", region_name="ca-central-1", config=cfg
    ).get_caller_identity()["Account"]
    ledger = Ledger(budget, account)
    return ledger, {
        b: boto3.client("s3", region_name=r[0], config=cfg) for b, r in RATES.items()
    }


def retryable(exc):
    from botocore.exceptions import (
        ClientError,
        ConnectionClosedError,
        EndpointConnectionError,
        ReadTimeoutError,
        ResponseStreamingError,
    )

    if isinstance(exc, ClientError):
        resp = exc.response
        return resp.get("ResponseMetadata", {}).get("HTTPStatusCode", 0) in (
            429,
            500,
            502,
            503,
            504,
        ) or resp["Error"]["Code"] in ("SlowDown", "Throttling")
    return isinstance(
        exc,
        (
            ConnectionClosedError,
            EndpointConnectionError,
            ReadTimeoutError,
            ResponseStreamingError,
        ),
    )


def head(row, ledger, s3):
    from botocore.exceptions import ClientError

    for attempt in range(4):
        ledger.reserve(row, "HEAD")
        try:
            result = s3[row["bucket"]].head_object(
                Bucket=row["bucket"], Key=row["key"], RequestPayer="requester"
            )
            if result.get("StorageClass", "STANDARD") != "STANDARD":
                raise ValueError("Unpriced storage class for " + row["key"])
            return {**row, "bytes": result["ContentLength"], "etag": result["ETag"]}
        except Exception as exc:
            if (
                isinstance(exc, ClientError)
                and exc.response["ResponseMetadata"]["HTTPStatusCode"] == 404
            ):
                return {**row, "status": "missing"}
            if not retryable(exc):
                raise
            if attempt == 2:
                return {
                    **row,
                    "status": "unresolved",
                    "error": type(exc).__name__ + ": " + str(exc),
                }
            time.sleep(0.25 + random.random() * 2**attempt)


def plan_days(root, days, ledger, s3, workers):
    stats = {
        "planned_days": 0,
        "available_objects": 0,
        "missing_objects": 0,
        "unresolved_objects": 0,
        "bytes": 0,
        "get_usd": 0,
    }
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for day in days:
            folder = day_dir(root, day)
            folder.mkdir(parents=True, exist_ok=True)
            path = folder / "plan.json"
            existing = json.loads(path.read_text()) if path.exists() else {}
            if existing and existing.get("account") != ledger.account:
                raise ValueError("Cached plan uses a different billing account")
            available = {r["key"]: r for r in existing.get("objects", [])}
            expected = list(items(day))
            missing = [row for row in expected if row["key"] not in available]

            def save_progress(folder=folder, available=available):
                atomic_json(
                    folder / "plan.partial.json",
                    {
                        "account": ledger.account,
                        "objects": [
                            r for r in available.values() if not r.get("status")
                        ],
                        "unresolved": [
                            r
                            for r in available.values()
                            if r.get("status") == "unresolved"
                        ],
                    },
                )

            try:
                from concurrent.futures import as_completed

                partial = folder / "plan.partial.json"
                if partial.exists():
                    cached = json.loads(partial.read_text())
                    if cached["account"] == ledger.account:
                        available.update({r["key"]: r for r in cached["objects"]})
                    missing = [row for row in expected if row["key"] not in available]
                tasks = [pool.submit(head, row, ledger, s3) for row in missing]
                for i, future in enumerate(as_completed(tasks)):
                    result = future.result()
                    available[result["key"]] = result
                    if i and i % 16 == 0:
                        save_progress()
            finally:
                save_progress()
            rows = [available[r["key"]] for r in expected]
            objects = [r for r in rows if not r.get("status")]
            unresolved = [r for r in rows if r.get("status") == "unresolved"]
            unavailable = [r for r in rows if r.get("status") == "missing"]
            cost = sum(estimated_cost(r["bucket"], r["bytes"]) for r in objects)
            atomic_json(
                path,
                {
                    "account": ledger.account,
                    "day": day.isoformat(),
                    "objects": objects,
                    "unavailable": unavailable,
                    "unresolved": unresolved,
                    "projected_get_usd": cost,
                },
            )
            (folder / "plan.partial.json").unlink(missing_ok=True)
            stats["planned_days"] += 1
            stats["unresolved_objects"] += len(unresolved)
            stats["available_objects"] += len(objects)
            stats["missing_objects"] += len(unavailable)
            stats["bytes"] += sum(r["bytes"] for r in objects)
            stats["get_usd"] += cost
            if not objects and len(unresolved) >= 3 and not unavailable:
                raise RuntimeError(
                    f"S3 HEADs all failed for {day}; diagnostic metadata retained"
                )
            if stats["planned_days"] % 14 == 0:
                print(json.dumps(stats), flush=True)
    print(
        json.dumps(
            {**stats, "gross_with_reservations": ledger.require(0) + stats["get_usd"]}
        ),
        flush=True,
    )


def load_plan(root, day, account):
    folder = day_dir(root, day)
    plan = json.loads((folder / "plan.json").read_bytes())
    expected = {(r["kind"], r["hour"], r["bucket"], r["key"]) for r in items(day)}
    rows = plan["objects"] + plan["unavailable"] + plan.get("unresolved", [])
    if (
        plan["account"] != account
        or plan["day"] != day.isoformat()
        or len(rows) != len(expected)
        or {(r["kind"], r["hour"], r["bucket"], r["key"]) for r in rows} != expected
    ):
        raise ValueError("Plan identity or source mismatch in " + str(folder))
    if any(
        type(r["bytes"]) != int or r["bytes"] < 0 or not r["etag"]
        for r in plan["objects"]
    ):
        raise ValueError("Invalid object metadata")
    return plan


def raw_paths(folder, row):
    raw = folder / ".raw" / f"{row['hour']}.{row['kind']}.lz4"
    return raw, raw.with_suffix(".receipt.json")


def valid_cache(folder, row):
    from archive import sha

    raw, receipt = raw_paths(folder, row)
    if not receipt.exists():
        return False
    rec = json.loads(receipt.read_text())
    if (
        rec["source"] != row
        or raw.stat().st_size != row["bytes"]
        or sha(raw) != rec["sha256"]
    ):
        raise ValueError("Damaged raw download at " + str(raw))
    return True


def fetch(folder, row, ledger, s3):
    """Only a fully verified, atomically published receipt authorizes offline reuse."""
    import tempfile

    if valid_cache(folder, row):
        return
    raw, receipt = raw_paths(folder, row)
    raw.parent.mkdir(exist_ok=True)
    ledger.reserve(row, "GET")
    tmp = None
    try:
        response = s3[row["bucket"]].get_object(
            Bucket=row["bucket"],
            Key=row["key"],
            RequestPayer="requester",
            IfMatch=row["etag"],
        )
        with contextlib.closing(response["Body"]) as body:
            if (
                response["ETag"] != row["etag"]
                or response["ContentLength"] != row["bytes"]
            ):
                raise ValueError("Server response differs from measured plan")
            digest = hashlib.sha256()
            size = 0
            with tempfile.NamedTemporaryFile(dir=raw.parent, delete=False) as dest:
                tmp = dest.name
                for chunk in body.iter_chunks(1024 * 1024):
                    dest.write(chunk)
                    size += len(chunk)
                    digest.update(chunk)
                if size != row["bytes"]:
                    raise ValueError("Incomplete body")
            os.replace(tmp, raw)
            tmp = None
            atomic_json(receipt, {"source": row, "sha256": digest.hexdigest()})
            raw.with_suffix(".get-error.json").unlink(missing_ok=True)
            return
    except Exception as exc:
        response = getattr(exc, "response", {})
        atomic_json(
            raw.with_suffix(".get-error.json"),
            {
                "source": row,
                "error": type(exc).__name__,
                "message": str(exc),
                "code": response.get("Error", {}).get("Code"),
                "request_id": response.get("ResponseMetadata", {}).get("RequestId"),
                "status": response.get("ResponseMetadata", {}).get("HTTPStatusCode"),
            },
        )
        raise
    finally:
        if tmp is not None:
            Path(tmp).unlink(missing_ok=True)


def packet_paths(folder, group):
    prefix = "context" if group[0]["kind"] == "context" else str(group[0]["hour"])
    target = "context-samples.json.gz" if prefix == "context" else prefix + ".json.gz"
    return folder / target, folder / (prefix + ".manifest.json")


def complete(folder, group):
    from archive import sha

    target, path = packet_paths(folder, group)
    if not path.exists():
        return False
    record = json.loads(path.read_bytes())
    if (
        record["origin"] != {"etags": [r["etag"] for r in group]}
        or [(s["bucket"], s["key"], s["bytes"]) for s in record["sources"]]
        != [(r["bucket"], r["key"], r["bytes"]) for r in group]
        or target.stat().st_size != record["packet_bytes"]
        or sha(target) != record["packet_sha256"]
    ):
        raise ValueError("Checksum/source mismatch in " + str(path))
    return True


def normalize_context(folder, row):
    import csv
    import gzip

    import lz4.frame
    from archive import sha

    raw, receipt = raw_paths(folder, row)
    samples = []
    with lz4.frame.open(raw, "rt") as src:
        for r in csv.DictReader(src):
            if r["coin"] != "BTC":
                continue
            dt = datetime.fromisoformat(r["time"])
            if dt.tzinfo is None:
                raise ValueError("Timezone absent from source context")
            t = int(dt.timestamp() * 1000)
            if t % HOUR == 0:
                samples.append({**r, "time": t})
    target, manifest = packet_paths(folder, [row])
    data = gzip.compress(
        json.dumps(samples, separators=(",", ":")).encode(), compresslevel=1, mtime=0
    )
    tmp = target.with_suffix(".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, target)
    source = {k: row[k] for k in ("bucket", "key", "bytes")}
    source["sha256"] = json.loads(receipt.read_bytes())["sha256"]
    atomic_json(
        manifest,
        {
            "hour": row["hour"],
            "sources": [source],
            "rows": len(samples),
            "origin": {"etags": [row["etag"]]},
            "packet_bytes": len(data),
            "packet_sha256": sha(target),
        },
    )


def normalize_group(folder, group):
    if group[0]["kind"] == "context":
        normalize_context(folder, group[0])
    else:
        try:
            from fast_archive import normalize
        except ModuleNotFoundError as exc:
            if exc.name != "fast_archive":
                raise
            from archive import normalize
        normalize(
            group[0]["hour"],
            [raw_paths(folder, r)[0] for r in group],
            folder,
            {"etags": [r["etag"] for r in group]},
        )
    for row in group:
        for path in reversed(raw_paths(folder, row)):
            path.unlink(missing_ok=True)


def groups(plan):
    by_hour = {}
    for row in plan["objects"]:
        key = (row["hour"], row["kind"] == "context")
        by_hour.setdefault(key, []).append(row)
    for group in by_hour.values():
        if len(group) == 2 or group[0]["kind"] == "context":
            yield sorted(group, key=lambda r: r["kind"])


def collect_days(root, days, ledger, s3, workers):
    import multiprocessing
    import shutil
    from concurrent.futures import ProcessPoolExecutor, as_completed

    # Verify all metadata and compute remaining transfers BEFORE the first GET.
    remaining = []
    projected = 0
    for day in days:
        folder = day_dir(root, day)
        plan = load_plan(root, day, ledger.account)
        for group in groups(plan):
            if complete(folder, group):
                continue
            for row in group:
                if not valid_cache(folder, row):
                    projected += estimated_cost(row["bucket"], row["bytes"])
            remaining.append((folder, group))
    used = ledger.require(projected)
    print(
        json.dumps(
            {
                "remaining_groups": len(remaining),
                "projected_GET_usd": projected,
                "gross_with_reservations": used + projected,
            }
        ),
        flush=True,
    )
    failures = 0
    # Independent fetch and process pools overlap network transfers with parsing.
    # Keep at most 3 times the worker count of hourly raw pairs in flight.
    with (
        ProcessPoolExecutor(
            max_workers=workers, mp_context=multiprocessing.get_context("spawn")
        ) as cpu,
        ThreadPoolExecutor(max_workers=workers * 3) as io,
    ):
        # Start every resumed run with a single group. A failing destination
        # therefore reserves at most one GET for the failing source on startup.
        paused = threading.Event()
        slots = threading.Semaphore(min(workers, 4))
        starts = (
            ([0] + list(range(1, len(remaining), workers * 3))) if remaining else []
        )
        for i in starts:
            batch = remaining[i : i + (1 if i == 0 else workers * 3)]
            pending_bytes = sum(r["bytes"] for _, group in batch for r in group)
            if shutil.disk_usage(root).free < 2 * pending_bytes + 2 * 1024**3:
                raise RuntimeError("Insufficient free space for next bounded batch")

            def get_group(folder, group):
                for row in group:
                    with slots:
                        if paused.is_set():
                            return False
                        try:
                            fetch(folder, row, ledger, s3)
                        except Exception as exc:
                            if retryable(exc) or getattr(exc, "response", {}).get(
                                "ResponseMetadata", {}
                            ).get("HTTPStatusCode") in (401, 403):
                                paused.set()
                            raise
                return True

            downloads = {
                io.submit(get_group, folder, g): (folder, g) for folder, g in batch
            }
            parses = {}
            for fut in as_completed(downloads):
                folder, g = downloads[fut]
                try:
                    if not fut.result():
                        continue
                except Exception as exc:
                    if not retryable(exc):
                        from botocore.exceptions import BotoCoreError, ClientError

                        if (
                            isinstance(exc, (BotoCoreError, ClientError))
                            or "budget" in str(exc).lower()
                        ):
                            raise
                    atomic_json(
                        folder / f"{g[0]['hour']}.{g[0]['kind']}.error.json",
                        {"stage": "GET", "error": str(exc)},
                    )
                    failures += 1
                    continue
                if len(g) == 2 or g[0]["kind"] == "context":
                    parses[cpu.submit(normalize_group, folder, g)] = (folder, g)
            for fut in as_completed(parses):
                folder, g = parses[fut]
                errfile = folder / f"{g[0]['hour']}.{g[0]['kind']}.error.json"
                try:
                    fut.result()
                    errfile.unlink(missing_ok=True)
                except (
                    OSError,
                    ValueError,
                    KeyError,
                    TypeError,
                    ArithmeticError,
                    RuntimeError,
                ) as exc:
                    atomic_json(errfile, {"stage": "normalize", "error": str(exc)})
                    failures += 1
            if paused.is_set():
                raise RuntimeError(
                    "S3 GET paused after a service error; verified inputs retained"
                )
            if (
                i == 0
                or (i // (workers * 3)) % 10 == 0
                or i + len(batch) == len(remaining)
            ):
                print(
                    json.dumps(
                        {
                            "processed_groups": i + len(batch),
                            "total": len(remaining),
                            "failures": failures,
                            "last_partition_day": batch[-1][0].name,
                        }
                    ),
                    flush=True,
                )
    if failures:
        raise RuntimeError(
            f"{failures} groups need diagnosis; other groups were processed"
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["plan", "collect"])
    parser.add_argument("root", type=Path)
    parser.add_argument("budget", type=Path)
    parser.add_argument("--start", type=date.fromisoformat, default=AFTER_WEEK)
    parser.add_argument(
        "--end",
        type=date.fromisoformat,
        default=min(LAST, datetime.now(UTC).date() - timedelta(days=1)),
    )
    parser.add_argument("--workers", type=int, default=None)
    args = parser.parse_args()
    if (
        not AFTER_WEEK
        <= args.start
        <= args.end
        <= min(LAST, datetime.now(UTC).date() - timedelta(days=1))
    ):
        parser.error(
            "Use complete UTC days after the reused first week within the study"
        )
    days = [
        args.start + timedelta(days=i) for i in range((args.end - args.start).days + 1)
    ]
    args.root.mkdir(parents=True, exist_ok=True)
    with (args.root / (args.mode + ".lock")).open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        ledger, s3 = clients(args.budget)
        if args.mode == "plan":
            plan_days(args.root, days, ledger, s3, args.workers or 12)
        else:
            collect_days(args.root, days, ledger, s3, args.workers or 4)


if __name__ == "__main__":
    main()
