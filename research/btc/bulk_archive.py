"""Budgeted daily acquisition for the BTC study, with isolated final-test storage."""

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
import random
import re
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


class ArchiveGetUnavailable(RuntimeError):
    def __init__(self, row, details):
        self.bucket = row["bucket"]
        self.key = row["key"]
        self.details = details
        self.isolated = False
        self.deferred_count = 0
        self.retry_after_seconds = details.get("retry_after_seconds")
        self.progress = {
            "verified_objects": 0,
            "verified_bytes": 0,
            "completed_groups": 0,
        }
        super().__init__(
            f"S3 GET paused for {self.bucket}/{self.key}: "
            f"{details.get('status') or details['error']} {details.get('code') or ''}".rstrip()
        )


_GATEWAY_MARKERS = (
    "upstream connect error",
    "disconnect/reset before headers",
    "upstream request timeout",
    "connection termination",
    "connection timeout",
    "no healthy upstream",
    "upstream reset",
)


def gateway_markers(exc):
    # Never publish a raw parser body or exception message. A proxy can put
    # arbitrary content in either one.
    text = str(exc).lower()
    return [marker for marker in _GATEWAY_MARKERS if marker in text]


def error_details(exc):
    from email.utils import parsedate_to_datetime
    from math import ceil

    resp = getattr(exc, "response", {}) or {}
    meta = resp.get("ResponseMetadata", {})
    headers = {str(k).lower(): str(v) for k, v in meta.get("HTTPHeaders", {}).items()}

    def identifier(value):
        return (
            value
            if isinstance(value, str)
            and re.fullmatch(r"[A-Za-z0-9_./+=-]{1,128}", value)
            else None
        )

    status = meta.get("HTTPStatusCode")
    evidence = {
        "error": type(exc).__name__,
        "status": status if type(status) is int else None,
        "code": identifier(resp.get("Error", {}).get("Code")),
        "request_id": identifier(
            meta.get("RequestId") or headers.get("x-amz-request-id")
        ),
    }
    markers = gateway_markers(exc)
    if markers:
        evidence["gateway_indications"] = markers
    # Project known upstream/server tokens only. Do not serialize arbitrary
    # header values such as authorization, cookies, origin addresses or URLs.
    server = headers.get("server", "").lower()
    servers = [
        name
        for name in ("envoy", "nginx", "cloudfront", "amazons3", "squid", "awselb")
        if re.search(r"(?<![a-z0-9])" + name + r"(?![a-z0-9])", server)
    ]
    if servers:
        evidence["servers"] = servers
    envoy_ms = headers.get("x-envoy-upstream-service-time", "")
    if re.fullmatch(r"[0-9]{1,10}", envoy_ms):
        evidence["envoy_upstream_ms"] = int(envoy_ms)
    retry = headers.get("retry-after", "")
    if re.fullmatch(r"[0-9]{1,10}", retry):
        evidence["retry_after_seconds"] = int(retry)
    elif retry:
        try:
            then = parsedate_to_datetime(retry)
            if then.tzinfo is not None:
                evidence["retry_after_seconds"] = max(
                    0, ceil((then - datetime.now(UTC)).total_seconds())
                )
        except (ValueError, TypeError, OverflowError):
            pass
    return evidence


def retryable(exc):
    from botocore.exceptions import (
        ClientError,
        ConnectionClosedError,
        ConnectTimeoutError,
        EndpointConnectionError,
        IncompleteReadError,
        ProxyConnectionError,
        ReadTimeoutError,
        ResponseStreamingError,
    )
    from botocore.parsers import ResponseParserError

    if isinstance(exc, ArchiveGetUnavailable):
        return True
    if isinstance(exc, ResponseParserError):
        return bool(gateway_markers(exc))
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
            ConnectTimeoutError,
            EndpointConnectionError,
            IncompleteReadError,
            ProxyConnectionError,
            ReadTimeoutError,
            ResponseStreamingError,
        ),
    )


class ArchiveHeadUnavailable(RuntimeError):
    """A metadata check needs a later retry; known source pairs remain usable."""

    def __init__(self, day, row, unresolved_count):
        self.day = day
        self.row = row
        self.details = row.get("details", {})
        self.unresolved_count = unresolved_count
        super().__init__(
            f"S3 HEAD incomplete for {day} at {row['bucket']}/{row['key']}"
        )


def head(row, ledger, s3, attempts=3):
    from botocore.exceptions import ClientError

    for attempt in range(attempts):
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
            if attempt + 1 == attempts:
                return {
                    **row,
                    "status": "unresolved",
                    "error": type(exc).__name__,
                    "details": error_details(exc),
                }
            time.sleep(0.25 + random.random() * 2**attempt)


def plan_days(root, days, ledger, s3, workers, *, retry_unresolved_only=False):
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
            if retry_unresolved_only:
                available.update({r["key"]: r for r in existing.get("unavailable", [])})
            expected = list(items(day))
            last_probe_key = None
            probe_state = {}
            missing = [row for row in expected if row["key"] not in available]

            def save_progress(
                folder=folder, available=available, probe_state=probe_state
            ):
                atomic_json(
                    folder / "plan.partial.json",
                    {
                        "account": ledger.account,
                        "probe": probe_state.copy(),
                        "unavailable": [
                            r
                            for r in available.values()
                            if r.get("status") == "missing"
                        ],
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
                        if retry_unresolved_only:
                            available.update(
                                {r["key"]: r for r in cached.get("unavailable", [])}
                            )
                        last_probe_key = cached.get("probe", {}).get("key")
                    missing = [row for row in expected if row["key"] not in available]
                if len(missing) >= 8:
                    # One journaled HEAD checks reachability before a concurrent fan-out.
                    # Rotate after a failure so an object-specific error cannot hold an entire day.
                    keys = [r["key"] for r in missing]
                    index = (
                        (keys.index(last_probe_key) + 1) % len(keys)
                        if last_probe_key in keys
                        else 0
                    )
                    row = missing.pop(index)
                    probe_state["key"] = row["key"]
                    checked = head(row, ledger, s3, attempts=1)
                    available[row["key"]] = checked
                    if checked.get("status") == "unresolved":
                        raise ArchiveHeadUnavailable(day, checked, len(missing) + 1)
                tasks = [
                    pool.submit(
                        head, row, ledger, s3, 1 if retry_unresolved_only else 3
                    )
                    for row in missing
                ]
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
                raise ArchiveHeadUnavailable(day, unresolved[0], len(unresolved))
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

    from botocore.exceptions import IncompleteReadError

    if valid_cache(folder, row):
        return False
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
                    raise IncompleteReadError(
                        actual_bytes=size, expected_bytes=row["bytes"]
                    )
            os.replace(tmp, raw)
            tmp = None
            atomic_json(receipt, {"source": row, "sha256": digest.hexdigest()})
            raw.with_suffix(".get-error.json").unlink(missing_ok=True)
            return True
    except Exception as exc:
        details = error_details(exc)
        atomic_json(raw.with_suffix(".get-error.json"), {"source": row, **details})
        if retryable(exc):
            raise ArchiveGetUnavailable(row, details) from None
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
        if folder.parent.name in ("training", "validation"):
            # Optional sampled evidence is independent of packet validation and cleanup.
            try:
                from fee_evidence import save, scheduled

                if scheduled(group[0]["hour"]):
                    raw, receipt = raw_paths(folder, group[1])
                    save(raw, receipt, folder / "weekly-fee-evidence.json", "weekly")
            except Exception as exc:  # noqa: BLE001 - an audit failure must not block raw cleanup
                print(
                    f"Fee sample unavailable at {group[0]['hour']}: {type(exc).__name__}",
                    flush=True,
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


def acquire_wave(batch, ledger, s3, cpu, io, slots_count, progress, probe_key):
    """Transfer a bounded wave and normalize downloaded groups while other GETs finish."""
    from concurrent.futures import as_completed

    from botocore.exceptions import BotoCoreError, ClientError

    pause, counter_lock = threading.Event(), threading.Lock()
    slots = threading.Semaphore(slots_count)
    transient = []
    skipped = []
    fresh_groups = failures = 0

    def transfer(folder, group):
        # Probe the affected bucket before spending on the group's other source.
        priority = (
            sorted(group, key=lambda r: r["key"] != probe_key) if probe_key else group
        )
        fresh = False
        for row in priority:
            with slots:
                if pause.is_set():
                    return None
                try:
                    fetched = fetch(folder, row, ledger, s3)
                except ArchiveGetUnavailable as exc:
                    with counter_lock:
                        transient.append(exc)
                        pause.set()
                    raise
                except (BotoCoreError, ClientError):
                    pause.set()
                    raise
                except ValueError as exc:
                    if "budget" in str(exc).lower():
                        pause.set()
                    raise
                if fetched:
                    fresh = True
                    with counter_lock:
                        progress["verified_objects"] += 1
                        progress["verified_bytes"] += row["bytes"]
        return fresh

    downloads = {
        io.submit(transfer, folder, group): i for i, (folder, group) in enumerate(batch)
    }
    parses = {}
    for future in as_completed(downloads):
        i = downloads[future]
        folder, group = batch[i]
        try:
            fresh = future.result()
        except ArchiveGetUnavailable as exc:
            atomic_json(
                folder / f"{group[0]['hour']}.{group[0]['kind']}.error.json",
                {"stage": "GET", "bucket": exc.bucket, "key": exc.key, **exc.details},
            )
            continue
        except (OSError, ValueError) as exc:
            if "budget" in str(exc).lower():
                raise
            atomic_json(
                folder / f"{group[0]['hour']}.{group[0]['kind']}.error.json",
                {"stage": "GET", **error_details(exc)},
            )
            failures += 1
            continue
        if fresh is None:
            skipped.append(i)
        else:
            parses[cpu.submit(normalize_group, folder, group)] = (folder, group, fresh)

    for future in as_completed(parses):
        folder, group, fresh = parses[future]
        error_file = folder / f"{group[0]['hour']}.{group[0]['kind']}.error.json"
        try:
            future.result()
            progress["completed_groups"] += 1
            fresh_groups += int(fresh)
            error_file.unlink(missing_ok=True)
        except (
            OSError,
            ValueError,
            KeyError,
            TypeError,
            ArithmeticError,
            RuntimeError,
        ) as exc:
            atomic_json(error_file, {"stage": "normalize", "error": str(exc)})
            failures += 1
    return transient, [batch[i] for i in sorted(skipped)], fresh_groups, failures


def alternate_group(queue, failure, ledger, s3):
    """One inexpensive HEAD against a different priced key in the failing bucket."""
    for entry in queue:
        folder, group = entry
        for row in group:
            if row["bucket"] != failure.bucket or row["key"] == failure.key:
                continue
            if raw_paths(folder, row)[1].exists():
                continue  # This group would not test a fresh GET to the failed route.
            check = head(row, ledger, s3, attempts=1)
            if check.get("status"):
                delay = check.get("details", {}).get("retry_after_seconds")
                if delay:
                    failure.retry_after_seconds = max(
                        failure.retry_after_seconds or 0, delay
                    )
                return None
            if check["bytes"] != row["bytes"] or check["etag"] != row["etag"]:
                raise ValueError(
                    "Alternate object changed since the measured plan: " + row["key"]
                )
            return entry, row["key"]
    return None


def collect_days(root, days, ledger, s3, workers, max_gets=4, start_gets=None):
    import multiprocessing
    import shutil
    from collections import deque
    from concurrent.futures import ProcessPoolExecutor

    active = max_gets if start_gets is None else start_gets
    if workers < 1 or not 1 <= active <= max_gets <= 4:
        raise ValueError(
            "Expected at least one normalization worker and 1 <= start_gets <= max_gets <= 4"
        )

    progress = {"verified_objects": 0, "verified_bytes": 0, "completed_groups": 0}
    remaining = deque()
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
    total = len(remaining)
    print(
        json.dumps(
            {
                "remaining_groups": total,
                "projected_GET_usd": projected,
                "gross_with_reservations": used + projected,
            }
        ),
        flush=True,
    )

    deferred = []
    probe_key = None
    successful_streak = failures = waves = 0
    with (
        ProcessPoolExecutor(
            max_workers=workers, mp_context=multiprocessing.get_context("spawn")
        ) as cpu,
        ThreadPoolExecutor(max_workers=workers * 3) as io,
    ):
        while remaining:
            single_probe = probe_key is not None or not progress["verified_objects"]
            window = active if active < max_gets else workers * 3
            count = min(len(remaining), 1 if single_probe else window)
            batch = [remaining.popleft() for _ in range(count)]
            pending_bytes = sum(r["bytes"] for _, group in batch for r in group)
            if shutil.disk_usage(root).free < 2 * pending_bytes + 2 * 1024**3:
                raise RuntimeError("Insufficient free space for next bounded batch")
            errors, skipped, fresh, bad = acquire_wave(
                batch,
                ledger,
                s3,
                cpu,
                io,
                1 if single_probe else active,
                progress,
                probe_key,
            )
            remaining.extendleft(reversed(skipped))
            failures += bad
            if errors:
                failure = errors[0]
                failure.progress = progress.copy()
                # Respect server-imposed throttles. Multiple simultaneous failures
                # or a failed alternate GET are sufficient reason to pause the route.
                if (
                    probe_key
                    or len(errors) > 1
                    or failure.retry_after_seconds is not None
                    or failure.details.get("status") == 429
                    or failure.details.get("code") in ("SlowDown", "Throttling")
                ):
                    raise failure
                alt = alternate_group(remaining, failure, ledger, s3)
                if alt is None:
                    raise failure
                entry, probe_key = alt
                remaining.remove(entry)
                remaining.appendleft(entry)
                deferred.append(failure)
                active = 1
                successful_streak = 0
            else:
                # A fresh GET of the alternate source ran first, if requested.
                probe_key = None
                successful_streak = 0 if bad else successful_streak + fresh
                if active < max_gets and successful_streak >= 4:
                    active = max_gets
                    print(
                        json.dumps(
                            {
                                "concurrent_GETs": active,
                                "verified_group_streak": successful_streak,
                            }
                        ),
                        flush=True,
                    )
            waves += 1
            if waves == 1 or waves % 10 == 0 or not remaining:
                print(
                    json.dumps(
                        {
                            "completed_groups": progress["completed_groups"],
                            "total": total,
                            "normalization_failures": failures,
                            "deferred_GETs": len(deferred),
                            "last_partition_day": batch[-1][0].name,
                        }
                    ),
                    flush=True,
                )
    if failures:
        raise RuntimeError(
            f"{failures} groups need diagnosis; other groups were processed"
        )
    if deferred:
        # Every deferred error had a distinct same-bucket alternate GET succeed,
        # and every other priced group has now been attempted. Subsequent invocations
        # validate the receipts and only issue GETs for the outstanding objects.
        failure = deferred[0]
        failure.progress = progress.copy()
        failure.isolated = True
        failure.deferred_count = len(deferred)
        raise failure
    return progress


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
    parser.add_argument("--max-gets", type=int, choices=range(1, 5), default=4)
    parser.add_argument("--start-gets", type=int, choices=range(1, 5))
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
            collect_days(
                args.root,
                days,
                ledger,
                s3,
                args.workers or 4,
                args.max_gets,
                args.start_gets,
            )


if __name__ == "__main__":
    main()
