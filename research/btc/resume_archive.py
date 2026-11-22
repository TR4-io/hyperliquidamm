"""Resume the budgeted archive in UTC batches while the authenticated worker is available."""

import argparse
import fcntl
import json
import logging
import subprocess
import time
from collections import deque
from contextlib import ExitStack, contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from archive import atomic_json
from bulk_archive import (
    AFTER_WEEK,
    AUXILIARY_START,
    FIRST,
    LAST,
    VALIDATION,
    ArchiveGetUnavailable,
    ArchiveHeadUnavailable,
    clients,
    collect_days,
    day_dir,
    error_details,
    plan_days,
    retryable,
)
from study_funding import materialize

LOG = logging.getLogger("archive")
ONE = timedelta(days=1)


@contextmanager
def lock(path, blocking=True):
    with path.open("a") as f:
        fcntl.flock(f, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
        yield


def transient(exc):
    if isinstance(exc, ArchiveHeadUnavailable) or retryable(exc):
        return True
    if isinstance(exc, RuntimeError):
        return str(exc).startswith(
            ("S3 HEADs all failed", "S3 GET paused after a service error")
        )
    return False


def batches(through):
    # Collect the four days already sized before moving on to seven-day batches.
    day = AFTER_WEEK
    while day <= through:
        end = min(through, day + (3 if day == AFTER_WEEK else 6) * ONE)
        yield [day + i * ONE for i in range((end - day).days + 1)]
        day = end + ONE


def pre_study_batches():
    # Start transferring as soon as the first new day is priced.
    yield [AUXILIARY_START]
    day = AUXILIARY_START + ONE
    while day < FIRST:
        stop = min(day + 7 * ONE, FIRST)
        yield [day + i * ONE for i in range((stop - day).days)]
        day = stop


def fit_prefix(data):
    subprocess.run(
        [
            "node",
            str(Path(__file__).with_name("annual.cjs")),
            "fit-ready",
            str(data),
            str(data / "annual-fit-resumed"),
        ],
        check=True,
    )


def validate_if_ready(data):
    model = data / "annual-fit-resumed" / "model.json"
    if not model.exists():
        return "Waiting for the complete training fit"
    from bulk_archive import FINAL
    from funding import HOUR
    from study_funding import timestamp

    for n in range((FINAL - VALIDATION).days):
        d = VALIDATION + ONE * n
        folder = day_dir(data / "annual", d)
        if not (folder / "context-prices.json").exists():
            return f"Waiting for the funding proxy for {d}"
        for h in range(24):
            t = timestamp(d) + HOUR * h
            if not all(
                (folder / f"{t}.{suffix}").exists()
                for suffix in ("json.gz", "manifest.json")
            ):
                return f"Waiting for validation hour {d} {h:02d}:00 UTC"
    output = model.parent / "validation-proxy.json"
    if not output.exists():
        subprocess.run(
            [
                "node",
                str(Path(__file__).with_name("annual.cjs")),
                "diagnose-validation",
                str(data),
                str(output),
                str(model),
            ],
            check=True,
        )
    return "Development validation diagnostic available"


def plan_batch(root, days, ledger, s3, retry_unresolved_only):
    """Hand every measured day to collection and retain outstanding HEADs for recovery."""
    head_failure = None
    try:
        if retry_unresolved_only:
            plan_days(root, days, ledger, s3, 8, retry_unresolved_only=True)
        else:
            plan_days(root, days, ledger, s3, 8)
    except ArchiveHeadUnavailable as exc:
        head_failure = exc

    measured_days, pending_days = [], []
    for day in days:
        plan_path = day_dir(root, day) / "plan.json"
        if not plan_path.exists():
            if head_failure is None:
                raise ValueError(
                    "Archive plan unexpectedly absent after metadata check"
                )
            pending_days.append(day)
            continue
        measured_days.append(day)
        unresolved = json.loads(plan_path.read_bytes()).get("unresolved", [])
        if unresolved or (head_failure and head_failure.day == day):
            pending_days.append(day)
            if head_failure is None:
                head_failure = ArchiveHeadUnavailable(
                    day, unresolved[0], len(unresolved)
                )
    return measured_days, pending_days, head_failure


def run(
    data, through, hours, download_only=False, include_pre_study=False, keep_raw=False
):
    if include_pre_study and not download_only:
        raise ValueError("Pre-study acquisition requires download-only mode")
    root = data / "annual"
    root.mkdir(parents=True, exist_ok=True)
    status_file = root / "transfer-status.json"
    deadline = time.monotonic() + hours * 3600

    def report(state, days, **fields):
        record = {
            "state": state,
            "updatedUtc": datetime.now(UTC).isoformat(),
            "batchStart": days[0].isoformat(),
            "batchEnd": days[-1].isoformat(),
            "through": through.isoformat(),
            "downloadOnly": download_only,
            "includePreStudy": include_pre_study,
            "keepRaw": keep_raw,
            **fields,
        }
        atomic_json(status_file, record)
        LOG.info("%s", json.dumps(record))

    with lock(root / "supervisor.lock", blocking=False):
        scheduled = list(pre_study_batches()) if include_pre_study else []
        scheduled.extend(batches(through))
        all_days = [day for batch in scheduled for day in batch]
        pending_batches = deque((batch, False) for batch in scheduled)
        analysis_errors = []

        def fit_after_download(batch):
            if not download_only and batch[0] < VALIDATION:
                try:
                    fit_prefix(data)
                except (OSError, subprocess.CalledProcessError) as exc:
                    analysis_errors.append(
                        {"through": batch[-1].isoformat(), "error": str(exc)}
                    )
                    LOG.error("Analysis held; continuing archive acquisition: %s", exc)

        while pending_batches:
            days, revisiting = pending_batches.popleft()
            delay = 30
            start_gets = 4
            planned = False
            planning_days = days
            retry_unresolved_only = False
            ledger = None
            probe = None
            with ExitStack() as resources:
                while True:
                    try:
                        if ledger is None:
                            stage = "checking AWS identity"
                            report(stage, days)
                            ledger, s3 = clients(data / "archive-budget.json")
                            resources.callback(ledger.file.close)
                        if not planned:
                            stage = "sizing archive"
                            with lock(root / "plan.lock"):
                                report(stage, days)
                                collect_scope, pending_days, head_failure = plan_batch(
                                    root,
                                    planning_days,
                                    ledger,
                                    s3,
                                    retry_unresolved_only,
                                )
                            planned = True
                        with lock(root / "collect.lock"):
                            if probe is not None:
                                # A journaled metadata request tests the failed object's route
                                # before another full transfer reservation. GET still uses IfMatch.
                                stage = "checking failed S3 object"
                                report(
                                    stage,
                                    days,
                                    bucket=probe["bucket"],
                                    key=probe["key"],
                                )
                                ledger.reserve(probe, "HEAD")
                                s3[probe["bucket"]].head_object(
                                    Bucket=probe["bucket"],
                                    Key=probe["key"],
                                    RequestPayer="requester",
                                )
                                probe = None
                            stage = "collecting verified pairs"
                            report(
                                stage,
                                days,
                                startingConcurrentGETs=start_gets,
                                maxConcurrentGETs=4,
                            )
                            progress = (
                                collect_days(
                                    root,
                                    collect_scope,
                                    ledger,
                                    s3,
                                    4,
                                    max_gets=4,
                                    start_gets=start_gets,
                                    keep_raw=keep_raw,
                                )
                                if collect_scope
                                else {}
                            )
                        if pending_days:
                            planned = False
                            planning_days = pending_days
                            retry_unresolved_only = True
                            stage = "recovering incomplete S3 metadata"
                            if isinstance(progress, dict) and progress.get(
                                "completed_groups"
                            ):
                                delay = 5
                                fit_after_download(days)
                            raise head_failure
                        break
                    except Exception as exc:
                        details = {"stage": stage, "errorType": type(exc).__name__}
                        retry_after = 0
                        if isinstance(exc, ArchiveGetUnavailable):
                            probe = {"bucket": exc.bucket, "key": exc.key}
                            start_gets = 1
                            details.update(
                                probe, transport=exc.details, progress=exc.progress
                            )
                            if (
                                exc.progress["verified_objects"]
                                or exc.progress["completed_groups"]
                            ):
                                delay = 5
                            retry_after = exc.retry_after_seconds or 0
                        elif isinstance(exc, ArchiveHeadUnavailable):
                            details.update(
                                bucket=exc.row["bucket"],
                                key=exc.row["key"],
                                unresolvedOrUnmeasured=exc.unresolved_count,
                                metadataDaysPending=len(planning_days),
                                transport=exc.details,
                            )
                            retry_after = exc.details.get("retry_after_seconds", 0)
                        else:
                            details["transport"] = error_details(exc)
                            retry_after = details["transport"].get(
                                "retry_after_seconds", 0
                            )
                            if probe is not None:
                                details.update(probe)
                        if not transient(exc):
                            report("stopped", days, reason=str(exc)[:500], **details)
                            raise
                        if (
                            download_only
                            and not revisiting
                            and pending_batches
                            and isinstance(exc, ArchiveGetUnavailable)
                            and getattr(exc, "isolated", False)
                            and not retry_after
                        ):
                            pending_batches.append((days, True))
                            report(
                                "returning to isolated object after other batches",
                                days,
                                **details,
                            )
                            break
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            report("upstream still unavailable", days, **details)
                            return 2
                        delay_now = min(max(delay, retry_after), remaining)
                        report(
                            "waiting for upstream recovery",
                            days,
                            retrySeconds=int(delay_now),
                            **details,
                        )
                        time.sleep(delay_now)
                        if delay_now >= remaining:
                            report("upstream still unavailable", days, **details)
                            return 2
                        delay = min(300, delay * 2)
            fit_after_download(days)
        materialize(data / "funding", root, data / "first-week")
        plans = [
            json.loads((day_dir(root, d) / "plan.json").read_bytes()) for d in all_days
        ]
        report(
            "finished available batch pass",
            all_days,
            missingObjects=sum(len(p["unavailable"]) for p in plans),
            unresolvedObjects=sum(len(p.get("unresolved", [])) for p in plans),
            analysisErrors=analysis_errors,
            validation="Deferred in download-only mode"
            if download_only
            else validate_if_ready(data),
        )
    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data", type=Path)
    parser.add_argument("--through", type=date.fromisoformat, required=True)
    parser.add_argument("--retry-hours", type=float, default=12)
    parser.add_argument(
        "--keep-raw",
        action="store_true",
        help="Retain newly acquired exact source objects and receipts in private daily folders",
    )
    parser.add_argument(
        "--include-pre-study",
        action="store_true",
        help="Prepend the separate November 22 through December 29, 2025 archive to a download-only pass",
    )
    parser.add_argument(
        "--download-only",
        action="store_true",
        help="Acquire and verify data without fitting or replaying a strategy",
    )
    args = parser.parse_args()
    if not AFTER_WEEK <= args.through <= min(LAST, datetime.now(UTC).date() - ONE):
        parser.error("Select elapsed complete UTC dates inside the study")
    if not 0 < args.retry_hours <= 12:
        parser.error("Select a bounded retry horizon up to 12 hours")
    raise SystemExit(
        run(
            args.data.resolve(),
            args.through,
            args.retry_hours,
            args.download_only,
            args.include_pre_study,
            args.keep_raw,
        )
    )
