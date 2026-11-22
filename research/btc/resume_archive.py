"""Resume the budgeted archive in UTC batches while the authenticated worker is available."""

import argparse
import fcntl
import json
import logging
import subprocess
import time
from contextlib import ExitStack, contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from archive import atomic_json
from bulk_archive import (
    AFTER_WEEK,
    LAST,
    VALIDATION,
    ArchiveGetUnavailable,
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
    if retryable(exc):
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


def run(data, through, hours):
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
            **fields,
        }
        atomic_json(status_file, record)
        LOG.info("%s", json.dumps(record))

    with lock(root / "supervisor.lock", blocking=False):
        all_days = []
        analysis_errors = []
        for days in batches(through):
            all_days.extend(days)
            delay = 30
            max_gets = 4
            planned = False
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
                                plan_days(root, days, ledger, s3, 8)
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
                            report(stage, days, concurrentGETs=max_gets)
                            collect_days(root, days, ledger, s3, 4, max_gets=max_gets)
                        break
                    except Exception as exc:
                        details = {"stage": stage, "errorType": type(exc).__name__}
                        retry_after = 0
                        if isinstance(exc, ArchiveGetUnavailable):
                            probe = {"bucket": exc.bucket, "key": exc.key}
                            max_gets = 1
                            details.update(
                                probe, transport=exc.details, progress=exc.progress
                            )
                            if (
                                exc.progress["verified_objects"]
                                or exc.progress["completed_groups"]
                            ):
                                delay = 5
                            retry_after = exc.retry_after_seconds or 0
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
            if days[0] < VALIDATION:
                try:
                    fit_prefix(data)
                except (OSError, subprocess.CalledProcessError) as exc:
                    analysis_errors.append(
                        {"through": days[-1].isoformat(), "error": str(exc)}
                    )
                    LOG.error("Analysis held; continuing archive acquisition: %s", exc)
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
            validation=validate_if_ready(data),
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
    args = parser.parse_args()
    if not AFTER_WEEK <= args.through <= min(LAST, datetime.now(UTC).date() - ONE):
        parser.error("Select elapsed complete UTC dates inside the study")
    if not 0 < args.retry_hours <= 12:
        parser.error("Select a bounded retry horizon up to 12 hours")
    raise SystemExit(run(args.data.resolve(), args.through, args.retry_hours))
