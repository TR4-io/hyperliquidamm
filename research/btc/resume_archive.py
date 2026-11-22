"""Resume the budgeted archive in UTC batches while the authenticated worker is available."""

import argparse
import fcntl
import json
import logging
import subprocess
import time
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from archive import atomic_json
from botocore.parsers import ResponseParserError
from bulk_archive import (
    AFTER_WEEK,
    LAST,
    VALIDATION,
    clients,
    collect_days,
    day_dir,
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
    if isinstance(exc, ResponseParserError):
        return "upstream connect error" in str(exc)
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
            while True:
                ledger = None
                try:
                    report("checking AWS identity", days)
                    ledger, s3 = clients(data / "archive-budget.json")
                    # Record availability errors distinctly; only verified complete pairs are downloaded.
                    with lock(root / "plan.lock"):
                        report("sizing archive", days)
                        plan_days(root, days, ledger, s3, 8)
                    with lock(root / "collect.lock"):
                        report("collecting verified pairs", days)
                        collect_days(root, days, ledger, s3, 4)
                    break
                except Exception as exc:
                    if not transient(exc):
                        report(
                            "stopped",
                            days,
                            errorType=type(exc).__name__,
                            reason=str(exc)[:500],
                        )
                        raise
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        report(
                            "upstream still unavailable",
                            days,
                            errorType=type(exc).__name__,
                        )
                        return 2
                    delay_now = min(delay, remaining)
                    report(
                        "waiting for upstream recovery",
                        days,
                        retrySeconds=int(delay_now),
                        errorType=type(exc).__name__,
                    )
                    time.sleep(delay_now)
                    delay = min(300, delay * 2)
                finally:
                    if ledger:
                        ledger.file.close()
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
