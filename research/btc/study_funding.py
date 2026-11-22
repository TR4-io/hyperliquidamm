"""Archive official BTC hourly funding observations by split, without scoring them."""

import argparse
import gzip
import hashlib
import json
import math
import os
import tempfile
import time
import urllib.error
import urllib.request
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from funding import HOUR, URL

PRE_STUDY = {"pre-study": (date(2025, 11, 22), date(2025, 12, 30))}

SPLITS = {
    "training": (date(2025, 12, 30), date(2026, 8, 27)),
    "validation": (date(2026, 8, 27), date(2026, 10, 26)),
    "sealed-final": (date(2026, 10, 26), date(2026, 12, 30)),
}


def timestamp(day):
    return int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp()) * 1000


def atomic(path, data):
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as f:
        f.write(data)
        temp = f.name
    os.replace(temp, path)


def write_json(path, obj):
    atomic(path, (json.dumps(obj, indent=2, allow_nan=False) + "\n").encode())


def validate(raw, start, end):
    rows = json.loads(raw)
    if not isinstance(rows, list):
        raise TypeError("Expected fundingHistory array")
    for r in rows:
        if (
            r["coin"] != "BTC"
            or type(r["time"]) != int
            or not math.isfinite(float(r["fundingRate"]))
        ):
            raise ValueError("Invalid BTC funding event")
    if [r["time"] // HOUR for r in rows] != list(range(start // HOUR, end // HOUR)):
        raise ValueError("Missing, unexpected or duplicate settlement hour")
    return len(rows)


def request_bytes(payload):
    return json.dumps(payload).encode()


def request(payload):
    req = urllib.request.Request(
        URL, request_bytes(payload), {"Content-Type": "application/json"}
    )
    for attempt in range(6):
        try:
            with urllib.request.urlopen(req, timeout=30) as res:
                return res.read(2_000_000)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
            if isinstance(err, urllib.error.HTTPError) and err.code not in (
                429,
                500,
                502,
                503,
                504,
            ):
                raise
            if attempt == 5:
                raise
            time.sleep(min(2**attempt, 15))


def archive(out, through, first_week=None, *, pre_study=False):
    """'through' is exclusive. Fetch only complete UTC days."""
    cap = min(through, datetime.now(UTC).date())
    windows = PRE_STUDY if pre_study else SPLITS
    for split, (day, stop) in windows.items():
        stop = min(stop, cap)
        if day >= stop:
            continue
        folder = out / split
        folder.mkdir(parents=True, exist_ok=True)
        manifest_path = folder / "manifest.json"
        if manifest_path.exists():
            previous_end = date.fromisoformat(
                json.loads(manifest_path.read_bytes())["end_exclusive"]
            )
            if stop < previous_end:
                raise ValueError("Requested end precedes already archived settlements")
        pages = []
        while day < stop:
            after = min(day + timedelta(days=7), stop)
            start, end = timestamp(day), timestamp(after)
            rawpath = folder / (day.isoformat() + ".raw.json")
            metapath = rawpath.with_suffix(".manifest.json")
            payload = {
                "type": "fundingHistory",
                "coin": "BTC",
                "startTime": start,
                "endTime": end - 1,
            }
            meta = json.loads(metapath.read_bytes()) if metapath.exists() else None
            # Extend a short last page in place if later UTC days have elapsed.
            if meta and meta["request"] == payload and meta["source"] == URL:
                raw = rawpath.read_bytes()
                if hashlib.sha256(raw).hexdigest() != meta["sha256"]:
                    raise ValueError(f"Corrupt archived page {rawpath}")
                count = validate(raw, start, end)
                if split == "pre-study" and (
                    meta["requestSha256"]
                    != hashlib.sha256(request_bytes(payload)).hexdigest()
                    or meta["settlements"] != count
                    or meta["bytes"] != len(raw)
                    or meta["file"] != rawpath.name
                ):
                    raise ValueError(
                        "Pre-study funding request or raw page metadata mismatch"
                    )
            else:
                source_meta = None
                if first_week and day == SPLITS["training"][0]:
                    source_meta = json.loads(
                        (first_week / "fundingHistory.manifest.json").read_bytes()
                    )
                if (
                    source_meta
                    and source_meta["request"] == payload
                    and source_meta["source"] == URL
                ):
                    raw = (first_week / "fundingHistory.raw.json").read_bytes()
                    if hashlib.sha256(raw).hexdigest() != source_meta["sha256"]:
                        raise ValueError("Corrupt first-week funding input")
                    retrieved = source_meta["retrieved_utc"]
                else:
                    raw = request(payload)
                    retrieved = datetime.now(UTC).isoformat()
                n = validate(raw, start, end)
                meta = {
                    "source": URL,
                    "request": payload,
                    "file": rawpath.name,
                    "bytes": len(raw),
                    "sha256": hashlib.sha256(raw).hexdigest(),
                    "settlements": n,
                    "retrieved_utc": retrieved,
                }
                if split == "pre-study":
                    meta["requestSha256"] = hashlib.sha256(
                        request_bytes(payload)
                    ).hexdigest()
                atomic(rawpath, raw)
                write_json(metapath, meta)
            pages.append(meta)
            day = after
        write_json(
            folder / "manifest.json",
            {
                "split": split,
                "start": windows[split][0].isoformat(),
                "end_exclusive": stop.isoformat(),
                "pages": pages,
                "settlements": sum(p["settlements"] for p in pages),
                "coverage": "hourly BTC settled rates; settlement oracle not supplied by fundingHistory",
            },
        )
        print(
            split,
            len(pages),
            "pages,",
            sum(p["settlements"] for p in pages),
            "hours",
            flush=True,
        )


def join_samples(rates, samples, source):
    clocks = [r["time"] // HOUR * HOUR for r in rates]
    by_time = {r["time"]: r for r in samples}
    if len(by_time) != len(samples):
        raise ValueError("Duplicate context sample")
    if set(by_time) - set(clocks):
        raise ValueError("Context timestamp outside daily funding clock")
    out = []
    for rate, clock in zip(rates, clocks):
        sample = by_time.get(clock)
        if sample is None:
            continue  # Preserve missing coverage; never interpolate.
        px = [Decimal(sample[k]) for k in ("oracle_px", "mark_px")]
        if sample["coin"] != "BTC" or any(not v.is_finite() or v <= 0 for v in px):
            raise ValueError("Invalid BTC context sample")
        out.append(
            {
                "time": rate["time"],
                "sampleTime": clock,
                "oraclePx": sample["oracle_px"],
                "markPx": sample["mark_px"],
                "sourceBucket": source["bucket"],
                "sourceKey": source["key"],
                "sourceSha256": source["sha256"],
                "sourceField": "oracle_px",
                "priceEvidence": "minute-sampled context proxy",
                "snapshotFundingMatchesSettledRate": Decimal(sample["funding"])
                == Decimal(rate["fundingRate"]),
            }
        )
    return out


def materialize(funding, annual, first_week, *, pre_study=False):
    """Publish per-day derived rates and any joined context proxies we can verify."""
    old = {}
    if first_week and not pre_study:
        for row in json.loads((first_week / "context-prices.json").read_bytes()):
            old.setdefault(row["time"] // (24 * HOUR), []).append(row)
    counts = {}
    for split in PRE_STUDY if pre_study else SPLITS:
        path = funding / split / "manifest.json"
        if not path.exists():
            continue
        manifest = json.loads(path.read_bytes())
        n_days = n_joined = n_proxy = 0
        for page in manifest["pages"]:
            body = (path.parent / page["file"]).read_bytes()
            if hashlib.sha256(body).hexdigest() != page["sha256"]:
                raise ValueError("Funding page checksum mismatch")
            payload = page["request"]
            if (
                split == "pre-study"
                and page["requestSha256"]
                != hashlib.sha256(request_bytes(payload)).hexdigest()
            ):
                raise ValueError("Pre-study funding request checksum mismatch")
            validate(body, payload["startTime"], payload["endTime"] + 1)
            rows = json.loads(body)
            for i in range(0, len(rows), 24):
                rates = rows[i : i + 24]
                start = rates[0]["time"] // (24 * HOUR) * (24 * HOUR)
                if len(rates) != 24 or rates[-1]["time"] // HOUR != start // HOUR + 23:
                    raise ValueError("Incomplete UTC funding day")
                folder = (
                    annual
                    / split
                    / datetime.fromtimestamp(start / 1000, UTC).strftime("%Y%m%d")
                )
                folder.mkdir(parents=True, exist_ok=True)
                rates_file = folder / "fundingHistory.raw.json"
                write_json(rates_file, rates)
                n_days += 1
                summary = {
                    "source": URL,
                    "rawPage": page["file"],
                    "rawPageSha256": page["sha256"],
                    "rawPageRequest": payload,
                    "rateCount": len(rates),
                    "startUtcMs": start,
                    "dailyRowsSha256": hashlib.sha256(
                        rates_file.read_bytes()
                    ).hexdigest(),
                    "settlementOracleVerified": False,
                }
                context = folder / "context.manifest.json"
                proxy = None
                if context.exists():
                    cm = json.loads(context.read_bytes())
                    data = (folder / "context-samples.json.gz").read_bytes()
                    if (
                        hashlib.sha256(data).hexdigest() != cm["packet_sha256"]
                        or cm["hour"] != start
                    ):
                        raise ValueError("Context packet checksum or clock mismatch")
                    proxy = join_samples(
                        rates, json.loads(gzip.decompress(data)), cm["sources"][0]
                    )
                elif start // (24 * HOUR) in old:
                    proxy = old[start // (24 * HOUR)]
                    if [r["time"] for r in proxy] != [r["time"] for r in rates]:
                        raise ValueError("First week proxy clock mismatch")
                if proxy is not None:
                    write_json(folder / "context-prices.json", proxy)
                    n_joined += 1
                    n_proxy += len(proxy)
                summary["sampledProxyCount"] = len(proxy) if proxy is not None else 0
                write_json(folder / "funding.manifest.json", summary)
        counts[split] = {
            "rateDays": n_days,
            "joinedDays": n_joined,
            "proxyObservations": n_proxy,
        }
    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument(
        "--end-exclusive", type=date.fromisoformat, default=SPLITS["sealed-final"][1]
    )
    parser.add_argument("--first-week", type=Path)
    parser.add_argument(
        "--annual",
        type=Path,
        help="publish daily rates and join available context samples",
    )
    parser.add_argument(
        "--pre-study",
        action="store_true",
        help="Archive only the separate 2025-11-22 through 2025-12-29 acquisition window",
    )
    args = parser.parse_args()
    archive(args.output, args.end_exclusive, args.first_week, pre_study=args.pre_study)
    if args.annual:
        print(
            materialize(
                args.output, args.annual, args.first_week, pre_study=args.pre_study
            )
        )
