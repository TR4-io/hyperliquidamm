"""First training week only. Staged HEAD, cost estimate, GET and offline normalization.

Raw objects are downloaded in batches of up to four hours. Retain BTC-only packets.
The spend journal reserves gross estimates BEFORE calls, including failed attempts.
"""

import argparse
import contextlib
import fcntl
import gzip
import hashlib
import itertools
import json
import math
import os
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

from funding import END, HOUR, START

RATES = {
    "hyperliquid-archive": ("us-east-1", 0.00000040, 0.090),
    "hl-mainnet-node-data": ("ap-northeast-1", 0.00000037, 0.114),
}


# Decimal GB, no free allowance or credits netted; conservative for binary billing.
def estimated_cost(bucket, size):
    _, request_usd, egress_usd = RATES[bucket]
    return request_usd + size / 1e9 * egress_usd


def sources(t):
    d = datetime.fromtimestamp(t / 1000, UTC)
    return [
        ("hyperliquid-archive", f"market_data/{d:%Y%m%d}/{d.hour}/l2Book/BTC.lz4"),
        ("hl-mainnet-node-data", f"node_fills_by_block/hourly/{d:%Y%m%d}/{d.hour}.lz4"),
    ]


def atomic_json(path, obj):
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False, mode="w") as f:
        json.dump(obj, f, indent=2, allow_nan=False)
        f.write("\n")
        tmp = f.name
    os.replace(tmp, path)


def sha(path):
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def read_rows(path):
    import lz4.frame

    with lz4.frame.open(path, "rt") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def block_ms(value):
    if isinstance(value, int):
        return value
    dt = datetime.fromisoformat(value)
    return int((dt if dt.tzinfo else dt.replace(tzinfo=UTC)).timestamp() * 1000)


def normalize(t, paths, out, origin):
    books, trades, block_times = [], {}, []
    btc_accounts = crossed = stale_books = 0
    for row in read_rows(paths[0]):
        msg = row.get("raw", row)
        data = msg.get("data", msg)
        if data.get("coin") != "BTC" or msg.get("channel", "l2Book") != "l2Book":
            raise ValueError("Unexpected instrument or channel in BTC book object")
        levels = data["levels"]
        if len(levels) != 2 or any(not side for side in levels):
            raise ValueError("Invalid L2 sides")
        for i, side in enumerate(levels):
            prices = [float(v["px"]) for v in side]
            if any(
                not math.isfinite(v) or v <= 0
                for v in prices + [float(v["sz"]) for v in side]
            ):
                raise ValueError("Invalid L2 price or size")
            if any(
                (a - b) * (1 if i == 0 else -1) <= 0
                for a, b in itertools.pairwise(prices)
            ):
                raise ValueError("Invalid L2 ordering")
        if float(levels[0][0]["px"]) >= float(levels[1][0]["px"]):
            raise ValueError("Crossed snapshot")
        # A stale snapshot cannot have informed an earlier decision.
        if books and data["time"] <= books[-1]["time"]:
            stale_books += 1
            continue
        books.append({"time": data["time"], "levels": levels})
    for block in read_rows(paths[1]):
        block_times.append(block_ms(block["block_time"]))
        for account, fill in block["events"]:
            if fill["coin"] != "BTC":
                continue
            btc_accounts += 1
            if fill["crossed"] is False:
                continue
            if fill["crossed"] is not True or fill["side"] not in ("A", "B"):
                raise ValueError("Unknown execution flags")
            crossed += 1
            trade = {k: fill[k] for k in ("time", "px", "sz", "side")}
            trade["tid"] = str(fill["tid"])
            if any(
                not math.isfinite(float(trade[k])) or float(trade[k]) <= 0
                for k in ("px", "sz")
            ):
                raise ValueError("Invalid BTC fill")
            if trade != trades.setdefault(trade["tid"], trade):
                raise ValueError("Conflicting BTC taker trade ID")

    def check_clock(times, kind):
        if not times or any(a > b for a, b in itertools.pairwise(times)):
            raise ValueError("Empty or unordered " + kind)
        gap = max((b - a for a, b in itertools.pairwise(times)), default=0)
        return {
            "first": times[0],
            "last": times[-1],
            "rows": len(times),
            "max_gap_ms": gap,
        }

    book_clock = check_clock([b["time"] for b in books], "books")
    block_clock = check_clock(block_times, "venue fill blocks")
    if any(not t - 10000 <= tr["time"] < t + HOUR + 10000 for tr in trades.values()):
        raise ValueError("Unexpected BTC trade timestamp outside file window")
    with (
        tempfile.NamedTemporaryFile(dir=out, delete=False) as tmp,
        gzip.GzipFile(fileobj=tmp, mode="wb", mtime=0, filename="") as zipped,
    ):
        zipped.write(
            json.dumps(
                {
                    "books": books,
                    "trades": sorted(trades.values(), key=lambda r: r["time"]),
                },
                separators=(",", ":"),
                allow_nan=False,
            ).encode()
        )
    target = out / f"{t}.json.gz"
    os.replace(tmp.name, target)
    manifest = {
        "hour": t,
        "sources": [
            {"bucket": b, "key": k, "bytes": p.stat().st_size, "sha256": sha(p)}
            for (b, k), p in zip(sources(t), paths)
        ],
        "books": book_clock,
        "stale_books_skipped": stale_books,
        "venue_blocks": block_clock,
        "btc_account_rows": btc_accounts,
        "crossed_rows": crossed,
        "unique_takers": len(trades),
        "packet_sha256": sha(target),
        "packet_bytes": target.stat().st_size,
        "origin": origin,
    }
    atomic_json(out / f"{t}.manifest.json", manifest)
    return manifest


def run(mode, folder, budget_path):
    import boto3
    from botocore.config import Config

    folder.mkdir(parents=True, exist_ok=True)
    cfg = Config(
        region_name="ap-northeast-1",
        retries={"total_max_attempts": 1},
        connect_timeout=15,
        read_timeout=90,
    )
    account = boto3.client("sts", config=cfg).get_caller_identity()["Account"]
    budget = json.loads(budget_path.read_text())
    if account != budget["account"] or budget["cap_usd"] != 100:
        raise ValueError("Unexpected AWS billing identity or cap")
    prior = budget["prior_archive_charges_usd"]
    if not isinstance(prior, (float, int)) or not math.isfinite(prior) or prior < 0:
        raise ValueError(
            "Prior archive charges must be reconciled before paid archive access"
        )
    if budget.get("remaining_applicable_credits_usd") is None:
        raise ValueError("Check applicable credit balance before paid archive access")
    # One shared budget journal across runs and output folders.
    journal = budget_path.parent / "archive-reservations.jsonl"
    s3 = {b: boto3.client("s3", region_name=v[0], config=cfg) for b, v in RATES.items()}
    with journal.open("a+") as log:
        fcntl.flock(log, fcntl.LOCK_EX)
        log.seek(0)
        used = prior + sum(json.loads(s)["reserved_usd"] for s in log if s.strip())

        def reserve(bucket, key, verb, size):
            nonlocal used
            cost = estimated_cost(bucket, size)
            if used + cost > 100:
                raise ValueError("Archive budget exhausted")
            log.write(
                json.dumps(
                    {
                        "utc": datetime.now(UTC).isoformat(),
                        "account": account,
                        "bucket": bucket,
                        "key": key,
                        "verb": verb,
                        "bytes": size,
                        "reserved_usd": cost,
                    }
                )
                + "\n"
            )
            log.flush()
            os.fsync(log.fileno())
            used += cost

        expected = [(t, *src) for t in range(START, END, HOUR) for src in sources(t)]
        planpath = folder / "s3-plan.json"
        if mode == "plan":
            rows = []
            for t, b, k in expected:
                reserve(b, k, "HEAD", 0)
                r = s3[b].head_object(Bucket=b, Key=k, RequestPayer="requester")
                if r.get("StorageClass", "STANDARD") != "STANDARD":
                    raise ValueError("Review storage-class retrieval cost before GET")
                rows.append(
                    {
                        "hour": t,
                        "bucket": b,
                        "key": k,
                        "bytes": r["ContentLength"],
                        "etag": r["ETag"],
                    }
                )
            projected = sum(estimated_cost(r["bucket"], r["bytes"]) for r in rows)
            atomic_json(
                planpath,
                {"objects": rows, "projected_get_usd": projected, "account": account},
            )
            print(
                {
                    "objects": len(rows),
                    "get_usd": projected,
                    "total_including_prior_and_reservations": used + projected,
                }
            )
            return
        plan = json.loads(planpath.read_text())
        rows = plan["objects"]
        if (
            plan["account"] != account
            or [(r["hour"], r["bucket"], r["key"]) for r in rows] != expected
        ):
            raise ValueError("Invalid plan")
        if any(
            type(r["bytes"]) is not int or r["bytes"] <= 0 or not r["etag"]
            for r in rows
        ):
            raise ValueError("Invalid planned object size or ETag")
        remaining = sum(
            estimated_cost(r["bucket"], r["bytes"])
            for r in rows
            if not (folder / f"{r['hour']}.manifest.json").exists()
        )
        if used + remaining > 100:
            raise ValueError(
                "Planned remaining downloads would exceed total archive budget"
            )
        pending = []
        for i in range(0, len(rows), 2):
            pair = rows[i : i + 2]
            t = pair[0]["hour"]
            done = folder / f"{t}.manifest.json"
            if done.exists():
                m = json.loads(done.read_text())
                if (
                    [(s["bucket"], s["key"], s["bytes"]) for s in m["sources"]]
                    != [(r["bucket"], r["key"], r["bytes"]) for r in pair]
                    or m["origin"] != {"etags": [r["etag"] for r in pair]}
                    or sha(folder / f"{t}.json.gz") != m["packet_sha256"]
                ):
                    raise ValueError("Invalid existing packet")
                continue
            pending.append(pair)

        def collect_hour(pair):
            t = pair[0]["hour"]
            temp = tempfile.mkdtemp(prefix=f"raw-{t}-", dir=folder)
            try:
                paths = []
                for idx, r in enumerate(pair):
                    b, k, n = r["bucket"], r["key"], r["bytes"]
                    p = Path(temp) / str(idx)
                    response = s3[b].get_object(
                        Bucket=b, Key=k, RequestPayer="requester", IfMatch=r["etag"]
                    )
                    with (
                        contextlib.closing(response["Body"]) as body,
                        p.open("wb") as dest,
                    ):
                        for chunk in body.iter_chunks(1024 * 1024):
                            dest.write(chunk)
                    if p.stat().st_size != n:
                        raise ValueError("Incomplete object")
                    paths.append(p)
                normalize(t, paths, folder, {"etags": [r["etag"] for r in pair]})
            except Exception as exc:
                raise RuntimeError(
                    f"Hour {t} failed; raw inputs retained in {temp}"
                ) from exc
            else:
                shutil.rmtree(temp)

        with ThreadPoolExecutor(max_workers=4) as workers:
            for i in range(0, len(pending), 4):
                batch = pending[i : i + 4]
                for pair in batch:
                    for r in pair:
                        reserve(r["bucket"], r["key"], "GET", r["bytes"])
                for _ in workers.map(collect_hour, batch):
                    pass
                if (i + len(batch)) % 24 == 0 or i + len(batch) == len(pending):
                    print(
                        f"Collected {i + len(batch)}/{len(pending)} remaining hours",
                        flush=True,
                    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["plan", "collect"])
    ap.add_argument("folder", type=Path)
    ap.add_argument("budget", type=Path)
    args = ap.parse_args()
    run(args.mode, args.folder, args.budget)
