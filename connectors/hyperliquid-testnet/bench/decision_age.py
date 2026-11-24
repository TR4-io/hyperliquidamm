"""Compare books that the client had published at shared 100 ms decision times."""
import asyncio
import json
import math
import sys
import time
from bisect import bisect_right
from collections import Counter
from decimal import Decimal
from pathlib import Path
from statistics import median


def clock_ms():
    return time.time_ns() // 1_000_000


def available(row):
    return row.get("observedMs", row.get("quoteReadyMs", row["responseReceivedMs"]))


def read_capture(rows):
    times = [available(row) for row in rows]
    if not times or times != sorted(times):
        raise ValueError("Need frames on a nondecreasing receiver clock")
    return rows, times


def percentiles(values):
    ordered = sorted(values)
    if not ordered:
        return {}
    def quantile(q):
        return ordered[math.ceil(q * len(ordered)) - 1]
    return {"n": len(ordered), "median": median(ordered), "p95": quantile(.95),
            "p99": quantile(.99), "max": ordered[-1], "min": ordered[0]}


def touch(row):
    return tuple(None if row[side] is None else Decimal(row[side]) for side in ("bid", "ask"))


def price_changes(rows, start, end):
    changes = {}
    previous = None
    for row in rows:
        prices = touch(row)
        if previous is not None and prices != previous and None not in prices and start <= available(row) < end:
            changes.setdefault((row["exchangeTimeMs"], prices), available(row))
        previous = prices
    return changes


def report(captures, start, end):
    if end - start < 100:
        raise ValueError("Streams have insufficient overlap")
    summaries = {}
    first = next(iter(captures))
    ticks = range(start, end, 100)
    tick_quotes = {}
    for name, (rows, times) in captures.items():
        if start < times[0]:
            raise ValueError("Observation window starts before a stream is ready")
        quotes = [rows[bisect_right(times, tick) - 1] for tick in ticks]
        tick_quotes[name] = quotes
        usable = [tick - row["exchangeTimeMs"] for tick, row in zip(ticks, quotes) if None not in touch(row)]
        seen = [r for r in rows if start <= available(r) < end]
        summaries[name] = {
            "decisionAgeMs": percentiles(usable), "missingTwoSidedTicks": len(ticks) - len(usable),
            "overOneSecondTicks": sum(age > 1000 for age in usable),
            "negativeAgeTicks": sum(age < 0 for age in usable),
            "receiptAgeMs": percentiles([r["snapshotAgeAtReceiptMs"] for r in seen]),
            "frameToQuoteUs": percentiles([r["frameToQuoteUs"] for r in seen if "frameToQuoteUs" in r]),
            "publishedBySource": dict(Counter(r["source"] for r in seen)),
            "publishedReadRttMs": percentiles([r["bookRequestRttMs"] for r in seen if "bookRequestRttMs" in r]),
        }
        counts = [r["bookRequestsSent"] for r in rows if "bookRequestsSent" in r and available(r) < end]
        if counts:
            summaries[name]["requestsSentThroughLastPublication"] = counts[-1]

    baseline_by_time = {r["exchangeTimeMs"]: touch(r) for r in captures[first][0] if start <= available(r) < end}
    base_changes = price_changes(captures[first][0], start, end)
    for name in list(captures)[1:]:
        comparable = [(a, b) for a, b in zip(tick_quotes[first], tick_quotes[name])
                      if None not in touch(a) and None not in touch(b)]
        if not comparable:
            raise ValueError("No simultaneous two-sided quotes")
        differences = [b["exchangeTimeMs"] - a["exchangeTimeMs"] for a, b in comparable]
        other_changes = price_changes(captures[name][0], start, end)
        matched = base_changes.keys() & other_changes.keys()
        candidate_by_time = {r["exchangeTimeMs"]: touch(r) for r in captures[name][0] if start <= available(r) < end}
        common = candidate_by_time.keys() & baseline_by_time.keys()
        summaries[name]["vsBaseline"] = {
            "sharedExchangeTimestamps": len(common),
            "differentTopAtSharedTimestamp": sum(candidate_by_time[t] != baseline_by_time[t] for t in common),
            "comparableTicks": len(comparable), "fresher": sum(d > 0 for d in differences),
            "tied": differences.count(0), "older": sum(d < 0 for d in differences),
            "differentPrices": sum(touch(a) != touch(b) for a, b in comparable),
            "baselinePriceChanges": len(base_changes), "candidatePriceChanges": len(other_changes),
            "matchedPriceChangeLeadMs": percentiles([base_changes[k] - other_changes[k] for k in matched]),
        }
    return {"utcStartMs": start, "utcEndExclusiveMs": end, "ticks": len(ticks), "modes": summaries}


# Preserve the two-file report for existing capture consumers.
def load(path):
    rows = [json.loads(line) for line in Path(path).read_text().splitlines()]
    times = [row["responseReceivedMs"] for row in rows]
    if len(rows) < 2 or times != sorted(times):
        raise ValueError("Need multiple frames on a nondecreasing receipt clock")
    return rows, times


def summarize(values):
    return {"medianMs": median(values), "maxMs": max(values), "minMs": min(values)}


def compare(l2, top):
    captures = [load(l2), load(top)]
    start = max(times[0] for _, times in captures)
    end = min(times[-1] for _, times in captures)
    ticks = range(start, end + 1, 100)
    if len(ticks) < 2:
        raise ValueError("Streams have insufficient overlap")
    ages = [[], []]
    missing = [0, 0]
    for tick in ticks:
        quotes = [rows[bisect_right(times, tick) - 1] for rows, times in captures]
        for i, quote in enumerate(quotes):
            missing[i] += int(quote["bid"] is None or quote["ask"] is None)
        if all(q["bid"] is not None and q["ask"] is not None for q in quotes):
            for samples, quote in zip(ages, quotes):
                samples.append(tick - quote["exchangeTimeMs"])
    if not ages[0]:
        raise ValueError("No simultaneous two-sided quotes")
    summary = {}
    for i, (name, (rows, _)) in enumerate(zip(["l2", "combined"], captures)):
        summary[name] = dict(summarize(ages[i]), missingTwoSidedTicks=missing[i],
                             publishedBySource=dict(Counter(r["source"] for r in rows)))
    summary.update(overlapMs=end - start, comparableTicks=len(ages[0]), totalTicks=len(ticks),
                   combinedFresher=sum(b < a for a, b in zip(*ages)),
                   sameTimestamp=sum(b == a for a, b in zip(*ages)),
                   l2Fresher=sum(b > a for a, b in zip(*ages)))
    return summary


async def record(process, ready, rows):
    try:
        while line := await process.stdout.readline():
            seen = clock_ms()  # After publication of a complete line on the consumer's clock.
            row = json.loads(line)
            row["observedMs"] = seen
            rows.append(row)
            if not ready.done():
                ready.set_result(None)
        raise RuntimeError(f"Stream exited early with status {await process.wait()}")
    except Exception as error:
        if not ready.done():
            ready.set_exception(error)
        raise


async def benchmark(binary, seconds, directory):
    directory.mkdir(parents=True, exist_ok=True)
    modes = {"bbo": ["--bbo"], "incremental": ["--incremental"], "fast": ["--fast"]}
    processes, monitors, ready, series = [], [], [], {}
    try:
        for name, flags in modes.items():
            process = await asyncio.create_subprocess_exec(binary, "stream", *flags, stdout=asyncio.subprocess.PIPE)
            processes.append(process)
            signal = asyncio.get_running_loop().create_future()
            ready.append(signal)
            series[name] = []
            monitors.append(asyncio.create_task(record(process, signal, series[name])))
        await asyncio.wait_for(asyncio.gather(*ready), 40)
        start = clock_ms()
        window = asyncio.create_task(asyncio.sleep(seconds))
        try:
            done, _ = await asyncio.wait([window, *monitors], return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
            for process in processes:
                if process.returncode is not None:
                    raise RuntimeError("Stream ended before the common cutoff")
        finally:
            window.cancel()
        end = start + seconds * 1000
        if abs(clock_ms() - end) > 250:
            raise RuntimeError("Receiver clock or scheduler moved during capture")
        summary = report({name: read_capture(rows) for name, rows in series.items()}, start, end)
        (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print("Decision-age comparison", json.dumps(summary))
    finally:
        for process in processes:
            if process.returncode is None:
                process.terminate()
        await asyncio.gather(*(p.wait() for p in processes))
        await asyncio.gather(*monitors, return_exceptions=True)
        for name, rows in series.items():
            (directory / (name + ".jsonl")).write_text("".join(json.dumps(r) + "\n" for r in rows))


if __name__ == "__main__":
    if len(sys.argv) == 5 and sys.argv[1] == "capture":
        asyncio.run(benchmark(sys.argv[2], 90, Path(sys.argv[3]) / sys.argv[4]))
    else:
        print("Decision-age comparison", json.dumps(compare(*sys.argv[1:])))
