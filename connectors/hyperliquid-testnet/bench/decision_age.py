"""Compare two concurrent public book captures on the same receiver clock."""
import json
import sys
from bisect import bisect_right
from collections import Counter
from pathlib import Path
from statistics import median


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


if __name__ == "__main__":
    print("Decision-age comparison", json.dumps(compare(*sys.argv[1:])))
