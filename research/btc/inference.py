"""Prespecified evaluation statistic. Takes consecutive daily P&L including zero days."""

import math
import random
from datetime import date, timedelta

SEED = 20261121
REPLICATES = 20000


def summarize(rows):
    dates = [date.fromisoformat(r["day"]) for r in rows]
    if not dates or dates != [dates[0] + timedelta(days=i) for i in range(len(dates))]:
        raise ValueError("Missing or unordered UTC day")
    xs = [float(r["net"]) for r in rows]
    if not all(map(math.isfinite, xs)):
        raise ValueError("Invalid daily net P&L")
    n = len(xs)
    if n < 14:
        raise ValueError("Insufficient days for 14-day sensitivity")

    def lower(block):
        # Percentile convention: linearly interpolated empirical 5th percentile.
        sums = [sum(xs[(i + j) % n] for j in range(block)) for i in range(n)]
        rem = n % block
        tail = [sum(xs[(i + j) % n] for j in range(rem)) for i in range(n)]
        rng = random.Random(SEED)
        means = []
        for _ in range(REPLICATES):
            total = sum(sums[rng.randrange(n)] for _ in range(n // block))
            if rem:
                total += tail[rng.randrange(n)]
            means.append(total / n)
        means.sort()
        k = 0.05 * (REPLICATES - 1)
        i = int(k)
        return means[i] + (k - i) * (means[i + 1] - means[i])

    return {
        "n": n,
        "total": sum(xs),
        "mean": sum(xs) / n,
        "seed": SEED,
        "replicates": REPLICATES,
        "primary_lower_usd_per_day": lower(7),
        "sensitivity_lower_usd_per_day": {k: lower(k) for k in (3, 14)},
    }


if __name__ == "__main__":
    import sys

    if sys.argv[1:] != ["self-test"]:
        raise SystemExit("Only the inference fixture is exposed until study freeze")
    fixture = [
        {"day": (date(2020, 1, 1) + timedelta(days=i)).isoformat(), "net": 0}
        for i in range(65)
    ]
    result = summarize(fixture)
    assert result["total"] == result["primary_lower_usd_per_day"] == 0
    fixture[0]["net"] = 65
    assert summarize(fixture)["mean"] == 1
    try:
        summarize(fixture[0:2] + fixture[3:])
    except ValueError:
        pass
    else:
        raise AssertionError("Missing day was accepted")
    print("Circular bootstrap self-test passed")
