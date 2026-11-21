"""Collect the first training week's *settled* BTC rates from the official info API."""

import hashlib
import json
import math
import sys
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

START = int(datetime(2025, 12, 30, tzinfo=UTC).timestamp() * 1000)
HOUR = 3600000
END = START + 168 * HOUR
URL = "https://api.hyperliquid.xyz/info"


def collect(out):
    out.mkdir(parents=True, exist_ok=True)
    payload = {
        "type": "fundingHistory",
        "coin": "BTC",
        "startTime": START,
        "endTime": END - 1,
    }
    req = urllib.request.Request(
        URL, json.dumps(payload).encode(), {"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        raw = response.read(2_000_000)
    rows = json.loads(raw)
    hours = []
    for r in rows:
        assert r["coin"] == "BTC" and math.isfinite(float(r["fundingRate"]))
        assert START <= r["time"] < END
        hours.append((r["time"] - START) // HOUR)
    if hours != list(range(168)):
        raise ValueError("Missing, duplicate or unordered BTC settlement hour")
    manifest = {
        "source": URL,
        "request": payload,
        "bytes": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "retrieved_utc": datetime.now(UTC).isoformat(),
        "count": len(rows),
        "first_time": rows[0]["time"],
        "last_time": rows[-1]["time"],
        "oracle_coverage": False,
        "mark_coverage": False,
    }
    (out / "fundingHistory.raw.json").write_bytes(raw)
    (out / "fundingHistory.manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n"
    )
    print(json.dumps(manifest))


if __name__ == "__main__":
    collect(Path(sys.argv[1]))
