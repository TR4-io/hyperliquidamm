"""Synthetic fixtures for archive -> first training-week workflow; never retained as venue data."""

import gzip
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

import lz4.frame
from archive import normalize, sha, sources
from funding import END, HOUR, START, URL

HERE = Path(__file__).parent


def dump(path, obj):
    path.write_text(json.dumps(obj))


class PipelineTest(unittest.TestCase):
    def test_raw_hour(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            books, blocks = root / "book", root / "block"
            with lz4.frame.open(books, "wt") as a, lz4.frame.open(blocks, "wt") as b:
                for off in range(0, HOUR, 5000):
                    t = START + off
                    book = {
                        "channel": "l2Book",
                        "data": {
                            "coin": "BTC",
                            "time": t,
                            "levels": [
                                [{"px": "9999", "sz": "1"}],
                                [{"px": "10001", "sz": "1"}],
                            ],
                        },
                    }
                    a.write(json.dumps({"raw": book}) + "\n")
                    fill = {
                        "coin": "BTC",
                        "side": "A",
                        "tid": t,
                        "time": t,
                        "px": "9997",
                        "sz": "0.01",
                        "crossed": True,
                    }
                    events = (
                        [
                            ["maker", dict(fill, crossed=False)],
                            ["taker", fill],
                            ["copy", fill],
                        ]
                        if off == 5000
                        else []
                    )
                    b.write(json.dumps({"block_time": t, "events": events}) + "\n")
            m = normalize(START, [books, blocks], root, "synthetic")
            self.assertEqual(
                [m["btc_account_rows"], m["crossed_rows"], m["unique_takers"]],
                [3, 2, 1],
            )
            self.assertEqual(m["sources"][1]["sha256"], sha(blocks))
            self.assertEqual(
                json.loads(gzip.decompress((root / f"{START}.json.gz").read_bytes()))[
                    "trades"
                ][0]["side"],
                "A",
            )

    def test_seven_day_driver(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rates, prices = [], []
            model = root / "model.json"
            for t in range(START, END, HOUR):
                if t == START + 120 * HOUR:
                    # Train before either replay day or any settlement file exists.
                    subprocess.run(
                        ["node", HERE / "first-week.cjs", "fit", root, model],
                        check=True,
                    )

                books = [
                    {
                        "time": x,
                        "levels": [[{"px": 9999, "sz": 1}], [{"px": 10001, "sz": 1}]],
                    }
                    for x in range(t, t + HOUR, 10000)
                ]
                trades = [
                    {
                        "tid": str(x),
                        "time": x,
                        "px": (9990 if i % 2 == 0 else 10010),
                        "sz": 1,
                        "side": ("A" if i % 2 == 0 else "B"),
                    }
                    for i, x in enumerate(range(t + 1000, t + HOUR, 60000))
                ]
                packet = root / f"{t}.json.gz"
                packet.write_bytes(
                    gzip.compress(
                        json.dumps({"books": books, "trades": trades}).encode()
                    )
                )
                clock = {
                    "rows": len(books),
                    "first": t,
                    "last": t + HOUR - 10000,
                    "max_gap_ms": 10000,
                }
                dump(
                    root / f"{t}.manifest.json",
                    {
                        "hour": t,
                        "books": clock,
                        "venue_blocks": clock,
                        "sources": [
                            {"bucket": b, "key": k, "sha256": "0" * 64, "bytes": 10}
                            for b, k in sources(t)
                        ],
                        "packet_sha256": sha(packet),
                        "packet_bytes": packet.stat().st_size,
                        "unique_takers": 60,
                    },
                )
                rates.append({"time": t + 10, "coin": "BTC", "fundingRate": "0.00001"})
                prices.append(
                    {
                        "time": t + 10,
                        "sourceKey": "fixture",
                        "sourceSha256": "1" * 64,
                        "sourceField": "oraclePx",
                        "oraclePx": 10000,
                        "markPx": 10001,
                    }
                )
            raw = root / "fundingHistory.raw.json"
            dump(raw, rates)
            dump(
                root / "fundingHistory.manifest.json",
                {
                    "source": URL,
                    "sha256": sha(raw),
                    "request": {
                        "type": "fundingHistory",
                        "coin": "BTC",
                        "startTime": START,
                        "endTime": END - 1,
                    },
                },
            )
            dump(root / "settlement-prices.json", prices)
            result = root / "out.json"
            subprocess.run(["node", HERE / "first-week.cjs", root, result], check=True)
            out = json.loads(result.read_text())
            self.assertEqual(out["model"]["groups"]["1/3"]["n"], 7198)
            split_result = root / "split.json"
            subprocess.run(
                ["node", HERE / "first-week.cjs", "replay", root, model, split_result],
                check=True,
            )
            self.assertEqual(out, json.loads(split_result.read_text()))
            self.assertEqual(out["model"], json.loads(model.read_text()))
            changed = root / f"{START}.manifest.json"
            changed.write_text(changed.read_text() + "\n")
            invalid_result = root / "invalid.json"
            attempt = subprocess.run(
                [
                    "node",
                    HERE / "first-week.cjs",
                    "replay",
                    root,
                    model,
                    invalid_result,
                ],
                capture_output=True,
                check=False,
            )
            self.assertNotEqual(attempt.returncode, 0)
            self.assertIn(b"Training manifest changed", attempt.stderr)
            self.assertFalse(invalid_result.exists())
            r = out["results"]["model"]
            self.assertGreater(r["buys"], 0)
            self.assertGreater(r["sells"], 0)
            self.assertEqual(len(r["daily"]), 2)
            self.assertAlmostEqual(sum(d["net"] for d in r["daily"]), r["net"])
            self.assertEqual(out["results"]["cash"]["net"], 0)
            self.assertGreater(r["mean30sAfterMakerBps"], 0)


if __name__ == "__main__":
    unittest.main()
