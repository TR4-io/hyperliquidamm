"""Offline routing and source checks for the separately authorized pre-study dates."""

import gzip
import json
import tempfile
import unittest
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import lz4.frame
from archive import atomic_json, sha
from bulk_archive import (
    AFTER_WEEK,
    AUXILIARY_START,
    FINAL,
    FIRST,
    LAST,
    VALIDATION,
    acquisition_days,
    complete,
    day_dir,
    items,
    normalize_group,
    partition,
    raw_paths,
)


class AuxiliaryArchive(unittest.TestCase):
    def test_range_keeps_seven_reused_dates_and_original_splits(self):
        one = timedelta(days=1)
        auxiliary = acquisition_days(AUXILIARY_START, FIRST - one)
        self.assertEqual(len(auxiliary), 38)
        self.assertEqual(auxiliary[0], date(2025, 11, 22))
        self.assertEqual(auxiliary[-1], date(2025, 12, 29))
        self.assertEqual(set(map(partition, auxiliary)), {"pre-study"})
        self.assertEqual(acquisition_days(FIRST, AFTER_WEEK - one), [])
        self.assertEqual(
            acquisition_days(FIRST - one, AFTER_WEEK), [FIRST - one, AFTER_WEEK]
        )
        self.assertEqual(
            [
                partition(d)
                for d in (
                    FIRST,
                    AFTER_WEEK,
                    VALIDATION - one,
                    VALIDATION,
                    FINAL - one,
                    FINAL,
                    LAST,
                )
            ],
            [
                "training",
                "training",
                "training",
                "validation",
                "validation",
                "sealed-final",
                "sealed-final",
            ],
        )
        self.assertEqual(sum(len(list(items(day))) for day in auxiliary), 1862)
        self.assertEqual(len(list(items(date(2026, 10, 25)))), 37)
        self.assertEqual(len(list(items(FINAL))), 49)
        for invalid in (AUXILIARY_START - one, LAST + one):
            with self.assertRaises(ValueError):
                partition(invalid)
        with self.assertRaises(ValueError):
            acquisition_days(AUXILIARY_START - one, FIRST)
        with self.assertRaises(ValueError):
            acquisition_days(FIRST, AUXILIARY_START)

    def test_auxiliary_packet_and_canonical_source_manifest(self):
        day = AUXILIARY_START
        start = int(datetime.combine(day, datetime.min.time(), UTC).timestamp() * 1000)
        requests = list(items(day))
        self.assertEqual(requests[0]["key"], "market_data/20251122/0/l2Book/BTC.lz4")
        self.assertEqual(
            requests[1]["key"], "node_fills_by_block/hourly/20251122/0.lz4"
        )
        self.assertEqual(requests[-1]["key"], "asset_ctxs/20251122.csv.lz4")
        levels = [[{"px": "89001", "sz": "0.005"}], [{"px": "89002", "sz": "0.005"}]]
        data = {"coin": "BTC", "levels": levels, "time": start}
        # Both supported official book envelopes; exchange block timestamp as an ISO string.
        book = "\n".join(
            json.dumps(v)
            for v in (
                {"raw": {"channel": "l2Book", "data": data}},
                {"channel": "l2Book", "data": {**data, "time": start + 5000}},
            )
        )
        fill = {
            "coin": "BTC",
            "crossed": True,
            "time": start + 1000,
            "tid": 123,
            "side": "A",
            "px": "89000",
            "sz": "0.002",
        }
        fills = json.dumps(
            {
                "block_time": "2025-11-22T00:00:01Z",
                "events": [["fixture", fill], ["other", dict(fill, crossed=False)]],
            }
        )
        ctx = "time,coin,funding,mark_px,oracle_px\n2025-11-22T00:00:00Z,BTC,0.0001,89001,89001\n"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            folder = day_dir(root, day)
            folder.mkdir(parents=True)
            self.assertEqual(folder.relative_to(root).as_posix(), "pre-study/20251122")
            rows = []
            for request, text in zip(
                (requests[0], requests[1], requests[-1]),
                (book, fills, ctx),
                strict=True,
            ):
                source = lz4.frame.compress(text.encode())
                row = {**request, "bytes": len(source), "etag": '"fixture"'}
                p, receipt = raw_paths(folder, row)
                p.parent.mkdir(exist_ok=True)
                p.write_bytes(source)
                atomic_json(receipt, {"source": row, "sha256": sha(p)})
                rows.append(row)
            for group in (rows[:2], rows[2:]):
                normalize_group(folder, group)
                self.assertTrue(complete(folder, group))
            packet = json.loads(
                gzip.decompress((folder / f"{start}.json.gz").read_bytes())
            )
            self.assertEqual(len(packet["books"]), 2)
            self.assertEqual(len(packet["trades"]), 1)
            manifest = json.loads((folder / f"{start}.manifest.json").read_bytes())
            self.assertEqual(
                [s["key"] for s in manifest["sources"]], [r["key"] for r in rows[:2]]
            )
            self.assertTrue((folder / "context-series.manifest.json").exists())
            self.assertFalse((root / "training").exists())


if __name__ == "__main__":
    unittest.main()
