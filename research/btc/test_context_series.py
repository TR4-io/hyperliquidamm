"""Offline BTC-only asset-context projection, provenance and resume fixtures."""

import gzip
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import lz4.frame
from archive import atomic_json
from bulk_archive import AFTER_WEEK, complete, items, normalize_context, raw_paths

HEADER = "time,coin,funding,mark_px,oracle_px,premium,account_tag\n"
DAY = "2026-01-06"


class ContextSeries(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.folder = Path(tmp.name)

    def source(self, rows, header=HEADER):
        contents = lz4.frame.compress((header + "\n".join(rows) + "\n").encode())
        source = {
            **list(items(AFTER_WEEK))[-1],
            "bytes": len(contents),
            "etag": '"fixture"',
        }
        path, receipt = raw_paths(self.folder, source)
        path.parent.mkdir(exist_ok=True)
        path.write_bytes(contents)
        atomic_json(
            receipt, {"source": source, "sha256": hashlib.sha256(contents).hexdigest()}
        )
        return source

    def test_minute_series_and_hourly_compatibility(self):
        source = self.source(
            [
                f"{DAY}T00:00:00Z,BTC,-0.01,20,21,,unpublished-account",
                f"{DAY}T00:00:05Z,ETH,bad,bad,bad,private,unpublished-account",
                f"{DAY}T00:00:30.500Z,BTC,-0.003,19,19,-0.015,unpublished-account",
                f"{DAY}T00:02:00Z,BTC,-0.005,20,20,0.124,unpublished-account",
                f"{DAY}T01:00:00Z,BTC,-0.01,19,20,,unpublished-account",
            ]
        )
        normalize_context(self.folder, source)
        packet = self.folder / "context-series.json.gz"
        series_path = self.folder / "context-series.manifest.json"
        original = packet.read_bytes()
        series = json.loads(gzip.decompress(original))
        hourly = json.loads(
            gzip.decompress((self.folder / "context-samples.json.gz").read_bytes())
        )
        hourly_manifest = json.loads(
            (self.folder / "context.manifest.json").read_bytes()
        )
        meta = json.loads(series_path.read_bytes())
        self.assertEqual(meta["sources"], hourly_manifest["sources"])
        self.assertEqual(meta["origin"], hourly_manifest["origin"])
        self.assertEqual(meta["packet_sha256"], hashlib.sha256(original).hexdigest())
        self.assertEqual(meta["packet_bytes"], len(original))
        self.assertEqual(meta["rows"], 4)
        start = source["hour"]
        self.assertEqual(
            meta["coverage"],
            {
                "first_ms": start,
                "last_ms": start + 3_600_000,
                "observed_minutes": 3,
                "expected_minutes": 1440,
                "max_gap_ms": 3_480_000,
            },
        )
        self.assertEqual(hourly, [series[0], series[-1]])
        self.assertEqual(
            set(series[0]),
            {"time", "coin", "funding", "mark_px", "oracle_px", "premium"},
        )
        self.assertNotIn(b"unpublished-account", gzip.decompress(original))
        self.assertEqual(series[0]["premium"], "")
        normalize_context(self.folder, source)
        self.assertEqual(packet.read_bytes(), original)
        self.assertTrue(complete(self.folder, [source]))
        packet.write_bytes(b"tampered")
        with self.assertRaisesRegex(ValueError, "Checksum"):
            complete(self.folder, [source])
        # Existing hourly-only packets remain complete and trigger no new transfer.
        packet.unlink()
        series_path.unlink()
        self.assertTrue(complete(self.folder, [source]))

    def test_empty_and_invalid_coverage(self):
        source = self.source([f"{DAY}T03:00:00Z,ETH,0,1,1,,masked"])
        normalize_context(self.folder, source)
        record = json.loads((self.folder / "context-series.manifest.json").read_bytes())
        self.assertEqual(record["rows"], 0)
        self.assertEqual(record["coverage"]["first_ms"], None)
        self.assertEqual(record["coverage"]["observed_minutes"], 0)
        for times in (
            ["2026-01-07T00:00:00Z"],
            [f"{DAY}T00:00:00"],
            [f"{DAY}T00:00:00.0000001Z"],
            [f"{DAY}T00:02:00Z", f"{DAY}T00:01:00Z"],
            [f"{DAY}T00:02:00Z", f"{DAY}T00:02:00Z"],
        ):
            with self.subTest(times=times):
                source = self.source([f"{t},BTC,0,20,20,,sensitive" for t in times])
                with self.assertRaises(ValueError):
                    normalize_context(self.folder, source)

    def test_rejects_wrong_schema_nonfinite_and_unverified_source(self):
        for hourly in [
            f"{DAY}T00:00:00Z,BTC,0,NaN,21,,private",
            f"{DAY}T00:00:00Z,BTC,0,20,,0,private",
        ]:
            with (
                self.subTest(hourly=hourly),
                self.assertRaisesRegex(ValueError, "numeric"),
            ):
                normalize_context(self.folder, self.source([hourly]))
        source = self.source(
            [f"{DAY}T00:00:00Z,BTC,0,20,21"], "time,coin,funding,mark_px,mark_px\n"
        )
        with self.assertRaisesRegex(ValueError, "columns"):
            normalize_context(self.folder, source)
        valid = self.source([f"{DAY}T00:00:00Z,BTC,0,20,21,,masked"])
        bad = {**valid, "key": "asset_ctxs/20260109.csv.lz4"}
        with self.assertRaisesRegex(ValueError, "source"):
            normalize_context(self.folder, bad)
        path, _ = raw_paths(self.folder, valid)
        path.write_bytes(b"x" * valid["bytes"])
        with self.assertRaisesRegex(ValueError, "Damaged"):
            normalize_context(self.folder, valid)


if __name__ == "__main__":
    unittest.main()
