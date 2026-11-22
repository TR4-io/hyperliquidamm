"""Offline regression fixtures for publication boundaries, remote verification, and resume."""

import gzip
import itertools
import json
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main
from unittest.mock import patch

import publish_development as pub


class PublicationTests(TestCase):
    def test_date_and_schema_allowlist(self):
        self.assertEqual(len(pub.days(date(2026, 5, 1), date(2026, 5, 31))), 31)
        for start, end in [
            ("2026-01-06", "2026-02-06"),
            ("2026-08-26", "2026-08-27"),
            ("2026-10-25", "2026-10-26"),
            ("2026-11-01", "2026-11-02"),
        ]:
            with self.assertRaises(ValueError):
                pub.days(date.fromisoformat(start), date.fromisoformat(end))
        t = 1767744000000
        row = {"tid": "19", "time": t, "side": "A", "px": "90500", "sz": "0.01"}
        obj = {"books": [], "trades": [row]}
        clock = {"first": t, "last": t, "rows": 0, "max_gap_ms": 0}
        sources = []
        for bucket, key in [
            ("hyperliquid-archive", "market_data/20260107/0/l2Book/BTC.lz4"),
            ("hl-mainnet-node-data", "node_fills_by_block/hourly/20260107/0.lz4"),
        ]:
            sources.append(
                {"bucket": bucket, "key": key, "sha256": "f" * 64, "bytes": 20}
            )
        meta = {
            "hour": t,
            "sources": sources,
            "origin": {"etags": ['"' + "a" * 32 + '"'] * 2},
            "books": clock,
            "venue_blocks": clock,
            "unique_takers": 1,
            "crossed_rows": 1,
            "btc_account_rows": 2,
            "stale_books_skipped": 0,
        }

        def verify():
            packet = gzip.compress(json.dumps(obj).encode(), mtime=0)
            meta.update(packet_bytes=len(packet), packet_sha256=pub.digest(packet))
            pub.verify_pair(packet, json.dumps(meta).encode(), "20260107", 0)

        verify()
        row["address"] = "private"
        with self.assertRaises(ValueError):
            verify()
        del row["address"]
        row["time"] = pub.FINAL_MS
        with self.assertRaises(ValueError):
            verify()
        row["time"] = t
        meta["billing"] = "private"
        with self.assertRaises(ValueError):
            verify()

    def test_partially_uploaded_draft_resumes_and_rejects_content_changes(self):
        release = {}
        assets = {}
        writes = []
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            day = out / "sample.tar"
            day.write_bytes(b"training sample")
            sums = {day.name: pub.file_digest(day)}
            checksum = out / "SHA256SUMS"
            checksum.write_text(sums[day.name] + "  sample.tar\n")
            tag = "btc-training-fixture"

            def api(endpoint, body=None, paginate=False):
                if body:
                    release.update(**body, id=1, html_url="fixture")
                    return release
                if endpoint.startswith("releases?"):
                    return [release] if release else []
                if "/assets?" in endpoint:
                    return list(assets.values())
                return {**release, "assets": list(assets.values())}

            interrupted = [True]

            def run(args, check):
                self.assertTrue(check)
                if "upload" in args:
                    p = Path(args[4])
                    if p.name == "SHA256SUMS" and interrupted[0]:
                        interrupted[0] = False
                        raise OSError("simulated disconnect")
                    writes.append(p.name)
                    assets[p.name] = {
                        "name": p.name,
                        "state": "uploaded",
                        "size": p.stat().st_size,
                        "digest": "sha256:" + pub.file_digest(p),
                    }
                elif "edit" in args:
                    release["draft"] = False

            with (
                patch.object(pub, "api", api),
                patch.object(pub.subprocess, "run", run),
            ):
                call = lambda: pub.publish(
                    tag, sums, out, "training", [date(2026, 1, 7)]
                )
                with self.assertRaises(OSError):
                    call()
                self.assertTrue(release["draft"])
                call()
                self.assertFalse(release["draft"])
                call()
                self.assertEqual(writes, ["sample.tar", "SHA256SUMS"])
                assets["sample.tar"]["digest"] = "corrupt"
                with self.assertRaisesRegex(ValueError, "digest"):
                    call()

    def test_release_notes_record_sample_scope(self):
        class Captured(Exception):
            pass

        expected = {"name": "", "body": ""}

        def stop_after_draft(endpoint, body=None, paginate=False):
            if body is None:
                return []
            expected.update(body)
            raise Captured()

        with TemporaryDirectory() as temp, patch.object(pub, "api", stop_after_draft):
            out = Path(temp)
            (out / "SHA256SUMS").write_text("")
            for series in (False, True):
                with self.assertRaises(Captured):
                    pub.publish(
                        "fixture",
                        {},
                        out,
                        "training",
                        [date(2026, 1, 7)],
                        has_series=series,
                    )
                self.assertEqual(
                    "24 sampled hourly context rows" in expected["body"], not series
                )
                self.assertEqual("report minute coverage" in expected["body"], series)


class FullDayContextTests(TestCase):
    stamp = "20260107"
    start = 1767744000000

    def setUp(self):
        td = TemporaryDirectory()
        self.addCleanup(td.cleanup)
        self.root = Path(td.name)
        self.base = self.root / "annual" / "training" / self.stamp
        self.base.mkdir(parents=True)
        self.ctx = {
            "hour": self.start,
            "sources": [
                {
                    "bucket": "hyperliquid-archive",
                    "key": f"asset_ctxs/{self.stamp}.csv.lz4",
                    "bytes": 100,
                    "sha256": "c" * 64,
                }
            ],
            "origin": {"etags": ['"' + "a" * 32 + '"']},
        }
        self.sparse = [
            {
                "time": self.start + t,
                "coin": "BTC",
                "funding": "-0.0001",
                "oracle_px": "92300",
                "mark_px": "92300.1",
                "prev_day_px": "",
            }
            for t in (0, 800, 120000)
        ]

    def payload(self, rows, extra):
        packet = gzip.compress(json.dumps(rows).encode(), mtime=0)
        meta = {
            **self.ctx,
            "rows": len(rows),
            "packet_bytes": len(packet),
            "packet_sha256": pub.digest(packet),
            **extra,
        }
        return packet, json.dumps(meta).encode()

    def series(self, rows, **changes):
        times = [r["time"] for r in rows]
        coverage = {
            "first_ms": times[0] if times else None,
            "last_ms": times[-1] if times else None,
            "expected_minutes": 1440,
            "observed_minutes": len({t // 60000 for t in times}),
            "max_gap_ms": max((b - a for a, b in itertools.pairwise(times)), default=0),
        }
        packet, meta = self.payload(rows, {"coverage": coverage, **changes})
        (self.base / "context-series.json.gz").write_bytes(packet)
        (self.base / "context-series.manifest.json").write_bytes(meta)

    def verify(self, samples):
        packet, manifest = self.payload(samples, {})
        return pub.optional_series(self.base, self.stamp, packet, manifest)

    def test_sparse_and_empty_series(self):
        self.series(self.sparse)
        self.assertEqual(
            [name for name, _ in self.verify(self.sparse[:1])],
            ["context-series.json.gz", "context-series.manifest.json"],
        )
        self.series([])
        self.assertEqual(len(self.verify([])), 2)
        self.series(self.sparse)
        with self.assertRaisesRegex(ValueError, "subset"):
            self.verify([])
        with self.assertRaisesRegex(ValueError, "subset"):
            self.verify([{**self.sparse[0], "mark_px": "9000"}])

    def test_manifest_source_and_orphans(self):
        self.series(
            self.sparse, sources=[{**self.ctx["sources"][0], "sha256": "d" * 64}]
        )
        with self.assertRaisesRegex(ValueError, "source mismatch"):
            self.verify(self.sparse[:1])
        self.series(self.sparse, origin={"etags": ['"' + "e" * 32 + '"']})
        with self.assertRaisesRegex(ValueError, "source mismatch"):
            self.verify(self.sparse[:1])
        self.series(self.sparse, coverage={"observed_minutes": 3})
        with self.assertRaisesRegex(ValueError, "coverage"):
            self.verify(self.sparse[:1])
        self.series(self.sparse)
        (self.base / "context-series.json.gz").unlink()
        with self.assertRaisesRegex(ValueError, "Orphan"):
            self.verify(self.sparse[:1])
        self.series(self.sparse)
        (self.base / "context-series.manifest.json").unlink()
        with self.assertRaisesRegex(ValueError, "Orphan"):
            self.verify(self.sparse[:1])

    def test_series_gzip_metadata_is_not_published(self):
        self.series(self.sparse)
        path = self.base / "context-series.json.gz"
        packet = path.read_bytes()
        meta = self.base / "context-series.manifest.json"
        manifest = json.loads(meta.read_bytes())
        for malformed in (
            packet + gzip.compress(b"", mtime=0),
            packet[:3] + b"\x08" + packet[4:10] + b"private\x00" + packet[10:],
        ):
            path.write_bytes(malformed)
            manifest.update(
                packet_sha256=pub.digest(malformed), packet_bytes=len(malformed)
            )
            meta.write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, "gzip"):
                self.verify(self.sparse[:1])

    def test_schema_and_utc_bounds(self):
        for changes in (
            {"address": "private"},
            {"coin": "ETH"},
            {"time": self.start - 1},
            {"time": self.start + 24 * pub.HOUR},
            {"time": self.start},
            {"funding": "NaN"},
            {"oracle_px": "private"},
            {"mark_px": 100},
            {"oracle_px": ""},
            {"time": True},
        ):
            with self.subTest(changes=changes):
                mutated = [self.sparse[0], {**self.sparse[1], **changes}]
                self.series(mutated)
                with self.assertRaises(ValueError):
                    self.verify(mutated[:1])
        self.series([self.sparse[0], {**self.sparse[1], "premium": "0.0"}])
        with self.assertRaisesRegex(ValueError, "schema"):
            self.verify(self.sparse[:1])
        self.series(self.sparse, private_data="private")
        with self.assertRaisesRegex(ValueError, "manifest"):
            self.verify(self.sparse[:1])

    def test_pack_members_and_order(self):
        import tarfile

        day = date.fromisoformat(self.stamp)
        for h in range(24):
            t = self.start + h * pub.HOUR
            packet = gzip.compress(
                json.dumps(
                    {
                        "books": [
                            {
                                "time": t,
                                "levels": [
                                    [{"px": "100", "sz": "1"}],
                                    [{"px": "101", "sz": "1"}],
                                ],
                            }
                        ],
                        "trades": [],
                    }
                ).encode(),
                mtime=0,
            )
            clock = {"first": t, "last": t, "rows": 1, "max_gap_ms": 0}
            sources = [
                {
                    "bucket": "hyperliquid-archive",
                    "key": f"market_data/{self.stamp}/{h}/l2Book/BTC.lz4",
                    "bytes": 10,
                    "sha256": "e" * 64,
                },
                {
                    "bucket": "hl-mainnet-node-data",
                    "key": f"node_fills_by_block/hourly/{self.stamp}/{h}.lz4",
                    "bytes": 10,
                    "sha256": "d" * 64,
                },
            ]
            meta = {
                "hour": t,
                "sources": sources,
                "origin": {"etags": ['"' + "a" * 32 + '"'] * 2},
                "packet_bytes": len(packet),
                "packet_sha256": pub.digest(packet),
                "books": clock,
                "venue_blocks": clock,
                "unique_takers": 0,
                "crossed_rows": 0,
                "stale_books_skipped": 0,
                "btc_account_rows": 0,
            }
            (self.base / f"{t}.json.gz").write_bytes(packet)
            (self.base / f"{t}.manifest.json").write_bytes(json.dumps(meta).encode())
        hourly = [
            {
                "time": self.start + h * pub.HOUR,
                "coin": "BTC",
                **{k: "1.0" for k in sorted(pub.CONTEXT_FIELDS)},
            }
            for h in range(24)
        ]
        packet, meta = self.payload(hourly, {})
        (self.base / "context-samples.json.gz").write_bytes(packet)
        (self.base / "context.manifest.json").write_bytes(meta)
        (self.base / "other.json").write_text("private")
        out = self.root / "out"
        out.mkdir()
        legacy, _ = pub.pack_day(self.root, out, day)
        with tarfile.open(legacy) as tar:
            oldnames = tar.getnames()
            self.assertEqual(len(oldnames), 51)
        self.series(hourly)
        out2 = self.root / "out2"
        out2.mkdir()
        asset, digest = pub.pack_day(self.root, out2, day)
        with tarfile.open(asset) as tar:
            names = tar.getnames()
            self.assertEqual(names[:-3], oldnames[:-1])
            self.assertEqual(
                [Path(n).name for n in names[-3:]],
                [
                    "context-series.json.gz",
                    "context-series.manifest.json",
                    "SHA256SUMS",
                ],
            )
            sums = tar.extractfile(names[-1]).read().decode().splitlines()
            self.assertEqual(
                sums,
                [f"{pub.digest(tar.extractfile(n).read())}  {n}" for n in names[:-1]],
            )
        self.assertEqual(pub.pack_day(self.root, out2, day)[1], digest)
        with self.assertRaisesRegex(ValueError, "Published inputs have changed"):
            pub.pack_day(self.root, out, day)


if __name__ == "__main__":
    main()
