"""Offline regression fixtures for publication boundaries, remote verification, and resume."""

import gzip
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


if __name__ == "__main__":
    main()
