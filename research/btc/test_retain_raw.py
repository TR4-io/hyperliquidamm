"""Verify retention and resume without contacting S3."""

import contextlib
import gzip
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import lz4.frame
from archive import atomic_json
from botocore.response import StreamingBody
from bulk_archive import AFTER_WEEK, RATES, collect_days, day_dir, items, raw_paths


class Retention(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.folder = day_dir(self.root, AFTER_WEEK)
        self.folder.mkdir(parents=True)
        self.rows = list(items(AFTER_WEEK))
        self.payloads = {}
        for book, fills in (self.rows[:2], self.rows[2:4]):
            t = book["hour"]
            self.payloads[book["key"]] = lz4.frame.compress(
                json.dumps(
                    {
                        "data": {
                            "coin": "BTC",
                            "time": t,
                            "levels": [
                                [{"px": "20", "sz": 1}],
                                [{"px": "21", "sz": 1}],
                            ],
                        }
                    }
                ).encode()
            )
            self.payloads[fills["key"]] = lz4.frame.compress(
                json.dumps({"block_time": t, "events": []}).encode()
            )
        self.payloads[self.rows[-1]["key"]] = lz4.frame.compress(
            b"time,coin,mark_px,oracle_px,funding\n2026-01-06T00:00:00Z,BTC,20,21,0\n"
        )
        self.ledger = Mock(account="offline")
        self.ledger.require.return_value = 1
        self.gets = []

    def plan(self, indices):
        self.measured = []
        unavailable = []
        for i, r in enumerate(self.rows):
            if i in indices:
                n = len(self.payloads[r["key"]])
                self.measured.append({**r, "bytes": n, "etag": '"fixture"'})
            else:
                unavailable.append({**r, "status": "missing"})
        atomic_json(
            self.folder / "plan.json",
            {
                "account": "offline",
                "day": AFTER_WEEK.isoformat(),
                "objects": self.measured,
                "unavailable": unavailable,
                "unresolved": [],
            },
        )

    def get_object(self, Key, IfMatch, **kwargs):
        self.assertEqual(IfMatch, '"fixture"')
        self.gets.append(Key)
        body = self.payloads[Key]
        return {
            "ETag": IfMatch,
            "ContentLength": len(body),
            "Body": StreamingBody(io.BytesIO(body), len(body)),
        }

    def collect(self, keep=False):
        outputs = io.StringIO()
        with contextlib.redirect_stdout(outputs):
            progress = collect_days(
                self.root,
                [AFTER_WEEK],
                self.ledger,
                {k: self for k in RATES},
                1,
                keep_raw=keep,
            )
        preflight = json.loads(outputs.getvalue().splitlines()[0])
        return preflight, progress

    def test_one_transfer_for_new_and_legacy_groups(self):
        self.plan({0, 1})
        self.collect()
        self.assertFalse((self.folder / ".raw" / ".keep-raw").exists())
        self.plan({0, 1, 2, 3, 48})
        preflight, progress = self.collect(keep=True)
        self.assertEqual(preflight["raw_retention"]["compact_only_objects"], 2)
        self.assertEqual(progress["verified_objects"], 3)
        marker = json.loads((self.folder / ".raw" / ".keep-raw").read_bytes())
        self.assertEqual(
            set(marker["compact_only_keys"]), {r["key"] for r in self.rows[:2]}
        )
        for row in self.measured[2:]:
            raw, receipt = raw_paths(self.folder, row)
            self.assertEqual(raw.read_bytes(), self.payloads[row["key"]])
            self.assertEqual(
                json.loads(receipt.read_bytes()),
                {"source": row, "sha256": hashlib.sha256(raw.read_bytes()).hexdigest()},
            )
        self.assertEqual(len(self.gets), 5)
        # Exercise re-normalization with keep_raw omitted after restarting.
        later = self.measured[2]
        (self.folder / f"{later['hour']}.manifest.json").unlink()
        preflight, progress = self.collect()
        self.assertEqual(
            progress,
            {"verified_objects": 0, "verified_bytes": 0, "completed_groups": 1},
        )
        self.assertEqual(preflight["raw_retention"]["verified_existing_objects"], 3)
        self.assertEqual(preflight["raw_retention"]["compact_only_objects"], 2)
        self.assertEqual(len(self.gets), 5)
        self.assertTrue(
            all(
                p.exists() for r in self.measured[2:] for p in raw_paths(self.folder, r)
            )
        )
        self.assertEqual(
            len(
                json.loads(
                    gzip.decompress(
                        (self.folder / f"{later['hour']}.json.gz").read_bytes()
                    )
                )["books"]
            ),
            1,
        )
        # Independent compact digest can repair a receipt without new requests.
        raw, receipt = raw_paths(self.folder, later)
        receipt.unlink()
        preflight, progress = self.collect()
        self.assertEqual(progress["verified_objects"], 0)
        self.assertTrue(receipt.exists())
        self.assertEqual(len(self.gets), 5)
        # A compact-only legacy artifact never authorizes a paid backfill.
        legacy = self.measured[0]
        (self.folder / f"{legacy['hour']}.manifest.json").unlink()
        with self.assertRaisesRegex(ValueError, "explicit recovery required"):
            self.collect(keep=True)
        self.assertEqual(len(self.gets), 5)

    def test_lost_and_tampered_retained_originals_block_before_get(self):
        self.plan({0, 1})
        self.collect(keep=True)
        raw, receipt = raw_paths(self.folder, self.measured[0])
        original = raw.read_bytes()
        raw.write_bytes(b"x" * len(original))
        with self.assertRaisesRegex(ValueError, "Damaged raw"):
            self.collect()
        self.assertEqual(raw.read_bytes(), b"x" * len(original))
        raw.unlink()
        receipt.unlink()
        with self.assertRaisesRegex(ValueError, "retained original is missing"):
            self.collect(keep=True)
        self.assertEqual(len(self.gets), 2)

    def test_unpaired_original_is_retained_then_used_when_mate_appears(self):
        self.plan({0, 3})  # an unpaired book in one hour and fills in the next
        preflight, progress = self.collect(keep=True)
        self.assertEqual(progress["verified_objects"], 2)
        self.assertEqual(progress["completed_groups"], 0)
        self.assertEqual(preflight["raw_retention"]["unpaired_measured_objects"], 2)
        for row in self.measured:
            self.assertTrue(all(p.exists() for p in raw_paths(self.folder, row)))
            self.assertFalse((self.folder / f"{row['hour']}.manifest.json").exists())
            self.assertFalse((self.folder / f"{row['hour']}.json.gz").exists())
        self.assertEqual(len(self.gets), 2)
        preflight, progress = self.collect()
        self.assertEqual(
            preflight["raw_retention"]["unpaired_verified_existing_objects"], 2
        )
        self.assertEqual(progress["verified_objects"], 0)
        self.plan({0, 1, 2, 3})
        preflight, progress = self.collect()
        self.assertEqual(progress["verified_objects"], 2)
        self.assertEqual(progress["completed_groups"], 2)
        self.assertEqual(set(self.gets), {r["key"] for r in self.measured})
        self.assertEqual(len(self.gets), 4)
        # A temporarily unresolved mate in a later plan does not invalidate a
        # packet whose two sources had already been verified.
        self.plan({0, 3})
        _, progress = self.collect()
        self.assertEqual(progress["verified_objects"], 0)
        self.assertEqual(len(self.gets), 4)

    def test_disk_guard_protects_retained_waves(self):
        self.plan({0, 1})
        with (
            patch("shutil.disk_usage", return_value=Mock(free=1500)),
            self.assertRaisesRegex(RuntimeError, "Insufficient free space"),
        ):
            self.collect(keep=True)
        self.assertEqual(self.gets, [])
        self.ledger.reserve.assert_not_called()


if __name__ == "__main__":
    unittest.main()
