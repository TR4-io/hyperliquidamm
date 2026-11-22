"""Exercise isolation and gradual transfer recovery on fake, fully priced S3 objects."""

import io
import json
import tempfile
import threading
import time
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import Mock

import lz4.frame
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from bulk_archive import AFTER_WEEK, ArchiveGetUnavailable, collect_days, day_dir, items


class Source:
    def __init__(self, root):
        self.rows = list(items(AFTER_WEEK))
        self.payloads = {}
        self.gets = Counter()
        self.heads = Counter()
        self.get_keys = []
        self.failed_gets = set()
        self.head_down = False
        self.concurrent = 0
        self.peaks = []
        self.lock = threading.Lock()
        self.ledger = Mock(account="fixture")
        self.ledger.require.return_value = 1
        selected = self.rows[:20]
        for book, fills in zip(selected[::2], selected[1::2]):
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
        self.measured = [{**r, **self.metadata(r["key"])} for r in selected]
        folder = day_dir(root, AFTER_WEEK)
        folder.mkdir(parents=True)
        (folder / "plan.json").write_text(
            json.dumps(
                {
                    "account": "fixture",
                    "day": AFTER_WEEK.isoformat(),
                    "objects": self.measured,
                    "unavailable": self.rows[20:],
                    "unresolved": [],
                }
            )
        )
        self.root = root

    def metadata(self, key):
        return {"bytes": len(self.payloads[key]), "etag": '"fixture"'}

    def busy(self, operation):
        raise ClientError(
            {
                "Error": {"Code": "503", "Message": "temporarily down"},
                "ResponseMetadata": {"HTTPStatusCode": 503},
            },
            operation,
        )

    def head_object(self, Key, **kwargs):
        self.heads[Key] += 1
        if self.head_down:
            self.busy("HeadObject")
        return {"ContentLength": len(self.payloads[Key]), "ETag": '"fixture"'}

    def get_object(self, Key, IfMatch, **kwargs):
        assert IfMatch == '"fixture"'
        with self.lock:
            self.concurrent += 1
            self.peaks.append(self.concurrent)
            self.get_keys.append(Key)
            self.gets[Key] += 1
        try:
            time.sleep(0.008)
            if Key in self.failed_gets:
                self.busy("GetObject")
            data = self.payloads[Key]
            return {
                "ContentLength": len(data),
                "ETag": IfMatch,
                "Body": StreamingBody(io.BytesIO(data), len(data)),
            }
        finally:
            with self.lock:
                self.concurrent -= 1

    def run(self, start_gets=None, max_gets=4, workers=3):
        return collect_days(
            self.root,
            [AFTER_WEEK],
            self.ledger,
            {r["bucket"]: self for r in self.rows},
            workers,
            max_gets=max_gets,
            start_gets=start_gets,
        )


class Transfer(unittest.TestCase):
    def test_isolated_fill_finishes_nine_other_pairs(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Source(Path(tmp))
            blocked = source.rows[1]["key"]
            alternate = source.rows[3]["key"]
            source.failed_gets.add(blocked)
            with self.assertRaises(ArchiveGetUnavailable) as caught:
                source.run(start_gets=1)
            failure = caught.exception
            self.assertTrue(failure.isolated)
            self.assertEqual(failure.deferred_count, 1)
            self.assertEqual(failure.key, blocked)
            self.assertEqual(failure.progress["completed_groups"], 9)
            self.assertEqual(failure.progress["verified_objects"], 19)
            self.assertEqual(source.heads, Counter({alternate: 1}))
            self.assertEqual(
                source.get_keys[:4],
                [source.rows[0]["key"], blocked, alternate, source.rows[2]["key"]],
            )
            self.assertEqual(
                source.gets, Counter({r["key"]: 1 for r in source.measured})
            )
            self.assertEqual(
                max(source.peaks[:10]), 1
            )  # startup, failed fill, 4 full healthier groups
            self.assertEqual(max(source.peaks), 4)
            source.failed_gets.clear()
            progress = source.run(start_gets=1)
            self.assertEqual(progress["verified_objects"], 1)
            self.assertEqual(source.gets[blocked], 2)
            self.assertEqual(sum(source.gets.values()), 21)

    def test_pending_groups_resume_after_midwave_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Source(Path(tmp))
            blocked = source.rows[5]["key"]
            source.failed_gets.add(blocked)
            with self.assertRaises(ArchiveGetUnavailable) as caught:
                source.run(max_gets=1, workers=1)
            self.assertTrue(caught.exception.isolated)
            self.assertEqual(caught.exception.progress["completed_groups"], 9)
            self.assertEqual(
                source.gets, Counter({r["key"]: 1 for r in source.measured})
            )

    def test_flat_outage_only_one_get(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Source(Path(tmp))
            source.failed_gets = set(source.payloads)
            source.head_down = True
            with self.assertRaises(ArchiveGetUnavailable) as caught:
                source.run()
            self.assertFalse(caught.exception.isolated)
            self.assertEqual(source.gets, Counter({source.rows[0]["key"]: 1}))
            self.assertEqual(source.heads, Counter({source.rows[2]["key"]: 1}))
            self.assertEqual(
                [c.args[1] for c in source.ledger.reserve.call_args_list],
                ["GET", "HEAD"],
            )

    def test_healthy_head_with_failing_get_stops_after_serial_probe(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Source(Path(tmp))
            source.failed_gets = set(source.payloads)
            with self.assertRaises(ArchiveGetUnavailable) as caught:
                source.run()
            self.assertFalse(caught.exception.isolated)
            self.assertEqual(sum(source.gets.values()), 2)
            self.assertEqual(sum(source.heads.values()), 1)
            self.assertEqual(max(source.peaks), 1)

    def test_explicit_serial_limit_stays_one(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Source(Path(tmp))
            self.assertEqual(
                source.run(start_gets=1, max_gets=1)["completed_groups"], 10
            )
            self.assertEqual(max(source.peaks), 1)


if __name__ == "__main__":
    unittest.main()
