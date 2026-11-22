"""Offline transport and resume checks, including a deliberately unavailable object."""

import gzip
import hashlib
import io
import json
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import lz4.frame
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from bulk_archive import (
    AFTER_WEEK,
    HOUR,
    RATES,
    ArchiveGetUnavailable,
    Ledger,
    collect_days,
    day_dir,
    error_details,
    items,
    plan_days,
    retryable,
)


class Archive(unittest.TestCase):
    def test_bulk(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            budget = root / "budget.json"
            budget.write_text(
                json.dumps(
                    {
                        "account": "test",
                        "cap_usd": 100,
                        "prior_archive_charges_usd": 1,
                        "remaining_applicable_credits_usd": 100,
                    }
                )
            )
            ledger = Ledger(budget, "test")
            self.addCleanup(ledger.file.close)
            day = AFTER_WEEK
            rows = list(items(day))
            t = rows[0]["hour"]
            books = [
                {
                    "raw": {
                        "data": {
                            "coin": "BTC",
                            "time": t,
                            "levels": [
                                [{"px": "10", "sz": "1"}],
                                [{"px": "11", "sz": "1"}],
                            ],
                        }
                    }
                }
            ]
            fills = [{"block_time": t, "events": []}]
            content = {
                rows[0]["key"]: lz4.frame.compress(
                    json.dumps(books[0]).encode() + b"\n"
                ),
                rows[1]["key"]: lz4.frame.compress(
                    json.dumps(fills[0]).encode() + b"\n"
                ),
                rows[-1]["key"]: lz4.frame.compress(
                    b"time,coin,oracle_px,mark_px,funding\n2026-01-06T00:00:00Z,BTC,10,11,0\n"
                ),
            }

            def metadata(key):
                return {
                    "ContentLength": len(content[key]),
                    "ETag": hashlib.md5(content[key]).hexdigest(),
                }

            class S3:
                def head_object(self, Bucket, Key, RequestPayer):
                    if Key == rows[2]["key"]:
                        raise ClientError(
                            {
                                "Error": {"Code": "503", "Message": "busy"},
                                "ResponseMetadata": {"HTTPStatusCode": 503},
                            },
                            "HeadObject",
                        )
                    if Key not in content:
                        raise ClientError(
                            {
                                "Error": {"Code": "404", "Message": "not found"},
                                "ResponseMetadata": {"HTTPStatusCode": 404},
                            },
                            "HeadObject",
                        )
                    return metadata(Key)

                def get_object(self, Bucket, Key, RequestPayer, IfMatch):
                    assert IfMatch == metadata(Key)["ETag"]
                    return {
                        **metadata(Key),
                        "Body": StreamingBody(
                            io.BytesIO(content[Key]), len(content[Key])
                        ),
                    }

            clients = {k: S3() for k in RATES}
            with patch("bulk_archive.time.sleep"):
                plan_days(root, [day], ledger, clients, 4)
            folder = day_dir(root, day)
            plan = json.loads((folder / "plan.json").read_bytes())
            self.assertEqual(
                (
                    len(plan["objects"]),
                    len(plan["unavailable"]),
                    len(plan["unresolved"]),
                ),
                (3, 45, 1),
            )

            class Down:
                def get_object(self, **kwargs):
                    raise ClientError(
                        {
                            "Error": {
                                "Code": "ServiceUnavailable",
                                "Message": "upstream connect error; do-not-echo-token",
                            },
                            "ResponseMetadata": {
                                "HTTPStatusCode": 503,
                                "HTTPHeaders": {
                                    "retry-after": "17",
                                    "server": "envoy",
                                    "authorization": "do-not-echo-token",
                                    "x-envoy-upstream-service-time": "0021",
                                    "x-debug-note": "do-not-echo-token",
                                },
                            },
                        },
                        "GetObject",
                    )

            def get_count():
                return sum(
                    json.loads(line)["verb"] == "GET"
                    for line in (root / "archive-reservations.jsonl")
                    .read_text()
                    .splitlines()
                )

            before_outage = get_count()
            with self.assertRaises(ArchiveGetUnavailable) as failure:
                collect_days(
                    root, [day], ledger, {key: Down() for key in RATES}, 4, max_gets=1
                )
            self.assertEqual(
                failure.exception.progress,
                {"verified_objects": 0, "verified_bytes": 0, "completed_groups": 0},
            )
            self.assertEqual(failure.exception.bucket, rows[0]["bucket"])
            self.assertEqual(failure.exception.key, rows[0]["key"])
            self.assertEqual(failure.exception.retry_after_seconds, 17)
            self.assertTrue(retryable(failure.exception))
            self.assertEqual(
                failure.exception.details["gateway_indications"],
                ["upstream connect error"],
            )
            self.assertEqual(failure.exception.details["servers"], ["envoy"])
            self.assertEqual(failure.exception.details["envoy_upstream_ms"], 21)
            self.assertNotIn("do-not-echo", json.dumps(failure.exception.details))
            self.assertNotIn("do-not-echo", str(failure.exception))
            self.assertEqual(get_count() - before_outage, 1)
            detail = json.loads(
                next((folder / ".raw").glob("*.get-error.json")).read_bytes()
            )
            self.assertEqual(detail["status"], 503)
            self.assertNotIn("do-not-echo", json.dumps(detail))
            with self.assertRaises(ArchiveGetUnavailable) as partial:
                collect_days(
                    root,
                    [day],
                    ledger,
                    {rows[0]["bucket"]: S3(), rows[1]["bucket"]: Down()},
                    3,
                    max_gets=1,
                )
            self.assertEqual(
                partial.exception.progress,
                {
                    "verified_objects": 1,
                    "verified_bytes": len(content[rows[0]["key"]]),
                    "completed_groups": 0,
                },
            )
            self.assertEqual(partial.exception.key, rows[1]["key"])
            # A successful cached book is reused in the recovery attempt.
            done = collect_days(root, [day], ledger, clients, 3, max_gets=1)
            self.assertEqual(
                done,
                {
                    "verified_objects": 2,
                    "verified_bytes": sum(
                        len(content[r["key"]]) for r in (rows[1], rows[-1])
                    ),
                    "completed_groups": 2,
                },
            )
            self.assertEqual(
                json.loads(
                    gzip.decompress((folder / "context-samples.json.gz").read_bytes())
                )[0]["time"],
                t,
            )
            self.assertEqual(
                json.loads(gzip.decompress((folder / f"{t}.json.gz").read_bytes()))[
                    "trades"
                ],
                [],
            )
            self.assertEqual(list((folder / ".raw").iterdir()), [])
            before = (root / "archive-reservations.jsonl").read_bytes()
            empty = collect_days(root, [day], ledger, {k: None for k in RATES}, 1)
            self.assertEqual(
                empty,
                {"verified_objects": 0, "verified_bytes": 0, "completed_groups": 0},
            )
            self.assertEqual(before, (root / "archive-reservations.jsonl").read_bytes())

    def test_gateway_classification(self):
        from datetime import UTC, datetime
        from email.utils import format_datetime

        from botocore.parsers import ResponseParserError

        upstream = ResponseParserError(
            "cannot parse response: upstream connect error or disconnect/reset before headers [secret-url]"
        )
        denied = ClientError(
            {
                "Error": {"Code": "AccessDenied", "Message": "Denied"},
                "ResponseMetadata": {"HTTPStatusCode": 403},
            },
            "GetObject",
        )
        self.assertTrue(retryable(upstream))
        self.assertFalse(retryable(denied))
        self.assertFalse(retryable(ResponseParserError("unrecognized response")))
        self.assertEqual(
            error_details(upstream)["gateway_indications"],
            ["upstream connect error", "disconnect/reset before headers"],
        )
        self.assertNotIn("secret-url", json.dumps(error_details(upstream)))
        denied.response["ResponseMetadata"]["HTTPHeaders"] = {
            "Retry-After": format_datetime(datetime.now(UTC) + timedelta(seconds=61))
        }
        self.assertIn(error_details(denied)["retry_after_seconds"], (60, 61))

    def test_pilot_and_split(self):
        from datetime import date

        from bulk_archive import FINAL

        pilot = list(items(date(2026, 10, 25)))
        self.assertEqual(len(pilot), 37)
        self.assertEqual((pilot[0]["hour"] // HOUR) % 24, 6)
        self.assertEqual(day_dir(Path("a"), FINAL).parent.name, "sealed-final")
        self.assertEqual(
            day_dir(Path("a"), FINAL - timedelta(days=1)).parent.name, "validation"
        )


if __name__ == "__main__":
    unittest.main()
