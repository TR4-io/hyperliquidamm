"""Acquisition-only tests for sealed storage, resumability and market coverage gates."""

import io
import json
import unittest
from datetime import UTC, date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import lz4.frame
import study_settlements as ss
from bulk_archive import Ledger


class ArchiveFixture:
    def __init__(self, start):
        self.heads = self.gets = 0
        self.store = {}
        for n in range(24):
            t = start + n * ss.HOUR
            fmt = datetime.fromtimestamp(t / 1000, UTC).replace(tzinfo=None).isoformat()
            rows = [
                {
                    "user": str(i),
                    "coin": "BTC",
                    "szi": str(i - 5),
                    "funding_rate": "0.0001",
                    "funding_amount": str((5 - i) * 10),
                }
                for i in range(11)
            ]
            block = {
                "block_time": fmt,
                "block_number": 123 + n,
                "events": [{"inner": {"Funding": {"deltas": rows}}}],
            }
            self.store[ss.source(t)["key"]] = lz4.frame.compress(
                json.dumps(block, separators=(",", ":")).encode()
            )

    def head_object(self, Key, **kwargs):
        self.heads += 1
        return {"ETag": "fixture", "ContentLength": len(self.store[Key])}

    def get_object(self, Key, Range, **kwargs):
        self.gets += 1
        lo, hi = map(int, Range.removeprefix("bytes=").split("-"))
        b = self.store[Key]
        return {
            "Body": io.BytesIO(b[lo : hi + 1]),
            "ETag": "fixture",
            "ContentRange": f"bytes {lo}-{hi}/{len(b)}",
        }


class FundingAcquisitionTests(unittest.TestCase):
    def test_reuse_without_network_and_isolate_final_dir(self):
        self._verify_isolated_day(date(2026, 10, 26), "sealed-final")

    def test_prestudy_fetch_uses_separate_partition(self):
        self._verify_isolated_day(date(2025, 11, 22), "pre-study")

    def _verify_isolated_day(self, day, partition):
        start = ss.timestamp(day)
        rates = [
            {"time": start + i * ss.HOUR, "coin": "BTC", "fundingRate": "0.0001"}
            for i in range(24)
        ]
        contexts = {r["time"]: {"time": r["time"], "oraclePx": "100000"} for r in rates}
        api = ArchiveFixture(start)
        with TemporaryDirectory() as tmp:
            data = Path(tmp)
            budget = data / "budget.json"
            budget.write_text(
                json.dumps(
                    {
                        "account": "test",
                        "cap_usd": 100,
                        "remaining_applicable_credits_usd": 100,
                        "prior_archive_charges_usd": 0.5,
                    }
                )
            )
            ledger = Ledger(budget, "test")
            with (
                ledger.file,
                patch.object(ss, "compact_day_complete", return_value=True),
                patch.object(
                    ss,
                    "inputs",
                    return_value=(
                        rates,
                        contexts,
                        {"dailyRatesSha256": "rates", "contextSha256": "context"},
                    ),
                ),
            ):
                stats = ss.collect_settlement_days(
                    data, [day], ledger, {ss.BUCKET: api}
                )
                self.assertEqual(stats["verifiedHours"], 24)
                self.assertEqual((api.gets, api.heads), (24, 24))
                path = (
                    data
                    / "annual"
                    / partition
                    / day.strftime("%Y%m%d")
                    / "settlement-cash.json"
                )
                self.assertTrue(path.is_file())
                self.assertFalse((data / "annual/training").exists())
                self.assertFalse(
                    list((data / "private-settlement-prefixes").rglob("*.lz4"))
                )
                self.assertNotIn('"user"', path.read_text())
                ss.collect_settlement_days(data, [day], ledger, {})
                self.assertEqual((api.gets, api.heads), (24, 24))
                changed = json.loads(path.read_bytes())
                changed["events"][0]["blockNumber"] = -1
                path.write_text(json.dumps(changed))
                with self.assertRaisesRegex(ValueError, "differs"):
                    ss.collect_settlement_days(data, [day], ledger, {})

    def test_incomplete_day_skips_aws(self):
        with TemporaryDirectory() as tmp:
            stats = ss.collect_settlement_days(Path(tmp), [date(2026, 4, 1)], None, {})
            self.assertEqual(stats["waitingForInputsDays"], 1)


if __name__ == "__main__":
    unittest.main()
