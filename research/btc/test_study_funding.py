"""Regression checks for structural funding coverage and UTC joins."""

import json
import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import study_funding as sf


def rate(start, index):
    return {
        "coin": "BTC",
        "time": start + index * sf.HOUR + 126,
        "fundingRate": "-0.00003",
    }


def response(payload):
    n = (payload["endTime"] + 1 - payload["startTime"]) // sf.HOUR
    return json.dumps([rate(payload["startTime"], i) for i in range(n)]).encode()


class FundingTests(unittest.TestCase):
    def test_reject_duplicate_and_missing_hours(self):
        start = sf.timestamp(date(2026, 1, 1))
        rows = [rate(start, i) for i in range(3)]
        for broken in (rows[:2], rows[:1] + rows[:1] + rows[2:]):
            with self.assertRaises(ValueError):
                sf.validate(json.dumps(broken).encode(), start, start + 3 * sf.HOUR)

    def test_join_exact_rate_with_context_proxy(self):
        start = sf.timestamp(date(2026, 1, 1))
        rates = [rate(start, i) for i in range(2)]
        s = {
            "coin": "BTC",
            "time": start,
            "mark_px": "125",
            "oracle_px": "120",
            "funding": "0.00001",
        }
        source = {"bucket": "fixture", "key": "asset_ctxs/fixture", "sha256": "1" * 64}
        result = sf.join_samples(rates, [s], source)
        self.assertEqual(len(result), 1)
        self.assertEqual(
            (result[0]["time"], result[0]["sampleTime"]), (start + 126, start)
        )
        self.assertFalse(result[0]["snapshotFundingMatchesSettledRate"])
        self.assertEqual(result[0]["oraclePx"], "120")
        with self.assertRaises(ValueError):
            sf.join_samples(rates, [s, s], source)

    def test_resume_short_last_page_and_check_integrity(self):
        with TemporaryDirectory() as tmp:
            folder = Path(tmp)
            splits = {"training": (date(2026, 1, 1), date(2026, 1, 10))}
            with (
                patch.object(sf, "SPLITS", splits),
                patch.object(sf, "request", side_effect=response) as fetch,
            ):
                sf.archive(folder, date(2026, 1, 9))
                sf.archive(folder, date(2026, 1, 10))
                sf.archive(folder, date(2026, 1, 10))
                self.assertEqual(fetch.call_count, 3)
                summary = sf.materialize(folder, folder / "daily", None)
                self.assertEqual(summary["training"]["rateDays"], 9)
                bad = folder / "training/2026-01-01.raw.json"
                bad.write_bytes(b"[]")
                with self.assertRaisesRegex(ValueError, "Corrupt archived page"):
                    sf.archive(folder, date(2026, 1, 10))


if __name__ == "__main__":
    unittest.main()
