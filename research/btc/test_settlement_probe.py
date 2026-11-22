"""Offline fixtures for ledger cash bounds and event identity."""

import json
import unittest
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

import lz4.frame
from bulk_archive import Ledger
from settlement_probe import extract
from settlement_week import BUCKET, WeekBudget

TIME = 1767052800010
RATE = {"coin": "BTC", "time": TIME, "fundingRate": "-0.00001"}
CONTEXT = {"time": TIME, "oraclePx": "100000"}


def delta(user, position, cash):
    return {
        "user": user,
        "coin": "BTC",
        "szi": position,
        "funding_amount": cash,
        "funding_rate": "-0.00001",
    }


def encoded(rows):
    data = {
        "block_time": "2025-12-30T00:00:00.010012000",
        "block_number": 1,
        "events": [{"inner": {"Funding": {"deltas": rows}}}],
    }
    return lz4.frame.compress(json.dumps(data, separators=(",", ":")).encode())


class SettlementEvidenceTests(unittest.TestCase):
    def test_signed_bounds_across_two_positions(self):
        result = extract(
            encoded([delta("a", "3", "3"), delta("b", "-5", "-5")]), RATE, CONTEXT
        )
        self.assertEqual((result["longCount"], result["shortCount"]), (1, 1))
        self.assertLess(Decimal(result["chargePerLongBtcUsdcLower"]), Decimal(-1))
        self.assertGreater(Decimal(result["chargePerLongBtcUsdcUpper"]), Decimal(-1))
        self.assertTrue(result["sampleInEquivalentOracleBounds"])
        self.assertNotIn("user", result)

    def test_rate_disagreement_and_inconsistent_cash(self):
        for rows in (
            [delta("a", "3", "3"), delta("b", "-5", "-6")],
            [delta("a", "3", "3"), {**delta("b", "-5", "-5"), "funding_rate": "0.001"}],
        ):
            with self.assertRaises(ValueError):
                extract(encoded(rows), RATE, CONTEXT)

    def test_cannot_pair_other_hour_or_repeat_account(self):
        rows = [delta("a", "3", "3"), delta("a", "-5", "-5")]
        for r in [RATE, {**RATE, "time": TIME + 1}]:
            with self.assertRaises(ValueError):
                extract(encoded(rows), r, CONTEXT)

    def test_small_cap_and_shared_ledger_include_previous_calls(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "budget.json"
            path.write_text(
                json.dumps(
                    {
                        "cap_usd": 100,
                        "account": "test",
                        "prior_archive_charges_usd": 1,
                        "remaining_applicable_credits_usd": 99,
                    }
                )
            )
            row = {"key": "test", "bucket": BUCKET, "bytes": 100_000_000}
            ledger = Ledger(path, "test")
            with ledger.file:
                ledger.reserve(row, "GET")
                weekly = WeekBudget(ledger, path, {"test"})
                self.assertAlmostEqual(weekly.gross, 0.01140037)
                with self.assertRaisesRegex(ValueError, "cap"):
                    weekly.reserve({**row, "bytes": 1_000_000_000}, "GET")
                self.assertEqual(
                    len(
                        (path.parent / "archive-reservations.jsonl")
                        .read_text()
                        .splitlines()
                    ),
                    1,
                )
                weekly.reserve(row, "HEAD")
                self.assertGreater(ledger.require(0), 1 + 0.01140037)


if __name__ == "__main__":
    unittest.main()
