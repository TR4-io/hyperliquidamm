import hashlib
import json
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import lz4.frame
from fee_evidence import FINAL, FIRST, HOUR, audit, scheduled


class HistoricalFees(unittest.TestCase):
    hour = int(datetime(2026, 1, 12, tzinfo=UTC).timestamp() * 1000)

    def fill(self, i, **values):
        return dict(
            {
                "coin": "BTC",
                "px": "100000",
                "sz": "0.001",
                "fee": "0.015000",
                "crossed": False,
                "feeToken": "USDC",
                "time": self.hour + 1000,
                "tid": i,
                "oid": i + 1,
                "dir": "Open Long",
            },
            **values,
        )

    def load(self, fills, corrupt=False):
        with tempfile.TemporaryDirectory() as folder:
            raw = Path(folder) / "fills.lz4"
            contents = (
                json.dumps(
                    {"events": [["private-identity", x] for x in fills]}
                ).encode()
                + b"\n"
            )
            compressed = lz4.frame.compress(contents)
            raw.write_bytes(compressed)
            rec = {
                "source": {
                    "kind": "fills",
                    "bucket": "hl-mainnet-node-data",
                    "hour": self.hour,
                    "key": "node_fills_by_block/hourly/20260112/0.lz4",
                    "bytes": len(compressed),
                },
                "sha256": hashlib.sha256(compressed).hexdigest()
                if not corrupt
                else "incorrect",
            }
            return audit(raw, rec, "weekly")

    def test_roles(self):
        result = self.load(
            [
                self.fill(1),
                self.fill(1),
                self.fill(2, crossed=True, fee="0.055000", builderFee="0.010000"),
                self.fill(3, fee="-0.001"),
                self.fill(4, sz="0.00001"),
                self.fill(5, coin="ETH"),
                self.fill(6, dir="Liquidated Cross Long"),
                self.fill(7, priorityFee="0.001"),
                self.fill(8, time=self.hour - 1),
                self.fill(9, feeToken="OTHER"),
                self.fill(10, fee="0.020"),
                self.fill(11, crossed=True, fee="0.044999"),
            ]
        )
        self.assertEqual(
            result["byRole"]["maker"],
            {
                "analyzed": 3,
                "within_base_tolerance": 1,
                "below_base": 1,
                "excluded_under_10_usdc": 1,
                "above_base": 1,
            },
        )
        self.assertEqual(
            result["byRole"]["taker"],
            {"builder_fee_removed": 1, "analyzed": 2, "within_base_tolerance": 2},
        )
        self.assertEqual(
            result["counts"],
            {
                "btc_account_fill_rows": 11,
                "duplicate": 1,
                "liquidation_rows": 1,
                "other_fee_component": 1,
                "outside_exchange_hour": 1,
                "missing_role_or_usdc_fee": 1,
            },
        )
        self.assertNotIn("private-identity", json.dumps(result))
        self.assertEqual(result["mostFrequentEffectiveBps"]["taker"], {"4.50": 2})

    def test_verification(self):
        with self.assertRaisesRegex(ValueError, "verification"):
            self.load([], corrupt=True)
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            self.load([self.fill(1), self.fill(1, fee="0.03")])

    def test_archive_cleanup_after_audit_failure(self):
        from bulk_archive import normalize_group, raw_paths

        with tempfile.TemporaryDirectory() as temp:
            folder = Path(temp) / "training" / "20260112"
            (folder / ".raw").mkdir(parents=True)
            rows = [
                {"hour": self.hour, "kind": k, "etag": "fixture"}
                for k in ("book", "fills")
            ]
            for row in rows:
                for file in raw_paths(folder, row):
                    file.touch()
            with (
                patch("fast_archive.normalize"),
                patch(
                    "fee_evidence.save", side_effect=ValueError("incomplete")
                ) as save,
                patch("builtins.print"),
            ):
                normalize_group(folder, rows)
            save.assert_called_once()
            self.assertFalse(any((folder / ".raw").iterdir()))

    def test_boundaries(self):
        self.assertTrue(scheduled(self.hour))
        for t in (
            self.hour + 1,
            self.hour + HOUR,
            self.hour + 24 * HOUR,
            FIRST - 1,
            FINAL,
            FINAL + 7 * 24 * HOUR,
        ):
            self.assertFalse(scheduled(t))
        with self.assertRaisesRegex(ValueError, "development hour"):
            audit(Path("/nonexistent"), {"source": {"hour": FINAL}}, "opportunistic")


if __name__ == "__main__":
    unittest.main()
