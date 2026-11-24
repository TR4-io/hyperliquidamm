import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from decision_age import compare, read_capture, report


def row(observed, exchange, bid="100"):
    return {"observedMs": observed, "responseReceivedMs": observed, "exchangeTimeMs": exchange,
            "snapshotAgeAtReceiptMs": observed - exchange, "bid": bid, "ask": "102", "source": "l2"}


class DecisionAgeTest(unittest.TestCase):
    def test_late_publication_missing_side_and_quiet_end(self):
        baseline = [row(1000, 900), row(1251, 1200, "101")]
        candidate = [row(1000, 900), row(1200, 1150), row(1250, 1200, "101"), row(1450, 1400, None)]
        caps = {"base": read_capture(baseline), "candidate": read_capture(candidate)}
        result = report(caps, 1000, 1600)
        base = result["modes"]["base"]
        other = result["modes"]["candidate"]
        self.assertEqual((result["ticks"], base["decisionAgeMs"]["max"]), (6, 300))
        self.assertEqual((other["decisionAgeMs"]["n"], other["missingTwoSidedTicks"]), (5, 1))
        pair = other["vsBaseline"]
        self.assertEqual((pair["fresher"], pair["tied"], pair["older"], pair["differentPrices"]), (1, 4, 0, 0))
        self.assertEqual(pair["matchedPriceChangeLeadMs"]["median"], 1)

    def test_two_file_comparison_preserves_receipt_clock_and_output(self):
        rows = [row(1000, 900), row(1250, 1200)]
        rows[0]["observedMs"] = 1100
        with TemporaryDirectory() as folder:
            path = Path(folder, "capture.jsonl")
            path.write_text("".join(json.dumps(r) + "\n" for r in rows))
            result = compare(path, path)
        self.assertEqual(result["l2"]["medianMs"], 200)
        self.assertEqual((result["comparableTicks"], result["sameTimestamp"]), (3, 3))


if __name__ == "__main__":
    unittest.main()
