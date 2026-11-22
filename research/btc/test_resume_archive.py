"""Exercise transport recovery and budget refusal without contacting AWS."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import resume_archive as app
from botocore.exceptions import ClientError
from botocore.parsers import ResponseParserError


class Resume(unittest.TestCase):
    def test_upstream_recovery_then_collect_and_fit(self):
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            day = app.AFTER_WEEK
            client = Mock()
            routes = [ResponseParserError("upstream connect error"), (client, {})]

            def plan(root, days, ledger, s3, workers):
                self.assertEqual(days, [day])
                folder = app.day_dir(root, day)
                folder.mkdir(parents=True)
                (folder / "plan.json").write_text(
                    '{"unavailable": [], "unresolved": []}'
                )

            with (
                patch.object(app, "clients", side_effect=routes),
                patch.object(app, "plan_days", side_effect=plan),
                patch.object(app, "collect_days") as collect,
                patch.object(app, "fit_prefix") as fit,
                patch.object(app, "materialize"),
                patch.object(app.time, "sleep") as sleep,
            ):
                self.assertEqual(app.run(data, day, 1), 0)
            sleep.assert_called_once()
            collect.assert_called_once()
            fit.assert_called_once_with(data)
            client.file.close.assert_called_once()
            status = json.loads((data / "annual/transfer-status.json").read_bytes())
            self.assertEqual(status["state"], "finished available batch pass")
            self.assertIn("Waiting", status["validation"])

    def test_resume_failed_object_without_replanning(self):
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            day = app.AFTER_WEEK
            row = {
                "bucket": "hl-mainnet-node-data",
                "key": "node_fills_by_block/hourly/20260106/11.lz4",
            }
            ledger = Mock()
            bucket = Mock()
            bucket.head_object.side_effect = [
                ClientError(
                    {
                        "Error": {"Code": "503"},
                        "ResponseMetadata": {"HTTPStatusCode": 503},
                    },
                    "HeadObject",
                ),
                {},
            ]
            transient_get = app.ArchiveGetUnavailable(
                row, {"status": 503, "error": "ClientError", "retry_after_seconds": 8}
            )
            transient_get.progress["verified_objects"] = 2
            folder = app.day_dir(data / "annual", day)
            folder.mkdir(parents=True)
            (folder / "plan.json").write_text('{"unavailable": [], "unresolved": []}')
            with (
                patch.object(
                    app, "clients", return_value=(ledger, {row["bucket"]: bucket})
                ) as clients,
                patch.object(app, "plan_days") as plan,
                patch.object(
                    app, "collect_days", side_effect=[transient_get, {}]
                ) as get,
                patch.object(app, "fit_prefix"),
                patch.object(app, "materialize"),
                patch.object(app.time, "sleep") as sleep,
            ):
                self.assertEqual(app.run(data, day, 1), 0)
            clients.assert_called_once()
            plan.assert_called_once()
            self.assertEqual([x.kwargs["max_gets"] for x in get.call_args_list], [4, 1])
            self.assertEqual([x.args[0] for x in sleep.call_args_list], [8, 10])
            self.assertEqual(
                [c.args for c in ledger.reserve.call_args_list], [(row, "HEAD")] * 2
            )
            ledger.file.close.assert_called_once()

    def test_budget_and_identity_failures_stop(self):
        with tempfile.TemporaryDirectory() as tmp:
            with (
                patch.object(
                    app, "clients", side_effect=ValueError("Unexpected account")
                ),
                patch.object(app.time, "sleep") as sleep,
                self.assertRaises(ValueError),
            ):
                app.run(Path(tmp), app.AFTER_WEEK, 1)
            sleep.assert_not_called()
            status = json.loads(
                (Path(tmp) / "annual/transfer-status.json").read_bytes()
            )
            self.assertEqual(status["state"], "stopped")
        self.assertFalse(app.transient(ValueError("Gross budget exhausted")))

    def test_batches_cover_once(self):
        days = list(app.batches(app.AFTER_WEEK + 11 * app.ONE))
        self.assertEqual([len(batch) for batch in days], [4, 7, 1])
        self.assertEqual(
            [d for batch in days for d in batch],
            [app.AFTER_WEEK + i * app.ONE for i in range(12)],
        )


if __name__ == "__main__":
    unittest.main()
