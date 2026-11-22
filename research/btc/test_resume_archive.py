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
            self.assertEqual([x.kwargs["max_gets"] for x in get.call_args_list], [4, 4])
            self.assertEqual(
                [x.kwargs["start_gets"] for x in get.call_args_list], [4, 1]
            )
            self.assertEqual([x.args[0] for x in sleep.call_args_list], [8, 10])
            self.assertEqual(
                [c.args for c in ledger.reserve.call_args_list], [(row, "HEAD")] * 2
            )
            ledger.file.close.assert_called_once()

    def test_download_only_revisits_isolated_object_after_other_dates(self):
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            first = app.AFTER_WEEK
            second = first + app.ONE
            for day in (first, second):
                folder = app.day_dir(data / "annual", day)
                folder.mkdir(parents=True)
                (folder / "plan.json").write_text('{"unavailable":[],"unresolved":[]}')
            error = app.ArchiveGetUnavailable(
                {"bucket": "test", "key": "isolated"}, {"status": 503}
            )
            error.isolated = True
            with (
                patch.object(app, "batches", return_value=iter([[first], [second]])),
                patch.object(app, "clients", return_value=(Mock(), {})),
                patch.object(app, "plan_days"),
                patch.object(
                    app, "collect_days", side_effect=[error, {}, {}]
                ) as collect,
                patch.object(app, "fit_prefix") as fit,
                patch.object(app, "validate_if_ready") as validate,
                patch.object(app, "materialize") as funding,
                patch.object(app.time, "sleep") as sleep,
            ):
                self.assertEqual(app.run(data, second, 1, download_only=True), 0)
                self.assertEqual(
                    [call.args[1] for call in collect.call_args_list],
                    [[first], [second], [first]],
                )
                funding.assert_called_once()
                fit.assert_not_called()
                validate.assert_not_called()
                sleep.assert_not_called()

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

    def test_partial_head_recovered_without_repeating_downloads(self):
        import io
        from collections import Counter

        import lz4.frame
        from botocore.response import StreamingBody
        from bulk_archive import items

        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            day = app.AFTER_WEEK
            rows = list(items(day))
            payloads = {}
            for book, fills in (rows[:2], rows[2:4]):
                t = book["hour"]
                payloads[book["key"]] = lz4.frame.compress(
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
                payloads[fills["key"]] = lz4.frame.compress(
                    json.dumps({"block_time": t, "events": []}).encode()
                )
            heads, gets = Counter(), Counter()
            blocked_key = rows[3]["key"]

            class S3:
                def head_object(self, Key, **kwargs):
                    heads[Key] += 1
                    transient = Key == blocked_key and heads[Key] <= 3
                    status = 503 if transient else 404 if Key not in payloads else 200
                    if status != 200:
                        raise ClientError(
                            {
                                "Error": {"Code": str(status)},
                                "ResponseMetadata": {"HTTPStatusCode": status},
                            },
                            "HeadObject",
                        )
                    return {"ContentLength": len(payloads[Key]), "ETag": '"fixture"'}

                def get_object(self, Key, IfMatch, **kwargs):
                    assert IfMatch == '"fixture"'
                    gets[Key] += 1
                    return {
                        "ContentLength": len(payloads[Key]),
                        "ETag": IfMatch,
                        "Body": StreamingBody(
                            io.BytesIO(payloads[Key]), len(payloads[Key])
                        ),
                    }

            folder = app.day_dir(data / "annual", day)
            fitted = []

            def fit(data):
                fitted.append(sum(1 for _ in folder.glob("*.manifest.json")))

            ledger = Mock(account="fixture")
            ledger.require.return_value = 0
            with (
                patch.object(
                    app,
                    "clients",
                    return_value=(ledger, {r["bucket"]: S3() for r in rows}),
                ),
                patch.object(app, "fit_prefix", side_effect=fit),
                patch.object(app, "materialize"),
                patch.object(app.time, "sleep"),
            ):
                self.assertEqual(app.run(data, day, 1), 0)
            self.assertEqual(fitted, [1, 2])
            self.assertEqual(gets, Counter({k: 1 for k in payloads}))
            self.assertEqual(
                heads,
                Counter({r["key"]: 4 if r["key"] == blocked_key else 1 for r in rows}),
            )
            self.assertEqual(
                json.loads((folder / "plan.json").read_bytes())["unresolved"], []
            )
            self.assertEqual(
                json.loads((data / "annual/transfer-status.json").read_bytes())[
                    "unresolvedObjects"
                ],
                0,
            )

    def test_outage_probe_bounds_and_rotates(self):
        from bulk_archive import items

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            day = app.AFTER_WEEK
            rows = list(items(day))
            folder = app.day_dir(root, day)
            folder.mkdir(parents=True)
            initial = {
                "account": "fixture",
                "day": day.isoformat(),
                "objects": [],
                "unavailable": [],
                "unresolved": [{**r, "status": "unresolved"} for r in rows],
            }
            (folder / "plan.json").write_text(json.dumps(initial))
            ledger = Mock(account="fixture")
            client = Mock()
            client.head_object.side_effect = ClientError(
                {"Error": {"Code": "503"}, "ResponseMetadata": {"HTTPStatusCode": 503}},
                "HeadObject",
            )
            s3 = {r["bucket"]: client for r in rows}
            for i in range(2):
                with self.assertRaises(app.ArchiveHeadUnavailable) as failure:
                    app.plan_days(
                        root, [day], ledger, s3, 8, retry_unresolved_only=True
                    )
                self.assertEqual(failure.exception.row["key"], rows[i]["key"])
                self.assertEqual(failure.exception.unresolved_count, 49)
            self.assertEqual(
                [c.kwargs["Key"] for c in client.head_object.call_args_list],
                [rows[0]["key"], rows[1]["key"]],
            )
            self.assertEqual(json.loads((folder / "plan.json").read_bytes()), initial)

    def test_full_outage_preserves_previously_measured_days(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            days = [app.AFTER_WEEK + i * app.ONE for i in range(3)]
            first = app.day_dir(root, days[0])
            first.mkdir(parents=True)
            (first / "plan.json").write_text('{"unresolved":[],"unavailable":[]}')
            failure = app.ArchiveHeadUnavailable(
                days[1], {"bucket": "fixture", "key": "canary"}, 49
            )
            with patch.object(app, "plan_days", side_effect=failure):
                measured, pending, error = app.plan_batch(root, days, Mock(), {}, True)
            self.assertEqual(measured, days[:1])
            self.assertEqual(pending, days[1:])
            self.assertIs(error, failure)

    def test_partial_metadata_deadline_is_not_completion(self):
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            day = app.AFTER_WEEK
            folder = app.day_dir(data / "annual", day)
            folder.mkdir(parents=True)
            unresolved = {
                "bucket": "fixture",
                "key": "books",
                "details": {"status": 503},
            }
            (folder / "plan.json").write_text(
                json.dumps({"unresolved": [unresolved], "unavailable": []})
            )
            with (
                patch.object(app, "clients", return_value=(Mock(), {})),
                patch.object(app, "plan_days"),
                patch.object(app, "collect_days", return_value={}),
                patch.object(app, "fit_prefix") as fit,
                patch.object(app.time, "sleep"),
            ):
                self.assertEqual(app.run(data, day, 1 / 3600), 2)
            status = json.loads((data / "annual/transfer-status.json").read_bytes())
            self.assertEqual(status["state"], "upstream still unavailable")
            self.assertEqual(status["key"], "books")
            fit.assert_not_called()

    def test_batches_cover_once(self):
        days = list(app.batches(app.AFTER_WEEK + 11 * app.ONE))
        self.assertEqual([len(batch) for batch in days], [4, 7, 1])
        self.assertEqual(
            [d for batch in days for d in batch],
            [app.AFTER_WEEK + i * app.ONE for i in range(12)],
        )


if __name__ == "__main__":
    unittest.main()
