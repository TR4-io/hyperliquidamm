"""Bulk compression must preserve exact source projection, errors, and provenance."""

import gzip
import json
import tempfile
import unittest
from pathlib import Path

import lz4.frame

import archive
import fast_archive
from funding import START


class BulkNormalizerTest(unittest.TestCase):
    def test_full_depth_and_large_trade_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            src = [folder / "book", folder / "fills"]
            levels = [
                [
                    {"px": str(10000 + sign * (i + 1)), "sz": "0.1", "n": 3}
                    for i in range(20)
                ]
                for sign in (-1, 1)
            ]
            book = {"coin": "BTC", "time": START, "levels": levels}
            tid = 2**100 + 3
            fill = {
                "coin": "BTC",
                "tid": tid,
                "time": START + 2,
                "px": "9980",
                "sz": "0.01",
                "side": "A",
                "crossed": True,
            }
            with lz4.frame.open(src[0], "wt") as f:
                for t in (START, START + 500, START + 400):
                    f.write(json.dumps(dict(book, time=t)) + "\n")
            with lz4.frame.open(src[1], "wt") as f:
                rows = [
                    {"block_time": START, "events": []},
                    {
                        "block_time": START + 2,
                        "events": [
                            ["m", dict(fill, crossed=False)],
                            ["t", fill],
                            ["copy", fill],
                            ["other", dict(fill, coin="ETH")],
                        ],
                    },
                    {"block_time": START + 11002, "events": []},
                ]
                for row in rows:
                    f.write(json.dumps(row) + "\n")

            payloads = []
            manifests = []
            for name, worker in [("standard", archive), ("bulk", fast_archive)]:
                out = folder / name
                out.mkdir()
                m = worker.normalize(START, src, out, "synthetic")
                packet = out / f"{START}.json.gz"
                self.assertEqual(archive.sha(packet), m.pop("packet_sha256"))
                self.assertEqual(packet.stat().st_size, m.pop("packet_bytes"))
                payloads.append(gzip.decompress(packet.read_bytes()))
                manifests.append(m)
            self.assertEqual(payloads[0], payloads[1])
            self.assertEqual(manifests[0], manifests[1])
            self.assertEqual(manifests[0]["stale_books_skipped"], 1)
            self.assertEqual(
                manifests[0]["venue_blocks"]["gaps_gt_10s"],
                [[START + 2, START + 11002]],
            )
            projected = json.loads(payloads[1])
            self.assertEqual(projected["trades"][0]["tid"], str(tid))
            self.assertEqual(projected["books"][0]["levels"], levels)
            with lz4.frame.open(src[1], "wt") as f:
                f.write(
                    json.dumps(
                        {
                            "block_time": START + 2,
                            "events": [
                                ["t", fill],
                                ["conflict", dict(fill, px="9981")],
                            ],
                        }
                    )
                    + "\n"
                )
            with self.assertRaisesRegex(ValueError, "Conflicting BTC taker trade ID"):
                fast_archive.normalize(START, src, folder, "synthetic")


if __name__ == "__main__":
    unittest.main()
