#!/usr/bin/env python3
"""Tests for send_floorsheet_telegram.py"""

import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import send_floorsheet_telegram as sender


SAMPLE_ROWS = [
    {
        "contractId": "1",
        "stockSymbol": "NABIL",
        "contractQuantity": "100",
        "contractRate": "500",
        "contractAmount": "50000",
        "businessDate": "2026-08-14",
    },
    {
        "contractId": "2",
        "stockSymbol": "NICA",
        "contractQuantity": "10",
        "contractRate": "800",
        "contractAmount": "8000",
        "businessDate": "2026-08-14",
    },
]


def write_csv(path: Path, rows) -> None:
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


class FindLatestTests(unittest.TestCase):
    def test_picks_newest_dated_file_and_ignores_generic_copy(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_csv(root / "floorsheet_2026-08-13.csv", SAMPLE_ROWS)
            write_csv(root / "floorsheet_2026-08-14.csv", SAMPLE_ROWS)
            write_csv(root / "floorsheet.csv", SAMPLE_ROWS)
            write_csv(root / "floorsheet_dividend_2026-08-14.csv", SAMPLE_ROWS)
            latest = sender.find_latest_floorsheet(str(root))
            self.assertEqual(latest.name, "floorsheet_2026-08-14.csv")

    def test_returns_none_when_no_dated_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_csv(root / "floorsheet.csv", SAMPLE_ROWS)
            self.assertIsNone(sender.find_latest_floorsheet(str(root)))


class SummaryTests(unittest.TestCase):
    def test_summarize_and_format_message(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "floorsheet_2026-08-14.csv"
            write_csv(path, SAMPLE_ROWS)
            summary = sender.summarize_csv(path)
            self.assertEqual(summary["trades"], 2)
            self.assertEqual(summary["stocks"], 2)
            self.assertEqual(summary["shares"], 110.0)
            self.assertEqual(summary["turnover"], 58000.0)
            self.assertEqual(summary["business_dates"], ["2026-08-14"])
            msg = sender.format_summary_message(summary, path.name)
            self.assertIn("NEPSE Floorsheet — 2026-08-14", msg)
            self.assertIn("`2`", msg)
            self.assertIn(path.name, msg)


class SendTests(unittest.TestCase):
    def test_dry_run_does_not_call_telegram(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "floorsheet_2026-08-14.csv"
            write_csv(path, SAMPLE_ROWS)
            with patch.object(sender, "send_message") as mock_msg, patch.object(
                sender, "send_document"
            ) as mock_doc:
                rc = sender.send_floorsheet(path, dry_run=True)
            self.assertEqual(rc, 0)
            mock_msg.assert_not_called()
            mock_doc.assert_not_called()

    def test_missing_file_returns_error(self):
        rc = sender.send_floorsheet(Path("/tmp/does-not-exist-floorsheet.csv"))
        self.assertEqual(rc, 1)

    def test_cli_uses_explicit_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "floorsheet_2026-08-14.csv"
            write_csv(path, SAMPLE_ROWS)
            rc = sender.main(["--csv", str(path), "--dry-run"])
            self.assertEqual(rc, 0)

    def test_send_document_posts_multipart(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "floorsheet_2026-08-14.csv"
            write_csv(path, SAMPLE_ROWS)

            class FakeResponse:
                def read(self):
                    return json.dumps({"ok": True, "result": {"document": {"file_name": path.name}}}).encode()

                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    return False

            captured = {}

            def fake_urlopen(req, timeout=0):
                captured["url"] = req.full_url
                captured["timeout"] = timeout
                captured["body"] = req.data
                captured["content_type"] = req.headers.get("Content-type") or req.headers.get("Content-Type")
                return FakeResponse()

            with patch("urllib.request.urlopen", side_effect=fake_urlopen):
                resp = sender.send_document("123", path, caption="test caption")

            self.assertTrue(resp["ok"])
            self.assertIn("sendDocument", captured["url"])
            self.assertEqual(captured["timeout"], sender.SEND_TIMEOUT_SEC)
            self.assertIn(b"test caption", captured["body"])
            self.assertIn(b"floorsheet_2026-08-14.csv", captured["body"])
            self.assertIn(b"NABIL", captured["body"])


if __name__ == "__main__":
    unittest.main()
