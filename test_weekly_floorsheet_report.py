#!/usr/bin/env python3
"""Unit tests for weekly floorsheet report helpers (no live NEPSE calls)."""

import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

import weekly_floorsheet_report as wfr


class CompactReportTest(unittest.TestCase):
    def test_document_url_and_compact(self):
        raw = {
            "id": 1,
            "modifiedDate": "2026-09-05T10:00:00",
            "fiscalReport": {
                "peValue": 12.5,
                "epsValue": 8.1,
                "profitAmount": 1_000_000,
                "netWorthPerShare": 150,
                "paidUpCapital": 10_000_000,
                "quarterMaster": {"quarterName": "Fourth Quarter"},
                "reportTypeMaster": {"reportName": "Quarterly Report"},
                "financialYear": {"fyName": "2025/2026", "fyNameNepali": "2082/83"},
            },
            "applicationDocumentDetailsList": [
                {
                    "submittedDate": "2026-09-05",
                    "filePath": "nabil_user/2026-09-05/Nabil Bank Limited 4th Quarter.pdf",
                }
            ],
        }
        compact = wfr.compact_report(raw)
        self.assertEqual(compact["reportType"], "Quarterly Report")
        self.assertEqual(compact["eps"], 8.1)
        self.assertTrue(compact["documents"][0]["url"].startswith(wfr.DOCUMENT_BASE_URL))
        self.assertIn("Nabil", compact["documents"][0]["fileName"])


class FloorsheetDedupeTest(unittest.TestCase):
    def test_unique_days_collapses_identical_copies(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            csv_a = (
                "contractId,stockSymbol,buyerMemberId,sellerMemberId,"
                "contractQuantity,contractRate,contractAmount,businessDate,"
                "tradeBookId,stockId,buyerBrokerName,sellerBrokerName,tradeTime,securityName\n"
                "1,NABIL,1,2,10,500,5000,2026-09-01,1,1,BuyCo,SellCo,10:00,Nabil\n"
            )
            (root / "floorsheet_2026-09-01.csv").write_text(csv_a)
            (root / "floorsheet_2026-09-02.csv").write_text(csv_a)  # identical copy
            csv_b = csv_a.replace("2026-09-01", "2026-09-03").replace(",1,", ",2,", 1)
            (root / "floorsheet_2026-09-03.csv").write_text(csv_b)

            with mock.patch("weekly_floorsheet_report.glob.glob", side_effect=lambda p: sorted(str(x) for x in root.glob("floorsheet*.csv"))):
                days = wfr.unique_floorsheet_days()
            self.assertEqual(len(days), 2)
            dates = sorted(d[0] for d in days)
            self.assertEqual(dates, ["2026-09-01", "2026-09-03"])


class FilingsWindowTest(unittest.TestCase):
    def test_filings_in_window(self):
        reports = [
            {
                "symbol": "NABIL",
                "reports": [
                    {
                        "reportType": "Quarterly Report",
                        "quarter": "Fourth Quarter",
                        "fiscalYearNepali": "2082/83",
                        "modifiedDate": "2026-09-05T12:00:00",
                        "eps": 10,
                        "pe": 15,
                        "profitAmount": 100,
                        "documents": [{"fileName": "q.pdf", "url": "https://example/q.pdf"}],
                    },
                    {
                        "reportType": "Annual Report",
                        "modifiedDate": "2026-08-01T12:00:00",
                        "documents": [],
                    },
                ],
            }
        ]
        hits = wfr.filings_in_window(reports, "2026-09-01", "2026-09-07")
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0]["symbol"], "NABIL")


if __name__ == "__main__":
    unittest.main()
