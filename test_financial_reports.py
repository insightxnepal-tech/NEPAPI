import unittest

from financial_reports import (
    compact_report,
    document_url,
    is_active_equity,
    pick_latest,
    summarize_company_reports,
)


SAMPLE_REPORT = {
    "id": 46727,
    "activeStatus": "A",
    "modifiedDate": "2026-08-06T09:37:02.187",
    "fiscalReport": {
        "quarterMaster": {"quarterName": "Fourth Quarter"},
        "reportTypeMaster": {"reportName": "Quarterly Report"},
        "financialYear": {
            "fyName": "2025-2026",
            "fromYear": "2025-07-17",
            "toYear": "2026-07-16",
            "fyNameNepali": "2082-2083",
        },
        "peValue": 18.86,
        "epsValue": 28.36,
        "paidUpCapital": 27056996729.0,
        "profitAmount": 7905796000.0,
        "netWorthPerShare": 247.28,
    },
    "applicationDocumentDetailsList": [
        {
            "submittedDate": "2026-08-06",
            "filePath": "nabil_user/2026-08-06/Nabil Bank Limited 4th Quarter.pdf",
        }
    ],
}

ANNUAL_REPORT = {
    "id": 1,
    "modifiedDate": "2025-12-01T00:00:00",
    "fiscalReport": {
        "quarterMaster": None,
        "reportTypeMaster": {"reportName": "Annual Report"},
        "financialYear": {"fyName": "2024-2025", "fyNameNepali": "2081-2082"},
        "peValue": 20.1,
        "epsValue": 25.0,
        "paidUpCapital": 1.0,
        "profitAmount": 2.0,
        "netWorthPerShare": 200.0,
    },
    "applicationDocumentDetailsList": [],
}


class FinancialReportHelpersTest(unittest.TestCase):
    def test_document_url_encodes_spaces(self):
        url = document_url("nabil_user/2026-08-06/Nabil Bank Limited 4th Quarter.pdf")
        self.assertIn("fileLocation=", url)
        self.assertIn("Nabil%20Bank", url)

    def test_compact_report_extracts_fundamentals(self):
        compact = compact_report(SAMPLE_REPORT)
        self.assertEqual(compact["reportType"], "Quarterly Report")
        self.assertEqual(compact["quarter"], "Fourth Quarter")
        self.assertEqual(compact["fiscalYearNepali"], "2082-2083")
        self.assertEqual(compact["eps"], 28.36)
        self.assertEqual(compact["pe"], 18.86)
        self.assertEqual(compact["profitAmount"], 7905796000.0)
        self.assertEqual(len(compact["documents"]), 1)
        self.assertTrue(compact["documents"][0]["url"].endswith(".pdf") or "fileLocation=" in compact["documents"][0]["url"])

    def test_compact_report_handles_missing_fiscal_data(self):
        compact = compact_report({"id": 9, "fiscalReport": None})
        self.assertEqual(compact["id"], 9)
        self.assertIsNone(compact["eps"])
        self.assertEqual(compact["documents"], [])

    def test_summarize_company_reports_picks_latest_of_each_type(self):
        older_quarter = dict(SAMPLE_REPORT)
        older_quarter["id"] = 10
        older_quarter["modifiedDate"] = "2025-01-01T00:00:00"
        summary = summarize_company_reports(
            "NABIL",
            [older_quarter, SAMPLE_REPORT, ANNUAL_REPORT],
            company={
                "companyName": "Nabil Bank Limited",
                "securityName": "Nabil Bank Limited",
                "sectorName": "Commercial Banks",
            },
        )
        self.assertEqual(summary["symbol"], "NABIL")
        self.assertEqual(summary["reportCount"], 3)
        self.assertEqual(summary["latestQuarterly"]["id"], 46727)
        self.assertEqual(summary["latestAnnual"]["reportType"], "Annual Report")
        self.assertEqual(summary["reports"][0]["id"], 46727)

    def test_pick_latest_returns_none_when_missing(self):
        self.assertIsNone(pick_latest([compact_report(SAMPLE_REPORT)], "Annual Report"))

    def test_is_active_equity(self):
        self.assertTrue(is_active_equity({"status": "A", "instrumentType": "Equity"}))
        self.assertFalse(is_active_equity({"status": "D", "instrumentType": "Equity"}))
        self.assertFalse(is_active_equity({"status": "A", "instrumentType": "Mutual Funds"}))


class LiveNepseReportsTest(unittest.TestCase):
    def test_nabil_latest_quarterly_report(self):
        from nepse import Nepse

        nepse = Nepse()
        nepse.setTLSVerification(False)
        raw = nepse.getCompanyReports("NABIL")
        self.assertIsInstance(raw, list)
        self.assertGreater(len(raw), 0)
        summary = summarize_company_reports("NABIL", raw)
        latest = summary["latestQuarterly"]
        self.assertIsNotNone(latest)
        self.assertEqual(latest["reportType"], "Quarterly Report")
        self.assertIsInstance(latest["eps"], (int, float))
        self.assertTrue(latest["documents"])
        self.assertIn("fileLocation=", latest["documents"][0]["url"])


if __name__ == "__main__":
    unittest.main()
