#!/usr/bin/env python3
"""
Build a printable NEPSE financial report (HTML + PDF) from listed-stock filings.

Reads a `/LatestFinancialReports` snapshot (or fetches one live) and renders a
paginated report with market totals, sector aggregates, leader boards, and a
full appendix of every listed equity.

Examples:
  python build_financial_report.py --fetch
  python build_financial_report.py --input latest_listed_financial_reports.json
"""

from __future__ import annotations

import argparse
import asyncio
import html
import json
import shutil
import subprocess
import tempfile
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

CHROME_CANDIDATES = [
    "google-chrome",
    "chromium",
    "chromium-browser",
    "google-chrome-stable",
]

# NEPSE occasionally publishes net worth per share in absolute rupees rather
# than per-share, which produces values in the billions.
MAX_PLAUSIBLE_NET_WORTH_PER_SHARE = 5000


def number(value: Any) -> Optional[float]:
    return value if isinstance(value, (int, float)) else None


def fmt(value: Optional[float], digits: int = 2) -> str:
    value = number(value)
    return "–" if value is None else f"{value:,.{digits}f}"


def fmt_billions(value: Optional[float]) -> str:
    value = number(value)
    return "–" if value is None else f"{value / 1e9:,.2f}"


def fmt_millions(value: Optional[float]) -> str:
    value = number(value)
    return "–" if value is None else f"{value / 1e6:,.1f}"


def quarterly(row: Dict[str, Any]) -> Dict[str, Any]:
    return row.get("latestQuarterly") or {}


def annual(row: Dict[str, Any]) -> Dict[str, Any]:
    return row.get("latestAnnual") or {}


def period(report: Dict[str, Any]) -> str:
    quarter = (report.get("quarter") or "").replace(" Quarter", "")
    quarter_short = {
        "First": "Q1",
        "Second": "Q2",
        "Third": "Q3",
        "Fourth": "Q4",
    }.get(quarter, quarter or "Annual")
    return f"{quarter_short} {report.get('fiscalYearNepali') or '–'}"


def filed_on(report: Dict[str, Any]) -> str:
    return (report.get("modifiedDate") or "")[:10] or "–"


def net_worth(report: Dict[str, Any]) -> str:
    value = number(report.get("netWorthPerShare"))
    if value is None or not 0 < value < MAX_PLAUSIBLE_NET_WORTH_PER_SHARE:
        return "n/a"
    return fmt(value)


def rank_by(
    rows: List[Dict[str, Any]],
    key: str,
    reverse: bool = True,
    limit: int = 10,
    predicate=None,
) -> List[Dict[str, Any]]:
    scored = []
    for row in rows:
        value = number(quarterly(row).get(key))
        if value is None:
            continue
        if predicate and not predicate(value):
            continue
        scored.append((value, row))
    scored.sort(key=lambda item: item[0], reverse=reverse)
    return [row for _, row in scored[:limit]]


def sector_aggregates(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row.get("sector") or "Unclassified"].append(row)

    aggregates = []
    for sector, items in grouped.items():
        eps = [number(quarterly(i).get("eps")) for i in items]
        eps = [value for value in eps if value is not None]
        pe = [number(quarterly(i).get("pe")) for i in items]
        pe = [value for value in pe if value is not None and value > 0]
        profit = sum(number(quarterly(i).get("profitAmount")) or 0 for i in items)
        aggregates.append(
            {
                "sector": sector,
                "count": len(items),
                "q4": sum(1 for i in items if quarterly(i).get("quarter") == "Fourth Quarter"),
                "avgEps": sum(eps) / len(eps) if eps else None,
                "avgPe": sum(pe) / len(pe) if pe else None,
                "profit": profit,
            }
        )
    aggregates.sort(key=lambda item: item["profit"], reverse=True)
    return aggregates


def table(headers: List[str], rows: List[List[str]], numeric_from: int = 0) -> str:
    head = "".join(
        f"<th class=\"{'num' if index >= numeric_from and numeric_from else ''}\">{html.escape(header)}</th>"
        for index, header in enumerate(headers)
    )
    body = []
    for row in rows:
        cells = "".join(
            f"<td class=\"{'num' if index >= numeric_from and numeric_from else ''}\">{cell}</td>"
            for index, cell in enumerate(row)
        )
        body.append(f"<tr>{cells}</tr>")
    return (
        "<table><thead><tr>"
        + head
        + "</tr></thead><tbody>"
        + "".join(body)
        + "</tbody></table>"
    )


def esc(value: Any) -> str:
    return html.escape(str(value if value is not None else "–"))


STYLES = """
@page { size: A4; margin: 14mm 12mm 16mm 12mm; }
* { box-sizing: border-box; }
body {
  font-family: "DejaVu Sans", "Helvetica Neue", Arial, sans-serif;
  color: #16202c; margin: 0; font-size: 9.2pt; line-height: 1.42;
}
h1 { font-size: 23pt; margin: 0 0 4px; letter-spacing: -0.4px; }
h2 {
  font-size: 12.5pt; margin: 20px 0 8px; padding-bottom: 5px;
  border-bottom: 2px solid #123a63; color: #123a63;
  page-break-after: avoid;
}
h3 { font-size: 10.2pt; margin: 14px 0 6px; color: #123a63; page-break-after: avoid; }
p { margin: 6px 0; }
.cover { border-bottom: 3px solid #123a63; padding-bottom: 12px; margin-bottom: 16px; }
.subtitle { color: #56657a; font-size: 10.5pt; margin-top: 2px; }
.meta { margin-top: 10px; color: #56657a; font-size: 8.6pt; }
.meta strong { color: #16202c; }
.cards { display: flex; gap: 8px; margin: 14px 0 4px; }
.card {
  flex: 1; border: 1px solid #d7dee8; border-radius: 6px;
  padding: 9px 10px; background: #f7f9fc;
}
.card .value { font-size: 16pt; font-weight: 700; color: #123a63; }
.card .label { font-size: 7.8pt; text-transform: uppercase; letter-spacing: 0.4px; color: #56657a; }
table { width: 100%; border-collapse: collapse; margin: 6px 0 4px; }
th, td { padding: 4px 6px; border-bottom: 1px solid #e3e8f0; text-align: left; }
th {
  background: #123a63; color: #fff; font-size: 7.9pt;
  text-transform: uppercase; letter-spacing: 0.3px; border-bottom: none;
}
td { font-size: 8.5pt; }
td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
tbody tr:nth-child(even) { background: #f7f9fc; }
.note { font-size: 8pt; color: #56657a; margin-top: 4px; }
.negative { color: #b4232a; font-weight: 600; }
.page-break { page-break-before: always; }
thead { display: table-header-group; }
tr { page-break-inside: avoid; }
.disclaimer {
  margin-top: 18px; padding: 9px 11px; border-left: 3px solid #c8ac4a;
  background: #fdf8e8; font-size: 8.1pt; color: #4a4636;
}
"""


def render_html(payload: Dict[str, Any], generated_at: str) -> str:
    rows = payload["results"]
    filed = [row for row in rows if quarterly(row)]
    missing = [row for row in rows if not quarterly(row)]
    q4 = [row for row in filed if quarterly(row).get("quarter") == "Fourth Quarter"]
    q3 = [row for row in filed if quarterly(row).get("quarter") == "Third Quarter"]
    total_profit = sum(number(quarterly(row).get("profitAmount")) or 0 for row in filed)
    fiscal_years = Counter(quarterly(row).get("fiscalYearNepali") for row in filed)
    latest_fy = fiscal_years.most_common(1)[0][0] if fiscal_years else "–"

    recent = sorted(filed, key=lambda row: quarterly(row).get("modifiedDate") or "", reverse=True)
    banks = sorted(
        [row for row in rows if row.get("sector") == "Commercial Banks"],
        key=lambda row: number(quarterly(row).get("profitAmount")) or 0,
        reverse=True,
    )
    hydro = sorted(
        [row for row in rows if row.get("sector") == "Hydro Power"],
        key=lambda row: number(quarterly(row).get("profitAmount")) or 0,
        reverse=True,
    )

    parts: List[str] = []
    parts.append(
        f"""
<div class="cover">
  <h1>NEPSE Listed Stock Financial Reports</h1>
  <div class="subtitle">Latest quarterly and annual filings for Nepal Stock Exchange equities</div>
  <div class="meta">
    <strong>Generated:</strong> {esc(generated_at)} &nbsp;·&nbsp;
    <strong>Coverage:</strong> {len(rows)} active equity listings &nbsp;·&nbsp;
    <strong>Predominant fiscal year:</strong> {esc(latest_fy)} (BS) &nbsp;·&nbsp;
    <strong>Source:</strong> NEPSE /api/nots/application/reports
  </div>
</div>
<div class="cards">
  <div class="card"><div class="value">{len(filed)}</div><div class="label">With quarterly filing</div></div>
  <div class="card"><div class="value">{len(q4)}</div><div class="label">Fourth quarter filed</div></div>
  <div class="card"><div class="value">{len(q3)}</div><div class="label">Still on third quarter</div></div>
  <div class="card"><div class="value">{fmt_billions(total_profit)}</div><div class="label">Total profit, Rs bn</div></div>
</div>
"""
    )

    parts.append("<h2>Sector summary</h2>")
    parts.append(
        table(
            ["Sector", "Listings", "Q4 filed", "Avg EPS", "Avg P/E", "Profit (Rs bn)"],
            [
                [
                    esc(item["sector"]),
                    str(item["count"]),
                    str(item["q4"]),
                    fmt(item["avgEps"]),
                    fmt(item["avgPe"]),
                    fmt_billions(item["profit"]),
                ]
                for item in sector_aggregates(rows)
            ],
            numeric_from=1,
        )
    )
    parts.append(
        '<p class="note">Averages use each company\'s most recent quarterly filing. '
        "Negative or zero P/E values are excluded from the average P/E.</p>"
    )

    parts.append("<h2>Largest reported profit</h2>")
    parts.append(
        table(
            ["#", "Symbol", "Company", "Sector", "Period", "Profit (Rs bn)", "EPS", "P/E"],
            [
                [
                    str(index),
                    esc(row.get("symbol")),
                    esc(row.get("companyName")),
                    esc(row.get("sector")),
                    esc(period(quarterly(row))),
                    fmt_billions(quarterly(row).get("profitAmount")),
                    fmt(quarterly(row).get("eps")),
                    fmt(quarterly(row).get("pe")),
                ]
                for index, row in enumerate(rank_by(filed, "profitAmount"), 1)
            ],
            numeric_from=5,
        )
    )

    parts.append("<h2>Highest earnings per share</h2>")
    parts.append(
        table(
            ["#", "Symbol", "Company", "Sector", "Period", "EPS", "P/E", "Profit (Rs bn)"],
            [
                [
                    str(index),
                    esc(row.get("symbol")),
                    esc(row.get("companyName")),
                    esc(row.get("sector")),
                    esc(period(quarterly(row))),
                    fmt(quarterly(row).get("eps")),
                    fmt(quarterly(row).get("pe")),
                    fmt_billions(quarterly(row).get("profitAmount")),
                ]
                for index, row in enumerate(rank_by(filed, "eps"), 1)
            ],
            numeric_from=5,
        )
    )

    parts.append("<h2>Lowest positive price-to-earnings</h2>")
    parts.append(
        table(
            ["#", "Symbol", "Company", "Sector", "Period", "P/E", "EPS", "Net worth/share"],
            [
                [
                    str(index),
                    esc(row.get("symbol")),
                    esc(row.get("companyName")),
                    esc(row.get("sector")),
                    esc(period(quarterly(row))),
                    fmt(quarterly(row).get("pe")),
                    fmt(quarterly(row).get("eps")),
                    net_worth(quarterly(row)),
                ]
                for index, row in enumerate(
                    rank_by(filed, "pe", reverse=False, predicate=lambda value: value > 0), 1
                )
            ],
            numeric_from=5,
        )
    )

    losses = rank_by(filed, "eps", reverse=False, limit=15, predicate=lambda value: value < 0)
    parts.append("<h2>Loss-making latest quarter</h2>")
    parts.append(
        table(
            ["Symbol", "Company", "Sector", "Period", "EPS", "Loss (Rs m)"],
            [
                [
                    esc(row.get("symbol")),
                    esc(row.get("companyName")),
                    esc(row.get("sector")),
                    esc(period(quarterly(row))),
                    f'<span class="negative">{fmt(quarterly(row).get("eps"))}</span>',
                    f'<span class="negative">{fmt_millions(quarterly(row).get("profitAmount"))}</span>',
                ]
                for row in losses
            ],
            numeric_from=4,
        )
    )

    parts.append("<h2>Recently submitted filings</h2>")
    parts.append(
        table(
            ["Filed", "Symbol", "Company", "Sector", "Period", "EPS", "P/E", "Profit (Rs bn)"],
            [
                [
                    esc(filed_on(quarterly(row))),
                    esc(row.get("symbol")),
                    esc(row.get("companyName")),
                    esc(row.get("sector")),
                    esc(period(quarterly(row))),
                    fmt(quarterly(row).get("eps")),
                    fmt(quarterly(row).get("pe")),
                    fmt_billions(quarterly(row).get("profitAmount")),
                ]
                for row in recent[:18]
            ],
            numeric_from=5,
        )
    )

    parts.append("<h2>Commercial banks</h2>")
    parts.append(
        table(
            ["Symbol", "Company", "Period", "EPS", "P/E", "Net worth/share", "Profit (Rs bn)", "Filed"],
            [
                [
                    esc(row.get("symbol")),
                    esc(row.get("companyName")),
                    esc(period(quarterly(row))),
                    fmt(quarterly(row).get("eps")),
                    fmt(quarterly(row).get("pe")),
                    net_worth(quarterly(row)),
                    fmt_billions(quarterly(row).get("profitAmount")),
                    esc(filed_on(quarterly(row))),
                ]
                for row in banks
            ],
            numeric_from=3,
        )
    )
    parts.append(
        '<p class="note">Net worth per share is shown as n/a where NEPSE reports an implausible value. '
        "Banks still showing a third-quarter period had not filed fourth-quarter results when this snapshot was taken.</p>"
    )

    parts.append("<h3>Hydropower leaders by profit</h3>")
    parts.append(
        table(
            ["Symbol", "Company", "Period", "EPS", "P/E", "Profit (Rs m)"],
            [
                [
                    esc(row.get("symbol")),
                    esc(row.get("companyName")),
                    esc(period(quarterly(row))),
                    fmt(quarterly(row).get("eps")),
                    fmt(quarterly(row).get("pe")),
                    fmt_millions(quarterly(row).get("profitAmount")),
                ]
                for row in hydro[:12]
            ],
            numeric_from=3,
        )
    )

    if missing:
        parts.append("<h3>No quarterly filing on NEPSE</h3>")
        parts.append(
            table(
                ["Symbol", "Company", "Sector", "Latest annual FY"],
                [
                    [
                        esc(row.get("symbol")),
                        esc(row.get("companyName")),
                        esc(row.get("sector")),
                        esc(annual(row).get("fiscalYearNepali")),
                    ]
                    for row in missing
                ],
            )
        )

    parts.append('<div class="page-break"></div>')
    parts.append(f"<h2>Appendix: all {len(rows)} listings</h2>")
    appendix = sorted(rows, key=lambda row: (row.get("sector") or "", row.get("symbol") or ""))
    parts.append(
        table(
            ["Symbol", "Sector", "Period", "EPS", "P/E", "Net worth/share", "Profit (Rs m)", "Filed"],
            [
                [
                    esc(row.get("symbol")),
                    esc(row.get("sector")),
                    esc(period(quarterly(row))) if quarterly(row) else "–",
                    fmt(quarterly(row).get("eps")),
                    fmt(quarterly(row).get("pe")),
                    net_worth(quarterly(row)),
                    fmt_millions(quarterly(row).get("profitAmount")),
                    esc(filed_on(quarterly(row))),
                ]
                for row in appendix
            ],
            numeric_from=3,
        )
    )

    parts.append(
        """
<div class="disclaimer">
  Quarterly figures are unaudited as submitted to NEPSE and may be restated. Data is sourced from an
  unofficial NEPSE endpoint for educational and research use only, with no guarantee of accuracy or
  completeness. This report is not financial advice.
</div>
"""
    )

    return (
        "<!doctype html><html><head><meta charset=\"utf-8\">"
        "<title>NEPSE Listed Stock Financial Reports</title>"
        f"<style>{STYLES}</style></head><body>{''.join(parts)}</body></html>"
    )


def find_chrome() -> Optional[str]:
    for candidate in CHROME_CANDIDATES:
        path = shutil.which(candidate)
        if path:
            return path
    return None


def html_to_pdf(html_path: Path, pdf_path: Path, timeout: int = 120) -> None:
    chrome = find_chrome()
    if not chrome:
        raise RuntimeError(
            "No Chrome/Chromium binary found; install one or use the generated HTML."
        )
    if pdf_path.exists():
        pdf_path.unlink()

    with tempfile.TemporaryDirectory() as profile_dir:
        process = subprocess.Popen(
            [
                chrome,
                "--headless",
                "--disable-gpu",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                f"--user-data-dir={profile_dir}",
                "--no-pdf-header-footer",
                "--virtual-time-budget=20000",
                f"--print-to-pdf={pdf_path}",
                html_path.as_uri(),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        # Some Chrome wrappers keep the browser alive after printing, so stop
        # waiting once the PDF has been written and its size has settled.
        deadline = time.monotonic() + timeout
        last_size = -1
        while time.monotonic() < deadline:
            if process.poll() is not None:
                break
            size = pdf_path.stat().st_size if pdf_path.exists() else 0
            if size > 0 and size == last_size:
                break
            last_size = size
            time.sleep(1)
        if process.poll() is None:
            process.kill()
            process.wait(timeout=30)

    if not pdf_path.exists() or pdf_path.stat().st_size == 0:
        raise RuntimeError(f"Chrome did not produce a PDF at {pdf_path}")


async def fetch_snapshot() -> Dict[str, Any]:
    from nepse import AsyncNepse
    from financial_reports import fetch_latest_listed_reports

    nepse = AsyncNepse()
    nepse.setTLSVerification(False)
    return await fetch_latest_listed_reports(nepse)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a NEPSE financial report PDF")
    parser.add_argument("--input", help="Path to a /LatestFinancialReports JSON snapshot")
    parser.add_argument("--fetch", action="store_true", help="Fetch a fresh snapshot from NEPSE")
    parser.add_argument("--output-dir", default=".", help="Directory for the HTML and PDF output")
    parser.add_argument("--basename", default="nepse_financial_report", help="Output file basename")
    args = parser.parse_args()

    if args.fetch or not args.input:
        payload = asyncio.run(fetch_snapshot())
    else:
        payload = json.loads(Path(args.input).read_text())

    generated_at = datetime.now(timezone.utc).strftime("%d %B %Y %H:%M UTC")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    html_path = output_dir / f"{args.basename}.html"
    pdf_path = output_dir / f"{args.basename}.pdf"
    html_path.write_text(render_html(payload, generated_at))
    html_to_pdf(html_path, pdf_path)

    print(f"Companies: {payload.get('count')}")
    print(f"HTML: {html_path}")
    print(f"PDF:  {pdf_path} ({pdf_path.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
