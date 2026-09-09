#!/usr/bin/env python3
"""
Weekly NEPSE floorsheet + financial PDF report.

Combines unique trading-day floorsheets with downloadable NEPSE quarterly/annual
filing PDFs for the week's top scrips (and portfolio holdings).

Examples:
  python weekly_floorsheet_report.py --days 5 --no-telegram
  python weekly_floorsheet_report.py --end 2026-09-07 --days 5
  python weekly_floorsheet_report.py --days 5 --html --pdf
"""

from __future__ import annotations

import argparse
import asyncio
import glob
import hashlib
import html
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote

import pandas as pd

PORTFOLIO_PATH = Path(os.getenv("PORTFOLIO_FILE", "portfolio_data.json"))
ARTIFACT_DIR = Path(os.getenv("ARTIFACT_DIR", "artifacts"))
REPORT_DIR = Path(os.getenv("WEEKLY_REPORT_DIR", "."))

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

DOCUMENT_BASE_URL = "https://www.nepalstock.com/api/nots/security/fetchFiles"
WHALE_AMOUNT = 10_000_000  # Rs 1 Cr+
TOP_N = 10
REPORT_SYMBOL_LIMIT = 20


def get_date_from_filename(filename: str) -> Optional[str]:
    match = re.search(r"(\d{4})[_-](\d{2})[_-](\d{2})", os.path.basename(filename))
    return "-".join(match.groups()) if match else None


def unique_floorsheet_days() -> List[Tuple[str, str, str]]:
    """[(session_date, path, md5), ...] newest-first, collapsing identical copies."""
    files = glob.glob("floorsheet*.csv")
    dated: List[Tuple[str, str]] = []
    for path in files:
        dt = get_date_from_filename(path)
        if dt:
            dated.append((dt, path))
    dated.sort(key=lambda item: item[0])

    by_hash: Dict[str, Tuple[str, str]] = {}
    for dt, path in dated:
        digest = hashlib.md5(Path(path).read_bytes()).hexdigest()
        if digest in by_hash:
            continue
        session = dt
        try:
            sample = pd.read_csv(path, usecols=["businessDate"], nrows=5)
            if not sample.empty:
                session = str(sample["businessDate"].iloc[0])[:10]
        except Exception:
            pass
        by_hash[digest] = (session, path)

    days = [(session, path, digest) for digest, (session, path) in by_hash.items()]
    days.sort(key=lambda item: item[0], reverse=True)
    return days


def fmt_rs(amount: float) -> str:
    if abs(amount) >= 1_00_00_000:
        return f"Rs {amount / 1_00_00_000:,.2f} Cr"
    if abs(amount) >= 1_00_000:
        return f"Rs {amount / 1_00_000:,.2f} L"
    return f"Rs {amount:,.0f}"


def fmt_pct(value: float) -> str:
    return f"{value:+.2f}%"


def load_ltp(path: str) -> Dict[str, float]:
    df = pd.read_csv(path)
    df = df.sort_values("contractId", ascending=False)
    return df.groupby("stockSymbol")["contractRate"].first().astype(float).to_dict()


def analyze_week(week: List[Tuple[str, str, str]]) -> Dict[str, Any]:
    """Aggregate floorsheet metrics across unique trading days."""
    frames = []
    daily = []
    for session, path, _ in week:
        df = pd.read_csv(
            path,
            usecols=[
                "contractId",
                "stockSymbol",
                "buyerMemberId",
                "sellerMemberId",
                "contractQuantity",
                "contractRate",
                "contractAmount",
                "buyerBrokerName",
                "sellerBrokerName",
                "businessDate",
            ],
        )
        df["session"] = session
        frames.append(df)
        daily.append(
            {
                "date": session,
                "turnover": float(df["contractAmount"].sum()),
                "volume": float(df["contractQuantity"].sum()),
                "trades": int(len(df)),
            }
        )

    all_df = pd.concat(frames, ignore_index=True)
    start_ltp = load_ltp(week[0][1])
    end_ltp = load_ltp(week[-1][1])

    by_symbol = (
        all_df.groupby("stockSymbol")
        .agg(
            turnover=("contractAmount", "sum"),
            volume=("contractQuantity", "sum"),
            trades=("contractId", "count"),
            avg_rate=("contractRate", "mean"),
        )
        .reset_index()
    )
    by_symbol["start"] = by_symbol["stockSymbol"].map(start_ltp)
    by_symbol["end"] = by_symbol["stockSymbol"].map(end_ltp)
    by_symbol["change_pct"] = by_symbol.apply(
        lambda row: ((row["end"] - row["start"]) / row["start"] * 100)
        if row["start"] and row["end"]
        else 0.0,
        axis=1,
    )

    top_turnover = by_symbol.sort_values("turnover", ascending=False).head(TOP_N)
    top_volume = by_symbol.sort_values("volume", ascending=False).head(TOP_N)
    gainers = (
        by_symbol[by_symbol["start"] > 0]
        .sort_values("change_pct", ascending=False)
        .head(TOP_N)
    )
    losers = (
        by_symbol[by_symbol["start"] > 0]
        .sort_values("change_pct", ascending=True)
        .head(TOP_N)
    )

    buy = all_df.groupby("buyerBrokerName")["contractAmount"].sum()
    sell = all_df.groupby("sellerBrokerName")["contractAmount"].sum()
    brokers = pd.DataFrame({"bought": buy, "sold": sell}).fillna(0.0)
    brokers["net"] = brokers["bought"] - brokers["sold"]
    brokers = brokers.reset_index().rename(columns={"index": "broker"})
    # groupby index name may be buyerBrokerName / sellerBrokerName depending on frame
    if "broker" not in brokers.columns:
        name_col = [c for c in brokers.columns if c not in {"bought", "sold", "net"}][0]
        brokers = brokers.rename(columns={name_col: "broker"})
    accumulators = brokers.sort_values("net", ascending=False).head(8)
    distributors = brokers.sort_values("net", ascending=True).head(8)

    whales = all_df[all_df["contractAmount"] >= WHALE_AMOUNT].copy()
    whales = whales.sort_values("contractAmount", ascending=False).head(15)

    return {
        "daily": daily,
        "total_turnover": float(all_df["contractAmount"].sum()),
        "total_volume": float(all_df["contractQuantity"].sum()),
        "total_trades": int(len(all_df)),
        "unique_symbols": int(all_df["stockSymbol"].nunique()),
        "top_turnover": top_turnover.to_dict("records"),
        "top_volume": top_volume.to_dict("records"),
        "gainers": gainers.to_dict("records"),
        "losers": losers.to_dict("records"),
        "accumulators": accumulators.to_dict("records"),
        "distributors": distributors.to_dict("records"),
        "whales": whales.to_dict("records"),
        "start_ltp": start_ltp,
        "end_ltp": end_ltp,
        "symbol_stats": by_symbol.set_index("stockSymbol").to_dict("index"),
    }


def portfolio_week(
    portfolio: Dict[str, Any],
    start_ltp: Dict[str, float],
    end_ltp: Dict[str, float],
) -> Dict[str, Any]:
    rows = []
    start_value = end_value = cost = 0.0
    for symbol, data in portfolio.items():
        qty = float(data["qty"])
        avg = float(data["rate"])
        s_px = float(start_ltp.get(symbol, 0) or 0)
        e_px = float(end_ltp.get(symbol, 0) or 0)
        s_val = qty * s_px
        e_val = qty * e_px
        c_val = qty * avg
        start_value += s_val
        end_value += e_val
        cost += c_val
        move_pct = ((e_px - s_px) / s_px * 100) if s_px else 0.0
        rows.append(
            {
                "symbol": symbol,
                "qty": qty,
                "avg": avg,
                "start": s_px,
                "end": e_px,
                "week_pl": e_val - s_val,
                "move_pct": move_pct,
                "pl_pct": ((e_val - c_val) / c_val * 100) if c_val else 0.0,
            }
        )
    rows.sort(key=lambda row: row["week_pl"], reverse=True)
    week_pl = end_value - start_value
    week_pct = (week_pl / start_value * 100) if start_value else 0.0
    return {
        "rows": rows,
        "start_value": start_value,
        "end_value": end_value,
        "cost": cost,
        "week_pl": week_pl,
        "week_pct": week_pct,
    }


def document_url(file_path: Optional[str]) -> Optional[str]:
    if not file_path:
        return None
    return f"{DOCUMENT_BASE_URL}?fileLocation={quote(file_path, safe='/')}"


def compact_report(raw: Dict[str, Any]) -> Dict[str, Any]:
    fiscal = raw.get("fiscalReport") or {}
    quarter = fiscal.get("quarterMaster") or {}
    report_type = fiscal.get("reportTypeMaster") or {}
    fiscal_year = fiscal.get("financialYear") or {}
    documents = []
    for document in raw.get("applicationDocumentDetailsList") or []:
        file_path = document.get("filePath")
        documents.append(
            {
                "submittedDate": document.get("submittedDate"),
                "fileName": file_path.rsplit("/", 1)[-1] if file_path else None,
                "url": document_url(file_path),
            }
        )
    return {
        "id": raw.get("id"),
        "reportType": report_type.get("reportName"),
        "quarter": quarter.get("quarterName"),
        "fiscalYear": fiscal_year.get("fyName"),
        "fiscalYearNepali": fiscal_year.get("fyNameNepali"),
        "pe": fiscal.get("peValue"),
        "eps": fiscal.get("epsValue"),
        "paidUpCapital": fiscal.get("paidUpCapital"),
        "profitAmount": fiscal.get("profitAmount"),
        "netWorthPerShare": fiscal.get("netWorthPerShare"),
        "modifiedDate": raw.get("modifiedDate"),
        "documents": documents,
    }


async def fetch_symbol_reports(nepse, symbol: str) -> Dict[str, Any]:
    try:
        raw = await nepse.getCompanyReports(symbol)
        compact = sorted(
            (compact_report(item) for item in (raw or [])),
            key=lambda item: item.get("modifiedDate") or "",
            reverse=True,
        )
        latest_q = next((r for r in compact if r.get("reportType") == "Quarterly Report"), None)
        latest_a = next((r for r in compact if r.get("reportType") == "Annual Report"), None)
        return {
            "symbol": symbol,
            "latestQuarterly": latest_q,
            "latestAnnual": latest_a,
            "reports": compact[:6],
            "error": None,
        }
    except Exception as exc:  # noqa: BLE001 — keep weekly job resilient
        return {
            "symbol": symbol,
            "latestQuarterly": None,
            "latestAnnual": None,
            "reports": [],
            "error": str(exc),
        }


async def fetch_financial_reports(
    symbols: Iterable[str],
    concurrency: int = 6,
    per_symbol_timeout: float = 45.0,
) -> List[Dict[str, Any]]:
    from nepse import AsyncNepse

    nepse = AsyncNepse()
    nepse.setTLSVerification(False)
    semaphore = asyncio.Semaphore(concurrency)
    symbol_list = list(symbols)

    async def one(symbol: str) -> Dict[str, Any]:
        async with semaphore:
            try:
                return await asyncio.wait_for(
                    fetch_symbol_reports(nepse, symbol),
                    timeout=per_symbol_timeout,
                )
            except asyncio.TimeoutError:
                return {
                    "symbol": symbol,
                    "latestQuarterly": None,
                    "latestAnnual": None,
                    "reports": [],
                    "error": f"timeout after {per_symbol_timeout:.0f}s",
                }

    print(f"Fetching financial reports for {len(symbol_list)} symbols...", flush=True)
    results = await asyncio.gather(*[one(symbol) for symbol in symbol_list])
    ok = sum(1 for item in results if not item.get("error"))
    print(f"Financial reports: {ok}/{len(results)} ok", flush=True)
    return list(results)


def filings_in_window(
    reports: List[Dict[str, Any]],
    start: str,
    end: str,
) -> List[Dict[str, Any]]:
    hits = []
    for item in reports:
        for report in item.get("reports") or []:
            modified = (report.get("modifiedDate") or "")[:10]
            if not modified or modified < start or modified > end:
                continue
            docs = [d for d in (report.get("documents") or []) if d.get("url")]
            hits.append(
                {
                    "symbol": item["symbol"],
                    "reportType": report.get("reportType"),
                    "quarter": report.get("quarter"),
                    "fiscalYearNepali": report.get("fiscalYearNepali"),
                    "modifiedDate": modified,
                    "eps": report.get("eps"),
                    "pe": report.get("pe"),
                    "profitAmount": report.get("profitAmount"),
                    "documents": docs,
                }
            )
    hits.sort(key=lambda row: row["modifiedDate"], reverse=True)
    return hits


def md_link(label: str, url: Optional[str]) -> str:
    if not url:
        return label
    return f"[{label}]({url})"


def build_markdown(
    start: str,
    end: str,
    week_dates: List[str],
    market: Dict[str, Any],
    portfolio: Optional[Dict[str, Any]],
    financials: List[Dict[str, Any]],
    week_filings: List[Dict[str, Any]],
) -> str:
    lines: List[str] = []
    lines.append("# NEPSE Weekly Floorsheet & Financial PDF Report")
    lines.append(
        f"**Week:** {start} → {end}  |  "
        f"**Trading days:** {len(week_dates)} ({', '.join(week_dates)})"
    )
    lines.append(f"**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Market pulse (floorsheet)")
    lines.append("")
    lines.append(
        f"- **Week turnover:** {fmt_rs(market['total_turnover'])}  |  "
        f"**Avg/day:** {fmt_rs(market['total_turnover'] / max(len(week_dates), 1))}"
    )
    lines.append(
        f"- **Volume:** {market['total_volume']:,.0f} shares  |  "
        f"**Trades:** {market['total_trades']:,}  |  "
        f"**Symbols:** {market['unique_symbols']}"
    )
    lines.append("")
    lines.append("| Date | Turnover | Volume | Trades |")
    lines.append("|------|---------:|-------:|-------:|")
    for day in market["daily"]:
        lines.append(
            f"| {day['date']} | {fmt_rs(day['turnover'])} | "
            f"{day['volume']:,.0f} | {day['trades']:,} |"
        )

    lines.append("")
    lines.append("## Top scrips by turnover")
    lines.append("")
    lines.append("| # | Symbol | Turnover | Volume | Start → End | Week Δ |")
    lines.append("|--:|--------|---------:|-------:|-------------|-------:|")
    for idx, row in enumerate(market["top_turnover"], 1):
        lines.append(
            f"| {idx} | **{row['stockSymbol']}** | {fmt_rs(row['turnover'])} | "
            f"{row['volume']:,.0f} | {row.get('start', 0):.2f} → {row.get('end', 0):.2f} | "
            f"{fmt_pct(row.get('change_pct', 0))} |"
        )

    lines.append("")
    lines.append("## Price movers")
    lines.append("")
    lines.append("### Gainers")
    lines.append("| Symbol | Start → End | Week Δ | Turnover |")
    lines.append("|--------|-------------|-------:|---------:|")
    for row in market["gainers"]:
        lines.append(
            f"| **{row['stockSymbol']}** | {row.get('start', 0):.2f} → {row.get('end', 0):.2f} | "
            f"{fmt_pct(row.get('change_pct', 0))} | {fmt_rs(row['turnover'])} |"
        )
    lines.append("")
    lines.append("### Losers")
    lines.append("| Symbol | Start → End | Week Δ | Turnover |")
    lines.append("|--------|-------------|-------:|---------:|")
    for row in market["losers"]:
        lines.append(
            f"| **{row['stockSymbol']}** | {row.get('start', 0):.2f} → {row.get('end', 0):.2f} | "
            f"{fmt_pct(row.get('change_pct', 0))} | {fmt_rs(row['turnover'])} |"
        )

    lines.append("")
    lines.append("## Smart money (broker net flow)")
    lines.append("")
    lines.append("### Net accumulators")
    lines.append("| Broker | Net | Bought | Sold |")
    lines.append("|--------|----:|-------:|-----:|")
    for row in market["accumulators"]:
        name = row.get("broker") or "Unknown"
        lines.append(
            f"| {name} | **{fmt_rs(row['net'])}** | {fmt_rs(row['bought'])} | {fmt_rs(row['sold'])} |"
        )

    lines.append("")
    lines.append("### Net distributors")
    lines.append("| Broker | Net | Bought | Sold |")
    lines.append("|--------|----:|-------:|-----:|")
    for row in market["distributors"]:
        name = row.get("broker") or "Unknown"
        lines.append(
            f"| {name} | **{fmt_rs(row['net'])}** | {fmt_rs(row['bought'])} | {fmt_rs(row['sold'])} |"
        )

    lines.append("")
    lines.append("## Block / whale trades (≥ Rs 1 Cr)")
    lines.append("")
    if market["whales"]:
        lines.append("| Date | Symbol | Qty | Rate | Amount | Buyer | Seller |")
        lines.append("|------|--------|----:|-----:|-------:|-------|--------|")
        for row in market["whales"]:
            session = row.get("session") or str(row.get("businessDate", ""))[:10]
            buyer = str(row.get("buyerBrokerName") or "")[:28]
            seller = str(row.get("sellerBrokerName") or "")[:28]
            lines.append(
                f"| {session} | **{row['stockSymbol']}** | {row['contractQuantity']:,.0f} | "
                f"{row['contractRate']:,.2f} | {fmt_rs(row['contractAmount'])} | "
                f"{buyer} | {seller} |"
            )
    else:
        lines.append("_No ≥ Rs 1 Cr single trades this week._")

    if portfolio:
        lines.append("")
        lines.append("## Portfolio week")
        lines.append("")
        lines.append(
            f"- Start value: **{fmt_rs(portfolio['start_value'])}** → "
            f"End: **{fmt_rs(portfolio['end_value'])}** "
            f"({fmt_rs(portfolio['week_pl'])}, {fmt_pct(portfolio['week_pct'])})"
        )
        lines.append("")
        lines.append("| Symbol | Start → End | Week Δ | Week P/L | vs Cost |")
        lines.append("|--------|-------------|-------:|---------:|--------:|")
        for row in portfolio["rows"]:
            marker = "🟢" if row["week_pl"] >= 0 else "🔴"
            lines.append(
                f"| {marker} **{row['symbol']}** | {row['start']:.2f} → {row['end']:.2f} | "
                f"{fmt_pct(row['move_pct'])} | {fmt_rs(row['week_pl'])} | {fmt_pct(row['pl_pct'])} |"
            )

    lines.append("")
    lines.append("## Downloadable financial PDF reports")
    lines.append("")
    lines.append(
        "Latest quarterly/annual filings for this week's top turnover scrips "
        "and portfolio holdings. PDF links point at NEPSE `fetchFiles`."
    )
    lines.append("")

    if week_filings:
        lines.append("### Filed during this week")
        lines.append("")
        lines.append("| Filed | Symbol | Type | Period | EPS | P/E | PDF |")
        lines.append("|-------|--------|------|--------|----:|----:|-----|")
        for row in week_filings:
            period = " / ".join(
                part for part in [row.get("quarter"), row.get("fiscalYearNepali")] if part
            ) or "—"
            docs = row.get("documents") or []
            pdf = ", ".join(
                md_link(doc.get("fileName") or "PDF", doc.get("url")) for doc in docs[:2]
            ) or "—"
            eps = f"{row['eps']:.2f}" if isinstance(row.get("eps"), (int, float)) else "—"
            pe = f"{row['pe']:.2f}" if isinstance(row.get("pe"), (int, float)) else "—"
            lines.append(
                f"| {row['modifiedDate']} | **{row['symbol']}** | {row.get('reportType') or '—'} | "
                f"{period} | {eps} | {pe} | {pdf} |"
            )
        lines.append("")

    lines.append("### Latest filings (watched symbols)")
    lines.append("")
    lines.append("| Symbol | Quarterly | Annual | EPS | P/E | Profit | PDF downloads |")
    lines.append("|--------|-----------|--------|----:|----:|-------:|---------------|")
    for item in financials:
        if item.get("error"):
            lines.append(f"| **{item['symbol']}** | _error_ | — | — | — | — | {item['error'][:40]} |")
            continue
        q = item.get("latestQuarterly") or {}
        a = item.get("latestAnnual") or {}
        q_label = " / ".join(
            part for part in [q.get("quarter"), q.get("fiscalYearNepali")] if part
        ) or "—"
        a_label = a.get("fiscalYearNepali") or a.get("fiscalYear") or "—"
        eps = q.get("eps")
        pe = q.get("pe")
        profit = q.get("profitAmount")
        docs = []
        for report in (q, a):
            for doc in report.get("documents") or []:
                if doc.get("url"):
                    docs.append(md_link(doc.get("fileName") or "PDF", doc["url"]))
        pdf = ", ".join(docs[:3]) or "—"
        lines.append(
            f"| **{item['symbol']}** | {q_label} | {a_label} | "
            f"{eps if isinstance(eps, (int, float)) else '—'} | "
            f"{pe if isinstance(pe, (int, float)) else '—'} | "
            f"{fmt_rs(profit) if isinstance(profit, (int, float)) else '—'} | {pdf} |"
        )

    lines.append("")
    lines.append("---")
    lines.append("_Not financial advice. Educational / research use only. PDF links are NEPSE official filings._")
    lines.append("")
    return "\n".join(lines)


def markdown_to_html(md_text: str, title: str) -> str:
    """Minimal markdown→HTML for printable weekly report (tables + headings)."""
    body_lines: List[str] = []
    in_table = False
    table_rows: List[str] = []

    def flush_table() -> None:
        nonlocal in_table, table_rows
        if not table_rows:
            return
        header = table_rows[0]
        rows = table_rows[2:] if len(table_rows) > 1 and set(table_rows[1].replace("|", "").strip()) <= {"-", ":"} else table_rows[1:]
        def cells(line: str) -> List[str]:
            parts = [c.strip() for c in line.strip().strip("|").split("|")]
            return parts

        thead = "".join(f"<th>{html.escape(c)}</th>" for c in cells(header))
        tbody = []
        for row in rows:
            tbody.append(
                "<tr>"
                + "".join(f"<td>{_inline(c)}</td>" for c in cells(row))
                + "</tr>"
            )
        body_lines.append(
            f"<table><thead><tr>{thead}</tr></thead><tbody>{''.join(tbody)}</tbody></table>"
        )
        table_rows = []
        in_table = False

    def _inline(text: str) -> str:
        text = html.escape(text)
        text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
        text = re.sub(r"\[(.+?)\]\((https?://[^)]+)\)", r'<a href="\2">\1</a>', text)
        return text

    for raw in md_text.splitlines():
        line = raw.rstrip()
        if line.startswith("|"):
            in_table = True
            table_rows.append(line)
            continue
        if in_table:
            flush_table()
        if line.startswith("# "):
            body_lines.append(f"<h1>{_inline(line[2:])}</h1>")
        elif line.startswith("## "):
            body_lines.append(f"<h2>{_inline(line[3:])}</h2>")
        elif line.startswith("### "):
            body_lines.append(f"<h3>{_inline(line[4:])}</h3>")
        elif line.startswith("- "):
            body_lines.append(f"<li>{_inline(line[2:])}</li>")
        elif line.startswith("---"):
            body_lines.append("<hr/>")
        elif line.strip() == "":
            body_lines.append("")
        else:
            body_lines.append(f"<p>{_inline(line)}</p>")
    if in_table:
        flush_table()

    styles = """
    @page { size: A4; margin: 14mm 12mm; }
    body { font-family: DejaVu Sans, Helvetica, Arial, sans-serif; color:#16202c;
           font-size:9.2pt; line-height:1.4; margin:0; padding:8px; }
    h1 { font-size:20pt; color:#123a63; margin:0 0 6px; }
    h2 { font-size:12.5pt; color:#123a63; border-bottom:2px solid #123a63;
         padding-bottom:4px; margin:18px 0 8px; }
    h3 { font-size:10.5pt; color:#123a63; margin:12px 0 6px; }
    table { width:100%; border-collapse:collapse; margin:6px 0 10px; }
    th { background:#123a63; color:#fff; text-align:left; padding:4px 6px; font-size:7.8pt; }
    td { border-bottom:1px solid #e3e8f0; padding:4px 6px; font-size:8.3pt; }
    tr:nth-child(even) td { background:#f7f9fc; }
    a { color:#123a63; }
    li { margin-left:16px; }
    hr { border:none; border-top:1px solid #d7dee8; margin:14px 0; }
    """
    return (
        "<!DOCTYPE html><html><head><meta charset='utf-8'/>"
        f"<title>{html.escape(title)}</title><style>{styles}</style></head>"
        f"<body>{''.join(body_lines)}</body></html>"
    )


def render_pdf(html_path: Path, pdf_path: Path, timeout: int = 90) -> bool:
    chrome = None
    for candidate in (
        "/opt/google/chrome/chrome",
        "google-chrome-stable",
        "google-chrome",
        "chromium",
        "chromium-browser",
    ):
        if candidate.startswith("/") and Path(candidate).exists():
            chrome = candidate
            break
        found = shutil.which(candidate)
        if found:
            chrome = found
            break
    if not chrome:
        return False

    user_data = Path(tempfile.mkdtemp(prefix="weekly-report-chrome-"))
    try:
        cmd = [
            chrome,
            "--headless=new",
            "--disable-gpu",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            f"--user-data-dir={user_data}",
            "--no-pdf-header-footer",
            f"--print-to-pdf={pdf_path.resolve()}",
            html_path.resolve().as_uri(),
        ]
        subprocess.run(cmd, check=True, capture_output=True, timeout=timeout)
        return pdf_path.exists() and pdf_path.stat().st_size > 0
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        print(f"PDF render issue: {exc}", file=sys.stderr)
        return pdf_path.exists() and pdf_path.stat().st_size > 0
    finally:
        shutil.rmtree(user_data, ignore_errors=True)


def send_telegram_text(message: str) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    import requests

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    # Telegram hard limit ~4096 chars
    chunk = message[:4000]
    requests.post(
        url,
        json={"chat_id": TELEGRAM_CHAT_ID, "text": chunk, "parse_mode": "Markdown"},
        timeout=30,
    )


def send_telegram_document(path: Path, caption: str) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    import requests

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
    with path.open("rb") as handle:
        requests.post(
            url,
            data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption[:900]},
            files={"document": (path.name, handle)},
            timeout=60,
        )


def select_symbols(market: Dict[str, Any], portfolio: Dict[str, Any]) -> List[str]:
    symbols: List[str] = []
    # Portfolio first so holdings always get PDF filings
    symbols.extend(portfolio.keys())
    for row in market["top_turnover"][:12]:
        symbols.append(row["stockSymbol"])
    for row in market["gainers"][:5]:
        symbols.append(row["stockSymbol"])
    for row in market["losers"][:5]:
        symbols.append(row["stockSymbol"])
    seen = set()
    ordered = []
    for symbol in symbols:
        if symbol not in seen:
            seen.add(symbol)
            ordered.append(symbol)
    return ordered[:REPORT_SYMBOL_LIMIT]



def build_report(
    trading_days: int = 5,
    end_date: Optional[str] = None,
    skip_financials: bool = False,
) -> Tuple[str, Dict[str, Any]]:
    days = unique_floorsheet_days()
    if not days:
        raise SystemExit("No floorsheet CSV files found.")
    if end_date:
        days = [d for d in days if d[0] <= end_date]
    if len(days) < 2:
        raise SystemExit("Need at least 2 unique trading days for a weekly report.")

    week = list(reversed(days[:trading_days]))
    start_dt, _, _ = week[0]
    end_dt, _, _ = week[-1]
    week_dates = [d[0] for d in week]

    market = analyze_week(week)

    portfolio_data = {}
    if PORTFOLIO_PATH.exists():
        with open(PORTFOLIO_PATH) as handle:
            portfolio_data = json.load(handle)
    portfolio = (
        portfolio_week(portfolio_data, market["start_ltp"], market["end_ltp"])
        if portfolio_data
        else None
    )

    symbols = select_symbols(market, portfolio_data)
    if skip_financials:
        financials: List[Dict[str, Any]] = []
        week_filings: List[Dict[str, Any]] = []
    else:
        financials = asyncio.run(fetch_financial_reports(symbols))
        week_filings = filings_in_window(financials, start_dt, end_dt)

    md = build_markdown(
        start_dt,
        end_dt,
        week_dates,
        market,
        portfolio,
        financials,
        week_filings,
    )
    meta = {
        "start": start_dt,
        "end": end_dt,
        "week_dates": week_dates,
        "market": {
            "total_turnover": market["total_turnover"],
            "total_trades": market["total_trades"],
            "unique_symbols": market["unique_symbols"],
        },
        "symbols": symbols,
        "filing_count": len(week_filings),
        "portfolio_week_pct": portfolio["week_pct"] if portfolio else None,
    }
    return md, meta


def main() -> None:
    parser = argparse.ArgumentParser(description="Weekly floorsheet + financial PDF report")
    parser.add_argument("--days", type=int, default=5, help="Unique trading days to include")
    parser.add_argument("--end", default="", help="End date YYYY-MM-DD (blank = latest)")
    parser.add_argument("--no-telegram", action="store_true", help="Skip Telegram send")
    parser.add_argument("--skip-financials", action="store_true", help="Skip NEPSE PDF fetch")
    parser.add_argument("--html", action="store_true", help="Also write HTML")
    parser.add_argument("--pdf", action="store_true", help="Also render PDF via Chrome")
    parser.add_argument("-o", "--output", default="", help="Markdown output path")
    args = parser.parse_args()

    md, meta = build_report(
        trading_days=args.days,
        end_date=args.end or None,
        skip_financials=args.skip_financials,
    )

    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    stem = f"weekly_floorsheet_report_{meta['end']}"
    md_path = Path(args.output) if args.output else REPORT_DIR / f"{stem}.md"
    md_path.write_text(md, encoding="utf-8")
    artifact_md = ARTIFACT_DIR / md_path.name
    artifact_md.write_text(md, encoding="utf-8")
    print(f"Wrote {md_path}")
    print(f"Wrote {artifact_md}")

    html_path = ARTIFACT_DIR / f"{stem}.html"
    pdf_path = ARTIFACT_DIR / f"{stem}.pdf"
    if args.html or args.pdf:
        html_path.write_text(
            markdown_to_html(md, f"NEPSE Weekly Report {meta['start']} → {meta['end']}"),
            encoding="utf-8",
        )
        print(f"Wrote {html_path}")
    if args.pdf:
        try:
            ok = render_pdf(html_path, pdf_path)
            if ok:
                print(f"Wrote {pdf_path}")
            else:
                print("Chrome/Chromium not found — skipped PDF (HTML retained).", file=sys.stderr)
        except subprocess.CalledProcessError as exc:
            print(f"PDF render failed: {exc}", file=sys.stderr)

    if not args.no_telegram:
        # Rebuild market/portfolio briefly for summary would be heavy; parse meta instead
        summary = (
            f"📊 *NEPSE Weekly Floorsheet Report*\n"
            f"{meta['start']} → {meta['end']}\n"
            f"Turnover: *{fmt_rs(meta['market']['total_turnover'])}*\n"
            f"Symbols: {meta['market']['unique_symbols']}  |  "
            f"Trades: {meta['market']['total_trades']:,}\n"
            f"New filings watched: {meta['filing_count']}\n"
        )
        if meta.get("portfolio_week_pct") is not None:
            summary += f"Portfolio week: *{fmt_pct(meta['portfolio_week_pct'])}*\n"
        summary += "📎 Full report attached"
        send_telegram_text(summary)
        send_telegram_document(md_path, caption=f"Weekly floorsheet report {meta['end']}")
        if pdf_path.exists():
            send_telegram_document(pdf_path, caption=f"Weekly PDF {meta['end']}")


if __name__ == "__main__":
    main()
