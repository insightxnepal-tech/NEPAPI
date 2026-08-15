"""
Helpers for NEPSE listed-company financial reports.

NEPSE exposes quarterly and annual filings at
`/api/nots/application/reports/{securityId}`. This module compact those
payloads into a stable JSON shape and can fetch the latest report for
every active equity listing.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote

logger = logging.getLogger(__name__)

DOCUMENT_BASE_URL = "https://www.nepalstock.com/api/nots/security/fetchFiles"
LATEST_CACHE_TTL_SECONDS = 6 * 60 * 60
BULK_CONCURRENCY = 12

_latest_cache: Dict[str, Any] = {"expires_at": 0.0, "payload": None}
_latest_cache_lock = asyncio.Lock()


def document_url(file_path: Optional[str]) -> Optional[str]:
    if not file_path:
        return None
    return f"{DOCUMENT_BASE_URL}?fileLocation={quote(file_path, safe='/')}"


def compact_documents(documents: Optional[Iterable[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    compact = []
    for document in documents or []:
        file_path = document.get("filePath")
        compact.append(
            {
                "submittedDate": document.get("submittedDate"),
                "filePath": file_path,
                "fileName": file_path.rsplit("/", 1)[-1] if file_path else None,
                "url": document_url(file_path),
            }
        )
    return compact


def compact_report(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Reduce a NEPSE report payload to the fields investors typically need."""
    fiscal = raw.get("fiscalReport") or {}
    quarter = fiscal.get("quarterMaster") or {}
    report_type = fiscal.get("reportTypeMaster") or {}
    fiscal_year = fiscal.get("financialYear") or {}
    return {
        "id": raw.get("id"),
        "reportType": report_type.get("reportName"),
        "quarter": quarter.get("quarterName"),
        "fiscalYear": fiscal_year.get("fyName"),
        "fiscalYearNepali": fiscal_year.get("fyNameNepali"),
        "fromYear": fiscal_year.get("fromYear"),
        "toYear": fiscal_year.get("toYear"),
        "pe": fiscal.get("peValue"),
        "eps": fiscal.get("epsValue"),
        "paidUpCapital": fiscal.get("paidUpCapital"),
        "profitAmount": fiscal.get("profitAmount"),
        "netWorthPerShare": fiscal.get("netWorthPerShare"),
        "modifiedDate": raw.get("modifiedDate"),
        "documents": compact_documents(raw.get("applicationDocumentDetailsList")),
    }


def sort_reports(reports: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        reports,
        key=lambda report: report.get("modifiedDate") or "",
        reverse=True,
    )


def pick_latest(reports: Iterable[Dict[str, Any]], report_type: str) -> Optional[Dict[str, Any]]:
    for report in reports:
        if report.get("reportType") == report_type:
            return report
    return None


def summarize_company_reports(
    symbol: str,
    raw_reports: Optional[Iterable[Dict[str, Any]]],
    company: Optional[Dict[str, Any]] = None,
    include_reports: bool = True,
) -> Dict[str, Any]:
    compact = sort_reports(compact_report(report) for report in (raw_reports or []))
    company = company or {}
    summary = {
        "symbol": symbol.upper(),
        "companyName": company.get("companyName"),
        "securityName": company.get("securityName"),
        "sector": company.get("sectorName"),
        "reportCount": len(compact),
        "latestQuarterly": pick_latest(compact, "Quarterly Report"),
        "latestAnnual": pick_latest(compact, "Annual Report"),
    }
    if include_reports:
        summary["reports"] = compact
    return summary


def is_active_equity(company: Dict[str, Any]) -> bool:
    return company.get("status") == "A" and company.get("instrumentType") == "Equity"


def _company_index(companies: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {company["symbol"].upper(): company for company in companies if company.get("symbol")}


async def build_symbol_report(
    nepse,
    symbol: str,
    company: Optional[Dict[str, Any]] = None,
    include_reports: bool = True,
) -> Dict[str, Any]:
    raw_reports = await nepse.getCompanyReports(symbol)
    if company is None:
        companies = _company_index(await nepse.getCompanyList())
        company = companies.get(symbol.upper(), {})
    return summarize_company_reports(
        symbol=symbol,
        raw_reports=raw_reports,
        company=company,
        include_reports=include_reports,
    )


async def fetch_latest_listed_reports(
    nepse,
    symbols: Optional[Iterable[str]] = None,
    sector: Optional[str] = None,
    force_refresh: bool = False,
    concurrency: int = BULK_CONCURRENCY,
) -> Dict[str, Any]:
    """
    Return the latest quarterly and annual report for listed equity stocks.

    Results for the full market are cached in memory because NEPSE requires
    one request per company.
    """
    requested_symbols = [symbol.upper() for symbol in (symbols or []) if symbol]
    sector_filter = sector.strip().lower() if sector else None
    use_cache = not requested_symbols and not sector_filter and not force_refresh

    if use_cache:
        async with _latest_cache_lock:
            cached = _latest_cache.get("payload")
            if cached and time.time() < _latest_cache.get("expires_at", 0):
                return cached

    companies = [
        company
        for company in await nepse.getCompanyList()
        if is_active_equity(company)
    ]
    if requested_symbols:
        wanted = set(requested_symbols)
        companies = [company for company in companies if company["symbol"].upper() in wanted]
    if sector_filter:
        companies = [
            company
            for company in companies
            if (company.get("sectorName") or "").lower() == sector_filter
        ]

    semaphore = asyncio.Semaphore(concurrency)

    async def fetch_one(company: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        symbol = company["symbol"]
        async with semaphore:
            try:
                return await build_symbol_report(
                    nepse,
                    symbol,
                    company=company,
                    include_reports=False,
                )
            except Exception as exc:
                logger.warning("Failed to fetch financial reports for %s: %s", symbol, exc)
                return {
                    "symbol": symbol,
                    "companyName": company.get("companyName"),
                    "securityName": company.get("securityName"),
                    "sector": company.get("sectorName"),
                    "reportCount": 0,
                    "latestQuarterly": None,
                    "latestAnnual": None,
                    "error": str(exc),
                }

    results = [item for item in await asyncio.gather(*[fetch_one(company) for company in companies]) if item]
    results.sort(
        key=lambda item: (
            (item.get("latestQuarterly") or {}).get("modifiedDate")
            or (item.get("latestAnnual") or {}).get("modifiedDate")
            or ""
        ),
        reverse=True,
    )
    payload = {
        "count": len(results),
        "cached": False,
        "results": results,
    }
    if not requested_symbols and not sector_filter:
        async with _latest_cache_lock:
            _latest_cache["payload"] = {**payload, "cached": True}
            _latest_cache["expires_at"] = time.time() + LATEST_CACHE_TTL_SECONDS
    return payload
