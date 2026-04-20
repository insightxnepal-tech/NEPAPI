#!/usr/bin/env python3
"""
fetch_floorsheet_csv.py
=======================
Fetch NEPSE floorsheet data and save to CSV.

Usage examples:
  # Today's full floorsheet
  python fetch_floorsheet_csv.py

  # Specific date
  python fetch_floorsheet_csv.py --date 2026-04-14

  # Specific symbol only (today)
  python fetch_floorsheet_csv.py --symbol NABIL

  # Specific symbol + specific date
  python fetch_floorsheet_csv.py --symbol NABIL --date 2026-04-14

  # Custom output path
  python fetch_floorsheet_csv.py --date 2026-04-14 --out /tmp/my_floorsheet.csv

  # Also save a companion JSON file
  python fetch_floorsheet_csv.py --save-json
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional

# ── third-party ───────────────────────────────────────────────────────────────
try:
    from nepse import AsyncNepse
except ImportError:
    print("[ERROR] 'nepse' package not found. Install it with:")
    print("  pip install git+https://github.com/basic-bgnr/NepseUnofficialApi.git")
    sys.exit(1)

try:
    import tqdm.asyncio as tqdm_asyncio
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

# ── helpers ───────────────────────────────────────────────────────────────────

def _today_str() -> str:
    return date.today().isoformat()  # e.g. "2026-04-15"


def _default_csv_name(date_str: str, symbol: Optional[str]) -> str:
    if symbol:
        return f"floorsheet_{symbol.upper()}_{date_str}.csv"
    return f"floorsheet_{date_str}.csv"


def _print(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ── core fetch ────────────────────────────────────────────────────────────────

async def _fetch_full_floorsheet(
    nepse: AsyncNepse,
    business_date: Optional[str],
) -> List[dict]:
    """Fetch ALL pages of the full market floorsheet for a given business date."""

    # Build URL — identical pattern to get_historical_fs.py
    base_url = nepse.api_end_points["floor_sheet"]
    if business_date:
        url = (
            f"{base_url}?&businessDate={business_date}"
            f"&size={nepse.floor_sheet_size}&sort=contractId,desc"
        )
    else:
        url = f"{base_url}?&size={nepse.floor_sheet_size}&sort=contractId,desc"

    _print(f"Fetching page 1 …")
    sheet = await nepse.requestPOSTAPI(
        url=url, payload_generator=nepse.getPOSTPayloadIDForFloorSheet
    )

    if not sheet or "floorsheets" not in sheet:
        _print("No floorsheet data returned from NEPSE.")
        return []

    content      = sheet["floorsheets"]["content"]
    max_page     = sheet["floorsheets"]["totalPages"]
    total_elements = sheet["floorsheets"].get("totalElements", "?")
    _print(f"Page 1/{max_page}  — total records: {total_elements}")

    if max_page <= 1:
        return content

    page_range = range(1, max_page)

    _print(f"Fetching remaining {len(page_range)} pages …")
    awaitables = [
        nepse._getFloorSheetPageNumber(url, page_num)
        for page_num in page_range
    ]

    if HAS_TQDM:
        remaining = await tqdm_asyncio.tqdm.gather(*awaitables, desc="pages")
    else:
        remaining = await asyncio.gather(*awaitables)

    all_pages = [content] + list(remaining)
    flat = [row for page in all_pages for row in page]
    return flat


async def _fetch_symbol_floorsheet(
    nepse: AsyncNepse,
    symbol: str,
) -> List[dict]:
    """Fetch floorsheet entries for a single symbol (uses getFloorSheetOf)."""
    _print(f"Fetching floorsheet for symbol: {symbol.upper()} …")
    data = await nepse.getFloorSheetOf(symbol.upper())
    if data is None:
        return []
    # getFloorSheetOf may return a list directly or a dict with a content key
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("content", "floorsheets", "data"):
            if key in data:
                inner = data[key]
                if isinstance(inner, dict) and "content" in inner:
                    return inner["content"]
                if isinstance(inner, list):
                    return inner
    return []


# ── write helpers ─────────────────────────────────────────────────────────────

def _write_csv(records: List[dict], path: Path) -> None:
    if not records:
        _print("No records to write — CSV not created.")
        return
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    _print(f"CSV saved  → {path}  ({len(records):,} rows)")


def _write_json(records: List[dict], path: Path) -> None:
    if not records:
        return
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(records, fh, indent=2, ensure_ascii=False)
    _print(f"JSON saved → {path}  ({len(records):,} records)")


# ── main ──────────────────────────────────────────────────────────────────────

async def main(args: argparse.Namespace) -> int:
    business_date: Optional[str] = args.date  # None → today (live session)
    symbol: Optional[str]        = args.symbol
    save_json: bool              = args.save_json
    out_path: Optional[str]      = args.out

    date_label = business_date or _today_str()

    # Resolve output path
    if out_path:
        csv_path = Path(out_path)
    else:
        csv_path = Path(_default_csv_name(date_label, symbol))

    # ── Init NEPSE client ────────────────────────────────────────────────────
    _print("Initialising AsyncNepse client …")
    nepse = AsyncNepse()
    nepse.setTLSVerification(False)

    # ── Fetch ────────────────────────────────────────────────────────────────
    try:
        if symbol:
            records = await _fetch_symbol_floorsheet(nepse, symbol)
            # If a specific date was requested, filter by it (API may not support date+symbol)
            if business_date and records:
                records = [
                    r for r in records
                    if str(r.get("businessDate", "")).startswith(business_date)
                ]
        else:
            records = await _fetch_full_floorsheet(nepse, business_date)
    except Exception as exc:
        _print(f"[ERROR] Fetch failed: {exc}")
        return 1

    _print(f"Total records fetched: {len(records):,}")

    if not records:
        _print("Nothing to save. Exiting.")
        return 0

    # ── Write CSV ────────────────────────────────────────────────────────────
    _write_csv(records, csv_path)

    # ── Optionally write JSON ────────────────────────────────────────────────
    if save_json:
        json_path = csv_path.with_suffix(".json")
        _write_json(records, json_path)

    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch NEPSE floorsheet data and export to CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--date", "-d",
        default=None,
        metavar="YYYY-MM-DD",
        help="Business date to fetch (default: today / live session)",
    )
    parser.add_argument(
        "--symbol", "-s",
        default=None,
        metavar="SYMBOL",
        help="Fetch only trades for this stock symbol (e.g. NABIL)",
    )
    parser.add_argument(
        "--out", "-o",
        default=None,
        metavar="PATH",
        help="Output CSV file path (default: floorsheet_<date>.csv)",
    )
    parser.add_argument(
        "--save-json",
        action="store_true",
        help="Also save a companion JSON file alongside the CSV",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    sys.exit(asyncio.run(main(args)))
