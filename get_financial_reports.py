#!/usr/bin/env python3
"""
Fetch latest NEPSE listed-stock financial reports.

Examples:
  python get_financial_reports.py NABIL
  python get_financial_reports.py --latest
  python get_financial_reports.py --sector "Commercial Banks"
"""

import argparse
import asyncio
import json

from nepse import AsyncNepse
from financial_reports import build_symbol_report, fetch_latest_listed_reports


async def main():
    parser = argparse.ArgumentParser(description="Fetch NEPSE financial reports")
    parser.add_argument("symbol", nargs="?", help="Stock symbol, for example NABIL")
    parser.add_argument("--latest", action="store_true", help="Fetch latest reports for all active equity listings")
    parser.add_argument("--sector", help="Limit the latest snapshot to one sector")
    parser.add_argument("--force-refresh", action="store_true", help="Bypass the in-memory latest-reports cache")
    parser.add_argument("-o", "--output", default="financial_reports.json", help="Output JSON path")
    args = parser.parse_args()

    if not args.symbol and not args.latest and not args.sector:
        parser.error("provide a symbol, or use --latest / --sector")

    nepse = AsyncNepse()
    nepse.setTLSVerification(False)

    if args.symbol and not args.latest:
        data = await build_symbol_report(nepse, args.symbol.upper(), include_reports=True)
    else:
        symbols = [args.symbol] if args.symbol else None
        data = await fetch_latest_listed_reports(
            nepse,
            symbols=symbols,
            sector=args.sector,
            force_refresh=args.force_refresh,
        )

    with open(args.output, "w") as handle:
        json.dump(data, handle, indent=2)
    print(json.dumps(data, indent=2)[:4000])
    print(f"\nSaved to {args.output}")


if __name__ == "__main__":
    asyncio.run(main())
