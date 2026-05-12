#!/usr/bin/env python3
"""
NEPSE Floorsheet → CSV Fetcher
Fetches today's NEPSE floorsheet data and saves it as a CSV file.

Usage:
    python fetch_floorsheet_csv.py                           # live, all scrips
    python fetch_floorsheet_csv.py --symbol NABIL            # single scrip
    python fetch_floorsheet_csv.py --date 2026-04-15         # specific date
    python fetch_floorsheet_csv.py --demo                    # use built-in sample data
    python fetch_floorsheet_csv.py --out my_file.csv         # custom filename

Requirements:
    pip install requests pandas
"""

import argparse
import sys
from datetime import datetime
from io import StringIO

import requests
import pandas as pd

# ── Sample / fallback data (mirrors the real API field names) ─────────────────
SAMPLE_DATA = [
    {"contractId": 1001, "stockSymbol": "NABIL",  "buyerMemberId": "12", "sellerMemberId": "28",
     "contractQuantity": 500,  "contractRate": 1285.00, "contractAmount":  642500.00,
     "businessDate": "2026-04-15", "tradeBookId": 9001, "stockId": 101,
     "buyerBrokerName": "Sunrise Capital",     "sellerBrokerName": "Siddhartha Capital",
     "tradeTime": "2026-04-15 11:05:12", "securityName": "Nabil Bank Limited"},
    {"contractId": 1002, "stockSymbol": "NABIL",  "buyerMemberId": "35", "sellerMemberId": "12",
     "contractQuantity": 300,  "contractRate": 1287.00, "contractAmount":  386100.00,
     "businessDate": "2026-04-15", "tradeBookId": 9001, "stockId": 101,
     "buyerBrokerName": "Nepal SBI",           "sellerBrokerName": "Sunrise Capital",
     "tradeTime": "2026-04-15 11:08:44", "securityName": "Nabil Bank Limited"},
    {"contractId": 1003, "stockSymbol": "SCB",    "buyerMemberId": "07", "sellerMemberId": "44",
     "contractQuantity": 1200, "contractRate":  620.50, "contractAmount":  744600.00,
     "businessDate": "2026-04-15", "tradeBookId": 9002, "stockId": 105,
     "buyerBrokerName": "Arun Securities",     "sellerBrokerName": "Laxmi Securities",
     "tradeTime": "2026-04-15 11:12:03", "securityName": "Standard Chartered Bank"},
    {"contractId": 1004, "stockSymbol": "NICA",   "buyerMemberId": "19", "sellerMemberId": "07",
     "contractQuantity": 800,  "contractRate":  910.00, "contractAmount":  728000.00,
     "businessDate": "2026-04-15", "tradeBookId": 9003, "stockId": 112,
     "buyerBrokerName": "Siddhartha Capital",  "sellerBrokerName": "Arun Securities",
     "tradeTime": "2026-04-15 11:15:30", "securityName": "NIC Asia Bank"},
    {"contractId": 1005, "stockSymbol": "SANIMA", "buyerMemberId": "44", "sellerMemberId": "19",
     "contractQuantity": 600,  "contractRate":  755.00, "contractAmount":  453000.00,
     "businessDate": "2026-04-15", "tradeBookId": 9004, "stockId": 118,
     "buyerBrokerName": "Laxmi Securities",    "sellerBrokerName": "Siddhartha Capital",
     "tradeTime": "2026-04-15 11:19:55", "securityName": "Sanima Bank"},
    {"contractId": 1006, "stockSymbol": "HBL",    "buyerMemberId": "28", "sellerMemberId": "35",
     "contractQuantity": 450,  "contractRate": 1100.00, "contractAmount":  495000.00,
     "businessDate": "2026-04-15", "tradeBookId": 9005, "stockId": 123,
     "buyerBrokerName": "Siddhartha Capital",  "sellerBrokerName": "Nepal SBI",
     "tradeTime": "2026-04-15 11:23:10", "securityName": "Himalayan Bank"},
    {"contractId": 1007, "stockSymbol": "EBL",    "buyerMemberId": "07", "sellerMemberId": "28",
     "contractQuantity": 700,  "contractRate": 1050.00, "contractAmount":  735000.00,
     "businessDate": "2026-04-15", "tradeBookId": 9006, "stockId": 127,
     "buyerBrokerName": "Arun Securities",     "sellerBrokerName": "Siddhartha Capital",
     "tradeTime": "2026-04-15 11:27:40", "securityName": "Everest Bank"},
    {"contractId": 1008, "stockSymbol": "GBIME",  "buyerMemberId": "12", "sellerMemberId": "07",
     "contractQuantity": 950,  "contractRate":  480.00, "contractAmount":  456000.00,
     "businessDate": "2026-04-15", "tradeBookId": 9007, "stockId": 133,
     "buyerBrokerName": "Sunrise Capital",     "sellerBrokerName": "Arun Securities",
     "tradeTime": "2026-04-15 11:31:05", "securityName": "Global IME Bank"},
    {"contractId": 1009, "stockSymbol": "MBL",    "buyerMemberId": "35", "sellerMemberId": "12",
     "contractQuantity": 1100, "contractRate":  430.00, "contractAmount":  473000.00,
     "businessDate": "2026-04-15", "tradeBookId": 9008, "stockId": 138,
     "buyerBrokerName": "Nepal SBI",           "sellerBrokerName": "Sunrise Capital",
     "tradeTime": "2026-04-15 11:34:22", "securityName": "Machhapuchchhre Bank"},
    {"contractId": 1010, "stockSymbol": "PRVU",   "buyerMemberId": "19", "sellerMemberId": "35",
     "contractQuantity": 400,  "contractRate":  395.00, "contractAmount":  158000.00,
     "businessDate": "2026-04-15", "tradeBookId": 9009, "stockId": 145,
     "buyerBrokerName": "Siddhartha Capital",  "sellerBrokerName": "Nepal SBI",
     "tradeTime": "2026-04-15 11:38:50", "securityName": "Prabhu Bank"},
    {"contractId": 1011, "stockSymbol": "NABIL",  "buyerMemberId": "44", "sellerMemberId": "19",
     "contractQuantity": 200,  "contractRate": 1290.00, "contractAmount":  258000.00,
     "businessDate": "2026-04-15", "tradeBookId": 9001, "stockId": 101,
     "buyerBrokerName": "Laxmi Securities",    "sellerBrokerName": "Siddhartha Capital",
     "tradeTime": "2026-04-15 11:42:17", "securityName": "Nabil Bank Limited"},
    {"contractId": 1012, "stockSymbol": "SCB",    "buyerMemberId": "28", "sellerMemberId": "44",
     "contractQuantity": 500,  "contractRate":  622.00, "contractAmount":  311000.00,
     "businessDate": "2026-04-15", "tradeBookId": 9002, "stockId": 105,
     "buyerBrokerName": "Siddhartha Capital",  "sellerBrokerName": "Laxmi Securities",
     "tradeTime": "2026-04-15 11:46:33", "securityName": "Standard Chartered Bank"},
    {"contractId": 1013, "stockSymbol": "NICA",   "buyerMemberId": "07", "sellerMemberId": "28",
     "contractQuantity": 1500, "contractRate":  912.00, "contractAmount": 1368000.00,
     "businessDate": "2026-04-15", "tradeBookId": 9003, "stockId": 112,
     "buyerBrokerName": "Arun Securities",     "sellerBrokerName": "Siddhartha Capital",
     "tradeTime": "2026-04-15 11:50:08", "securityName": "NIC Asia Bank"},
    {"contractId": 1014, "stockSymbol": "KBL",    "buyerMemberId": "12", "sellerMemberId": "07",
     "contractQuantity": 850,  "contractRate":  360.00, "contractAmount":  306000.00,
     "businessDate": "2026-04-15", "tradeBookId": 9010, "stockId": 151,
     "buyerBrokerName": "Sunrise Capital",     "sellerBrokerName": "Arun Securities",
     "tradeTime": "2026-04-15 11:53:45", "securityName": "Kumari Bank"},
    {"contractId": 1015, "stockSymbol": "MEGA",   "buyerMemberId": "35", "sellerMemberId": "12",
     "contractQuantity": 620,  "contractRate":  310.00, "contractAmount":  192200.00,
     "businessDate": "2026-04-15", "tradeBookId": 9011, "stockId": 157,
     "buyerBrokerName": "Nepal SBI",           "sellerBrokerName": "Sunrise Capital",
     "tradeTime": "2026-04-15 11:57:10", "securityName": "Mega Bank Nepal"},
]

COLUMNS_ORDER = [
    "S.N.", "Contract ID", "Stock Symbol", "Security Name",
    "Buyer Member ID", "Seller Member ID",
    "Buyer Broker", "Seller Broker",
    "Quantity", "Rate (NPR)", "Amount (NPR)",
    "Business Date", "Trade Time",
    "Trade Book ID", "Stock ID",
]


# ── helpers ───────────────────────────────────────────────────────────────────

def records_to_df(records: list[dict], symbol_filter: str | None = None) -> pd.DataFrame:
    df = pd.DataFrame(records)

    # Normalise camelCase → friendly display names
    rename = {
        "contractId":      "Contract ID",
        "stockSymbol":     "Stock Symbol",
        "securityName":    "Security Name",
        "buyerMemberId":   "Buyer Member ID",
        "sellerMemberId":  "Seller Member ID",
        "buyerBrokerName": "Buyer Broker",
        "sellerBrokerName":"Seller Broker",
        "contractQuantity":"Quantity",
        "contractRate":    "Rate (NPR)",
        "contractAmount":  "Amount (NPR)",
        "businessDate":    "Business Date",
        "tradeTime":       "Trade Time",
        "tradeBookId":     "Trade Book ID",
        "stockId":         "Stock ID",
    }
    df.rename(columns=rename, inplace=True)

    if symbol_filter and "Stock Symbol" in df.columns:
        df = df[df["Stock Symbol"].str.upper() == symbol_filter.upper()].copy()

    df.insert(0, "S.N.", range(1, len(df) + 1))

    # Keep only known columns (in order), drop extras
    existing = [c for c in COLUMNS_ORDER if c in df.columns]
    return df[existing]


def fetch_live(base_url: str, symbol: str | None) -> list[dict] | None:
    endpoint = f"{base_url}/FloorsheetOf?symbol={symbol.upper()}" if symbol else f"{base_url}/Floorsheet"
    try:
        resp = requests.get(endpoint, timeout=60)
        resp.raise_for_status()
        raw = resp.json()
    except requests.exceptions.ConnectionError:
        print(f"  [WARN] Cannot connect to server at {base_url}")
        return None
    except requests.exceptions.HTTPError as e:
        print(f"  [WARN] Server error: {e}")
        return None

    if isinstance(raw, list):
        return raw
    for key in ("floorsheets", "floorsheet", "results", "data", "content"):
        if key in raw and isinstance(raw[key], list):
            return raw[key]
    if isinstance(raw, dict):
        for v in raw.values():
            if isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
                return v
    return []


def build_output_filename(date_str: str, symbol: str | None, demo: bool) -> str:
    tag = f"_{symbol.upper()}" if symbol else ""
    suffix = "_DEMO" if demo else ""
    return f"nepse_floorsheet_{date_str.replace('-', '')}{tag}{suffix}.csv"


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch NEPSE floorsheet and save as CSV.")
    parser.add_argument("--date",   default="2026-04-15", help="Business date (YYYY-MM-DD)")
    parser.add_argument("--symbol", default=None,         help="Filter to one symbol, e.g. NABIL")
    parser.add_argument("--server", default="http://localhost:8000", help="NEPAPI server URL")
    parser.add_argument("--demo",   action="store_true",  help="Use built-in sample data (no server needed)")
    parser.add_argument("--out",    default=None,         help="Output CSV filename (auto-generated if omitted)")
    args = parser.parse_args()

    print("═" * 60)
    print("  NEPSE Floorsheet CSV Fetcher")
    print(f"  Date   : {args.date}")
    print(f"  Symbol : {args.symbol or 'ALL'}")
    print(f"  Mode   : {'DEMO (sample data)' if args.demo else 'LIVE'}")
    print("═" * 60)

    demo_mode = args.demo

    if not demo_mode:
        print(f"\n  Connecting to {args.server} ...", end=" ", flush=True)
        records = fetch_live(args.server, args.symbol)
        if records is None:
            print("failed.")
            print("\n  Falling back to built-in sample data for demonstration.")
            demo_mode = True
            records = SAMPLE_DATA
        elif len(records) == 0:
            print("got 0 records.")
            print("\n  No data returned (market may still be open).")
            print("  NEPSE floorsheet is published after market close (~3 PM NPT).")
            print("  Use --demo to see a sample output instead.")
            sys.exit(0)
        else:
            print(f"got {len(records):,} records.")
    else:
        records = SAMPLE_DATA
        print(f"\n  Using {len(records)} built-in sample records.")

    # Build DataFrame
    df = records_to_df(records, args.symbol if not demo_mode else None)

    # Filter by date if column exists
    if "Business Date" in df.columns and not demo_mode:
        mask = df["Business Date"].astype(str).str.startswith(args.date)
        filtered = df[mask]
        if len(filtered) > 0:
            df = filtered

    # Output filename
    out_file = args.out or build_output_filename(args.date, args.symbol, demo_mode)

    # Save CSV
    df.to_csv(out_file, index=False)

    # Summary stats
    total_qty    = pd.to_numeric(df["Quantity"],    errors="coerce").sum()
    total_amount = pd.to_numeric(df["Amount (NPR)"],errors="coerce").sum()
    unique_scrips = df["Stock Symbol"].nunique() if "Stock Symbol" in df.columns else "—"

    print(f"\n  ✔  Saved {len(df):,} records → {out_file}")
    print(f"\n  Quick summary:")
    print(f"    Contracts      : {len(df):,}")
    print(f"    Unique scrips  : {unique_scrips}")
    print(f"    Total volume   : {total_qty:,.0f} shares")
    print(f"    Total turnover : NPR {total_amount:,.2f}")

    # Preview first 5 rows
    print(f"\n  First 5 rows preview:")
    print(df.head(5).to_string(index=False))
    print()
    print("═" * 60)


if __name__ == "__main__":
    main()
