#!/usr/bin/env python3
"""
NEPSE Live Floorsheet Analyzer
Fetches and analyzes today's NEPSE floorsheet data.

Usage:
    python analyze_floorsheet.py [--date 2026-04-15] [--symbol NABIL] [--server http://localhost:8000]

Requirements:
    pip install requests pandas tabulate
"""

import argparse
import sys
from collections import defaultdict
from datetime import datetime

import requests
import pandas as pd

# ─────────────────────────── helpers ────────────────────────────────────────

def fmt_npr(amount: float) -> str:
    """Format a number as Nepalese Rupees (crore / lakh / plain)."""
    if amount >= 1_00_00_000:
        return f"NPR {amount / 1_00_00_000:.2f} Cr"
    if amount >= 1_00_000:
        return f"NPR {amount / 1_00_000:.2f} L"
    return f"NPR {amount:,.0f}"


def hr(char: str = "─", width: int = 72) -> str:
    return char * width


def section(title: str) -> None:
    print()
    print(hr("═"))
    print(f"  {title}")
    print(hr("═"))


def sub_section(title: str) -> None:
    print()
    print(f"  {title}")
    print(hr("─", 60))


# ─────────────────────────── fetch ──────────────────────────────────────────

def fetch_floorsheet(base_url: str, symbol: str | None = None) -> list[dict]:
    """
    Fetch floorsheet from the running NEPAPI REST server.
    Returns a flat list of trade-contract dicts.
    """
    if symbol:
        endpoint = f"{base_url}/FloorsheetOf?symbol={symbol.upper()}"
    else:
        endpoint = f"{base_url}/Floorsheet"

    try:
        resp = requests.get(endpoint, timeout=60)
        resp.raise_for_status()
    except requests.exceptions.ConnectionError:
        print(f"[ERROR] Cannot connect to NEPAPI server at {base_url}")
        print("  Make sure the server is running:  python server.py")
        sys.exit(1)
    except requests.exceptions.HTTPError as e:
        print(f"[ERROR] HTTP {e.response.status_code}: {e}")
        sys.exit(1)

    raw = resp.json()

    # The REST API may return a list directly or wrap it in a key
    if isinstance(raw, list):
        return raw
    for key in ("floorsheets", "floorsheet", "results", "data", "content"):
        if key in raw and isinstance(raw[key], list):
            return raw[key]
    # Last resort – flatten whatever is there
    if isinstance(raw, dict):
        for v in raw.values():
            if isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
                return v
    return []


def fetch_market_status(base_url: str) -> dict:
    try:
        resp = requests.get(f"{base_url}/health", timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return {}


# ─────────────────────────── analysis ───────────────────────────────────────

def build_dataframe(records: list[dict]) -> pd.DataFrame:
    """Convert raw API records to a clean, typed DataFrame."""
    df = pd.DataFrame(records)

    # Normalise column names (API may use camelCase or snake_case)
    rename = {
        "contractId": "contract_id",
        "stockSymbol": "symbol",
        "buyerMemberId": "buyer_id",
        "sellerMemberId": "seller_id",
        "contractQuantity": "quantity",
        "contractRate": "rate",
        "contractAmount": "amount",
        "businessDate": "date",
        "tradeTime": "trade_time",
        "buyerBrokerName": "buyer_broker",
        "sellerBrokerName": "seller_broker",
        "securityName": "security_name",
        "tradeBookId": "trade_book_id",
        "stockId": "stock_id",
        # snake_case variants (already normalised)
        "stock_symbol": "symbol",
        "buyer_member_id": "buyer_id",
        "seller_member_id": "seller_id",
        "contract_quantity": "quantity",
        "contract_rate": "rate",
        "contract_amount": "amount",
        "business_date": "date",
        "buyer_broker_name": "buyer_broker",
        "seller_broker_name": "seller_broker",
    }
    df.rename(columns={k: v for k, v in rename.items() if k in df.columns}, inplace=True)

    for col in ("quantity", "rate", "amount"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "trade_time" in df.columns:
        df["trade_time"] = pd.to_datetime(df["trade_time"], errors="coerce")
        df["hour"] = df["trade_time"].dt.hour

    return df


# ─────────────────────────── report sections ─────────────────────────────────

def print_market_overview(df: pd.DataFrame, target_date: str) -> None:
    section(f"NEPSE FLOORSHEET ANALYSIS  ·  {target_date}")

    total_contracts = len(df)
    total_volume = df["quantity"].sum() if "quantity" in df.columns else 0
    total_turnover = df["amount"].sum() if "amount" in df.columns else 0
    unique_scrips = df["symbol"].nunique() if "symbol" in df.columns else 0
    avg_rate = (total_turnover / total_volume) if total_volume else 0

    print(f"\n  {'Total Contracts':30s}  {total_contracts:>12,}")
    print(f"  {'Unique Scrips Traded':30s}  {unique_scrips:>12,}")
    print(f"  {'Total Volume (shares)':30s}  {total_volume:>12,.0f}")
    print(f"  {'Total Turnover':30s}  {fmt_npr(total_turnover):>20}")
    print(f"  {'Avg Trade Rate':30s}  NPR {avg_rate:>12,.2f}")


def print_top_scrips_by_turnover(df: pd.DataFrame, n: int = 10) -> None:
    sub_section(f"Top {n} Scrips by Turnover")
    grp = (
        df.groupby("symbol")
        .agg(turnover=("amount", "sum"), volume=("quantity", "sum"), contracts=("amount", "count"))
        .sort_values("turnover", ascending=False)
        .head(n)
        .reset_index()
    )
    grp["turnover_str"] = grp["turnover"].apply(fmt_npr)
    grp["volume_str"] = grp["volume"].apply(lambda x: f"{x:,.0f}")
    grp.index += 1
    print(grp[["symbol", "turnover_str", "volume_str", "contracts"]]
          .rename(columns={"symbol": "Symbol", "turnover_str": "Turnover",
                           "volume_str": "Volume", "contracts": "Contracts"})
          .to_string())


def print_top_scrips_by_volume(df: pd.DataFrame, n: int = 10) -> None:
    sub_section(f"Top {n} Scrips by Volume (shares)")
    grp = (
        df.groupby("symbol")
        .agg(volume=("quantity", "sum"), avg_rate=("rate", "mean"), contracts=("amount", "count"))
        .sort_values("volume", ascending=False)
        .head(n)
        .reset_index()
    )
    grp["volume_str"] = grp["volume"].apply(lambda x: f"{x:,.0f}")
    grp["avg_rate_str"] = grp["avg_rate"].apply(lambda x: f"NPR {x:,.2f}")
    grp.index += 1
    print(grp[["symbol", "volume_str", "avg_rate_str", "contracts"]]
          .rename(columns={"symbol": "Symbol", "volume_str": "Volume",
                           "avg_rate_str": "Avg Rate", "contracts": "Contracts"})
          .to_string())


def print_top_scrips_by_contracts(df: pd.DataFrame, n: int = 10) -> None:
    sub_section(f"Top {n} Scrips by Number of Contracts")
    grp = (
        df.groupby("symbol")
        .agg(contracts=("amount", "count"), turnover=("amount", "sum"))
        .sort_values("contracts", ascending=False)
        .head(n)
        .reset_index()
    )
    grp["turnover_str"] = grp["turnover"].apply(fmt_npr)
    grp.index += 1
    print(grp[["symbol", "contracts", "turnover_str"]]
          .rename(columns={"symbol": "Symbol", "contracts": "Contracts",
                           "turnover_str": "Turnover"})
          .to_string())


def print_broker_analysis(df: pd.DataFrame, n: int = 10) -> None:
    section("BROKER ACTIVITY ANALYSIS")

    # ── Top Buyers ────────────────────────────────────────────────────────
    if "buyer_broker" in df.columns:
        sub_section(f"Top {n} Buying Brokers (by purchase value)")
        buyers = (
            df.groupby("buyer_broker")
            .agg(buy_amount=("amount", "sum"), buy_qty=("quantity", "sum"), contracts=("amount", "count"))
            .sort_values("buy_amount", ascending=False)
            .head(n)
            .reset_index()
        )
        buyers["buy_amount_str"] = buyers["buy_amount"].apply(fmt_npr)
        buyers["buy_qty_str"] = buyers["buy_qty"].apply(lambda x: f"{x:,.0f}")
        buyers.index += 1
        print(buyers[["buyer_broker", "buy_amount_str", "buy_qty_str", "contracts"]]
              .rename(columns={"buyer_broker": "Buyer Broker", "buy_amount_str": "Purchase Value",
                               "buy_qty_str": "Qty Bought", "contracts": "Contracts"})
              .to_string())

    # ── Top Sellers ───────────────────────────────────────────────────────
    if "seller_broker" in df.columns:
        sub_section(f"Top {n} Selling Brokers (by sale value)")
        sellers = (
            df.groupby("seller_broker")
            .agg(sell_amount=("amount", "sum"), sell_qty=("quantity", "sum"), contracts=("amount", "count"))
            .sort_values("sell_amount", ascending=False)
            .head(n)
            .reset_index()
        )
        sellers["sell_amount_str"] = sellers["sell_amount"].apply(fmt_npr)
        sellers["sell_qty_str"] = sellers["sell_qty"].apply(lambda x: f"{x:,.0f}")
        sellers.index += 1
        print(sellers[["seller_broker", "sell_amount_str", "sell_qty_str", "contracts"]]
              .rename(columns={"seller_broker": "Seller Broker", "sell_amount_str": "Sale Value",
                               "sell_qty_str": "Qty Sold", "contracts": "Contracts"})
              .to_string())

    # ── Net Flow (buy - sell) per broker ─────────────────────────────────
    if "buyer_broker" in df.columns and "seller_broker" in df.columns:
        sub_section("Broker Net Flow (Buy Value − Sell Value)  ·  Top 10 Net Buyers / Sellers")
        buy_map = df.groupby("buyer_broker")["amount"].sum()
        sell_map = df.groupby("seller_broker")["amount"].sum()
        all_brokers = set(buy_map.index) | set(sell_map.index)
        net = {b: buy_map.get(b, 0) - sell_map.get(b, 0) for b in all_brokers}
        net_df = pd.DataFrame(net.items(), columns=["Broker", "net_flow"]).sort_values("net_flow", ascending=False)
        net_df["Net Flow"] = net_df["net_flow"].apply(
            lambda x: f"+{fmt_npr(x)}" if x >= 0 else f"-{fmt_npr(abs(x))}"
        )
        print("  Net Buyers:")
        print(net_df[net_df["net_flow"] > 0].head(10)[["Broker", "Net Flow"]].to_string(index=False))
        print("\n  Net Sellers:")
        print(net_df[net_df["net_flow"] < 0].tail(10).iloc[::-1][["Broker", "Net Flow"]].to_string(index=False))


def print_price_analysis(df: pd.DataFrame, n: int = 10) -> None:
    section("PRICE ANALYSIS PER SCRIP")

    grp = df.groupby("symbol").agg(
        high=("rate", "max"),
        low=("rate", "min"),
        avg=("rate", "mean"),
        last=("rate", "last"),
        volume=("quantity", "sum"),
    ).reset_index()

    sub_section(f"Top {n} Scrips by High Price")
    top_high = grp.sort_values("high", ascending=False).head(n).copy()
    top_high.index = range(1, len(top_high) + 1)
    for col in ("high", "low", "avg", "last"):
        top_high[col] = top_high[col].apply(lambda x: f"NPR {x:,.2f}")
    top_high["volume"] = top_high["volume"].apply(lambda x: f"{x:,.0f}")
    print(top_high[["symbol", "high", "low", "avg", "last", "volume"]]
          .rename(columns={"symbol": "Symbol", "high": "High", "low": "Low",
                           "avg": "Avg Rate", "last": "Last Rate", "volume": "Volume"})
          .to_string())

    sub_section(f"Top {n} Highest Spread (High − Low)")
    grp["spread"] = (
        df.groupby("symbol")["rate"].max() - df.groupby("symbol")["rate"].min()
    ).values
    top_spread = grp.sort_values("spread", ascending=False).head(n).copy()
    top_spread.index = range(1, len(top_spread) + 1)
    top_spread["spread_str"] = top_spread["spread"].apply(lambda x: f"NPR {x:,.2f}")
    print(top_spread[["symbol", "spread_str"]]
          .rename(columns={"symbol": "Symbol", "spread_str": "Price Spread"})
          .to_string())


def print_hourly_distribution(df: pd.DataFrame) -> None:
    if "hour" not in df.columns or df["hour"].isna().all():
        return
    section("TRADE TIMELINE (Hourly Distribution)")
    hourly = (
        df.groupby("hour")
        .agg(contracts=("amount", "count"), turnover=("amount", "sum"), volume=("quantity", "sum"))
        .reset_index()
    )
    hourly["hour_str"] = hourly["hour"].apply(lambda h: f"{h:02d}:00")
    hourly["turnover_str"] = hourly["turnover"].apply(fmt_npr)
    hourly["volume_str"] = hourly["volume"].apply(lambda x: f"{x:,.0f}")
    # Simple bar chart
    max_c = hourly["contracts"].max()
    print(f"\n  {'Hour':8s} {'Contracts':>10s}  {'Volume':>14s}  {'Turnover':>18s}  Bar")
    print(f"  {hr('-', 68)}")
    for _, row in hourly.iterrows():
        bar_len = int(row["contracts"] / max_c * 30)
        bar = "█" * bar_len
        print(f"  {row['hour_str']:8s} {int(row['contracts']):>10,}  {row['volume_str']:>14s}  {row['turnover_str']:>18s}  {bar}")


def print_single_scrip_analysis(df: pd.DataFrame, symbol: str) -> None:
    df_s = df[df["symbol"].str.upper() == symbol.upper()].copy() if "symbol" in df.columns else df.copy()
    if df_s.empty:
        print(f"\n  No trades found for symbol: {symbol}")
        return

    section(f"SCRIP ANALYSIS  ·  {symbol.upper()}")
    total_contracts = len(df_s)
    total_volume = df_s["quantity"].sum()
    total_turnover = df_s["amount"].sum()
    high = df_s["rate"].max()
    low = df_s["rate"].min()
    avg = df_s["rate"].mean()
    last = df_s["rate"].iloc[-1]

    print(f"\n  {'Contracts':30s}  {total_contracts:>10,}")
    print(f"  {'Volume':30s}  {total_volume:>10,.0f}")
    print(f"  {'Turnover':30s}  {fmt_npr(total_turnover):>20}")
    print(f"  {'High Rate':30s}  NPR {high:>10,.2f}")
    print(f"  {'Low Rate':30s}  NPR {low:>10,.2f}")
    print(f"  {'Avg Rate':30s}  NPR {avg:>10,.2f}")
    print(f"  {'Last Rate':30s}  NPR {last:>10,.2f}")

    if "buyer_broker" in df_s.columns:
        sub_section("Top 5 Buyers for this scrip")
        b = df_s.groupby("buyer_broker")["amount"].sum().sort_values(ascending=False).head(5)
        for broker, amt in b.items():
            print(f"  {broker:<40s}  {fmt_npr(amt):>18}")

    if "seller_broker" in df_s.columns:
        sub_section("Top 5 Sellers for this scrip")
        s = df_s.groupby("seller_broker")["amount"].sum().sort_values(ascending=False).head(5)
        for broker, amt in s.items():
            print(f"  {broker:<40s}  {fmt_npr(amt):>18}")


# ─────────────────────────── save CSV ────────────────────────────────────────

def save_csv(df: pd.DataFrame, target_date: str, symbol: str | None) -> None:
    filename = f"floorsheet_{target_date.replace('-', '')}"
    if symbol:
        filename += f"_{symbol.upper()}"
    filename += ".csv"
    df.to_csv(filename, index=False)
    print(f"\n  Saved raw data to: {filename}")


# ─────────────────────────── main ────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Analyze NEPSE live floorsheet data via the NEPAPI REST server."
    )
    parser.add_argument("--date", default="2026-04-15",
                        help="Business date to analyse (YYYY-MM-DD). Default: 2026-04-15")
    parser.add_argument("--symbol", default=None,
                        help="Filter analysis to a single stock symbol, e.g. NABIL")
    parser.add_argument("--server", default="http://localhost:8000",
                        help="Base URL of the running NEPAPI server. Default: http://localhost:8000")
    parser.add_argument("--top", type=int, default=10,
                        help="Number of top items to show in ranked lists. Default: 10")
    parser.add_argument("--save-csv", action="store_true",
                        help="Save the raw floorsheet data to a CSV file.")
    args = parser.parse_args()

    target_date = args.date
    symbol = args.symbol
    base_url = args.server.rstrip("/")
    n = args.top

    print(hr("═"))
    print(f"  NEPSE FLOORSHEET LIVE ANALYSIS")
    print(f"  Date    : {target_date}")
    print(f"  Symbol  : {symbol or 'ALL'}")
    print(f"  Server  : {base_url}")
    print(f"  Run at  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(hr("═"))

    print("\n  Fetching floorsheet data...", end=" ", flush=True)
    records = fetch_floorsheet(base_url, symbol)
    print(f"got {len(records):,} records.")

    if not records:
        print("\n  [INFO] No records returned.")
        print("  Possible reasons:")
        print("    • Market is still open  (floorsheet is published after close)")
        print("    • No trades for the requested symbol")
        print("    • Server returned an empty response")
        sys.exit(0)

    df = build_dataframe(records)

    # Filter by date if the column exists and date differs
    if "date" in df.columns:
        dates_present = df["date"].dropna().unique().tolist()
        matching = df[df["date"].astype(str).str.startswith(target_date)]
        if len(matching) > 0:
            df = matching
        else:
            print(f"\n  [WARN] No records found for date {target_date}.")
            print(f"  Dates in data: {dates_present[:5]}")
            print("  Showing all records instead.\n")

    if args.save_csv:
        save_csv(df, target_date, symbol)

    # ── Report ───────────────────────────────────────────────────────────
    if symbol:
        print_single_scrip_analysis(df, symbol)
    else:
        print_market_overview(df, target_date)

        section("SCRIP-LEVEL ACTIVITY")
        print_top_scrips_by_turnover(df, n)
        print_top_scrips_by_volume(df, n)
        print_top_scrips_by_contracts(df, n)

        print_broker_analysis(df, n)
        print_price_analysis(df, n)
        print_hourly_distribution(df)

    print()
    print(hr("═"))
    print("  Analysis complete.")
    print(hr("═"))


if __name__ == "__main__":
    main()
