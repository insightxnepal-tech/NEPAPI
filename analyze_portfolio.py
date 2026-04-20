import json
import csv
import pandas as pd
from pathlib import Path

def analyze_portfolio():
    # Paths
    portfolio_path = Path("/Users/sanishtamang/Downloads/floorsheet/portfolio_data.json")
    floorsheet_path = Path("/Users/sanishtamang/NEPAPI/floorsheet_2026-04-18.csv")
    stockmap_path = Path("/Users/sanishtamang/NEPAPI/stockmap.json")

    # Load Portfolio
    with open(portfolio_path, "r") as f:
        portfolio = json.load(f)

    # Load Stock Map for Names and Sectors
    with open(stockmap_path, "r") as f:
        stock_map = json.load(f)

    # Load Floorsheet to get LTP (Last Traded Price)
    # The floorsheet is usually sorted such that the most recent trades are first or manageable.
    # We'll build a map of Symbol -> Last Price
    ltp_map = {}
    
    # We read the CSV and keep the FIRST occurrence of each symbol (since it's sorted by ID desc)
    df_fs = pd.read_csv(floorsheet_path)
    # Group by stockSymbol and take the first contractRate
    # Assuming the file is already sorted by time/contractId desc
    # If not, we should sort it.
    df_fs = df_fs.sort_values('contractId', ascending=False)
    latest_prices = df_fs.groupby('stockSymbol')['contractRate'].first().to_dict()

    analysis_data = []
    total_cost = 0
    total_value = 0

    for symbol, data in portfolio.items():
        qty = data['qty']
        avg_rate = data['rate']
        cost = qty * avg_rate
        
        ltp = latest_prices.get(symbol, 0)
        current_value = qty * ltp
        pl = current_value - cost
        pl_pct = (pl / cost * 100) if cost > 0 else 0
        
        stock_info = stock_map.get(symbol, {})
        name = stock_info.get("name", "Unknown")
        sector = stock_info.get("sector", "Unknown")

        total_cost += cost
        total_value += current_value

        analysis_data.append({
            "Symbol": symbol,
            "Name": name,
            "Sector": sector,
            "Qty": qty,
            "Avg Rate": avg_rate,
            "LTP": ltp,
            "Cost": cost,
            "Value": current_value,
            "P/L": pl,
            "P/L %": pl_pct
        })

    # Convert to DataFrame for easier display
    df = pd.DataFrame(analysis_data)
    
    # Sort by P/L % descending
    df = df.sort_values("P/L %", ascending=False)

    print("\n" + "="*100)
    print(f"{'PORTFOLIO ANALYSIS REPORT':^100}")
    print("="*100)
    
    # Print Table
    print(df.to_string(index=False, formatters={
        'Avg Rate': '{:,.2f}'.format,
        'LTP': '{:,.2f}'.format,
        'Cost': '{:,.2f}'.format,
        'Value': '{:,.2f}'.format,
        'P/L': '{:,.2f}'.format,
        'P/L %': '{:+.2f}%'.format
    }))

    print("-" * 100)
    total_pl = total_value - total_cost
    total_pl_pct = (total_pl / total_cost * 100) if total_cost > 0 else 0
    
    print(f"Total Investment: Rs. {total_cost:,.2f}")
    print(f"Current Value:    Rs. {total_value:,.2f}")
    print(f"Overall P/L:      Rs. {total_pl:,.2f} ({total_pl_pct:+.2f}%)")
    print("="*100)

    # Sector Breakdown
    print("\nSECTOR BREAKDOWN:")
    sector_summary = df.groupby("Sector")["Value"].sum()
    sector_pct = (sector_summary / total_value * 100)
    for s, v in sector_pct.items():
        print(f"  {s:.<25} {v:>6.2f}% (Rs. {sector_summary[s]:,.2f})")

if __name__ == "__main__":
    analyze_portfolio()
