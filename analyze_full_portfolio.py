import pandas as pd
import glob
import os
import re
import json
from pathlib import Path

def get_date_from_filename(filename):
    match = re.search(r'(\d{4})[_-](\d{2})[_-](\d{2})', filename)
    if match:
        return "-".join(match.groups())
    return None

def analyze_full_portfolio(days=15):
    portfolio_path = Path("/Users/sanishtamang/Downloads/floorsheet/portfolio_data.json")
    with open(portfolio_path, "r") as f:
        portfolio = json.load(f)
    
    symbols = list(portfolio.keys())
    
    paths = [
        "/Users/sanishtamang/NEPAPI/floorsheet*.csv",
        "/Users/sanishtamang/Downloads/floorsheet/floorsheet*.csv",
        "/Users/sanishtamang/Downloads/floorsheet/floorsheet*.xlsx"
    ]
    
    all_files = []
    for p in paths:
        all_files.extend(glob.glob(p))
    
    file_info = []
    for f in all_files:
        dt = get_date_from_filename(os.path.basename(f))
        if dt:
            file_info.append((dt, f))
    
    file_info.sort(key=lambda x: x[0], reverse=True)
    unique_files = {}
    for dt, f in file_info:
        if dt not in unique_files:
            unique_files[dt] = f
        else:
            if f.endswith('.csv'):
                unique_files[dt] = f

    selected_dates = sorted(unique_files.keys(), reverse=True)[:days]
    print(f"Analyzing {len(selected_dates)} days of data for {len(symbols)} portfolio stocks...")
    
    cumulative_data = {sym: pd.DataFrame() for sym in symbols}
    
    for dt in selected_dates:
        f = unique_files[dt]
        try:
            if f.endswith('.csv'):
                df = pd.read_csv(f)
            else:
                df = pd.read_excel(f, engine='openpyxl')
            
            for sym in symbols:
                df_sym = df[df['stockSymbol'] == sym]
                cumulative_data[sym] = pd.concat([cumulative_data[sym], df_sym])
        except Exception as e:
            print(f"Error loading {f}: {e}")

    results = []

    for sym in symbols:
        df_sym = cumulative_data[sym]
        if df_sym.empty:
            continue
            
        total_qty = df_sym['contractQuantity'].sum()
        total_amt = df_sym['contractAmount'].sum()
        vwap = total_amt / total_qty
        
        # Broker Stats
        buyer_stats = df_sym.groupby('buyerMemberId')['contractQuantity'].sum()
        seller_stats = df_sym.groupby('sellerMemberId')['contractQuantity'].sum()
        
        all_brokers = set(buyer_stats.index) | set(seller_stats.index)
        summary = []
        for b in all_brokers:
            summary.append({
                'Broker': b,
                'Net': buyer_stats.get(b, 0) - seller_stats.get(b, 0)
            })
            
        df_br = pd.DataFrame(summary)
        top_5_acc = df_br.sort_values('Net', ascending=False).head(5)['Net'].sum()
        top_5_dist = df_br.sort_values('Net', ascending=True).head(5)['Net'].sum()
        
        # Trend Logic
        # If top 5 buyers net volume > top 5 sellers net volume (abs) -> Accumulation
        # We'll use a ratio or sign
        score = top_5_acc + top_5_dist # dist is negative
        
        sentiment = "Neutral"
        if score > total_qty * 0.05: # Strong accumulation
            sentiment = "Accumulation"
        elif score < -total_qty * 0.05: # Strong distribution
            sentiment = "Distribution"
            
        results.append({
            "Symbol": sym,
            "Volume": total_qty,
            "VWAP": vwap,
            "Top 5 Buyers": top_5_acc,
            "Top 5 Sellers": top_5_dist,
            "Net Score": score,
            "Sentiment": sentiment
        })

    df_final = pd.DataFrame(results)
    
    print("\n" + "="*100)
    print(f"{'FULL PORTFOLIO INSTITUTIONAL ANALYSIS (15 DAYS)':^100}")
    print("="*100)
    print(df_final.to_string(index=False, formatters={
        'Volume': '{:,.0f}'.format,
        'VWAP': '{:,.2f}'.format,
        'Top 5 Buyers': '{:+,.0f}'.format,
        'Top 5 Sellers': '{:,.0f}'.format,
        'Net Score': '{:+,.0f}'.format
    }))
    print("="*100)

if __name__ == "__main__":
    analyze_full_portfolio(days=15)
