import pandas as pd
import glob
import os
import re
from pathlib import Path

def get_date_from_filename(filename):
    match = re.search(r'(\d{4})[_-](\d{2})[_-](\d{2})', filename)
    if match:
        return "-".join(match.groups())
    return None

def analyze_historical(symbols, days=15):
    paths = [
        "/Users/sanishtamang/NEPAPI/floorsheet*.csv",
        "/Users/sanishtamang/Downloads/floorsheet/floorsheet*.csv",
        "/Users/sanishtamang/Downloads/floorsheet/floorsheet*.xlsx"
    ]
    
    all_files = []
    for p in paths:
        all_files.extend(glob.glob(p))
    
    # Filter and sort files by date
    file_info = []
    for f in all_files:
        dt = get_date_from_filename(os.path.basename(f))
        if dt:
            file_info.append((dt, f))
    
    # Sort by date desc and take unique dates (to avoid duplicates if both csv and xlsx exist)
    file_info.sort(key=lambda x: x[0], reverse=True)
    
    unique_files = {}
    for dt, f in file_info:
        if dt not in unique_files:
            unique_files[dt] = f
        else:
            # Prefer CSV over XLSX for speed
            if f.endswith('.csv'):
                unique_files[dt] = f

    selected_dates = sorted(unique_files.keys(), reverse=True)[:days]
    print(f"Analyzing {len(selected_dates)} days of data: {selected_dates[-1]} to {selected_dates[0]}")
    
    cumulative_data = {sym: pd.DataFrame() for sym in symbols}
    
    for dt in selected_dates:
        f = unique_files[dt]
        print(f"  Loading {dt} ({os.path.basename(f)}) ...")
        try:
            if f.endswith('.csv'):
                df = pd.read_csv(f)
            else:
                df = pd.read_excel(f, engine='openpyxl')
            
            for sym in symbols:
                df_sym = df[df['stockSymbol'] == sym]
                cumulative_data[sym] = pd.concat([cumulative_data[sym], df_sym])
        except Exception as e:
            print(f"    Error loading {f}: {e}")

    print("\n" + "="*80)
    print(f"{'15-DAY CUMULATIVE BROKER ANALYSIS':^80}")
    print("="*80)

    for sym in symbols:
        df_sym = cumulative_data[sym]
        if df_sym.empty:
            print(f"\nNo data found for {sym}")
            continue
            
        total_qty = df_sym['contractQuantity'].sum()
        total_amt = df_sym['contractAmount'].sum()
        vwap = total_amt / total_qty
        
        # Broker Stats
        buyer_stats = df_sym.groupby('buyerMemberId')['contractQuantity'].sum()
        seller_stats = df_sym.groupby('sellerMemberId')['contractQuantity'].sum()
        
        all_brokers = sorted(list(set(buyer_stats.index) | set(seller_stats.index)))
        summary = []
        for b in all_brokers:
            b_qty = buyer_stats.get(b, 0)
            s_qty = seller_stats.get(b, 0)
            summary.append({
                'Broker': b,
                'Bought': b_qty,
                'Sold': s_qty,
                'Net': b_qty - s_qty
            })
            
        df_br = pd.DataFrame(summary)
        
        print(f"\n--- {sym} Summary ({len(selected_dates)} Days) ---")
        print(f"Cumulative Volume: {total_qty:,} units")
        print(f"Period VWAP:       Rs. {vwap:.2f}")
        
        print("\nTop Net Accumulators (HODLers):")
        top_acc = df_br.sort_values('Net', ascending=False).head(5)
        for _, row in top_acc.iterrows():
            print(f"  Broker {int(row['Broker']):<2}: +{int(row['Net']):>8,} units (Bought: {int(row['Bought']):>8,} | Sold: {int(row['Sold']):>8,})")
            
        print("\nTop Net Sellers (Exitors):")
        top_dist = df_br.sort_values('Net', ascending=True).head(5)
        for _, row in top_dist.iterrows():
            print(f"  Broker {int(row['Broker']):<2}: {int(row['Net']):>9,} units (Bought: {int(row['Bought']):>8,} | Sold: {int(row['Sold']):>8,})")
            
    print("\n" + "="*80)

if __name__ == "__main__":
    analyze_historical(['HLI', 'GBIME'], days=15)
