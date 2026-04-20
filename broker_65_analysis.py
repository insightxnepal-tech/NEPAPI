import pandas as pd
import glob
import os
import re
import json
from pathlib import Path
from datetime import datetime

# --- CONFIGURATION ---
BROKER_ID = 65
DAYS_TO_SCAN = 15 # 2-3 weeks of data
# ---------------------

def get_date_from_filename(filename):
    match = re.search(r'(\d{4})[_-](\d{2})[_-](\d{2})', filename)
    if match: return "-".join(match.groups())
    return None

def analyze_broker_65():
    paths = [
        "/Users/sanishtamang/NEPAPI/floorsheet*.csv",
        "/Users/sanishtamang/Downloads/floorsheet/floorsheet*.csv",
        "/Users/sanishtamang/Downloads/floorsheet/floorsheet*.xlsx"
    ]
    
    all_files = []
    for p in paths: all_files.extend(glob.glob(p))
    
    file_info = []
    for f in all_files:
        dt = get_date_from_filename(os.path.basename(f))
        if dt: file_info.append((dt, f))
    
    file_info.sort(key=lambda x: x[0], reverse=True)
    unique_files = {}
    for dt, f in file_info:
        if dt not in unique_files:
            unique_files[dt] = f
        else:
            if f.endswith('.csv'): unique_files[dt] = f

    selected_dates = sorted(unique_files.keys(), reverse=True)[:DAYS_TO_SCAN]
    print(f"Analyzing Broker {BROKER_ID} activity over the last {len(selected_dates)} trading sessions...")
    
    broker_trades = []
    
    for dt in selected_dates:
        f = unique_files[dt]
        try:
            if f.endswith('.csv'): df = pd.read_csv(f)
            else: df = pd.read_excel(f, engine='openpyxl')
            
            # Filter for Broker 65 trades
            df_b = df[(df['buyerMemberId'] == BROKER_ID) | (df['sellerMemberId'] == BROKER_ID)].copy()
            df_b['date'] = dt
            broker_trades.append(df_b)
        except Exception as e:
            print(f"Error loading {dt}: {e}")

    if not broker_trades:
        print("No trades found.")
        return

    df_all = pd.concat(broker_trades)
    
    # Calculate Net Positions per Stock
    results = []
    symbols = df_all['stockSymbol'].unique()
    
    for sym in symbols:
        df_sym = df_all[df_all['stockSymbol'] == sym]
        bought = df_sym[df_sym['buyerMemberId'] == BROKER_ID]['contractQuantity'].sum()
        sold = df_sym[df_sym['sellerMemberId'] == BROKER_ID]['contractQuantity'].sum()
        net = bought - sold
        
        # Calculate daily trend (is it a sudden shift?)
        daily_nets = []
        for dt in selected_dates:
            df_day = df_sym[df_sym['date'] == dt]
            b = df_day[df_day['buyerMemberId'] == BROKER_ID]['contractQuantity'].sum()
            s = df_day[df_day['sellerMemberId'] == BROKER_ID]['contractQuantity'].sum()
            daily_nets.append({'date': dt, 'net': b - s})
            
        results.append({
            'Symbol': sym,
            'Bought': bought,
            'Sold': sold,
            'Net': net,
            'Total_Trades': len(df_sym),
            'Daily_History': daily_nets
        })
        
    df_results = pd.DataFrame(results)
    
    print("\n" + "="*80)
    print(f"{'BROKER 65 (SHAREPRO) - INSTITUTIONAL ACTIVITY REPORT':^80}")
    print("="*80)
    
    # Identify UNUSUAL ACTIVITY
    print("\n🚨 UNUSUAL ACCUMULATION (High Concentration):")
    top_acc = df_results.sort_values('Net', ascending=False).head(5)
    for _, row in top_acc.iterrows():
        if row['Net'] > 5000:
            print(f"  {row['Symbol']:<8}: Net Buy +{int(row['Net']):,} units across {row['Total_Trades']} trades.")

    print("\n🚨 UNUSUAL DISTRIBUTION (High Selling):")
    top_dist = df_results.sort_values('Net', ascending=True).head(5)
    for _, row in top_dist.iterrows():
        if row['Net'] < -5000:
            print(f"  {row['Symbol']:<8}: Net Sell {int(row['Net']):,} units across {row['Total_Trades']} trades.")

    print("\n🚨 SUDDEN ACTIVITY SHIFTS (Last 3 Days):")
    for _, row in df_results.iterrows():
        history = row['Daily_History']
        if len(history) < 5: continue
        
        recent_net = sum(d['net'] for d in history[:3])
        past_net = sum(d['net'] for d in history[3:10])
        
        # If they were neutral/selling and suddenly bought 20k+ in 3 days
        if recent_net > 20000 and past_net <= 0:
            print(f"  {row['Symbol']:<8}: SUDDEN BUYING! {int(recent_net):+,} units in last 3 days.")
        elif recent_net < -20000 and past_net >= 0:
            print(f"  {row['Symbol']:<8}: SUDDEN SELLING! {int(recent_net):+,} units in last 3 days.")

    print("\n" + "="*80)

if __name__ == "__main__":
    analyze_broker_65()
