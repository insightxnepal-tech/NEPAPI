import pandas as pd
import glob
import os
import re
import json
from pathlib import Path
from datetime import datetime

# --- CONFIGURATION ---
BROKER_ID = 65
DAYS_TO_SCAN = 15 
# ---------------------

def get_date_from_filename(filename):
    match = re.search(r'(\d{4})[_-](\d{2})[_-](\d{2})', filename)
    if match: return "-".join(match.groups())
    return None

def analyze_broker_65():
    paths = ["floorsheet*.csv"] # Relative for cloud/local
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
    
    broker_trades = []
    for dt in selected_dates:
        f = unique_files[dt]
        try:
            df = pd.read_csv(f)
            df_b = df[(df['buyerMemberId'] == BROKER_ID) | (df['sellerMemberId'] == BROKER_ID)].copy()
            df_b['date'] = dt
            broker_trades.append(df_b)
        except: pass

    if not broker_trades:
        return "No recent trades found for Sharepro (65)."

    df_all = pd.concat(broker_trades)
    results = []
    symbols = df_all['stockSymbol'].unique()
    
    for sym in symbols:
        df_sym = df_all[df_all['stockSymbol'] == sym]
        bought = df_sym[df_sym['buyerMemberId'] == BROKER_ID]['contractQuantity'].sum()
        sold = df_sym[df_sym['sellerMemberId'] == BROKER_ID]['contractQuantity'].sum()
        net = bought - sold
        
        # Trend check
        daily_nets = []
        for dt in selected_dates:
            df_day = df_sym[df_sym['date'] == dt]
            b = df_day[df_day['buyerMemberId'] == BROKER_ID]['contractQuantity'].sum()
            s = df_day[df_day['sellerMemberId'] == BROKER_ID]['contractQuantity'].sum()
            daily_nets.append({'date': dt, 'net': b - s})
            
        results.append({'Symbol': sym, 'Bought': bought, 'Sold': sold, 'Net': net, 'Total_Trades': len(df_sym), 'Daily_History': daily_nets})
        
    df_results = pd.DataFrame(results)
    
    output = "🔍 *SHAREPRO (65) INTEL:*\n"
    
    # Accumulation
    top_acc = df_results.sort_values('Net', ascending=False).head(3)
    output += "\n*Top Accumulation:*\n"
    for _, row in top_acc.iterrows():
        if row['Net'] > 0:
            output += f"• {row['Symbol']}: +{int(row['Net']):,} units\n"

    # Distribution
    top_dist = df_results.sort_values('Net', ascending=True).head(3)
    output += "\n*Top Distribution:*\n"
    for _, row in top_dist.iterrows():
        if row['Net'] < 0:
            output += f"• {row['Symbol']}: {int(row['Net']):,} units\n"

    # Shifts
    output += "\n*Recent Sudden Shifts:*\n"
    found_shift = False
    for _, row in df_results.iterrows():
        history = row['Daily_History']
        if len(history) < 5: continue
        recent_net = sum(d['net'] for d in history[:3])
        past_net = sum(d['net'] for d in history[3:10])
        if recent_net > 15000 and past_net <= 0:
            output += f"• {row['Symbol']}: Sudden Buy! ({int(recent_net):+,})\n"
            found_shift = True
        elif recent_net < -15000 and past_net >= 0:
            output += f"• {row['Symbol']}: Sudden Sell! ({int(recent_net):+,})\n"
            found_shift = True
    
    if not found_shift: output += "No major shifts detected."
    
    return output

if __name__ == "__main__":
    print(analyze_broker_65())
