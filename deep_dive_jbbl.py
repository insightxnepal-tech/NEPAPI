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

def analyze_jbbl_deep_dive(days=15):
    symbol = "JBBL"
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
    
    all_trades = pd.DataFrame()
    
    for dt in selected_dates:
        f = unique_files[dt]
        try:
            if f.endswith('.csv'):
                df = pd.read_csv(f)
            else:
                df = pd.read_excel(f, engine='openpyxl')
            
            df_sym = df[df['stockSymbol'] == symbol]
            all_trades = pd.concat([all_trades, df_sym])
        except:
            pass

    if all_trades.empty:
        print("No JBBL data found.")
        return

    total_qty = all_trades['contractQuantity'].sum()
    total_amt = all_trades['contractAmount'].sum()
    vwap_all = total_amt / total_qty
    ltp = all_trades.sort_values(['businessDate', 'contractId'], ascending=False).iloc[0]['contractRate']

    # Broker Analysis
    buyer_stats = all_trades.groupby('buyerMemberId').agg({
        'contractQuantity': 'sum',
        'contractAmount': 'sum'
    })
    seller_stats = all_trades.groupby('sellerMemberId').agg({
        'contractQuantity': 'sum',
        'contractAmount': 'sum'
    })
    
    all_brokers = sorted(list(set(buyer_stats.index) | set(seller_stats.index)))
    summary = []
    for b in all_brokers:
        bought = buyer_stats.loc[b]['contractQuantity'] if b in buyer_stats.index else 0
        sold = seller_stats.loc[b]['contractQuantity'] if b in seller_stats.index else 0
        b_amt = buyer_stats.loc[b]['contractAmount'] if b in buyer_stats.index else 0
        b_vwap = b_amt / bought if bought > 0 else 0
        
        summary.append({
            'Broker': b,
            'Bought': bought,
            'Sold': sold,
            'Net': bought - sold,
            'Buy_VWAP': b_vwap
        })
        
    df_br = pd.DataFrame(summary).sort_values('Net', ascending=False)
    
    print("\n" + "="*80)
    print(f"{'DEEP DIVE QUANT ANALYSIS: ' + symbol:^80}")
    print("="*80)
    print(f"Current LTP:        Rs. {ltp:,.2f}")
    print(f"15-Day Market VWAP: Rs. {vwap_all:,.2f}")
    print(f"Price vs VWAP:      {((ltp/vwap_all - 1)*100):+.2f}%")
    
    print("\nTOP 5 INSTITUTIONAL ACCUMULATORS (WHALES):")
    print(f"{'Broker':<8} {'Net Qty':<12} {'Buy VWAP':<12} {'Status':<15}")
    for _, row in df_br.head(5).iterrows():
        status = "In Profit" if ltp > row['Buy_VWAP'] else "In Loss"
        print(f"{int(row['Broker']):<8} {int(row['Net']):<12,} {row['Buy_VWAP']:<12.2f} {status:<15}")
        
    print("\nTOP 5 EXITORS (DISTRIBUTORS):")
    for _, row in df_br.tail(5).iterrows():
        print(f"  Broker {int(row['Broker'])}: {int(row['Net']):,} units")

    # Whale block trades
    whale_limit = 5000
    whales = all_trades[all_trades['contractQuantity'] >= whale_limit].sort_values('contractQuantity', ascending=False)
    
    print(f"\nSIGNIFICANT BLOCK TRADES (>= {whale_limit} Units):")
    if not whales.empty:
        for _, row in whales.head(10).iterrows():
            print(f"  {row['businessDate']}: {int(row['contractQuantity']):,} @ Rs.{row['contractRate']} (B:{row['buyerMemberId']} -> S:{row['sellerMemberId']})")
    else:
        print("  No major block trades detected.")

    print("\n" + "="*80)

if __name__ == "__main__":
    analyze_jbbl_deep_dive(days=15)
