import pandas as pd
import glob
import os
import re
import json
import matplotlib.pyplot as plt
from pathlib import Path

def get_date_from_filename(filename):
    match = re.search(r'(\d{4})[_-](\d{2})[_-](\d{2})', filename)
    if match:
        return "-".join(match.groups())
    return None

def generate_chart(days=15):
    portfolio_path = Path("portfolio_data.json")
    with open(portfolio_path, "r") as f:
        portfolio = json.load(f)
    
    symbols = list(portfolio.keys())
    
    paths = ["floorsheet*.csv"]
    
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
        except:
            pass

    results = []
    for sym in symbols:
        df_sym = cumulative_data[sym]
        if df_sym.empty:
            continue
        buyer_stats = df_sym.groupby('buyerMemberId')['contractQuantity'].sum()
        seller_stats = df_sym.groupby('sellerMemberId')['contractQuantity'].sum()
        all_brokers = set(buyer_stats.index) | set(seller_stats.index)
        summary = [{'Net': buyer_stats.get(b, 0) - seller_stats.get(b, 0)} for b in all_brokers]
        df_br = pd.DataFrame(summary)
        top_5_acc = df_br.sort_values('Net', ascending=False).head(5)['Net'].sum()
        top_5_dist = df_br.sort_values('Net', ascending=True).head(5)['Net'].sum()
        results.append({"Symbol": sym, "Net Score": top_5_acc + top_5_dist})

    df_final = pd.DataFrame(results).sort_values("Net Score", ascending=True)

    # Plotting
    plt.figure(figsize=(12, 8))
    colors = ['#ff4d4d' if x < 0 else '#2eb82e' for x in df_final['Net Score']]
    bars = plt.barh(df_final['Symbol'], df_final['Net Score'], color=colors, alpha=0.8)
    
    plt.axvline(0, color='black', linewidth=0.8)
    plt.xlabel('Cumulative Net Volume Score (Top 5 Buyers - Top 5 Sellers)', fontsize=12, fontweight='bold')
    plt.title(f'Institutional Sentiment Analysis (Last {len(selected_dates)} Days)', fontsize=14, fontweight='bold', pad=20)
    plt.grid(axis='x', linestyle='--', alpha=0.3)
    
    for bar in bars:
        width = bar.get_width()
        label_x = width if width > 0 else width
        plt.text(label_x, bar.get_y() + bar.get_height()/2, f' {int(width):,}', 
                 va='center', ha='left' if width > 0 else 'right', fontsize=10, fontweight='bold')

    plt.tight_layout()
    chart_dir = Path("artifacts")
    chart_dir.mkdir(parents=True, exist_ok=True)
    chart_path = chart_dir / "portfolio_sentiment_chart.png"
    plt.savefig(str(chart_path), dpi=300)
    print(f"Chart saved to: {chart_path}")

if __name__ == "__main__":
    generate_chart()
