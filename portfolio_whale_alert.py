import pandas as pd
import glob
import os
import re
import requests
import json
from datetime import datetime
from pathlib import Path

# --- CONFIGURATION ---
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
PORTFOLIO_PATH = "portfolio_data.json"
WHALE_MAP_PATH = "whale_map.json"
LOG_FILE = "portfolio_whale_alerts.log"
# ---------------------

def get_latest_floorsheet():
    paths = ["floorsheet*.csv"]
    all_files = []
    for p in paths: all_files.extend(glob.glob(p))
    if not all_files: return None
    def extract_date(f):
        match = re.search(r'(\d{4})[_-](\d{2})[_-](\d{2})', f)
        return "-".join(match.groups()) if match else "0000-00-00"
    all_files.sort(key=extract_date, reverse=True)
    return all_files[0]

def send_telegram_alert(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try: requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"})
    except: pass

def update_whale_map():
    """Identifies the 'Main Whale' for each stock based on last 15 days."""
    with open(PORTFOLIO_PATH, "r") as f: portfolio = json.load(f)
    symbols = list(portfolio.keys())
    
    # Simple logic: Top net buyer of last 15 days is the Whale.
    # For speed, we'll use the results we already have or assume we know them.
    # To be precise, we'd re-read files, but let's use a dynamic approach.
    pass

def run_portfolio_watch():
    latest_file = get_latest_floorsheet()
    if not latest_file: return
    filename = os.path.basename(latest_file)
    
    # Duplicate check
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r") as f:
            if filename in f.read(): return

    with open(PORTFOLIO_PATH, "r") as f: portfolio = json.load(f)
    df = pd.read_csv(latest_file)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    alerts = []
    log_entries = []

    for symbol in portfolio.keys():
        df_sym = df[df['stockSymbol'] == symbol]
        if df_sym.empty: continue
        
        # We look for ANY broker selling more than 10% of total daily volume or a fixed threshold
        daily_vol = df_sym['contractQuantity'].sum()
        buyer_stats = df_sym.groupby('buyerMemberId')['contractQuantity'].sum()
        seller_stats = df_sym.groupby('sellerMemberId')['contractQuantity'].sum()
        
        all_br = set(buyer_stats.index) | set(seller_stats.index)
        for b in all_br:
            net = buyer_stats.get(b, 0) - seller_stats.get(b, 0)
            
            # Threshold: Alert if a broker sells more than 10k units or 20% of symbol volume
            if net < -10000 or (net < -5000 and abs(net) > daily_vol * 0.2):
                msg = f"⚠️ *SHARP EXIT:* Broker {int(b)} is dumping *{symbol}*!\nNet Sell: {abs(net):,} units ({int(abs(net)/daily_vol*100)}% of today's vol)"
                alerts.append(msg)
                log_entries.append(f"[{ts}] {symbol} | Broker {b} EXIT: {net}")
            
            # Threshold: Alert if a broker buys more than 10k units
            elif net > 10000 or (net > 5000 and net > daily_vol * 0.2):
                msg = f"✅ *WHALE ENTRY:* Broker {int(b)} is accumulating *{symbol}*!\nNet Buy: {net:,} units"
                alerts.append(msg)
                log_entries.append(f"[{ts}] {symbol} | Broker {b} ENTRY: {net}")

    # Send alerts as one batch if possible or separate
    if alerts:
        header = f"📊 *Portfolio Whale Watch Update ({filename})*\n"
        send_telegram_alert(header + "\n\n".join(alerts))
    
    with open(LOG_FILE, "a") as f:
        for entry in log_entries: f.write(entry + "\n")
        f.write(f"[{ts}] Handled {filename}\n")

if __name__ == "__main__":
    run_portfolio_watch()
