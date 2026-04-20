import json
import pandas as pd
import requests
import glob
import os
import re
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime

# --- CONFIGURATION ---
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
PORTFOLIO_PATH = "portfolio_data.json" # Relative to project root
STOCKMAP_PATH = "stockmap.json"
ARTIFACT_DIR = Path("artifacts")
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

def send_telegram_text(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    requests.post(url, json=payload)

def send_telegram_photo(photo_path, caption):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    with open(photo_path, "rb") as photo:
        payload = {"chat_id": TELEGRAM_CHAT_ID, "caption": caption, "parse_mode": "Markdown"}
        files = {"photo": photo}
        requests.post(url, data=payload, files=files)

def run_daily_report():
    floorsheet_path = get_latest_floorsheet()
    if not floorsheet_path: return
    
    with open(PORTFOLIO_PATH, "r") as f: portfolio = json.load(f)
    with open(STOCKMAP_PATH, "r") as f: stock_map = json.load(f)
    
    df_fs = pd.read_csv(floorsheet_path)
    df_fs = df_fs.sort_values('contractId', ascending=False)
    latest_prices = df_fs.groupby('stockSymbol')['contractRate'].first().to_dict()

    total_cost = 0
    total_value = 0
    rows = []

    for symbol, data in portfolio.items():
        qty = data['qty']
        avg_rate = data['rate']
        ltp = latest_prices.get(symbol, 0)
        cost = qty * avg_rate
        val = qty * ltp
        pl = val - cost
        pl_pct = (pl / cost * 100) if cost > 0 else 0
        
        total_cost += cost
        total_value += val
        
        # Color coding logic for Telegram
        indicator = "🟢" if pl >= 0 else "🔴"
        rows.append(f"{indicator} *{symbol}*: {pl_pct:+.2f}% (Rs. {pl:,.0f})")

    total_pl = total_value - total_cost
    total_pl_pct = (total_pl / total_cost * 100) if total_cost > 0 else 0
    
    report = f"📊 *DAILY PORTFOLIO REPORT*\nDate: {datetime.now().strftime('%Y-%m-%d')}\n"
    report += f"Total Investment: Rs. {total_cost:,.2f}\n"
    report += f"Current Value: Rs. {total_value:,.2f}\n"
    report += f"Overall P/L: *{total_pl_pct:+.2f}%* (Rs. {total_pl:,.2f})\n\n"
    report += "\n".join(rows)
    
    send_telegram_text(report)

    # --- Generate Chart ---
    # Reusing chart logic from generate_portfolio_chart.py with slight adjustments
    # ... (Simplified for the report) ...
    symbols = list(portfolio.keys())
    selected_days = 15 # assume we want 15-day sentiment
    results = []
    # (Just a brief logic to get the same results as before without re-reading 15 files every time if possible, 
    # but for accuracy we should or use cached data. Here I'll just regenerate it for now)
    
    # Actually, I'll just call the external generate script and then send the file it creates
    # But it's cleaner to have it here. 
    # Actually, the chart script should also be updated to use relative paths
    os.system("python3 generate_portfolio_chart.py")
    
    chart_path = ARTIFACT_DIR / "portfolio_sentiment_chart.png"
    if chart_path.exists():
        send_telegram_photo(str(chart_path), "📈 *Institutional Sentiment (15-Day Net Score)*")

if __name__ == "__main__":
    run_daily_report()
