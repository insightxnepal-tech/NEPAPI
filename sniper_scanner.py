import pandas as pd
import numpy as np
import json
import os
import time
import sys
import ssl
import urllib.request
import requests
from pathlib import Path
from datetime import date, timedelta
from nepse import Nepse

# --- THE STABILITY PATCH ---
def patched_request_get(self, url, include_authorization_headers=True):
    full_url = f"https://www.nepalstock.com{url}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json'
    }
    if include_authorization_headers:
        access_token = self.token_manager.getAccessToken()
        headers["Authorization"] = f"Salter {access_token}"
    ctx = ssl._create_unverified_context()
    req = urllib.request.Request(full_url, headers=headers)
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=30) as response:
            return json.loads(response.read().decode())
    except Exception as e:
        return {}

Nepse.requestGETAPI = patched_request_get
sys.setrecursionlimit(10000)
# ------------------------------------

def calculate_indicators(df):
    if len(df) < 50: return None
    close = df['closePrice']
    ema20 = close.ewm(span=20, adjust=False).mean()
    ema50 = close.ewm(span=50, adjust=False).mean()
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss.replace(0, 0.001)
    rsi = 100 - (100 / (1 + rs))
    vol = df['totalTradedQuantity']
    vol_ma20 = vol.rolling(window=20).mean()
    vol_ma50 = vol.rolling(window=50).mean()
    return {
        'price': close.iloc[-1], 'ema20': ema20.iloc[-1], 'ema50': ema50.iloc[-1],
        'rsi': rsi.iloc[-1], 'vol_ma20': vol_ma20.iloc[-1], 'vol_ma50': vol_ma50.iloc[-1]
    }

def main(scan_all=False):
    n = Nepse()
    print(f"🎯 Initiating {'MARKET-WIDE' if scan_all else 'PORTFOLIO'} Sniper Scan...")
    
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8618135314:AAHoDrHGP2sncP1HxEGLDj0OKtIpSLeuD0U")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "8563709547")
    
    if scan_all:
        print("  Fetching full security list...")
        securities = n.getSecurityList()
        # Filter for active stocks (Equity only, skip mutual funds if needed)
        symbols = [s['symbol'] for s in securities if s['securityName'].find('Mutual Fund') == -1]
    else:
        with open("portfolio_data.json", "r") as f:
            portfolio = json.load(f)
        symbols = list(portfolio.keys())
    
    results = []
    print(f"  Scanning {len(symbols)} symbols. This may take a few minutes...")
    
    for i, symbol in enumerate(symbols):
        if i % 10 == 0 and i > 0: print(f"    Progress: {i}/{len(symbols)} stocks...")
        
        try:
            cid_map = n.getSecurityIDKeyMap()
            cid = cid_map.get(symbol)
            if not cid: continue
            
            # Fetch History
            # For speed in market-wide scan, we use 3 pages instead of 8 to get ~100+ days
            full_history = []
            for p in range(0, 4):
                end_date = date.today()
                start_date = end_date - timedelta(days=200)
                url = f"/api/nots/market/history/security/{cid}?&size=200&startDate={start_date}&endDate={end_date}&page={p}"
                res = n.requestGETAPI(url)
                if not res or 'content' not in res or not res['content']: break
                full_history.extend(res['content'])
                if len(res['content']) < 15: break
            
            if len(full_history) < 50: continue
            
            df = pd.DataFrame(full_history).sort_values('businessDate')
            stats = calculate_indicators(df)
            if not stats: continue

            # Sniper Logic
            rule1 = stats['price'] > stats['ema20'] and stats['price'] > stats['ema50']
            rule2 = stats['rsi'] > 60
            rule3 = stats['vol_ma20'] > stats['vol_ma50']
            
            buy_signal = rule1 and rule2 and rule3
            overextended = ((stats['price'] - stats['ema20']) / stats['ema20']) * 100 > 15
            dead_zone = (45 <= stats['rsi'] <= 55)
            
            status = "NEUTRAL"
            if buy_signal:
                if dead_zone: status = "💤 DEAD ZONE"
                elif overextended: status = "⚠️ BUY (OVEREXTENDED)"
                else: status = "🚀 BUY SIGNAL"
            elif stats['rsi'] >= 80: status = "💰 TAKE PROFIT"
            elif stats['price'] < stats['ema20']: status = "🛑 EXIT"
            
            # For Market-Wide, only store ACTIONS
            if scan_all:
                if status != "NEUTRAL":
                    results.append({'Symbol': symbol, 'Status': status, 'Price': stats['price'], 'RSI': stats['rsi']})
            else:
                results.append({'Symbol': symbol, 'Status': status, 'Price': stats['price'], 'RSI': stats['rsi']})
            
            time.sleep(0.3) # Faster sleep for market-wide
        except:
            continue

    print("\n✅ Scan Finished.")
    
    # Telegram
    header = "🎯 *MARKET SNIPER: BUY/SELL SIGNALS*" if scan_all else "🎯 *THE SNIPER SETUP: PORTFOLIO*"
    msg = f"{header}\n\n"
    if results:
        for r in results:
            if scan_all and "BUY" not in r['Status'] and "PROFIT" not in r['Status']: continue
            msg += f"• *{r['Symbol']}*: {r['Status']}\n"
            msg += f"  Price: Rs. {r['Price']} | RSI: {r['RSI']:.1f}\n"
    else:
        msg += "All stocks are currently *NEUTRAL* 💤"

    # Split message if it's too long
    if len(msg) > 4000: msg = msg[:4000] + "\n...(truncated)"
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"})

if __name__ == "__main__":
    # If explicitly asked for all, or via env
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--all', action='store_true')
    args = parser.parse_args()
    
    main(scan_all=args.all or os.getenv("SCAN_ALL_MARKET") == "true")
