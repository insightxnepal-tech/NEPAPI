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

async def fetch_accurate_history(symbol):
    """
    NEPSE often limits responses to 11-20 days per page. 
    We will try to fetch multiple pages to reach 100+ days.
    """
    n = Nepse()
    # We'll use the raw request method to handle pagination
    end_date = date.today()
    start_date = end_date - timedelta(days=200)
    company_id_map = await n.getSecurityIDKeyMap()
    if symbol not in company_id_map: return []
    
    cid = company_id_map[symbol]
    all_history = []
    
    for page in range(0, 5): # Try up to 5 pages
        url = f"/api/nots/market/history/security/{cid}?&size=200&startDate={start_date}&endDate={end_date}&page={page}"
        response = n.requestGETAPI(url)
        if not response or 'content' not in response or not response['content']:
            break
        all_history.extend(response['content'])
        if len(response['content']) < 200: # No more data on next page
            break
            
    return all_history

def main():
    n = Nepse()
    print("🎯 Initiating Deep-Scan for Sniper Setup...")
    
    with open("portfolio_data.json", "r") as f:
        portfolio = json.load(f)
    
    results = []
    
    for symbol in portfolio.keys():
        print(f"  Fetching deep history for {symbol}...", end="", flush=True)
        try:
            # We need to compute symbol IDs
            # But getSecurityIDKeyMap is async in the library's design? 
            # No, in Nepse(sync) it's sync.
            cid_map = n.getSecurityIDKeyMap()
            cid = cid_map.get(symbol)
            if not cid:
                print(" [ERROR: Symbol Map Missing]")
                continue
            
            end_date = date.today()
            start_date = end_date - timedelta(days=300) # Go back even further
            
            # Fetch with pagination manually
            full_history = []
            for p in range(0, 8): # Fetch up to 8 pages of ~20-50 trades
                url = f"/api/nots/market/history/security/{cid}?&size=500&startDate={start_date}&endDate={end_date}&page={p}"
                res = n.requestGETAPI(url)
                if not res or 'content' not in res or not res['content']:
                    break
                full_history.extend(res['content'])
                if len(res['content']) < 10: # Likely last page
                    break
            
            if len(full_history) < 50:
                print(f" [SKIP: Only {len(full_history)} days]")
                continue
            
            df = pd.DataFrame(full_history).sort_values('businessDate')
            stats = calculate_indicators(df)
            
            if not stats:
                print(" [SKIP: Math Error]")
                continue

            rule1 = stats['price'] > stats['ema20'] and stats['price'] > stats['ema50']
            rule2 = stats['rsi'] > 60
            # Rule 3: Volume MA20 > Vol MA50
            rule3 = stats['vol_ma20'] > stats['vol_ma50']
            
            buy_signal = rule1 and rule2 and rule3
            overextended = ((stats['price'] - stats['ema20']) / stats['ema20']) * 100 > 15
            dead_zone = (45 <= stats['rsi'] <= 55)
            
            status = "NEUTRAL"
            if buy_signal:
                if dead_zone: status = "💤 DEAD ZONE"
                elif overextended: status = "⚠️ OVEREXTENDED"
                else: status = "🚀 BUY SIGNAL"
            elif stats['rsi'] >= 80: status = "💰 TAKE PROFIT"
            elif stats['price'] < stats['ema20']: status = "🛑 EXIT"
            
            results.append({'Symbol': symbol, 'Status': status, 'Price': stats['price'], 'RSI': stats['rsi']})
            print(f" [DONE: {len(full_history)} days -> {status}]")
            time.sleep(0.5)
        except Exception as e:
            print(f" [ERROR: {e}]")

    print("\n✅ Deep-Scan Finished.")
    
    # Telegram Message
    msg = "🎯 *THE SNIPER SETUP: LIVE SCAN*\n"
    found_action = False
    for r in results:
        if r['Status'] != "NEUTRAL":
            found_action = True
            msg += f"• *{r['Symbol']}*: {r['Status']} (Rs. {r['Price']})\n"
    
    if not found_action:
        msg += "\nPortfolio Status: *NEUTRAL* 😴"

    url = f"https://api.telegram.org/bot{os.getenv('TELEGRAM_TOKEN', '8618135314:AAHoDrHGP2sncP1HxEGLDj0OKtIpSLeuD0U')}/sendMessage"
    requests.post(url, json={"chat_id": os.getenv("TELEGRAM_CHAT_ID", "8563709547"), "text": msg, "parse_mode": "Markdown"})

if __name__ == "__main__":
    main()
