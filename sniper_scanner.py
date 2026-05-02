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
sys.setrecursionlimit(10000); import logging
# ------------------------------------

def calculate_indicators(df):
    if len(df) < 200: return None
    close = df['closePrice']
    high = df['highPrice']
    low = df['lowPrice']
    open_price = df['openPrice']
    vol = df['totalTradedQuantity']

    ema9 = close.ewm(span=9, adjust=False).mean()
    ema21 = close.ewm(span=21, adjust=False).mean()
    ema50 = close.ewm(span=50, adjust=False).mean()
    ema200 = close.ewm(span=200, adjust=False).mean()

    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss.replace(0, 0.001)
    rsi = 100 - (100 / (1 + rs))

    # MACD
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    macd_signal = macd_line.ewm(span=9, adjust=False).mean()

    # ATR(14)
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=14).mean()

    vol_ma20 = vol.rolling(window=20).mean()

    return {
        'price': close.iloc[-1],
        'open': open_price.iloc[-1],
        'high': high.iloc[-1],
        'low': low.iloc[-1],
        'vol': vol.iloc[-1],
        
        'ema9': ema9.iloc[-1], 'prev_ema9': ema9.iloc[-2],
        'ema21': ema21.iloc[-1], 'prev_ema21': ema21.iloc[-2],
        'ema50': ema50.iloc[-1],
        'ema200': ema200.iloc[-1],
        
        'rsi': rsi.iloc[-1], 'prev_rsi': rsi.iloc[-2],
        
        'macd': macd_line.iloc[-1],
        'macd_signal': macd_signal.iloc[-1],
        
        'atr': atr.iloc[-1],
        'vol_ma20': vol_ma20.iloc[-1],
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
            # SNIPER SHOT PRO v2 requires EMA 200, so we need ~400 calendar days of history
            full_history = []
            for p in range(0, 4):
                end_date = date.today()
                start_date = end_date - timedelta(days=400)
                url = f"/api/nots/market/history/security/{cid}?&size=500&startDate={start_date}&endDate={end_date}&page={p}"
                res = n.requestGETAPI(url)
                if not res or 'content' not in res or not res['content']: break
                full_history.extend(res['content'])
                if len(res['content']) < 15: break
            
            if len(full_history) < 200: continue
            
            df = pd.DataFrame(full_history).sort_values('businessDate')
            stats = calculate_indicators(df)
            if not stats: continue

            # Sniper Logic (SNIPER SHOT PRO v2)
            body = abs(stats['open'] - stats['price'])
            upper_wick = stats['high'] - max(stats['open'], stats['price'])
            lower_wick = min(stats['open'], stats['price']) - stats['low']
            total_wick = upper_wick + lower_wick
            
            is_doji_or_wicky = total_wick > 2 * body
            is_price_below_200 = stats['price'] < stats['ema200']
            
            # EMA flat/intertwined filter (simplistic check: 1% spread)
            ema_max = max(stats['ema9'], stats['ema21'], stats['ema50'])
            ema_min = min(stats['ema9'], stats['ema21'], stats['ema50'])
            emas_flat = (ema_max - ema_min) / stats['price'] < 0.01

            no_trade = is_doji_or_wicky or is_price_below_200 or emas_flat

            # BUY (ALL 5 MUST ALIGN)
            # 1. EMA 9 crosses ABOVE EMA 21
            buy_rule1 = stats['prev_ema9'] <= stats['prev_ema21'] and stats['ema9'] > stats['ema21']
            # 2. Price ABOVE EMA 50 AND EMA 200
            buy_rule2 = stats['price'] > stats['ema50'] and stats['price'] > stats['ema200']
            # 3. RSI between 50-65 (rising)
            buy_rule3 = 50 <= stats['rsi'] <= 65 and stats['rsi'] > stats['prev_rsi']
            # 4. MACD line ABOVE signal AND above zero
            buy_rule4 = stats['macd'] > stats['macd_signal'] and stats['macd'] > 0
            # 5. Volume > 20-day average
            buy_rule5 = stats['vol'] > stats['vol_ma20']

            buy_signal = buy_rule1 and buy_rule2 and buy_rule3 and buy_rule4 and buy_rule5

            # SELL (ANY 1 TRIGGERS)
            # 1. EMA 9 crosses BELOW EMA 21
            sell_rule1 = stats['prev_ema9'] >= stats['prev_ema21'] and stats['ema9'] < stats['ema21']
            # 2. Price closes BELOW EMA 50
            sell_rule2 = stats['price'] < stats['ema50']
            # 3. RSI drops BELOW 45 + bearish MACD
            sell_rule3 = stats['rsi'] < 45 and stats['macd'] < stats['macd_signal']
            
            sell_signal = sell_rule1 or sell_rule2 or sell_rule3

            status = "NEUTRAL"
            if buy_signal:
                if no_trade:
                    status = "⚠️ BUY BLOCKED (FILTERED)"
                else:
                    status = "🚀 BUY SIGNAL"
            elif sell_signal:
                status = "🛑 SELL SIGNAL"
            
            # For Market-Wide, only store ACTIONS
            if scan_all:
                if status != "NEUTRAL":
                    results.append({'Symbol': symbol, 'Status': status, 'Price': stats['price'], 'RSI': stats['rsi'], 'MACD': stats['macd']})
            else:
                results.append({'Symbol': symbol, 'Status': status, 'Price': stats['price'], 'RSI': stats['rsi'], 'MACD': stats['macd']})
            
            time.sleep(0.3) # Faster sleep for market-wide
        except:
            continue

    print("\n✅ Scan Finished.")
    
    # Telegram
    header = "🎯 *MARKET SNIPER: BUY/SELL SIGNALS*" if scan_all else "🎯 *THE SNIPER SETUP: PORTFOLIO*"
    msg = f"{header}\n\n"
    if results:
        for r in results:
            if scan_all and "BUY" not in r['Status'] and "SELL" not in r['Status']: continue
            msg += f"• *{r['Symbol']}*: {r['Status']}\n"
            msg += f"  Price: Rs. {r['Price']} | RSI: {r['RSI']:.1f} | MACD: {r.get('MACD', 0):.2f}\n"
    else:
        msg += "All stocks are currently *NEUTRAL* 💤"

    # Split message if it's too long
    if len(msg) > 4000: msg = msg[:4000] + "\n...(truncated)"
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    res = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"}); print(res.text)

if __name__ == "__main__":
    # If explicitly asked for all, or via env
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--all', action='store_true')
    args = parser.parse_args()
    
    main(scan_all=args.all or os.getenv("SCAN_ALL_MARKET") == "true")
