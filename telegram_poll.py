#!/usr/bin/env python3
"""
NEPSE Telegram Bot — Polling mode (runs via GitHub Actions every 5 min)
No server needed. Reads new messages, responds, saves offset.
Commands: /start /help /floorsheet /strategy /sniper /top5 /whale /broker
"""

import os, glob, json, urllib.request, urllib.parse
from datetime import date
from pathlib import Path

import pandas as pd
import numpy as np

# ── Config ────────────────────────────────────────────────────────
TOKEN    = os.getenv("TELEGRAM_TOKEN",   "8618135314:AAHoDrHGP2sncP1HxEGLDj0OKtIpSLeuD0U")
CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID", "8563709547")
API      = f"https://api.telegram.org/bot{TOKEN}"
DATA_DIR = os.getenv("DATA_DIR", ".")
OFFSET_FILE = os.path.join(DATA_DIR, ".tg_offset")

# ── Telegram API ──────────────────────────────────────────────────
def tg(method, **params):
    url  = f"{API}/{method}"
    data = json.dumps(params).encode()
    req  = urllib.request.Request(url, data=data,
               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"TG error {method}: {e}")
        return {}

def send(chat_id, text):
    """Send message, splitting into chunks if needed."""
    for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
        tg("sendMessage", chat_id=chat_id, text=chunk, parse_mode="Markdown")

def send_file(chat_id, filepath, caption=""):
    """Send file using multipart — urllib only."""
    import mimetypes, uuid
    boundary = uuid.uuid4().hex
    mime     = mimetypes.guess_type(filepath)[0] or "application/octet-stream"
    fname    = Path(filepath).name
    with open(filepath, "rb") as f:
        file_data = f.read()

    body  = (f"--{boundary}\r\n"
             f'Content-Disposition: form-data; name="chat_id"\r\n\r\n'
             f"{chat_id}\r\n"
             f"--{boundary}\r\n"
             f'Content-Disposition: form-data; name="caption"\r\n\r\n'
             f"{caption}\r\n"
             f"--{boundary}\r\n"
             f'Content-Disposition: form-data; name="document"; filename="{fname}"\r\n'
             f"Content-Type: {mime}\r\n\r\n").encode() + \
             file_data + f"\r\n--{boundary}--\r\n".encode()

    req = urllib.request.Request(
        f"{API}/sendDocument", data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"File send error: {e}")

# ── Offset management ─────────────────────────────────────────────
def load_offset():
    if os.path.exists(OFFSET_FILE):
        with open(OFFSET_FILE) as f:
            return int(f.read().strip())
    return 0

def save_offset(offset):
    with open(OFFSET_FILE, "w") as f:
        f.write(str(offset))

# ── Data helpers ──────────────────────────────────────────────────
def get_csv_files():
    files = sorted(glob.glob(os.path.join(DATA_DIR, "floorsheet_2026-*.csv")))
    return [f for f in files if "dividend" not in f]

def load_data(days=1):
    files = get_csv_files()
    if not files:
        return None
    dfs = [pd.read_csv(f) for f in files[-days:]]
    df  = pd.concat(dfs, ignore_index=True)
    for col in ['contractAmount','contractQuantity','contractRate']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['businessDate'] = pd.to_datetime(df['businessDate'])
    return df

def get_latest_strategy():
    files = sorted(glob.glob(os.path.join(DATA_DIR, "strategy_*.md")))
    return files[-1] if files else None

# ── Sniper ────────────────────────────────────────────────────────
def sniper_scan(data, min_m=30):
    def build_ohlcv(d):
        g    = d.groupby(['stockSymbol','businessDate'])
        ohlc = g['contractRate'].agg(open='first',high='max',low='min',close='last').reset_index()
        vol  = g['contractQuantity'].sum().reset_index(name='volume')
        to   = g['contractAmount'].sum().reset_index(name='turnover')
        return ohlc.merge(vol,on=['stockSymbol','businessDate']).merge(to,on=['stockSymbol','businessDate'])

    ohlcv   = build_ohlcv(data)
    tv      = data.groupby('stockSymbol')['contractAmount'].sum()
    symbols = tv[tv >= min_m*1e6].index.tolist()
    results = []

    for sym in symbols:
        df = ohlcv[ohlcv['stockSymbol']==sym].sort_values('businessDate').reset_index(drop=True)
        if len(df) < 3:
            continue
        c,h,l,v = df['close'],df['high'],df['low'],df['volume']
        n = len(df)
        ema9  = c.ewm(span=min(9,n),  adjust=False).mean()
        ema21 = c.ewm(span=min(21,n), adjust=False).mean()
        ema50 = c.ewm(span=min(50,n), adjust=False).mean()
        delta = c.diff()
        gain  = delta.where(delta>0,0.).rolling(min(14,n)).mean()
        loss  = (-delta.where(delta<0,0.)).rolling(min(14,n)).mean()
        rsi   = 100-(100/(1+gain/loss.replace(0,0.001)))
        macd  = c.ewm(span=min(12,n),adjust=False).mean()-c.ewm(span=min(26,n),adjust=False).mean()
        msig  = macd.ewm(span=min(9,n),adjust=False).mean()
        tr    = pd.concat([h-l,(h-c.shift(1)).abs(),(l-c.shift(1)).abs()],axis=1).max(axis=1)
        atr   = tr.rolling(min(14,n)).mean()
        vm    = v.rolling(min(20,n)).mean()

        price = c.iloc[-1]
        b1 = ema9.iloc[-2]<=ema21.iloc[-2] and ema9.iloc[-1]>ema21.iloc[-1]
        b2 = price>ema50.iloc[-1]
        b3 = 50<=rsi.iloc[-1]<=70 and rsi.iloc[-1]>rsi.iloc[-2]
        b4 = macd.iloc[-1]>msig.iloc[-1] and macd.iloc[-1]>0
        b5 = v.iloc[-1]>vm.iloc[-1]
        score = sum([b1,b2,b3,b4,b5])
        sell  = (ema9.iloc[-2]>=ema21.iloc[-2] and ema9.iloc[-1]<ema21.iloc[-1]) or \
                price<ema50.iloc[-1] or \
                (rsi.iloc[-1]<45 and macd.iloc[-1]<msig.iloc[-1])

        a   = atr.iloc[-1] if atr.iloc[-1]>0 else price*0.02
        sig = "🚀 BUY" if score==5 else "👀 NEAR" if score>=4 else "🛑 SELL" if sell else ""
        if not sig:
            continue
        missing = [k for k,ok in {'EMA_cross':b1,'EMA50':b2,'RSI':b3,'MACD':b4,'Vol':b5}.items() if not ok]
        results.append({'symbol':sym,'signal':sig,'price':round(price,2),
            'sl':round(price-1.5*a,2),'t1':round(price+2*a,2),'t2':round(price+3.5*a,2),
            'rr':round((2*a)/max(1.5*a,0.01),2),'rsi':round(rsi.iloc[-1],1),
            'score':score,'missing':missing})
    return results

# ── Command handlers ──────────────────────────────────────────────
def handle_start(chat_id):
    send(chat_id,
        "🎯 *NEPSE Strategy Bot*\n\n"
        "Real-time insights from NEPSE floorsheet data.\n\n"
        "📋 *Commands:*\n"
        "/floorsheet — Today's summary + CSV\n"
        "/strategy   — Pre-market strategy report\n"
        "/sniper     — Sniper BUY/SELL signals\n"
        "/top5       — Top 5 stocks by turnover\n"
        "/whale      — Largest block trades\n"
        "/broker     — Smart money positions\n"
        "/help       — Show this menu\n\n"
        "_Data updates daily at 4 PM NPT._")

def handle_floorsheet(chat_id):
    files = get_csv_files()
    if not files:
        send(chat_id, "❌ No floorsheet data available."); return
    f     = files[-1]
    df    = pd.read_csv(f)
    bdate = Path(f).stem.replace("floorsheet_","")
    for col in ['contractAmount','contractQuantity']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    send(chat_id,
        f"📊 *Floorsheet — {bdate}*\n\n"
        f"• Trades: `{len(df):,}`\n"
        f"• Turnover: `Rs {df['contractAmount'].sum()/1e9:.2f}B`\n"
        f"• Shares: `{df['contractQuantity'].sum():,.0f}`\n"
        f"• Stocks: `{df['stockSymbol'].nunique()}`\n\n"
        "📎 Sending CSV...")
    send_file(chat_id, f, caption=f"NEPSE Floorsheet {bdate}")

def handle_strategy(chat_id):
    strat = get_latest_strategy()
    if not strat:
        send(chat_id, "❌ No strategy report yet. Check after 10 AM NPT."); return
    with open(strat) as f:
        content = f.read()
    lines    = content.split("\n")
    bias     = next((l for l in lines if "Market Bias:" in l), "")
    buy_l    = next((l for l in lines if "Sniper BUY entries:" in l), "")
    near_l   = next((l for l in lines if "Near-Buy watchlist:" in l), "")
    exit_l   = next((l for l in lines if "Exit / avoid:" in l), "")
    mom_l    = next((l for l in lines if "Momentum scrips:" in l), "")
    send(chat_id,
        f"📋 *Strategy — {Path(strat).stem}*\n\n"
        f"{bias.strip()}\n\n"
        f"🚀 {buy_l.strip()}\n"
        f"👀 {near_l.strip()}\n"
        f"🛑 {exit_l.strip()}\n"
        f"🔥 {mom_l.strip()}\n\n"
        "📎 Full report attached...")
    send_file(chat_id, strat, caption="Full Strategy Report")

def handle_sniper(chat_id):
    df = load_data(days=15)
    if df is None:
        send(chat_id, "❌ No data available."); return
    send(chat_id, "⏳ Running sniper scan...")
    results = sniper_scan(df)
    buys  = [r for r in results if r['signal']=='🚀 BUY']
    near  = [r for r in results if r['signal']=='👀 NEAR'][:5]
    sells = [r for r in results if r['signal']=='🛑 SELL'][:5]
    msg   = "🎯 *SNIPER SHOT PRO v2*\n\n"
    msg  += f"🚀 *BUY SIGNALS ({len(buys)})*\n"
    if buys:
        for r in buys:
            msg += (f"• *{r['symbol']}* @ Rs {r['price']}\n"
                    f"  SL:{r['sl']} T1:{r['t1']} T2:{r['t2']} R:R {r['rr']}x\n"
                    f"  RSI:{r['rsi']}\n\n")
    else:
        msg += "  None — wait for setup\n\n"
    msg += f"👀 *NEAR BUY ({len(near)})*\n"
    for r in near:
        msg += f"• *{r['symbol']}* Rs {r['price']} — ❌ {', '.join(r['missing'])}\n"
    msg += f"\n🛑 *EXIT ({len(sells)})*\n"
    for r in sells:
        msg += f"• *{r['symbol']}* Rs {r['price']} | RSI {r['rsi']}\n"
    msg += "\n_Not financial advice. DYOR._"
    send(chat_id, msg)

def handle_top5(chat_id):
    df = load_data(days=1)
    if df is None:
        send(chat_id, "❌ No data."); return
    top   = df.groupby('stockSymbol')['contractAmount'].sum().sort_values(ascending=False).head(5)
    bdate = df['businessDate'].max().date()
    msg   = f"🔥 *Top 5 — {bdate}*\n\n"
    for i,(sym,amt) in enumerate(top.items(),1):
        qty = df[df['stockSymbol']==sym]['contractQuantity'].sum()
        avg = df[df['stockSymbol']==sym]['contractRate'].mean()
        msg += f"{i}. *{sym}*\n   Rs {amt/1e6:.1f}M | {qty:,.0f} shares | avg Rs {avg:.1f}\n\n"
    send(chat_id, msg)

def handle_whale(chat_id):
    df = load_data(days=1)
    if df is None:
        send(chat_id, "❌ No data."); return
    top   = df.nlargest(5,'contractAmount')
    bdate = df['businessDate'].max().date()
    msg   = f"🐋 *Block Trades — {bdate}*\n\n"
    for _,r in top.iterrows():
        msg += (f"• *{r['stockSymbol']}* Rs {r['contractAmount']/1e6:.1f}M\n"
                f"  {int(r['contractQuantity']):,} @ Rs {r['contractRate']:.0f}\n"
                f"  🟢 {str(r['buyerBrokerName'])[:32]}\n"
                f"  🔴 {str(r['sellerBrokerName'])[:32]}\n\n")
    send(chat_id, msg)

def handle_broker(chat_id):
    df = load_data(days=15)
    if df is None:
        send(chat_id, "❌ No data."); return
    buy = df.groupby('buyerBrokerName')['contractAmount'].sum().rename('bought')
    sel = df.groupby('sellerBrokerName')['contractAmount'].sum().rename('sold')
    net = pd.concat([buy,sel],axis=1).fillna(0)
    net['net'] = net['bought']-net['sold']
    msg  = "🐋 *Smart Money (15 Days)*\n\n🟢 *Top Buyers*\n"
    for b,r in net.sort_values('net',ascending=False).head(5).iterrows():
        msg += f"• {str(b)[:35]}\n  *+Rs {r['net']/1e6:.1f}M*\n"
    msg += "\n🔴 *Top Sellers*\n"
    for b,r in net.sort_values('net',ascending=True).head(5).iterrows():
        msg += f"• {str(b)[:35]}\n  *Rs {r['net']/1e6:.1f}M*\n"
    send(chat_id, msg)

COMMANDS = {
    "/start":      handle_start,
    "/help":       handle_start,
    "/floorsheet": handle_floorsheet,
    "/strategy":   handle_strategy,
    "/sniper":     handle_sniper,
    "/top5":       handle_top5,
    "/whale":      handle_whale,
    "/broker":     handle_broker,
}

# ── Main polling loop ─────────────────────────────────────────────
def main():
    offset = load_offset()
    print(f"Polling from offset {offset}...")

    resp = tg("getUpdates", offset=offset, timeout=5, limit=20)
    updates = resp.get("result", [])
    print(f"Got {len(updates)} updates.")

    for update in updates:
        uid  = update["update_id"]
        msg  = update.get("message") or update.get("edited_message", {})
        if msg:
            chat_id = msg.get("chat", {}).get("id")
            text    = msg.get("text", "").strip().split("@")[0].lower()
            print(f"  [{chat_id}] {text}")
            handler = COMMANDS.get(text)
            if handler:
                handler(chat_id)
            else:
                send(chat_id, "❓ Unknown command. Send /help for the menu.")
        offset = uid + 1

    save_offset(offset)
    print(f"Done. New offset: {offset}")

if __name__ == "__main__":
    main()
