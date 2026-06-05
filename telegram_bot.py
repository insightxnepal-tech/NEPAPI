#!/usr/bin/env python3
"""
NEPSE Telegram Bot — Webhook server (FastAPI)
Self-contained: no imports from premarket_strategy.py
Commands: /start /help /floorsheet /strategy /sniper /top5 /whale /broker
"""

import os, glob, json
from datetime import date
from pathlib import Path

import pandas as pd
import numpy as np
from fastapi import FastAPI, Request
import httpx

# ── Config ────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN",   "8618135314:AAHoDrHGP2sncP1HxEGLDj0OKtIpSLeuD0U")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "8563709547")
TELEGRAM_API     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
DATA_DIR         = os.getenv("DATA_DIR", ".")

app = FastAPI()

# ── Telegram helpers ──────────────────────────────────────────────
async def send_message(chat_id, text, parse_mode="Markdown"):
    chunks = [text[i:i+4000] for i in range(0, len(text), 4000)]
    async with httpx.AsyncClient(timeout=15) as client:
        for chunk in chunks:
            try:
                await client.post(f"{TELEGRAM_API}/sendMessage", json={
                    "chat_id": chat_id, "text": chunk, "parse_mode": parse_mode
                })
            except Exception as e:
                print(f"Send error: {e}")

async def send_document(chat_id, file_path, caption=""):
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            with open(file_path, "rb") as f:
                await client.post(f"{TELEGRAM_API}/sendDocument",
                    data={"chat_id": chat_id, "caption": caption},
                    files={"document": (Path(file_path).name, f, "text/csv")}
                )
        except Exception as e:
            print(f"Doc send error: {e}")

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

# ── Sniper (self-contained) ───────────────────────────────────────
def build_ohlcv(data):
    g    = data.groupby(['stockSymbol','businessDate'])
    ohlc = g['contractRate'].agg(open='first',high='max',low='min',close='last').reset_index()
    vol  = g['contractQuantity'].sum().reset_index(name='volume')
    to   = g['contractAmount'].sum().reset_index(name='turnover')
    return ohlc.merge(vol,on=['stockSymbol','businessDate']).merge(to,on=['stockSymbol','businessDate'])

def sniper_scan(data, min_m=30):
    ohlcv   = build_ohlcv(data)
    tv_filt = data.groupby('stockSymbol')['contractAmount'].sum()
    symbols = tv_filt[tv_filt >= min_m*1e6].index.tolist()
    results = []
    for sym in symbols:
        df = ohlcv[ohlcv['stockSymbol']==sym].sort_values('businessDate').reset_index(drop=True)
        if len(df) < 3:
            continue
        c = df['close']; h = df['high']; l = df['low']; v = df['volume']
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
        b2 = price > ema50.iloc[-1]
        b3 = 50<=rsi.iloc[-1]<=70 and rsi.iloc[-1]>rsi.iloc[-2]
        b4 = macd.iloc[-1]>msig.iloc[-1] and macd.iloc[-1]>0
        b5 = v.iloc[-1]>vm.iloc[-1]
        score = sum([b1,b2,b3,b4,b5])

        s1 = ema9.iloc[-2]>=ema21.iloc[-2] and ema9.iloc[-1]<ema21.iloc[-1]
        s2 = price<ema50.iloc[-1]
        s3 = rsi.iloc[-1]<45 and macd.iloc[-1]<msig.iloc[-1]
        sell = s1 or s2 or s3

        a   = atr.iloc[-1] if atr.iloc[-1]>0 else price*0.02
        sig = ("🚀 BUY" if score==5 else "👀 NEAR" if score>=4 else "🛑 SELL" if sell else "")
        if not sig:
            continue
        results.append({
            'symbol':sym,'signal':sig,'price':round(price,2),
            'entry':round(price,2),
            'sl':round(price-1.5*a,2),
            't1':round(price+2*a,2),
            't2':round(price+3.5*a,2),
            'rr':round((2*a)/max(1.5*a,0.01),2),
            'rsi':round(rsi.iloc[-1],1),
            'score':score,
            'missing':[k for k,v in {'EMA_cross':b1,'Above_EMA50':b2,'RSI':b3,'MACD':b4,'Volume':b5}.items() if not v]
        })
    return results

# ── Command handlers ──────────────────────────────────────────────
async def cmd_start(chat_id):
    await send_message(chat_id,
        "🎯 *NEPSE Strategy Bot*\n\n"
        "Real-time NEPSE insights from floorsheet data.\n\n"
        "📋 *Commands:*\n"
        "/floorsheet — Today's summary + CSV file\n"
        "/strategy   — Pre-market strategy report\n"
        "/sniper     — Sniper BUY/SELL signals\n"
        "/top5       — Top 5 stocks by turnover\n"
        "/whale      — Largest block trades\n"
        "/broker     — Smart money positions\n"
        "/help       — Show this menu\n\n"
        "_Data updates daily at 4 PM NPT._")

async def cmd_floorsheet(chat_id):
    files = get_csv_files()
    if not files:
        await send_message(chat_id, "❌ No floorsheet data available."); return
    f    = files[-1]
    df   = pd.read_csv(f)
    bdate = Path(f).stem.replace("floorsheet_","")
    for col in ['contractAmount','contractQuantity']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    msg = (f"📊 *Floorsheet — {bdate}*\n\n"
           f"• Trades: `{len(df):,}`\n"
           f"• Turnover: `Rs {df['contractAmount'].sum()/1e9:.2f}B`\n"
           f"• Shares: `{df['contractQuantity'].sum():,.0f}`\n"
           f"• Stocks: `{df['stockSymbol'].nunique()}`\n\n"
           "📎 Sending CSV...")
    await send_message(chat_id, msg)
    await send_document(chat_id, f, caption=f"NEPSE Floorsheet {bdate}")

async def cmd_strategy(chat_id):
    strat = get_latest_strategy()
    if not strat:
        await send_message(chat_id, "❌ No strategy report yet. Check after 10 AM NPT."); return
    with open(strat) as f:
        content = f.read()
    lines     = content.split("\n")
    bias      = next((l for l in lines if "Market Bias:" in l), "")
    buy_line  = next((l for l in lines if "Sniper BUY entries:" in l), "")
    near_line = next((l for l in lines if "Near-Buy watchlist:" in l), "")
    exit_line = next((l for l in lines if "Exit / avoid:" in l), "")
    mom_line  = next((l for l in lines if "Momentum scrips:" in l), "")
    msg = (f"📋 *Strategy — {Path(strat).stem}*\n\n"
           f"{bias.strip()}\n\n"
           f"🚀 {buy_line.strip()}\n"
           f"👀 {near_line.strip()}\n"
           f"🛑 {exit_line.strip()}\n"
           f"🔥 {mom_line.strip()}\n\n"
           "📎 Full report attached...")
    await send_message(chat_id, msg)
    await send_document(chat_id, strat, caption="Full Strategy Report")

async def cmd_sniper(chat_id):
    df = load_data(days=15)
    if df is None:
        await send_message(chat_id, "❌ No data available."); return
    await send_message(chat_id, "⏳ Running sniper scan...")
    results = sniper_scan(df)
    buys  = [r for r in results if r['signal']=='🚀 BUY']
    near  = [r for r in results if r['signal']=='👀 NEAR'][:5]
    sells = [r for r in results if r['signal']=='🛑 SELL'][:5]
    msg = "🎯 *SNIPER SHOT PRO v2*\n\n"
    msg += f"🚀 *BUY SIGNALS ({len(buys)})*\n"
    if buys:
        for r in buys:
            msg += (f"• *{r['symbol']}* @ Rs {r['price']}\n"
                    f"  Entry:{r['entry']} SL:{r['sl']} T1:{r['t1']} T2:{r['t2']}\n"
                    f"  RSI:{r['rsi']} | R:R {r['rr']}x\n\n")
    else:
        msg += "  None — wait for setup\n\n"
    msg += f"👀 *NEAR BUY ({len(near)})*\n"
    for r in near:
        msg += f"• *{r['symbol']}* Rs {r['price']} — missing: {', '.join(r['missing'])}\n"
    msg += f"\n🛑 *EXIT ({len(sells)})*\n"
    for r in sells:
        msg += f"• *{r['symbol']}* Rs {r['price']} | RSI {r['rsi']}\n"
    msg += "\n_Not financial advice. DYOR._"
    await send_message(chat_id, msg)

async def cmd_top5(chat_id):
    df = load_data(days=1)
    if df is None:
        await send_message(chat_id, "❌ No data."); return
    top   = df.groupby('stockSymbol')['contractAmount'].sum().sort_values(ascending=False).head(5)
    bdate = df['businessDate'].max().date()
    msg   = f"🔥 *Top 5 — {bdate}*\n\n"
    for i,(sym,amt) in enumerate(top.items(),1):
        qty = df[df['stockSymbol']==sym]['contractQuantity'].sum()
        avg = df[df['stockSymbol']==sym]['contractRate'].mean()
        msg += f"{i}. *{sym}*\n   Rs {amt/1e6:.1f}M | {qty:,.0f} shares | avg Rs {avg:.1f}\n\n"
    await send_message(chat_id, msg)

async def cmd_whale(chat_id):
    df = load_data(days=1)
    if df is None:
        await send_message(chat_id, "❌ No data."); return
    top   = df.nlargest(5,'contractAmount')
    bdate = df['businessDate'].max().date()
    msg   = f"🐋 *Block Trades — {bdate}*\n\n"
    for _,r in top.iterrows():
        msg += (f"• *{r['stockSymbol']}* Rs {r['contractAmount']/1e6:.1f}M\n"
                f"  {int(r['contractQuantity']):,} shares @ Rs {r['contractRate']:.0f}\n"
                f"  🟢 {str(r['buyerBrokerName'])[:32]}\n"
                f"  🔴 {str(r['sellerBrokerName'])[:32]}\n\n")
    await send_message(chat_id, msg)

async def cmd_broker(chat_id):
    df = load_data(days=15)
    if df is None:
        await send_message(chat_id, "❌ No data."); return
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
    await send_message(chat_id, msg)

# ── Router ────────────────────────────────────────────────────────
COMMANDS = {
    "/start":      cmd_start,
    "/help":       cmd_start,
    "/floorsheet": cmd_floorsheet,
    "/strategy":   cmd_strategy,
    "/sniper":     cmd_sniper,
    "/top5":       cmd_top5,
    "/whale":      cmd_whale,
    "/broker":     cmd_broker,
}

# ── Endpoints ─────────────────────────────────────────────────────
@app.post("/webhook")
async def webhook(request: Request):
    try:
        body    = await request.json()
        msg     = body.get("message") or body.get("edited_message", {})
        if not msg:
            return {"ok": True}
        chat_id = msg.get("chat", {}).get("id")
        text    = msg.get("text", "").strip().split("@")[0].lower()
        handler = COMMANDS.get(text)
        if handler:
            await handler(chat_id)
        else:
            await send_message(chat_id, "❓ Unknown command. Send /help for the menu.")
    except Exception as e:
        print(f"Webhook error: {e}")
    return {"ok": True}

@app.get("/")
async def root():
    return {"status": "NEPSE Bot is running 🚀", "date": str(date.today())}

@app.get("/health")
async def health():
    files = get_csv_files()
    return {"status": "ok", "latest_data": Path(files[-1]).stem if files else "none"}
