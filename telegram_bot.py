#!/usr/bin/env python3
"""
NEPSE Telegram Bot — Webhook server (FastAPI)
Deploy on Render free tier.

Commands:
  /start      - Welcome message
  /floorsheet - Today's floorsheet summary
  /strategy   - Today's pre-market strategy report
  /sniper     - Current sniper BUY/SELL signals
  /top5       - Top 5 stocks by turnover today
  /whale      - Largest single trades today
  /broker     - Smart money net positions
  /help       - List all commands
"""

import os, json, glob, re
from datetime import date
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, Request
import httpx

# ── Config ────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN",   "8618135314:AAHoDrHGP2sncP1HxEGLDj0OKtIpSLeuD0U")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "8563709547")
TELEGRAM_API     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
DATA_DIR         = os.getenv("DATA_DIR", ".")   # directory with floorsheet CSVs

app = FastAPI()

# ── Telegram helpers ──────────────────────────────────────────────
async def send_message(chat_id, text, parse_mode="Markdown"):
    """Send a Telegram message, splitting if > 4000 chars."""
    chunks = [text[i:i+4000] for i in range(0, len(text), 4000)]
    async with httpx.AsyncClient() as client:
        for chunk in chunks:
            await client.post(f"{TELEGRAM_API}/sendMessage", json={
                "chat_id":    chat_id,
                "text":       chunk,
                "parse_mode": parse_mode
            })

async def send_document(chat_id, file_path, caption=""):
    """Send a file as document."""
    async with httpx.AsyncClient() as client:
        with open(file_path, "rb") as f:
            await client.post(f"{TELEGRAM_API}/sendDocument",
                data={"chat_id": chat_id, "caption": caption},
                files={"document": f}
            )

# ── Data helpers ──────────────────────────────────────────────────
def get_latest_floorsheet():
    files = sorted(glob.glob(os.path.join(DATA_DIR, "floorsheet_2026-*.csv")))
    files = [f for f in files if "dividend" not in f]
    return files[-1] if files else None

def load_latest_data(days=1):
    files = sorted(glob.glob(os.path.join(DATA_DIR, "floorsheet_2026-*.csv")))
    files = [f for f in files if "dividend" not in f]
    if not files:
        return None
    selected = files[-days:]
    dfs = [pd.read_csv(f) for f in selected]
    df  = pd.concat(dfs, ignore_index=True)
    df['contractAmount']   = pd.to_numeric(df['contractAmount'],   errors='coerce')
    df['contractQuantity'] = pd.to_numeric(df['contractQuantity'], errors='coerce')
    df['contractRate']     = pd.to_numeric(df['contractRate'],     errors='coerce')
    df['businessDate']     = pd.to_datetime(df['businessDate'])
    return df

def load_all_data(max_days=15):
    return load_latest_data(days=max_days)

def get_latest_strategy():
    files = sorted(glob.glob(os.path.join(DATA_DIR, "strategy_*.md")))
    return files[-1] if files else None

# ── Command handlers ──────────────────────────────────────────────

async def cmd_start(chat_id):
    msg = (
        "🎯 *NEPSE Strategy Bot*\n\n"
        "I give you real-time NEPSE insights pulled from floorsheet data.\n\n"
        "📋 *Commands:*\n"
        "/floorsheet — Today's floorsheet summary & CSV\n"
        "/strategy   — Full pre-market strategy report\n"
        "/sniper     — Sniper BUY/SELL signals\n"
        "/top5       — Top 5 stocks by turnover\n"
        "/whale      — Largest block trades today\n"
        "/broker     — Smart money net positions\n"
        "/help       — Show this menu\n\n"
        "_Data updates daily at 4 PM NPT via GitHub Actions._"
    )
    await send_message(chat_id, msg)

async def cmd_help(chat_id):
    await cmd_start(chat_id)

async def cmd_floorsheet(chat_id):
    f = get_latest_floorsheet()
    if not f:
        await send_message(chat_id, "❌ No floorsheet data found.")
        return
    df   = pd.read_csv(f)
    fname = Path(f).stem
    bdate = fname.replace("floorsheet_", "")
    total_trades    = len(df)
    total_turnover  = pd.to_numeric(df['contractAmount'], errors='coerce').sum()
    total_qty       = pd.to_numeric(df['contractQuantity'], errors='coerce').sum()
    unique_stocks   = df['stockSymbol'].nunique()

    msg = (
        f"📊 *Floorsheet Summary — {bdate}*\n\n"
        f"• Total Trades: `{total_trades:,}`\n"
        f"• Total Turnover: `Rs {total_turnover/1e9:.2f}B`\n"
        f"• Shares Traded: `{total_qty:,.0f}`\n"
        f"• Unique Stocks: `{unique_stocks}`\n\n"
        f"📎 Sending CSV file..."
    )
    await send_message(chat_id, msg)
    await send_document(chat_id, f, caption=f"NEPSE Floorsheet {bdate}")

async def cmd_strategy(chat_id):
    strat = get_latest_strategy()
    if not strat:
        await send_message(chat_id, "❌ No strategy report found. Check back after 10 AM NPT.")
        return
    with open(strat) as f:
        content = f.read()
    # Send full report as document + concise summary as message
    lines      = content.split("\n")
    bias_line  = next((l for l in lines if "Market Bias:" in l), "")
    buy_line   = next((l for l in lines if "Sniper BUY entries:" in l), "")
    near_line  = next((l for l in lines if "Near-Buy watchlist:" in l), "")
    exit_line  = next((l for l in lines if "Exit / avoid:" in l), "")
    mom_line   = next((l for l in lines if "Momentum scrips:" in l), "")

    msg = (
        f"📋 *Pre-Market Strategy Report*\n"
        f"_{Path(strat).stem}_\n\n"
        f"{bias_line.strip()}\n\n"
        f"🚀 {buy_line.strip()}\n"
        f"👀 {near_line.strip()}\n"
        f"🛑 {exit_line.strip()}\n"
        f"🔥 {mom_line.strip()}\n\n"
        f"📎 Full report attached below..."
    )
    await send_message(chat_id, msg)
    await send_document(chat_id, strat, caption="Full Strategy Report")

async def cmd_sniper(chat_id):
    from premarket_strategy import load_floorsheets, run_sniper_scan
    try:
        data, _ = load_floorsheets(DATA_DIR)
        sniper_df = run_sniper_scan(data, min_turnover_M=30)
    except Exception as e:
        await send_message(chat_id, f"❌ Sniper scan failed: {e}")
        return

    buys  = sniper_df[sniper_df['signal'] == '🚀 SNIPER BUY'].sort_values('confidence', ascending=False)
    near  = sniper_df[sniper_df['signal'] == '👀 NEAR BUY'].head(5)
    sells = sniper_df[sniper_df['signal'] == '🛑 SELL / EXIT'].head(5)

    msg = "🎯 *SNIPER SHOT PRO v2 — Live Scan*\n\n"

    msg += f"🚀 *BUY SIGNALS ({len(buys)})*\n"
    if len(buys) > 0:
        for _, r in buys.iterrows():
            msg += (f"• *{r['symbol']}* @ Rs {r['price']:.1f}\n"
                    f"  Entry:{r['entry']} | SL:{r['stop_loss']} | T1:{r['target1']} | T2:{r['target2']}\n"
                    f"  RSI:{r['rsi']} | R:R {r['rr']}x | {r['confidence']}% confidence\n\n")
    else:
        msg += "  None — market not ready for entry\n\n"

    msg += f"👀 *NEAR BUY ({len(near)})*\n"
    for _, r in near.iterrows():
        missing = [k for k, v in r['rules'].items() if not v]
        msg += f"• *{r['symbol']}* Rs {r['price']:.1f} — missing: {', '.join(missing)}\n"

    msg += f"\n🛑 *EXIT SIGNALS ({len(sells)})*\n"
    for _, r in sells.iterrows():
        msg += f"• *{r['symbol']}* Rs {r['price']:.1f} | RSI {r['rsi']}\n"

    msg += "\n_Not financial advice. DYOR._"
    await send_message(chat_id, msg)

async def cmd_top5(chat_id):
    df = load_latest_data(days=1)
    if df is None:
        await send_message(chat_id, "❌ No data available.")
        return
    top = (df.groupby('stockSymbol')['contractAmount'].sum()
             .sort_values(ascending=False).head(5))
    bdate = df['businessDate'].max().date()
    msg = f"🔥 *Top 5 Stocks by Turnover — {bdate}*\n\n"
    for i, (sym, amt) in enumerate(top.items(), 1):
        qty  = df[df['stockSymbol']==sym]['contractQuantity'].sum()
        avg  = df[df['stockSymbol']==sym]['contractRate'].mean()
        msg += f"{i}. *{sym}*\n   Rs {amt/1e6:.1f}M | {qty:,.0f} shares | avg Rs {avg:.1f}\n\n"
    await send_message(chat_id, msg)

async def cmd_whale(chat_id):
    df = load_latest_data(days=1)
    if df is None:
        await send_message(chat_id, "❌ No data available.")
        return
    top = df.nlargest(5, 'contractAmount')
    bdate = df['businessDate'].max().date()
    msg = f"🐋 *Largest Block Trades — {bdate}*\n\n"
    for _, r in top.iterrows():
        msg += (f"• *{r['stockSymbol']}* — Rs {r['contractAmount']/1e6:.1f}M\n"
                f"  {int(r['contractQuantity']):,} shares @ Rs {r['contractRate']:.0f}\n"
                f"  🟢 {r['buyerBrokerName'][:30]}\n"
                f"  🔴 {r['sellerBrokerName'][:30]}\n\n")
    await send_message(chat_id, msg)

async def cmd_broker(chat_id):
    df = load_all_data(max_days=15)
    if df is None:
        await send_message(chat_id, "❌ No data available.")
        return
    buy  = df.groupby('buyerBrokerName')['contractAmount'].sum().rename('bought')
    sell = df.groupby('sellerBrokerName')['contractAmount'].sum().rename('sold')
    net  = pd.concat([buy, sell], axis=1).fillna(0)
    net['net'] = net['bought'] - net['sold']

    top_acc  = net.sort_values('net', ascending=False).head(5)
    top_dist = net.sort_values('net', ascending=True).head(5)

    msg = "🐋 *Smart Money — Net Positions (15 Days)*\n\n"
    msg += "🟢 *Top Accumulators (Net Buyers)*\n"
    for broker, r in top_acc.iterrows():
        msg += f"• {broker[:35]}\n  Net *+Rs {r['net']/1e6:.1f}M*\n"
    msg += "\n🔴 *Top Distributors (Net Sellers)*\n"
    for broker, r in top_dist.iterrows():
        msg += f"• {broker[:35]}\n  Net *Rs {r['net']/1e6:.1f}M*\n"
    await send_message(chat_id, msg)

# ── Command router ────────────────────────────────────────────────
COMMANDS = {
    "/start":       cmd_start,
    "/help":        cmd_help,
    "/floorsheet":  cmd_floorsheet,
    "/strategy":    cmd_strategy,
    "/sniper":      cmd_sniper,
    "/top5":        cmd_top5,
    "/whale":       cmd_whale,
    "/broker":      cmd_broker,
}

# ── Webhook endpoint ──────────────────────────────────────────────
@app.post("/webhook")
async def webhook(request: Request):
    body = await request.json()
    msg  = body.get("message") or body.get("edited_message", {})
    if not msg:
        return {"ok": True}

    chat_id = msg.get("chat", {}).get("id")
    text    = msg.get("text", "").strip().lower().split("@")[0]  # strip bot username

    handler = COMMANDS.get(text)
    if handler:
        await handler(chat_id)
    else:
        await send_message(chat_id,
            "❓ Unknown command. Send /help to see all commands.")
    return {"ok": True}

@app.get("/")
async def root():
    return {"status": "NEPSE Bot running 🚀"}

@app.get("/health")
async def health():
    return {"status": "ok"}
