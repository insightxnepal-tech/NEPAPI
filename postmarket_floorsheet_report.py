#!/usr/bin/env python3
"""
Post-market NEPSE floorsheet analysis → Telegram.

Uses the latest floorsheet_YYYY-MM-DD.csv in the repo, builds a session
report (overview, top turnover, movers, whales, broker net flow), and
sends it to Telegram. Requires TELEGRAM_TOKEN and TELEGRAM_CHAT_ID.
"""

import glob
import json
import mimetypes
import os
import re
import sys
import urllib.error
import urllib.request
import uuid
from datetime import date
from pathlib import Path

import pandas as pd

TOKEN = os.getenv("TELEGRAM_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DATA_DIR = os.getenv("DATA_DIR", ".")
API = f"https://api.telegram.org/bot{TOKEN}"


def latest_floorsheet(data_dir="."):
    files = sorted(glob.glob(os.path.join(data_dir, "floorsheet_20*.csv")))
    files = [f for f in files if "dividend" not in f]
    if not files:
        return None

    def key(path):
        m = re.search(r"(\d{4}-\d{2}-\d{2})", os.path.basename(path))
        return m.group(1) if m else "0000-00-00"

    files.sort(key=key)
    return files[-1]


def latest_strategy(data_dir="."):
    files = sorted(glob.glob(os.path.join(data_dir, "strategy_*.md")))
    return files[-1] if files else None


def tg(method, **params):
    data = json.dumps(params).encode()
    req = urllib.request.Request(
        f"{API}/{method}",
        data=data,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"TG error {method}: HTTP {e.code} {body[:300]}")
        if "parse_mode" in params:
            params = dict(params)
            params.pop("parse_mode", None)
            return tg(method, **params)
        return {}
    except Exception as e:
        print(f"TG error {method}: {e}")
        return {}


def send(text):
    ok = True
    for chunk in [text[i:i + 3900] for i in range(0, len(text), 3900)]:
        res = tg("sendMessage", chat_id=CHAT_ID, text=chunk, parse_mode="Markdown")
        if not res.get("ok"):
            print("send failed:", res)
            ok = False
        else:
            print(f"sent {len(chunk)} chars")
    return ok


def send_file(filepath, caption=""):
    boundary = uuid.uuid4().hex
    mime = mimetypes.guess_type(filepath)[0] or "application/octet-stream"
    fname = Path(filepath).name
    with open(filepath, "rb") as f:
        file_data = f.read()
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="chat_id"\r\n\r\n'
        f"{CHAT_ID}\r\n"
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="caption"\r\n\r\n'
        f"{caption}\r\n"
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="document"; filename="{fname}"\r\n'
        f"Content-Type: {mime}\r\n\r\n"
    ).encode() + file_data + f"\r\n--{boundary}--\r\n".encode()
    req = urllib.request.Request(
        f"{API}/sendDocument",
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=90) as r:
            res = json.loads(r.read())
            print(f"file sent: {fname} ok={res.get('ok')}")
            return res
    except Exception as e:
        print(f"File send error: {e}")
        return {}


def esc(s):
    return str(s).replace("_", "\\_").replace("*", "\\*").replace("`", "\\`")


def npr(x):
    ax = abs(x)
    if ax >= 1e9:
        return f"Rs {x / 1e9:.2f}B"
    if ax >= 1e6:
        return f"Rs {x / 1e6:.1f}M"
    if ax >= 1e5:
        return f"Rs {x / 1e5:.1f}L"
    return f"Rs {x:,.0f}"


def load_session(csv_path):
    df = pd.read_csv(csv_path)
    for col in ["contractAmount", "contractQuantity", "contractRate"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["businessDate"] = pd.to_datetime(df["businessDate"])
    ohlc = (
        df.sort_values("contractId")
        .groupby("stockSymbol")
        .agg(
            open=("contractRate", "first"),
            high=("contractRate", "max"),
            low=("contractRate", "min"),
            close=("contractRate", "last"),
            volume=("contractQuantity", "sum"),
            turnover=("contractAmount", "sum"),
            trades=("contractId", "count"),
        )
    )
    ohlc["chg"] = (ohlc["close"] - ohlc["open"]) / ohlc["open"] * 100
    ohlc["spread"] = ohlc["high"] - ohlc["low"]
    return df, ohlc


def broker_net(df):
    buy = df.groupby("buyerBrokerName")["contractAmount"].sum().rename("bought")
    sel = df.groupby("sellerBrokerName")["contractAmount"].sum().rename("sold")
    net = pd.concat([buy, sel], axis=1).fillna(0)
    net["net"] = net["bought"] - net["sold"]
    return net


def send_report(df, ohlc, csv_path, strategy_path=None):
    bdate = df["businessDate"].max().date()
    total_to = df["contractAmount"].sum()
    total_qty = df["contractQuantity"].sum()
    n_trades = len(df)
    n_stocks = df["stockSymbol"].nunique()
    adv = int((ohlc["chg"] > 0).sum())
    dec = int((ohlc["chg"] < 0).sum())
    unch = int((ohlc["chg"] == 0).sum())
    bias = "BULLISH" if adv > dec else "BEARISH" if dec > adv else "MIXED"
    emoji = "📈" if adv > dec else "📉"

    send(
        f"📊 *NEPSE FLOORSHEET REPORT*\n"
        f"*Session:* `{bdate}`\n"
        f"_Post-market analysis from repo floorsheet_\n\n"
        f"• Trades: `{n_trades:,}`\n"
        f"• Turnover: `{npr(total_to)}`\n"
        f"• Shares: `{total_qty:,.0f}`\n"
        f"• Stocks: `{n_stocks}`\n"
        f"• Avg trade: `{npr(total_to / max(n_trades, 1))}`\n"
        f"• Breadth: `{adv}` up / `{dec}` down / `{unch}` flat\n"
        f"• Session bias: *{bias}*\n\n"
        f"📎 CSV + breakdown following..."
    )
    send_file(csv_path, caption=f"NEPSE Floorsheet {bdate} ({n_trades:,} trades)")

    top_to = ohlc.sort_values("turnover", ascending=False).head(10)
    msg = f"🔥 *TOP 10 BY TURNOVER — {bdate}*\n\n"
    for i, (sym, r) in enumerate(top_to.iterrows(), 1):
        arrow = "🟢" if r["chg"] >= 0 else "🔴"
        msg += (
            f"{i}. *{esc(sym)}* {arrow} `{r['chg']:+.2f}%`\n"
            f"   {npr(r['turnover'])} | {r['volume']:,.0f} sh | {int(r['trades']):,} txs\n"
            f"   O `{r['open']:.1f}` H `{r['high']:.1f}` L `{r['low']:.1f}` C `{r['close']:.1f}`\n\n"
        )
    send(msg)

    liquid = ohlc[ohlc["turnover"] >= 5e6]
    gainers = liquid.sort_values("chg", ascending=False).head(8)
    losers = liquid.sort_values("chg", ascending=True).head(8)
    msg = f"📈 *TOP GAINERS* (min Rs 5M turnover)\n\n"
    for i, (sym, r) in enumerate(gainers.iterrows(), 1):
        msg += (
            f"{i}. *{esc(sym)}* 🟢 `{r['chg']:+.2f}%`  "
            f"`{r['open']:.1f} → {r['close']:.1f}`  {npr(r['turnover'])}\n"
        )
    msg += f"\n📉 *TOP LOSERS*\n\n"
    for i, (sym, r) in enumerate(losers.iterrows(), 1):
        msg += (
            f"{i}. *{esc(sym)}* 🔴 `{r['chg']:+.2f}%`  "
            f"`{r['open']:.1f} → {r['close']:.1f}`  {npr(r['turnover'])}\n"
        )
    send(msg)

    msg = f"🐋 *BLOCK TRADES — {bdate}*\n\n"
    for _, r in df.nlargest(8, "contractAmount").iterrows():
        msg += (
            f"• *{esc(r['stockSymbol'])}* {npr(r['contractAmount'])}\n"
            f"  {int(r['contractQuantity']):,} @ Rs {r['contractRate']:.0f}\n"
            f"  🟢 {esc(str(r['buyerBrokerName'])[:34])}\n"
            f"  🔴 {esc(str(r['sellerBrokerName'])[:34])}\n\n"
        )
    send(msg)

    net = broker_net(df)
    msg = f"🏦 *BROKER NET FLOW — {bdate}*\n\n🟢 *Top Net Buyers*\n"
    for b, r in net.sort_values("net", ascending=False).head(8).iterrows():
        msg += (
            f"• {esc(str(b)[:36])}\n"
            f"  *+{npr(r['net'])}*  (buy {npr(r['bought'])} / sell {npr(r['sold'])})\n"
        )
    msg += "\n🔴 *Top Net Sellers*\n"
    for b, r in net.sort_values("net", ascending=True).head(8).iterrows():
        msg += (
            f"• {esc(str(b)[:36])}\n"
            f"  *{npr(r['net'])}*  (buy {npr(r['bought'])} / sell {npr(r['sold'])})\n"
        )
    send(msg)

    msg = f"🎯 *WHO ACCUMULATED TODAY'S TOP STOCKS*\n"
    for sym in top_to.head(5).index:
        sd = df[df["stockSymbol"] == sym]
        snet = broker_net(sd)
        close = ohlc.loc[sym, "close"]
        chg = ohlc.loc[sym, "chg"]
        arrow = "🟢" if chg >= 0 else "🔴"
        msg += (
            f"\n*{esc(sym)}* {arrow} Rs {close:.1f} ({chg:+.1f}%)  "
            f"{npr(ohlc.loc[sym, 'turnover'])}\n"
        )
        for b, r in snet.sort_values("net", ascending=False).head(3).iterrows():
            if r["net"] > 0:
                msg += f"  🟢 {esc(str(b)[:30])} +{npr(r['net'])}\n"
        for b, r in snet.sort_values("net", ascending=True).head(2).iterrows():
            if r["net"] < 0:
                msg += f"  🔴 {esc(str(b)[:30])} {npr(r['net'])}\n"
    send(msg)

    most_trades = ohlc.sort_values("trades", ascending=False).head(8)
    high_spread = liquid.assign(spread_pct=lambda x: x["spread"] / x["open"] * 100)
    high_spread = high_spread.sort_values("spread_pct", ascending=False).head(8)
    msg = f"⚡ *MOST ACTIVE (by trades) — {bdate}*\n\n"
    for i, (sym, r) in enumerate(most_trades.iterrows(), 1):
        msg += (
            f"{i}. *{esc(sym)}* `{int(r['trades']):,}` txs | "
            f"{npr(r['turnover'])} | C `{r['close']:.1f}`\n"
        )
    msg += f"\n📏 *WIDEST INTRADAY SPREAD* (min Rs 5M)\n\n"
    for i, (sym, r) in enumerate(high_spread.iterrows(), 1):
        msg += (
            f"{i}. *{esc(sym)}* `{r['spread_pct']:.1f}%`  "
            f"H `{r['high']:.1f}` L `{r['low']:.1f}` C `{r['close']:.1f}`\n"
        )
    send(msg)

    if strategy_path and os.path.exists(strategy_path):
        with open(strategy_path) as f:
            content = f.read()
        lines = content.split("\n")
        bias_line = next((l for l in lines if "Market Bias:" in l), "")
        buy_line = next((l for l in lines if "Sniper BUY entries:" in l), "")
        near_line = next((l for l in lines if "Near-Buy watchlist:" in l), "")
        exit_line = next((l for l in lines if "Exit / avoid:" in l), "")
        mom_line = next((l for l in lines if "Momentum scrips:" in l), "")
        send(
            f"📋 *PRE-MARKET STRATEGY*\n"
            f"_{esc(Path(strategy_path).stem)}_\n\n"
            f"{esc(bias_line.strip())}\n\n"
            f"🚀 {esc(buy_line.strip())}\n"
            f"👀 {esc(near_line.strip())}\n"
            f"🛑 {esc(exit_line.strip())}\n"
            f"🔥 {esc(mom_line.strip())}\n\n"
            f"📎 Full strategy report attached."
        )
        send_file(strategy_path, caption="Full Pre-Market Strategy Report")

    send(
        f"{emoji} *SESSION WRAP — {bdate}*\n\n"
        f"Advancers `{adv}`  |  Decliners `{dec}`  |  Unchanged `{unch}`\n"
        f"Session bias: *{bias}*\n"
        f"Turnover: *{npr(total_to)}* across `{n_stocks}` stocks\n\n"
        f"_Auto-generated from repo floorsheet. Not financial advice. DYOR._"
    )


def write_markdown(df, ohlc, out_path):
    bdate = df["businessDate"].max().date()
    total_to = df["contractAmount"].sum()
    total_qty = df["contractQuantity"].sum()
    n_trades = len(df)
    n_stocks = df["stockSymbol"].nunique()
    adv = int((ohlc["chg"] > 0).sum())
    dec = int((ohlc["chg"] < 0).sum())
    unch = int((ohlc["chg"] == 0).sum())
    net = broker_net(df)
    top_to = ohlc.sort_values("turnover", ascending=False).head(10)
    liquid = ohlc[ohlc["turnover"] >= 5e6]
    lines = [
        f"# NEPSE Floorsheet Report — {bdate}",
        "",
        f"- Trades: **{n_trades:,}**",
        f"- Turnover: **{npr(total_to)}**",
        f"- Shares: **{total_qty:,.0f}**",
        f"- Stocks: **{n_stocks}**",
        f"- Breadth: **{adv}** up / **{dec}** down / **{unch}** flat",
        "",
        "## Top 10 by Turnover",
        "",
        "| # | Symbol | Turnover | Volume | Trades | Open | Close | Change |",
        "|---|--------|----------|--------|--------|------|-------|--------|",
    ]
    for i, (sym, r) in enumerate(top_to.iterrows(), 1):
        lines.append(
            f"| {i} | **{sym}** | {npr(r['turnover'])} | {r['volume']:,.0f} | "
            f"{int(r['trades']):,} | {r['open']:.1f} | {r['close']:.1f} | {r['chg']:+.2f}% |"
        )
    lines += ["", "## Top Gainers (min Rs 5M)", "",
              "| Symbol | Change | Open | Close | Turnover |",
              "|--------|--------|------|-------|----------|"]
    for sym, r in liquid.sort_values("chg", ascending=False).head(8).iterrows():
        lines.append(
            f"| **{sym}** | {r['chg']:+.2f}% | {r['open']:.1f} | {r['close']:.1f} | {npr(r['turnover'])} |"
        )
    lines += ["", "## Top Losers (min Rs 5M)", "",
              "| Symbol | Change | Open | Close | Turnover |",
              "|--------|--------|------|-------|----------|"]
    for sym, r in liquid.sort_values("chg").head(8).iterrows():
        lines.append(
            f"| **{sym}** | {r['chg']:+.2f}% | {r['open']:.1f} | {r['close']:.1f} | {npr(r['turnover'])} |"
        )
    lines += ["", "## Block Trades", "",
              "| Stock | Qty | Price | Amount | Buyer | Seller |",
              "|-------|-----|-------|--------|-------|--------|"]
    for _, r in df.nlargest(8, "contractAmount").iterrows():
        lines.append(
            f"| **{r['stockSymbol']}** | {int(r['contractQuantity']):,} | "
            f"Rs {r['contractRate']:.0f} | {npr(r['contractAmount'])} | "
            f"{str(r['buyerBrokerName'])[:28]} | {str(r['sellerBrokerName'])[:28]} |"
        )
    lines += ["", "## Broker Net Buyers", "",
              "| Broker | Net | Bought | Sold |",
              "|--------|-----|--------|------|"]
    for b, r in net.sort_values("net", ascending=False).head(8).iterrows():
        lines.append(
            f"| {str(b)[:42]} | +{npr(r['net'])} | {npr(r['bought'])} | {npr(r['sold'])} |"
        )
    lines += ["", "## Broker Net Sellers", "",
              "| Broker | Net | Bought | Sold |",
              "|--------|-----|--------|------|"]
    for b, r in net.sort_values("net").head(8).iterrows():
        lines.append(
            f"| {str(b)[:42]} | {npr(r['net'])} | {npr(r['bought'])} | {npr(r['sold'])} |"
        )
    lines += ["", "---", "*Auto-generated from NEPSE floorsheet data. Not financial advice.*", ""]
    Path(out_path).write_text("\n".join(lines))
    print(f"saved {out_path}")


def main():
    if not TOKEN or not CHAT_ID:
        print("ERROR: TELEGRAM_TOKEN and TELEGRAM_CHAT_ID must be set.")
        sys.exit(1)

    csv_path = latest_floorsheet(DATA_DIR)
    if not csv_path:
        print("ERROR: no floorsheet_YYYY-MM-DD.csv found.")
        sys.exit(1)

    print(f"Using {csv_path}")
    df, ohlc = load_session(csv_path)
    bdate = df["businessDate"].max().date()
    md_path = os.path.join(DATA_DIR, f"floorsheet_report_{bdate}.md")
    write_markdown(df, ohlc, md_path)
    send_report(df, ohlc, csv_path, latest_strategy(DATA_DIR))
    print("DONE")


if __name__ == "__main__":
    main()
