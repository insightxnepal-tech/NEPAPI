#!/usr/bin/env python3
"""
NEPSE Pre-Market Strategy Report  (with SNIPER SHOT PRO v2)
Runs before market open using last 15 days of floorsheet data.
Sniper signals: EMA9/21/50/200 cross, RSI 50-65, MACD, Volume surge, ATR stops.
"""

import pandas as pd
import numpy as np
import glob, os, sys
from datetime import datetime, date
import warnings
warnings.filterwarnings('ignore')

# ═══════════════════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════════════════
def load_floorsheets(data_dir="."):
    files = sorted(glob.glob(os.path.join(data_dir, "floorsheet_2026-*.csv")))
    files = [f for f in files if "dividend" not in f]
    files = files[-15:]
    if not files:
        raise FileNotFoundError("No floorsheet CSV files found.")
    dfs = [pd.read_csv(f) for f in files]
    data = pd.concat(dfs, ignore_index=True)
    data['contractAmount']   = pd.to_numeric(data['contractAmount'],   errors='coerce')
    data['contractQuantity'] = pd.to_numeric(data['contractQuantity'], errors='coerce')
    data['contractRate']     = pd.to_numeric(data['contractRate'],     errors='coerce')
    data['businessDate']     = pd.to_datetime(data['businessDate'])
    return data, files

# ═══════════════════════════════════════════════════════════════════
# SNIPER SHOT PRO v2  — built from floorsheet OHLCV
# ═══════════════════════════════════════════════════════════════════
def build_ohlcv(data):
    """Build daily OHLCV per symbol from floorsheet contractRate & contractQuantity."""
    g = data.groupby(['stockSymbol', 'businessDate'])
    ohlcv = g['contractRate'].agg(
        open='first', high='max', low='min', close='last'
    ).reset_index()
    vol = g['contractQuantity'].sum().reset_index(name='volume')
    turnover = g['contractAmount'].sum().reset_index(name='turnover')
    ohlcv = ohlcv.merge(vol, on=['stockSymbol','businessDate'])
    ohlcv = ohlcv.merge(turnover, on=['stockSymbol','businessDate'])
    return ohlcv

def calc_sniper_indicators(df):
    """Compute indicators for one symbol's daily OHLCV. Returns dict or None."""
    df = df.sort_values('businessDate').reset_index(drop=True)
    if len(df) < 5:          # need at least 5 days; EMAs will be short but signal from available data
        return None
    close  = df['close']
    high   = df['high']
    low    = df['low']
    volume = df['volume']

    # EMAs — using available days (span adjusts naturally for small N)
    ema9   = close.ewm(span=min(9,  len(df)), adjust=False).mean()
    ema21  = close.ewm(span=min(21, len(df)), adjust=False).mean()
    ema50  = close.ewm(span=min(50, len(df)), adjust=False).mean()

    # RSI(14)
    delta = close.diff()
    gain  = delta.where(delta > 0, 0.0).rolling(min(14, len(df))).mean()
    loss  = (-delta.where(delta < 0, 0.0)).rolling(min(14, len(df))).mean()
    rs    = gain / loss.replace(0, 0.001)
    rsi   = 100 - (100 / (1 + rs))

    # MACD(12,26,9)
    ema12      = close.ewm(span=min(12, len(df)), adjust=False).mean()
    ema26      = close.ewm(span=min(26, len(df)), adjust=False).mean()
    macd_line  = ema12 - ema26
    macd_sig   = macd_line.ewm(span=min(9, len(df)), adjust=False).mean()

    # ATR(14)
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low  - close.shift(1)).abs()
    atr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1).rolling(min(14, len(df))).mean()

    # Volume MA(20)
    vol_ma = volume.rolling(min(20, len(df))).mean()

    i  = -1   # last row
    i2 = -2   # prev row

    price = close.iloc[i]
    return {
        'price':      price,
        'open':       df['open'].iloc[i],
        'high':       high.iloc[i],
        'low':        low.iloc[i],
        'volume':     volume.iloc[i],
        'turnover':   df['turnover'].iloc[i],
        # EMAs
        'ema9':       ema9.iloc[i],   'prev_ema9':  ema9.iloc[i2],
        'ema21':      ema21.iloc[i],  'prev_ema21': ema21.iloc[i2],
        'ema50':      ema50.iloc[i],
        # RSI
        'rsi':        rsi.iloc[i],    'prev_rsi':   rsi.iloc[i2],
        # MACD
        'macd':       macd_line.iloc[i], 'macd_sig': macd_sig.iloc[i],
        # ATR / Vol
        'atr':        atr.iloc[i],
        'vol_ma20':   vol_ma.iloc[i],
        'days':       len(df),
    }

def candle_filter(s):
    """Returns True if candle pattern blocks trade (doji / wicky)."""
    body        = abs(s['open'] - s['price'])
    upper_wick  = s['high'] - max(s['open'], s['price'])
    lower_wick  = min(s['open'], s['price']) - s['low']
    total_wick  = upper_wick + lower_wick
    return total_wick > 2 * body

def sniper_signal(s):
    """
    SNIPER SHOT PRO v2 rules (adapted for short history).
    Returns: signal str, entry, stop_loss, target1, target2, confidence%
    """
    # Candle & EMA flat filter
    blocked = candle_filter(s)
    ema_spread = (max(s['ema9'], s['ema21'], s['ema50']) -
                  min(s['ema9'], s['ema21'], s['ema50'])) / s['price']
    emas_flat  = ema_spread < 0.01

    # ── BUY RULES (all 5 must fire) ─────────────────────────────────
    b1 = s['prev_ema9'] <= s['prev_ema21'] and s['ema9'] > s['ema21']   # EMA9 crosses above EMA21
    b2 = s['price'] > s['ema50']                                          # Price above EMA50
    b3 = 50 <= s['rsi'] <= 70 and s['rsi'] > s['prev_rsi']              # RSI rising 50-70
    b4 = s['macd'] > s['macd_sig'] and s['macd'] > 0                    # MACD bullish above zero
    b5 = s['volume'] > s['vol_ma20']                                      # Volume surge

    buy_rules = [b1, b2, b3, b4, b5]
    buy_score = sum(buy_rules)

    # ── SELL RULES (any 1 triggers) ──────────────────────────────────
    s1 = s['prev_ema9'] >= s['prev_ema21'] and s['ema9'] < s['ema21']   # EMA9 crosses below EMA21
    s2 = s['price'] < s['ema50']                                          # Price below EMA50
    s3 = s['rsi'] < 45 and s['macd'] < s['macd_sig']                    # Weak RSI + bearish MACD

    sell_signal = s1 or s2 or s3

    # ── NEAR-BUY (4/5 rules) ─────────────────────────────────────────
    near_buy = buy_score >= 4

    # ── ATR-based levels ─────────────────────────────────────────────
    atr  = s['atr'] if s['atr'] > 0 else s['price'] * 0.02
    entry    = round(s['price'], 2)
    sl       = round(entry - 1.5 * atr, 2)
    target1  = round(entry + 2.0 * atr, 2)
    target2  = round(entry + 3.5 * atr, 2)
    rr_ratio = round((target1 - entry) / max(entry - sl, 0.01), 2)

    confidence = int((buy_score / 5) * 100)

    if buy_score == 5 and not blocked and not emas_flat:
        signal = "🚀 SNIPER BUY"
    elif buy_score == 5 and (blocked or emas_flat):
        signal = "⚠️ BUY BLOCKED"
    elif near_buy and not sell_signal:
        signal = "👀 NEAR BUY"
    elif sell_signal:
        signal = "🛑 SELL / EXIT"
        confidence = 100
    else:
        signal = "NEUTRAL"

    return {
        'signal':     signal,
        'entry':      entry,
        'stop_loss':  sl,
        'target1':    target1,
        'target2':    target2,
        'rr':         rr_ratio,
        'confidence': confidence,
        'buy_score':  buy_score,
        'rules':      {'EMA_cross': b1, 'Above_EMA50': b2, 'RSI': b3, 'MACD': b4, 'Volume': b5},
    }

def run_sniper_scan(data, min_turnover_M=50):
    """Run sniper on all symbols with sufficient turnover."""
    ohlcv = build_ohlcv(data)
    # Only scan stocks with meaningful turnover
    turnover_filter = data.groupby('stockSymbol')['contractAmount'].sum()
    active_symbols  = turnover_filter[turnover_filter >= min_turnover_M * 1e6].index.tolist()

    results = []
    for sym in active_symbols:
        df_sym = ohlcv[ohlcv['stockSymbol'] == sym].copy()
        stats  = calc_sniper_indicators(df_sym)
        if stats is None:
            continue
        sig = sniper_signal(stats)
        results.append({
            'symbol':     sym,
            'price':      stats['price'],
            'rsi':        round(stats['rsi'], 1),
            'macd':       round(stats['macd'], 2),
            'volume':     int(stats['volume']),
            'vol_ma20':   int(stats['vol_ma20']),
            'atr':        round(stats['atr'], 2),
            **sig,
        })

    return pd.DataFrame(results)

# ═══════════════════════════════════════════════════════════════════
# ORIGINAL STRATEGY HELPERS
# ═══════════════════════════════════════════════════════════════════
def momentum_scrips(data, top_n=10):
    daily_buy   = data.groupby(['businessDate','stockSymbol'])['contractAmount'].sum().reset_index()
    days_active = daily_buy.groupby('stockSymbol')['businessDate'].nunique().rename('days_active')
    total_buy   = data.groupby('stockSymbol')['contractAmount'].sum().rename('total_turnover')
    avg_price   = data.groupby('stockSymbol')['contractRate'].mean().rename('avg_price')
    last_date   = data['businessDate'].max()
    recent      = data[data['businessDate'] >= last_date - pd.Timedelta(days=4)]
    recent_buy  = recent.groupby('stockSymbol')['contractAmount'].sum().rename('recent_3d_turnover')
    combined    = pd.concat([days_active, total_buy, avg_price, recent_buy], axis=1).fillna(0)
    combined['momentum_score'] = (
        combined['days_active'] * 0.3 +
        combined['total_turnover'] / combined['total_turnover'].max() * 40 +
        combined['recent_3d_turnover'] / (combined['total_turnover'] + 1) * 30
    )
    return combined.sort_values('momentum_score', ascending=False).head(top_n)

def broker_net_positions(data):
    buy  = data.groupby('buyerBrokerName')['contractAmount'].sum().rename('bought')
    sell = data.groupby('sellerBrokerName')['contractAmount'].sum().rename('sold')
    net  = pd.concat([buy, sell], axis=1).fillna(0)
    net['net'] = net['bought'] - net['sold']
    return net

def stock_accumulation(data, top_stocks=5):
    top    = data.groupby('stockSymbol')['contractAmount'].sum().nlargest(top_stocks).index
    result = {}
    for sym in top:
        sd  = data[data['stockSymbol'] == sym]
        buy = sd.groupby('buyerBrokerName')['contractAmount'].sum()
        sel = sd.groupby('sellerBrokerName')['contractAmount'].sum()
        net = pd.concat([buy.rename('bought'), sel.rename('sold')], axis=1).fillna(0)
        net['net'] = net['bought'] - net['sold']
        net['avg_buy_price'] = sd.groupby('buyerBrokerName')['contractRate'].mean()
        result[sym] = net.sort_values('net', ascending=False).head(5)
    return result

def price_trends(data, top_n=15):
    last_date   = data['businessDate'].max()
    last_prices = data[data['businessDate']==last_date].groupby('stockSymbol')['contractRate'].mean().rename('last_price')
    avg_prices  = data.groupby('stockSymbol')['contractRate'].mean().rename('avg_price')
    vol         = data.groupby('stockSymbol')['contractAmount'].sum().rename('total_turnover')
    trend       = pd.concat([last_prices, avg_prices, vol], axis=1).dropna()
    trend['change_pct'] = ((trend['last_price'] - trend['avg_price']) / trend['avg_price'] * 100).round(2)
    return trend.sort_values('total_turnover', ascending=False).head(top_n)

def whale_trades(data, top_n=10):
    return data.nlargest(top_n, 'contractAmount')[
        ['businessDate','stockSymbol','contractQuantity','contractRate',
         'contractAmount','buyerBrokerName','sellerBrokerName']
    ]

# ═══════════════════════════════════════════════════════════════════
# REPORT GENERATOR
# ═══════════════════════════════════════════════════════════════════
def generate_report(data_dir="."):
    data, files  = load_floorsheets(data_dir)
    today        = date.today().strftime("%Y-%m-%d")
    trading_days = data['businessDate'].nunique()
    last_date    = data['businessDate'].max().date()

    lines = []
    lines.append(f"# 🎯 NEPSE PRE-MARKET STRATEGY REPORT")
    lines.append(f"**Date:** {today}  |  **Data:** Last {trading_days} trading days (up to {last_date})")
    lines.append(f"**Trades Analyzed:** {len(data):,}  |  **Total Turnover:** Rs {data['contractAmount'].sum()/1e9:.2f}B")
    lines.append("")

    # ── MARKET PULSE ────────────────────────────────────────────────
    lines.append("---")
    lines.append("## 📊 MARKET PULSE")
    daily      = data.groupby('businessDate')['contractAmount'].sum().sort_index()
    recent_avg = daily.iloc[-3:].mean() / 1e9
    prior_avg  = daily.iloc[:-3].mean() / 1e9 if len(daily) > 3 else recent_avg
    arrow      = "📈 BULLISH" if recent_avg > prior_avg else "📉 CAUTIOUS"
    lines.append(f"- Recent 3-day avg: **Rs {recent_avg:.2f}B**  |  Prior avg: **Rs {prior_avg:.2f}B**")
    lines.append(f"- **Market Bias: {arrow}**")
    lines.append("")

    # ══════════════════════════════════════════════════════════════
    # SNIPER SHOT PRO v2
    # ══════════════════════════════════════════════════════════════
    lines.append("---")
    lines.append("## 🎯 SNIPER SHOT PRO v2 — BUY / SELL SIGNALS")
    lines.append("*EMA9/21/50 cross · RSI 50–70 rising · MACD bullish · Volume surge · ATR stops*")
    lines.append("")

    sniper_df = run_sniper_scan(data, min_turnover_M=30)

    # BUY SIGNALS
    buys = sniper_df[sniper_df['signal'] == '🚀 SNIPER BUY'].sort_values('confidence', ascending=False)
    lines.append(f"### 🚀 SNIPER BUY SIGNALS ({len(buys)} stocks)")
    if len(buys) > 0:
        lines.append("")
        lines.append("| Symbol | Price | Entry | Stop Loss | Target 1 | Target 2 | R:R | RSI | MACD | Confidence |")
        lines.append("|--------|-------|-------|-----------|----------|----------|-----|-----|------|------------|")
        for _, r in buys.iterrows():
            lines.append(f"| **{r['symbol']}** | Rs {r['price']:.1f} | Rs {r['entry']} | Rs {r['stop_loss']} | Rs {r['target1']} | Rs {r['target2']} | {r['rr']}x | {r['rsi']} | {r['macd']:.2f} | {r['confidence']}% |")
    else:
        lines.append("*No full BUY signals today. Watch NEAR BUY setups below.*")
    lines.append("")

    # NEAR BUY (4/5 rules)
    near = sniper_df[sniper_df['signal'] == '👀 NEAR BUY'].sort_values('buy_score', ascending=False).head(10)
    lines.append(f"### 👀 NEAR BUY SETUPS — 4/5 Rules Met ({len(near)} stocks)")
    lines.append("*One trigger away from full signal — watch these closely*")
    if len(near) > 0:
        lines.append("")
        lines.append("| Symbol | Price | Missing Rule | RSI | MACD | Entry | Stop | Target 1 |")
        lines.append("|--------|-------|-------------|-----|------|-------|------|----------|")
        for _, r in near.iterrows():
            missing = [k for k, v in r['rules'].items() if not v]
            missing_str = ', '.join(missing) if missing else 'Volume'
            lines.append(f"| **{r['symbol']}** | Rs {r['price']:.1f} | ❌ {missing_str} | {r['rsi']} | {r['macd']:.2f} | Rs {r['entry']} | Rs {r['stop_loss']} | Rs {r['target1']} |")
    lines.append("")

    # SELL / EXIT SIGNALS
    sells = sniper_df[sniper_df['signal'] == '🛑 SELL / EXIT'].head(10)
    lines.append(f"### 🛑 SELL / EXIT SIGNALS ({len(sells)} stocks)")
    lines.append("*If you hold these — consider exiting or tightening stop loss*")
    if len(sells) > 0:
        lines.append("")
        lines.append("| Symbol | Price | RSI | MACD | Signal Reason |")
        lines.append("|--------|-------|-----|------|---------------|")
        for _, r in sells.iterrows():
            reason = "EMA9 < EMA21" if r['price'] > r['entry'] else "Price < EMA50" if r['rsi'] < 50 else "RSI weak + MACD bearish"
            lines.append(f"| **{r['symbol']}** | Rs {r['price']:.1f} | {r['rsi']} | {r['macd']:.2f} | {reason} |")
    lines.append("")

    # BLOCKED SIGNALS
    blocked = sniper_df[sniper_df['signal'] == '⚠️ BUY BLOCKED'].head(5)
    if len(blocked) > 0:
        lines.append(f"### ⚠️ BUY BLOCKED (Doji/Wicky Candle or Flat EMAs) — {len(blocked)} stocks")
        lines.append("*All 5 rules met but candle pattern filter rejected the trade*")
        lines.append("")
        lines.append("| Symbol | Price | RSI | Note |")
        lines.append("|--------|-------|-----|------|")
        for _, r in blocked.iterrows():
            lines.append(f"| {r['symbol']} | Rs {r['price']:.1f} | {r['rsi']} | Wait for clean candle |")
        lines.append("")

    # ── MOMENTUM WATCHLIST ───────────────────────────────────────────
    lines.append("---")
    lines.append("## 🔥 MOMENTUM WATCHLIST (Floorsheet Accumulation)")
    lines.append("")
    lines.append("| # | Symbol | Days Active | Avg Price | Recent 3D Turnover | Score |")
    lines.append("|---|--------|------------|-----------|-------------------|-------|")
    mom = momentum_scrips(data, top_n=10)
    for i, (sym, r) in enumerate(mom.iterrows(), 1):
        # Cross-reference with sniper
        sniper_tag = ""
        if sym in sniper_df['symbol'].values:
            sig = sniper_df[sniper_df['symbol']==sym]['signal'].values[0]
            if "BUY" in sig: sniper_tag = " 🚀"
            elif "SELL" in sig: sniper_tag = " 🛑"
            elif "NEAR" in sig: sniper_tag = " 👀"
        lines.append(f"| {i} | **{sym}**{sniper_tag} | {int(r['days_active'])}/{trading_days} | Rs {r['avg_price']:.1f} | Rs {r['recent_3d_turnover']/1e6:.1f}M | {r['momentum_score']:.1f} |")
    lines.append("")

    # ── PRICE TREND ──────────────────────────────────────────────────
    lines.append("---")
    lines.append("## 📈 PRICE TREND vs Period Average")
    lines.append("")
    lines.append("| Symbol | Last Price | Avg Price | Change % | Sniper |")
    lines.append("|--------|-----------|-----------|----------|--------|")
    pt = price_trends(data, top_n=12)
    for sym, r in pt.iterrows():
        arrow  = "🟢" if r['change_pct'] > 0 else "🔴"
        stag   = ""
        if sym in sniper_df['symbol'].values:
            stag = sniper_df[sniper_df['symbol']==sym]['signal'].values[0]
        lines.append(f"| {sym} | Rs {r['last_price']:.1f} | Rs {r['avg_price']:.1f} | {arrow} {r['change_pct']:+.1f}% | {stag} |")
    lines.append("")

    # ── BROKER NET POSITIONS ─────────────────────────────────────────
    lines.append("---")
    lines.append("## 🐋 SMART MONEY — Net Accumulators")
    lines.append("")
    lines.append("| Broker | Net Bought | Total Bought | Total Sold |")
    lines.append("|--------|-----------|--------------|------------|")
    net_pos = broker_net_positions(data)
    for broker, r in net_pos.sort_values('net', ascending=False).head(8).iterrows():
        lines.append(f"| {broker[:42]} | **+Rs {r['net']/1e6:.1f}M** | Rs {r['bought']/1e6:.1f}M | Rs {r['sold']/1e6:.1f}M |")
    lines.append("")
    lines.append("### Net Distributors (Selling Pressure)")
    lines.append("| Broker | Net Sold |")
    lines.append("|--------|---------|")
    for broker, r in net_pos.sort_values('net', ascending=True).head(5).iterrows():
        lines.append(f"| {broker[:42]} | Rs {r['net']/1e6:.1f}M |")
    lines.append("")

    # ── STOCK ACCUMULATION ───────────────────────────────────────────
    lines.append("---")
    lines.append("## 🎯 WHO IS ACCUMULATING TOP STOCKS")
    acc = stock_accumulation(data, top_stocks=5)
    for sym, df in acc.items():
        avg_p = data[data['stockSymbol']==sym]['contractRate'].mean()
        stag  = ""
        if sym in sniper_df['symbol'].values:
            stag = "  " + sniper_df[sniper_df['symbol']==sym]['signal'].values[0]
        lines.append(f"\n### {sym} — Avg Rs {avg_p:.1f}{stag}")
        lines.append("| Broker | Net | Bought | Avg Buy Price |")
        lines.append("|--------|-----|--------|---------------|")
        for broker, r in df.iterrows():
            if r['net'] > 0:
                lines.append(f"| {broker[:42]} | +Rs {r['net']/1e6:.1f}M | Rs {r['bought']/1e6:.1f}M | Rs {r.get('avg_buy_price',0):.1f} |")
    lines.append("")

    # ── BLOCK DEALS ──────────────────────────────────────────────────
    lines.append("---")
    lines.append("## ⚡ BLOCK DEALS / WHALE MOVES")
    lines.append("")
    lines.append("| Date | Stock | Qty | Price | Amount | Buyer | Seller |")
    lines.append("|------|-------|-----|-------|--------|-------|--------|")
    wt = whale_trades(data, top_n=8)
    for _, r in wt.iterrows():
        lines.append(f"| {r['businessDate'].date()} | **{r['stockSymbol']}** | {int(r['contractQuantity']):,} | Rs {r['contractRate']:.0f} | Rs {r['contractAmount']/1e6:.1f}M | {r['buyerBrokerName'][:22]} | {r['sellerBrokerName'][:22]} |")
    lines.append("")

    # ── STRATEGY SUMMARY ─────────────────────────────────────────────
    lines.append("---")
    lines.append("## 🧠 TODAY'S TRADING PLAN")
    lines.append("")
    buy_list  = list(buys['symbol'].head(5)) if len(buys) > 0 else ["None today"]
    near_list = list(near['symbol'].head(5)) if len(near) > 0 else []
    sell_list = list(sells['symbol'].head(5)) if len(sells) > 0 else ["None"]
    top_mom   = list(mom.index[:5])
    acc_brkrs = list(net_pos.sort_values('net', ascending=False).head(3).index)
    dis_brkrs = list(net_pos.sort_values('net', ascending=True).head(3).index)

    lines.append(f"**🚀 Sniper BUY entries:** {', '.join(buy_list)}")
    lines.append(f"**👀 Near-Buy watchlist:** {', '.join(near_list) if near_list else 'None'}")
    lines.append(f"**🛑 Exit / avoid:**      {', '.join(sell_list)}")
    lines.append(f"**🔥 Momentum scrips:**   {', '.join(top_mom)}")
    lines.append(f"**🟢 Smart money buying:** {', '.join([b[:28] for b in acc_brkrs])}")
    lines.append(f"**🔴 Selling pressure:**  {', '.join([b[:28] for b in dis_brkrs])}")
    lines.append("")

    if recent_avg > prior_avg:
        lines.append("### 📈 MARKET BIAS: BULLISH")
        lines.append("- Take sniper BUY entries with full position size")
        lines.append("- Trail stop-loss at 1.5× ATR below entry")
        lines.append("- Target 1 = 2× ATR above entry, Target 2 = 3.5× ATR")
        lines.append("- Prioritise stocks where smart-money brokers are also net buyers")
    else:
        lines.append("### 📉 MARKET BIAS: CAUTIOUS")
        lines.append("- Take only high-confidence sniper BUYs (score 5/5)")
        lines.append("- Use reduced position size (50%)")
        lines.append("- Keep stop-loss tight at 1× ATR")
        lines.append("- Avoid near-buy setups until market bias flips bullish")
    lines.append("")
    lines.append("---")
    lines.append("*Auto-generated from NEPSE floorsheet data. Not financial advice.*")

    return "\n".join(lines)

# ═══════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "."
    report   = generate_report(data_dir)
    today    = date.today().strftime("%Y-%m-%d")
    out      = os.path.join(data_dir, f"strategy_{today}.md")
    with open(out, "w") as f:
        f.write(report)
    print(report)
    print(f"\n✅ Saved → {out}")

    # Send to Telegram if credentials available
    token   = os.getenv("TELEGRAM_TOKEN",   "8618135314:AAHoDrHGP2sncP1HxEGLDj0OKtIpSLeuD0U")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "8563709547")
    if token and chat_id:
        send_telegram(report, token, chat_id)
    else:
        print("⚠️  TELEGRAM_TOKEN or TELEGRAM_CHAT_ID not set — skipping Telegram.")


# ═══════════════════════════════════════════════════════════════════
# TELEGRAM SENDER
# ═══════════════════════════════════════════════════════════════════
def send_telegram(report_text, token, chat_id):
    """Send pre-market strategy summary to Telegram (splits into chunks if needed)."""
    import urllib.request, json, re

    # Extract concise summary from full report
    lines      = report_text.split("\n")
    date_line  = next((l for l in lines if "Date:" in l), "")
    bias_line  = next((l for l in lines if "Market Bias:" in l), "")
    buy_line   = next((l for l in lines if "Sniper BUY entries:" in l), "")
    near_line  = next((l for l in lines if "Near-Buy watchlist:" in l), "")
    exit_line  = next((l for l in lines if "Exit / avoid:" in l), "")
    mom_line   = next((l for l in lines if "Momentum scrips:" in l), "")
    smart_line = next((l for l in lines if "Smart money buying:" in l), "")
    sell_line  = next((l for l in lines if "Selling pressure:" in l), "")

    # Sniper BUY table rows
    buy_rows = []
    in_buy   = False
    for l in lines:
        if "SNIPER BUY SIGNALS" in l:
            in_buy = True
        elif in_buy and l.startswith("| **"):
            parts = [p.strip() for p in l.split("|") if p.strip()]
            if len(parts) >= 8:
                buy_rows.append(f"  • *{parts[0].replace('**','')}*  Entry:{parts[2]}  SL:{parts[3]}  T1:{parts[4]}  T2:{parts[5]}  R:R {parts[6]}")
        elif in_buy and l.startswith("###"):
            break

    # Near-buy rows
    near_rows = []
    in_near   = False
    for l in lines:
        if "NEAR BUY SETUPS" in l:
            in_near = True
        elif in_near and l.startswith("| **"):
            parts = [p.strip() for p in l.split("|") if p.strip()]
            if len(parts) >= 7:
                near_rows.append(f"  • *{parts[0].replace('**','')}*  Rs {parts[1]}  Missing:{parts[2]}")
        elif in_near and l.startswith("---"):
            break

    msg  = "🎯 *NEPSE PRE-MARKET STRATEGY*\n"
    msg += f"_{date_line.replace('**','').strip()}_\n\n"
    msg += f"{bias_line.replace('**','').strip()}\n\n"

    msg += "━━━━━━━━━━━━━━━━━━━\n"
    msg += "🚀 *SNIPER BUY SIGNALS*\n"
    if buy_rows:
        msg += "\n".join(buy_rows) + "\n"
    else:
        msg += "  None today — market not ready\n"

    msg += "\n👀 *NEAR BUY (watch)*\n"
    if near_rows:
        msg += "\n".join(near_rows[:5]) + "\n"
    else:
        msg += "  None\n"

    msg += "\n━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🔥 {mom_line.replace('**','').strip()}\n"
    msg += f"🟢 {smart_line.replace('**','').strip()}\n"
    msg += f"🔴 {sell_line.replace('**','').strip()}\n"
    msg += f"🛑 {exit_line.replace('**','').strip()}\n"

    # Strategy tip
    if "BULLISH" in bias_line:
        msg += "\n💡 *Tip:* Take sniper BUYs with full size. Trail SL at 1.5× ATR.\n"
    else:
        msg += "\n💡 *Tip:* Cautious market. 50% size only. Wait for volume confirmation.\n"

    msg += "\n_Not financial advice. DYOR._"

    # Send (split if > 4096 chars)
    api_url = f"https://api.telegram.org/bot{token}/sendMessage"
    chunks  = [msg[i:i+4000] for i in range(0, len(msg), 4000)]
    for chunk in chunks:
        payload = json.dumps({
            "chat_id":    chat_id,
            "text":       chunk,
            "parse_mode": "Markdown"
        }).encode()
        req = urllib.request.Request(
            api_url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                result = json.loads(resp.read())
                if result.get("ok"):
                    print(f"✅ Telegram message sent.")
                else:
                    print(f"❌ Telegram error: {result}")
        except Exception as e:
            print(f"❌ Telegram send failed: {e}")
