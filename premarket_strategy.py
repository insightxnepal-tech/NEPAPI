#!/usr/bin/env python3
"""
NEPSE Pre-Market Strategy Report
Runs before market open using last 15 days of floorsheet data.
Outputs: watchlist, momentum scrips, whale activity, broker signals.
"""

import pandas as pd
import glob
import os
from datetime import datetime, date
import warnings
warnings.filterwarnings('ignore')

# ── Load last 15 available floorsheet CSVs ──────────────────────────────────
def load_floorsheets(data_dir="."):
    files = sorted(glob.glob(os.path.join(data_dir, "floorsheet_2026-*.csv")))
    files = [f for f in files if "with_dividend" not in f]
    files = files[-15:]  # last 15 trading days
    if not files:
        raise FileNotFoundError("No floorsheet CSV files found.")
    dfs = []
    for f in files:
        df = pd.read_csv(f)
        dfs.append(df)
    data = pd.concat(dfs, ignore_index=True)
    data['contractAmount']  = pd.to_numeric(data['contractAmount'],  errors='coerce')
    data['contractQuantity']= pd.to_numeric(data['contractQuantity'],errors='coerce')
    data['contractRate']    = pd.to_numeric(data['contractRate'],    errors='coerce')
    data['businessDate']    = pd.to_datetime(data['businessDate'])
    return data, files

# ── Momentum: stocks bought consistently across multiple days ────────────────
def momentum_scrips(data, top_n=10):
    daily_buy = data.groupby(['businessDate','stockSymbol'])['contractAmount'].sum().reset_index()
    days_active = daily_buy.groupby('stockSymbol')['businessDate'].nunique().rename('days_active')
    total_buy   = data.groupby('stockSymbol')['contractAmount'].sum().rename('total_turnover')
    total_qty   = data.groupby('stockSymbol')['contractQuantity'].sum().rename('total_qty')
    avg_price   = data.groupby('stockSymbol')['contractRate'].mean().rename('avg_price')
    # Recent 3-day vs prior trend (acceleration)
    last_date   = data['businessDate'].max()
    recent = data[data['businessDate'] >= last_date - pd.Timedelta(days=4)]
    recent_buy  = recent.groupby('stockSymbol')['contractAmount'].sum().rename('recent_3d_turnover')
    combined = pd.concat([days_active, total_buy, total_qty, avg_price, recent_buy], axis=1).fillna(0)
    combined['momentum_score'] = (
        combined['days_active'] * 0.3 +
        combined['total_turnover'] / combined['total_turnover'].max() * 40 +
        combined['recent_3d_turnover'] / (combined['total_turnover'] + 1) * 30
    )
    return combined.sort_values('momentum_score', ascending=False).head(top_n)

# ── Whale detector: single large trades ─────────────────────────────────────
def whale_trades(data, top_n=10):
    return data.nlargest(top_n, 'contractAmount')[
        ['businessDate','stockSymbol','contractQuantity','contractRate',
         'contractAmount','buyerBrokerName','sellerBrokerName']
    ]

# ── Net accumulation per broker ──────────────────────────────────────────────
def broker_net_positions(data):
    buy  = data.groupby('buyerBrokerName')['contractAmount'].sum().rename('bought')
    sell = data.groupby('sellerBrokerName')['contractAmount'].sum().rename('sold')
    net  = pd.concat([buy, sell], axis=1).fillna(0)
    net['net'] = net['bought'] - net['sold']
    return net

# ── Per-stock broker dominance (who is accumulating a specific stock) ────────
def stock_accumulation(data, top_stocks=5):
    top = data.groupby('stockSymbol')['contractAmount'].sum().nlargest(top_stocks).index
    result = {}
    for sym in top:
        sym_data = data[data['stockSymbol'] == sym]
        buy  = sym_data.groupby('buyerBrokerName')['contractAmount'].sum()
        sell = sym_data.groupby('sellerBrokerName')['contractAmount'].sum()
        net  = pd.concat([buy.rename('bought'), sell.rename('sold')], axis=1).fillna(0)
        net['net'] = net['bought'] - net['sold']
        net['avg_buy_price'] = sym_data.groupby('buyerBrokerName')['contractRate'].mean()
        result[sym] = net.sort_values('net', ascending=False).head(5)
    return result

# ── Price trend per stock (last price vs avg) ────────────────────────────────
def price_trends(data, top_n=15):
    last_date = data['businessDate'].max()
    last_prices = data[data['businessDate'] == last_date].groupby('stockSymbol')['contractRate'].mean().rename('last_price')
    avg_prices  = data.groupby('stockSymbol')['contractRate'].mean().rename('avg_15d_price')
    vol         = data.groupby('stockSymbol')['contractAmount'].sum().rename('total_turnover')
    trend = pd.concat([last_prices, avg_prices, vol], axis=1).dropna()
    trend['price_change_pct'] = ((trend['last_price'] - trend['avg_15d_price']) / trend['avg_15d_price'] * 100).round(2)
    return trend.sort_values('total_turnover', ascending=False).head(top_n)

# ── Generate report ──────────────────────────────────────────────────────────
def generate_report(data_dir="."):
    data, files = load_floorsheets(data_dir)
    today = date.today().strftime("%Y-%m-%d")
    trading_days = data['businessDate'].nunique()
    last_date = data['businessDate'].max().date()

    lines = []
    lines.append(f"# NEPSE PRE-MARKET STRATEGY REPORT")
    lines.append(f"**Generated:** {today} | **Data:** Last {trading_days} trading days (up to {last_date})")
    lines.append(f"**Total Trades Analyzed:** {len(data):,} | **Total Turnover:** Rs {data['contractAmount'].sum()/1e9:.2f}B")
    lines.append("")

    # ── MARKET PULSE ────────────────────────────────────────────────────────
    lines.append("---")
    lines.append("## 📊 MARKET PULSE (Last 3 Days vs Prior)")
    daily = data.groupby('businessDate')['contractAmount'].sum().sort_index()
    recent_avg = daily.iloc[-3:].mean() / 1e9
    prior_avg  = daily.iloc[:-3].mean() / 1e9 if len(daily) > 3 else recent_avg
    trend_arrow = "📈" if recent_avg > prior_avg else "📉"
    lines.append(f"- Recent 3-day avg turnover: **Rs {recent_avg:.2f}B** {trend_arrow}")
    lines.append(f"- Prior period avg turnover: **Rs {prior_avg:.2f}B**")
    lines.append(f"- Volume trend: **{'INCREASING - Bullish signal' if recent_avg > prior_avg else 'DECREASING - Cautious'}**")
    lines.append("")

    # ── MOMENTUM WATCHLIST ───────────────────────────────────────────────────
    lines.append("---")
    lines.append("## 🔥 TOP MOMENTUM SCRIPS (Today's Watchlist)")
    lines.append("*Stocks with consistent buying across multiple days + recent acceleration*")
    lines.append("")
    lines.append("| # | Symbol | Days Active | Avg Price | Recent 3D Turnover | Score |")
    lines.append("|---|--------|------------|-----------|-------------------|-------|")
    mom = momentum_scrips(data, top_n=12)
    for i, (sym, r) in enumerate(mom.iterrows(), 1):
        lines.append(f"| {i} | **{sym}** | {int(r['days_active'])}/{trading_days} | Rs {r['avg_price']:.1f} | Rs {r['recent_3d_turnover']/1e6:.1f}M | {r['momentum_score']:.1f} |")
    lines.append("")

    # ── PRICE TREND ──────────────────────────────────────────────────────────
    lines.append("---")
    lines.append("## 📈 PRICE TREND vs 15-Day Average")
    lines.append("")
    lines.append("| Symbol | Last Price | 15D Avg | Change % | Turnover |")
    lines.append("|--------|-----------|---------|----------|----------|")
    pt = price_trends(data, top_n=15)
    for sym, r in pt.iterrows():
        arrow = "🟢" if r['price_change_pct'] > 0 else "🔴"
        lines.append(f"| {sym} | Rs {r['last_price']:.1f} | Rs {r['avg_15d_price']:.1f} | {arrow} {r['price_change_pct']:+.1f}% | Rs {r['total_turnover']/1e6:.0f}M |")
    lines.append("")

    # ── BROKER NET POSITIONS ─────────────────────────────────────────────────
    lines.append("---")
    lines.append("## 🐋 WHALE BROKERS — Net Accumulators (Smart Money)")
    lines.append("*These brokers are net buyers — potential bullish signal*")
    lines.append("")
    lines.append("| Broker | Net Bought | Total Bought | Total Sold |")
    lines.append("|--------|-----------|--------------|------------|")
    net_pos = broker_net_positions(data)
    for broker, r in net_pos.sort_values('net', ascending=False).head(8).iterrows():
        lines.append(f"| {broker[:42]} | **+Rs {r['net']/1e6:.1f}M** | Rs {r['bought']/1e6:.1f}M | Rs {r['sold']/1e6:.1f}M |")
    lines.append("")
    lines.append("### Net Distributors (Selling Pressure)")
    lines.append("| Broker | Net Sold | Total Bought | Total Sold |")
    lines.append("|--------|---------|--------------|------------|")
    for broker, r in net_pos.sort_values('net', ascending=True).head(5).iterrows():
        lines.append(f"| {broker[:42]} | **Rs {r['net']/1e6:.1f}M** | Rs {r['bought']/1e6:.1f}M | Rs {r['sold']/1e6:.1f}M |")
    lines.append("")

    # ── STOCK-LEVEL ACCUMULATION ─────────────────────────────────────────────
    lines.append("---")
    lines.append("## 🎯 WHO IS ACCUMULATING TOP STOCKS")
    lines.append("*Broker-level net positions per high-turnover stock*")
    acc = stock_accumulation(data, top_stocks=5)
    for sym, df in acc.items():
        avg_p = data[data['stockSymbol']==sym]['contractRate'].mean()
        lines.append(f"\n### {sym} (15D Avg Price: Rs {avg_p:.1f})")
        lines.append("| Broker | Net | Bought | Avg Buy Price |")
        lines.append("|--------|-----|--------|---------------|")
        for broker, r in df.iterrows():
            if r['net'] > 0:
                lines.append(f"| {broker[:42]} | +Rs {r['net']/1e6:.1f}M | Rs {r['bought']/1e6:.1f}M | Rs {r.get('avg_buy_price', 0):.1f} |")
    lines.append("")

    # ── LARGEST SINGLE TRADES ────────────────────────────────────────────────
    lines.append("---")
    lines.append("## ⚡ LARGEST SINGLE TRADES (Block Deals / Whale Moves)")
    lines.append("")
    lines.append("| Date | Stock | Qty | Price | Amount | Buyer | Seller |")
    lines.append("|------|-------|-----|-------|--------|-------|--------|")
    wt = whale_trades(data, top_n=10)
    for _, r in wt.iterrows():
        lines.append(f"| {r['businessDate'].date()} | **{r['stockSymbol']}** | {int(r['contractQuantity']):,} | Rs {r['contractRate']:.0f} | Rs {r['contractAmount']/1e6:.1f}M | {r['buyerBrokerName'][:25]} | {r['sellerBrokerName'][:25]} |")
    lines.append("")

    # ── STRATEGY SUMMARY ─────────────────────────────────────────────────────
    lines.append("---")
    lines.append("## 🧠 TODAY'S TRADING STRATEGY SUMMARY")
    lines.append("")
    top_mom = list(momentum_scrips(data, top_n=5).index)
    top_acc_brokers = list(net_pos.sort_values('net', ascending=False).head(3).index)
    top_dist_brokers = list(net_pos.sort_values('net', ascending=True).head(3).index)
    lines.append(f"**📌 Top Watch Scrips:** {', '.join(top_mom)}")
    lines.append(f"**🟢 Smart Money Buying:** {', '.join([b[:30] for b in top_acc_brokers])}")
    lines.append(f"**🔴 Selling Pressure From:** {', '.join([b[:30] for b in top_dist_brokers])}")
    lines.append("")
    if recent_avg > prior_avg:
        lines.append("**📈 MARKET BIAS: BULLISH** — Volume increasing. Look for breakouts in momentum scrips.")
        lines.append("- Strategy: Buy dips on top momentum scrips, trail stop-loss at 3%")
        lines.append("- Watch for accumulation in stocks where smart-money brokers are net buyers")
    else:
        lines.append("**📉 MARKET BIAS: CAUTIOUS** — Volume declining. Avoid chasing, wait for confirmation.")
        lines.append("- Strategy: Reduce exposure, hold only high-conviction positions")
        lines.append("- Watch for volume spikes as early reversal signals")
    lines.append("")
    lines.append("---")
    lines.append("*Report auto-generated from NEPSE floorsheet data. Not financial advice.*")

    return "\n".join(lines)

if __name__ == "__main__":
    import sys
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "."
    report = generate_report(data_dir)
    today = date.today().strftime("%Y-%m-%d")
    out = os.path.join(data_dir, f"strategy_{today}.md")
    with open(out, "w") as f:
        f.write(report)
    print(report)
    print(f"\n✅ Report saved to {out}")
