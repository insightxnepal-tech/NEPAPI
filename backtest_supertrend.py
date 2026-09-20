#!/usr/bin/env python3
"""
SuperTrend strategy backtester for NEPSE.

Replays the same rules as supertrend_scanner.py over historical data:
  BUY  at close when SuperTrend flips bearish -> bullish
  SELL at close when SuperTrend flips bullish -> bearish

Any position still open at the end is marked to the last close.
Reports per-trade results, per-symbol stats, and portfolio aggregates,
with a buy & hold comparison over the same window.

Usage:
  python backtest_supertrend.py                    # portfolio symbols
  python backtest_supertrend.py --all              # every ordinary equity
  python backtest_supertrend.py --symbols NABIL,API
  python backtest_supertrend.py --days 730 -o report.json
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import pandas as pd

import candle_scanner as cs
import supertrend_scanner as st

DEFAULT_DAYS = 730  # ~2 years of calendar days
WARMUP_BARS = st.ATR_LEN * 3  # skip early bars where bands are still settling


@dataclass
class Trade:
    symbol: str
    entry_date: str
    entry_price: float
    exit_date: Optional[str]
    exit_price: Optional[float]
    return_pct: float
    holding_days: int
    open: bool  # still held at end of data


def backtest_frame(df: pd.DataFrame, symbol: str = "") -> list[Trade]:
    """Run flip-entry / flip-exit over one symbol's OHLCV history."""
    if df is None or len(df) < st.MIN_HISTORY:
        return []
    ind = df if "st_dir" in df.columns else st.compute_supertrend(df)

    trades: list[Trade] = []
    entry_i: Optional[int] = None
    dirs = ind["st_dir"].astype(int).tolist()
    closes = ind["close"].astype(float).tolist()
    dates = [
        d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)[:10]
        for d in ind["businessDate"]
    ]

    for i in range(max(1, WARMUP_BARS), len(ind)):
        flipped_up = dirs[i] == 1 and dirs[i - 1] == -1
        flipped_down = dirs[i] == -1 and dirs[i - 1] == 1
        if flipped_up and entry_i is None:
            entry_i = i
        elif flipped_down and entry_i is not None:
            ret = (closes[i] - closes[entry_i]) / closes[entry_i] * 100
            trades.append(
                Trade(
                    symbol=symbol,
                    entry_date=dates[entry_i],
                    entry_price=round(closes[entry_i], 2),
                    exit_date=dates[i],
                    exit_price=round(closes[i], 2),
                    return_pct=round(ret, 2),
                    holding_days=i - entry_i,
                    open=False,
                )
            )
            entry_i = None

    if entry_i is not None:
        last = len(ind) - 1
        ret = (closes[last] - closes[entry_i]) / closes[entry_i] * 100
        trades.append(
            Trade(
                symbol=symbol,
                entry_date=dates[entry_i],
                entry_price=round(closes[entry_i], 2),
                exit_date=None,
                exit_price=round(closes[last], 2),
                return_pct=round(ret, 2),
                holding_days=last - entry_i,
                open=True,
            )
        )
    return trades


def symbol_stats(df: pd.DataFrame, trades: list[Trade]) -> dict:
    """Per-symbol summary including buy & hold over the same window."""
    closes = df["close"].astype(float)
    start_i = min(max(1, WARMUP_BARS), len(closes) - 1)
    start_px = closes.iloc[start_i]
    if pd.isna(start_px) or start_px <= 0:
        valid = closes.iloc[start_i:].dropna()
        start_px = valid.iloc[0] if len(valid) else float("nan")
    buy_hold = (closes.iloc[-1] - start_px) / start_px * 100
    if pd.isna(buy_hold):
        buy_hold = 0.0

    strategy = 1.0
    for t in trades:
        strategy *= 1 + t.return_pct / 100
    strategy_pct = (strategy - 1) * 100

    closed = [t for t in trades if not t.open]
    wins = [t for t in closed if t.return_pct > 0]
    return {
        "trades": len(trades),
        "closed": len(closed),
        "wins": len(wins),
        "win_rate": round(len(wins) / len(closed) * 100, 1) if closed else None,
        "avg_return_pct": round(
            sum(t.return_pct for t in closed) / len(closed), 2
        )
        if closed
        else None,
        "best_pct": max((t.return_pct for t in trades), default=None),
        "worst_pct": min((t.return_pct for t in trades), default=None),
        "avg_holding_days": round(
            sum(t.holding_days for t in trades) / len(trades), 1
        )
        if trades
        else None,
        "strategy_return_pct": round(strategy_pct, 2),
        "buy_hold_return_pct": round(float(buy_hold), 2),
        "bars": len(df),
        "from": str(df["businessDate"].iloc[0])[:10],
        "to": str(df["businessDate"].iloc[-1])[:10],
    }


def aggregate_stats(all_trades: list[Trade], per_symbol: dict[str, dict]) -> dict:
    closed = [t for t in all_trades if not t.open]
    wins = [t for t in closed if t.return_pct > 0]
    beat = [
        s
        for s, r in per_symbol.items()
        if r["strategy_return_pct"] > r["buy_hold_return_pct"]
    ]
    return {
        "symbols": len(per_symbol),
        "total_trades": len(all_trades),
        "closed_trades": len(closed),
        "open_trades": len(all_trades) - len(closed),
        "win_rate": round(len(wins) / len(closed) * 100, 1) if closed else None,
        "avg_trade_return_pct": round(
            sum(t.return_pct for t in closed) / len(closed), 2
        )
        if closed
        else None,
        "median_trade_return_pct": round(
            float(pd.Series([t.return_pct for t in closed]).median()), 2
        )
        if closed
        else None,
        "avg_win_pct": round(sum(t.return_pct for t in wins) / len(wins), 2)
        if wins
        else None,
        "avg_loss_pct": round(
            sum(t.return_pct for t in closed if t.return_pct <= 0)
            / max(1, len(closed) - len(wins)),
            2,
        )
        if len(closed) > len(wins)
        else None,
        "best_trade_pct": max((t.return_pct for t in closed), default=None),
        "worst_trade_pct": min((t.return_pct for t in closed), default=None),
        "avg_holding_days": round(
            sum(t.holding_days for t in closed) / len(closed), 1
        )
        if closed
        else None,
        "avg_strategy_return_pct": round(
            sum(r["strategy_return_pct"] for r in per_symbol.values())
            / len(per_symbol),
            2,
        )
        if per_symbol
        else None,
        "avg_buy_hold_return_pct": round(
            sum(r["buy_hold_return_pct"] for r in per_symbol.values())
            / len(per_symbol),
            2,
        )
        if per_symbol
        else None,
        "symbols_beating_buy_hold": len(beat),
    }


def fetch_history(n, cid: int, days: int) -> list[dict]:
    """Like cs.fetch_symbol_history but with a configurable window."""
    full: list[dict] = []
    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    for page in range(0, 12):
        url = (
            f"/api/nots/market/history/security/{cid}"
            f"?&size=500&startDate={start_date}&endDate={end_date}&page={page}"
        )
        res = n.requestGETAPI(url)
        if not res or "content" not in res or not res["content"]:
            break
        full.extend(res["content"])
        if len(res["content"]) < 15:
            break
    return full


def run_backtest(
    scan_all: bool = False,
    symbols: Optional[list[str]] = None,
    days: int = DEFAULT_DAYS,
) -> dict:
    sys.setrecursionlimit(10000)
    from nepse import Nepse

    cs.patch_nepse_tls(Nepse)
    n = Nepse()
    n.setTLSVerification(False)

    universe = cs.resolve_symbols(n, scan_all=scan_all, only=symbols)
    print(
        f"🧪 SuperTrend backtest: {len(universe)} symbols, "
        f"{days} calendar days (ATR {st.ATR_LEN}, mult {st.MULT})"
    )
    cid_map = n.getSecurityIDKeyMap() or {}

    all_trades: list[Trade] = []
    per_symbol: dict[str, dict] = {}
    skipped = 0
    for i, symbol in enumerate(universe):
        if i and i % 10 == 0:
            print(f"    Progress: {i}/{len(universe)}")
        cid = cid_map.get(symbol)
        if not cid:
            skipped += 1
            continue
        try:
            rows = fetch_history(n, cid, days)
            if not rows:
                skipped += 1
                continue
            df = cs.history_to_ohlcv(rows)
            if len(df) < st.MIN_HISTORY:
                skipped += 1
                continue
            ind = st.compute_supertrend(df)
            trades = backtest_frame(ind, symbol)
            per_symbol[symbol] = symbol_stats(ind, trades)
            all_trades.extend(trades)
            s = per_symbol[symbol]
            print(
                f"  {symbol}: {s['trades']} trades, "
                f"strategy {s['strategy_return_pct']:+.1f}% "
                f"vs hold {s['buy_hold_return_pct']:+.1f}%"
            )
        except Exception as e:
            print(f"  {symbol}: error {e}")
            skipped += 1
        time.sleep(st.SLEEP_BETWEEN_SYMBOLS)

    agg = aggregate_stats(all_trades, per_symbol)
    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "atr_len": st.ATR_LEN,
        "multiplier": st.MULT,
        "calendar_days": days,
        "skipped": skipped,
        "aggregate": agg,
        "per_symbol": per_symbol,
        "trades": [asdict(t) for t in all_trades],
    }


def _pct(value, decimals: int = 2) -> str:
    if value is None:
        return "—"
    return f"{value:+.{decimals}f}%"


def format_report(result: dict) -> str:
    agg = result["aggregate"]
    win_rate = f"{agg['win_rate']}%" if agg["win_rate"] is not None else "—"
    lines = [
        "# SuperTrend Backtest Report",
        "",
        f"Generated: {result['generated_at']} · "
        f"ATR {result['atr_len']} · multiplier {result['multiplier']} · "
        f"window ~{result['calendar_days']} calendar days",
        "",
        "## Aggregate",
        "",
        f"- Symbols tested: **{agg['symbols']}** (skipped {result['skipped']})",
        f"- Trades: **{agg['total_trades']}** "
        f"({agg['closed_trades']} closed, {agg['open_trades']} still open)",
        f"- Win rate (closed): **{win_rate}**",
        f"- Avg trade: **{_pct(agg['avg_trade_return_pct'])}** "
        f"(median {_pct(agg['median_trade_return_pct'])})",
        f"- Avg win / avg loss: **{_pct(agg['avg_win_pct'])} / "
        f"{_pct(agg['avg_loss_pct'])}**",
        f"- Best / worst closed trade: {_pct(agg['best_trade_pct'])} / "
        f"{_pct(agg['worst_trade_pct'])}",
        f"- Avg holding: {agg['avg_holding_days']} trading days",
        f"- Avg strategy return per symbol: "
        f"**{_pct(agg['avg_strategy_return_pct'])}** "
        f"vs buy & hold **{_pct(agg['avg_buy_hold_return_pct'])}**",
        f"- Symbols beating buy & hold: {agg['symbols_beating_buy_hold']}"
        f"/{agg['symbols']}",
        "",
        "## Per symbol",
        "",
        "| Symbol | Trades | Win rate | Strategy | Buy & hold | Best | Worst |",
        "|--------|-------:|---------:|---------:|-----------:|-----:|------:|",
    ]
    ranked = sorted(
        result["per_symbol"].items(),
        key=lambda kv: kv[1]["strategy_return_pct"],
        reverse=True,
    )
    for sym, s in ranked:
        wr = f"{s['win_rate']}%" if s["win_rate"] is not None else "—"
        lines.append(
            f"| {sym} | {s['trades']} | {wr} | "
            f"{_pct(s['strategy_return_pct'], 1)} | "
            f"{_pct(s['buy_hold_return_pct'], 1)} | "
            f"{_pct(s['best_pct'], 1)} | {_pct(s['worst_pct'], 1)} |"
        )
    lines += [
        "",
        "_Entries/exits at the close of the flip day. No fees, slippage, or "
        "position sizing. Open trades marked to last close. Not financial "
        "advice._",
    ]
    return "\n".join(lines) + "\n"


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="SuperTrend NEPSE backtester")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Backtest all ordinary NEPSE equities (default: portfolio only)",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default="",
        help="Comma-separated symbols (overrides --all / portfolio)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Calendar days of history to fetch (default {DEFAULT_DAYS})",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default="supertrend_backtest_results.json",
        help="JSON results path",
    )
    parser.add_argument(
        "--report",
        type=str,
        default="supertrend_backtest_report.md",
        help="Markdown report path",
    )
    args = parser.parse_args(argv)

    only = [s for s in args.symbols.split(",") if s.strip()] or None
    result = run_backtest(scan_all=args.all, symbols=only, days=args.days)

    cs.save_json(args.output, result)
    report = format_report(result)
    with open(args.report, "w") as f:
        f.write(report)
    print("\n" + report)
    print(f"Saved {args.output} and {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
