#!/usr/bin/env python3
"""
NEPSE post-market strategy (5:00 PM NPT on trading days).

1. Fetch today's floorsheet (full session).
2. Rank next-session candidates with four models and pick the model
   that had the highest historical hit-rate (most accurate).
3. Publish a concise Telegram briefing + a markdown report.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import date, datetime

import numpy as np
import pandas as pd

from nepse_calendar import is_nepse_trading_day, npt_today

try:
    from premarket_strategy import calc_sniper_indicators, sniper_signal
except Exception:
    calc_sniper_indicators = None  # type: ignore
    sniper_signal = None  # type: ignore

MIN_TURNOVER = 15_000_000  # Rs 15M — skip illiquid spikes
MIN_TRADES = 30
LOOKBACK_DAYS = 30
BACKTEST_DAYS = 15
TOP_N = 5


def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ── data ──────────────────────────────────────────────────────────────────────

def floorsheet_files(data_dir: str) -> list[str]:
    files = sorted(glob.glob(os.path.join(data_dir, "floorsheet_20*.csv")))
    return [f for f in files if "dividend" not in os.path.basename(f).lower()]


def load_floorsheets(data_dir: str, lookback: int = LOOKBACK_DAYS) -> pd.DataFrame:
    files = floorsheet_files(data_dir)
    if not files:
        raise FileNotFoundError("No floorsheet CSV files found.")
    dfs = []
    for path in files:
        try:
            dfs.append(pd.read_csv(path, low_memory=False))
        except Exception as exc:
            log(f"Skipping {path}: {exc}")
    if not dfs:
        raise FileNotFoundError("Could not read any floorsheet CSV.")
    data = pd.concat(dfs, ignore_index=True)
    for col in ("contractAmount", "contractQuantity", "contractRate"):
        data[col] = pd.to_numeric(data[col], errors="coerce")
    data["businessDate"] = pd.to_datetime(data["businessDate"], errors="coerce")
    data = data.dropna(subset=["businessDate", "stockSymbol", "contractRate"])
    dates = sorted(data["businessDate"].dt.normalize().unique())
    if lookback and len(dates) > lookback:
        keep = dates[-lookback:]
        data = data[data["businessDate"].dt.normalize().isin(keep)]
    return data.reset_index(drop=True)


def fetch_today_floorsheet(data_dir: str) -> int:
    """Re-fetch the live/latest session into floorsheet_<today>.csv."""
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fetch_floorsheet_csv.py")
    today = npt_today().isoformat()
    out = os.path.join(data_dir, f"floorsheet_{today}.csv")
    log(f"Fetching floorsheet for {today} …")
    result = subprocess.run(
        [sys.executable, script, "--date", today, "--out", out],
        cwd=os.path.dirname(script) or ".",
        capture_output=False,
    )
    return result.returncode


# ── session features ──────────────────────────────────────────────────────────

def build_daily(data: pd.DataFrame) -> pd.DataFrame:
    """One row per symbol × session from floorsheet prints."""
    g = data.groupby(["stockSymbol", "businessDate"], sort=True)
    daily = g.agg(
        open=("contractRate", "first"),
        high=("contractRate", "max"),
        low=("contractRate", "min"),
        close=("contractRate", "last"),
        volume=("contractQuantity", "sum"),
        turnover=("contractAmount", "sum"),
        n_trades=("contractRate", "size"),
    ).reset_index()
    daily["vwap"] = daily["turnover"] / daily["volume"].replace(0, np.nan)
    span = (daily["high"] - daily["low"]).replace(0, np.nan)
    daily["intraday_ret"] = (daily["close"] / daily["open"] - 1.0) * 100.0
    daily["range_pos"] = ((daily["close"] - daily["low"]) / span).clip(0, 1)
    daily["close_vs_vwap"] = (daily["close"] / daily["vwap"] - 1.0) * 100.0
    daily["businessDate"] = pd.to_datetime(daily["businessDate"]).dt.normalize()
    return daily.sort_values(["stockSymbol", "businessDate"]).reset_index(drop=True)


def add_history_features(daily: pd.DataFrame) -> pd.DataFrame:
    daily = daily.copy()
    daily["vol_ma20"] = daily.groupby("stockSymbol")["volume"].transform(
        lambda s: s.shift(1).rolling(20, min_periods=5).mean()
    )
    daily["vol_ratio"] = daily["volume"] / daily["vol_ma20"].replace(0, np.nan)
    daily["next_close"] = daily.groupby("stockSymbol")["close"].shift(-1)
    daily["next_ret"] = (daily["next_close"] / daily["close"] - 1.0) * 100.0
    return daily


def broker_accumulation(data: pd.DataFrame, session: pd.Timestamp) -> pd.Series:
    """Net buy (Rs) per symbol for the session — buyer notional minus seller notional."""
    day = data[data["businessDate"].dt.normalize() == session]
    if day.empty:
        return pd.Series(dtype=float)
    bought = day.groupby("stockSymbol")["contractAmount"].sum()
    # Per-print: buyer received qty, seller provided qty. Net accumulation by
    # aggressive buyers ≈ sum(amount) when buyerBroker != sellerBroker already
    # counted on both sides. Use large-print buy pressure instead:
    large = day[day["contractAmount"] >= day["contractAmount"].quantile(0.9)]
    buy_pressure = large.groupby("stockSymbol")["contractAmount"].sum()
    return (buy_pressure / bought).reindex(bought.index).fillna(0.0)


def snipers_as_of(daily: pd.DataFrame, session: pd.Timestamp) -> dict[str, dict]:
    """Sniper signals using only OHLCV on or before `session` (no look-ahead)."""
    if calc_sniper_indicators is None or sniper_signal is None:
        return {}
    hist = daily[daily["businessDate"] <= session]
    out: dict[str, dict] = {}
    for sym, df_sym in hist.groupby("stockSymbol"):
        to = df_sym["turnover"].sum()
        if to < MIN_TURNOVER:
            continue
        stats = calc_sniper_indicators(df_sym)
        if stats is None:
            continue
        try:
            sig = sniper_signal(stats)
        except Exception:
            continue
        out[sym] = {
            "symbol": sym,
            "price": stats["price"],
            "rsi": round(stats["rsi"], 1) if stats.get("rsi") is not None else np.nan,
            **sig,
        }
    return out


# ── scoring models ────────────────────────────────────────────────────────────

def _clip01(x, lo, hi) -> float:
    if pd.isna(x) or hi == lo:
        return 0.0
    return float(np.clip((x - lo) / (hi - lo), 0.0, 1.0))


def score_row(row: pd.Series, sniper: dict | None, accum: float) -> dict:
    liq_ok = (row["turnover"] >= MIN_TURNOVER) and (row["n_trades"] >= MIN_TRADES)
    if not liq_ok:
        zeros = {k: 0.0 for k in ("continuation", "smart_money", "sniper", "composite")}
        zeros["eligible"] = False
        return zeros

    continuation = (
        25.0 * _clip01(row.get("range_pos", 0), 0.5, 1.0)
        + 25.0 * _clip01(row.get("intraday_ret", 0), 0.0, 4.0)
        + 25.0 * _clip01(row.get("close_vs_vwap", 0), 0.0, 1.5)
        + 25.0 * _clip01(row.get("vol_ratio", 0), 1.0, 2.5)
    )
    # Exhaustion / circuit-chase penalty
    if row.get("intraday_ret", 0) > 8:
        continuation *= 0.5
    if row.get("range_pos", 0) < 0.35:
        continuation *= 0.4

    smart = 50.0 + 50.0 * float(np.clip(accum, -1.0, 1.0))
    # Volume confirmation, but cap so panic prints cannot dominate.
    smart *= _clip01(row.get("vol_ratio", 1.0), 0.8, 2.0) * 0.35 + 0.65
    # Large-print buying into a collapse is usually distribution, not accumulation.
    if row.get("range_pos", 1.0) < 0.30:
        smart *= 0.45
    if row.get("intraday_ret", 0) < -5:
        smart *= 0.55
    if row.get("close_vs_vwap", 0) < -1.5:
        smart *= 0.75
    if sniper and "SELL" in str(sniper.get("signal", "")):
        smart *= 0.4

    buy_score = 0.0
    rsi = np.nan
    if sniper:
        buy_score = float(sniper.get("buy_score", 0) or 0)
        rsi = float(sniper.get("rsi", np.nan) or np.nan)
        sig = str(sniper.get("signal", ""))
        sniper_pts = buy_score / 5.0 * 100.0
        if "SELL" in sig:
            sniper_pts *= 0.2
        if "BUY BLOCKED" in sig:
            sniper_pts *= 0.5
    else:
        sniper_pts = 0.0

    if not pd.isna(rsi) and rsi > 75:
        continuation *= 0.7
        sniper_pts *= 0.7

    composite = 0.40 * continuation + 0.25 * smart + 0.35 * sniper_pts
    return {
        "continuation": round(continuation, 2),
        "smart_money": round(smart, 2),
        "sniper": round(sniper_pts, 2),
        "composite": round(composite, 2),
        "eligible": True,
        "buy_score": buy_score,
        "rsi": rsi,
    }


def attach_scores(daily: pd.DataFrame, data: pd.DataFrame, session: pd.Timestamp,
                  snipers: dict[str, dict] | None = None) -> pd.DataFrame:
    snipers = snipers if snipers is not None else snipers_as_of(daily, session)
    today = daily[daily["businessDate"] == session].copy()
    accum = broker_accumulation(data, session)
    rows = []
    for _, row in today.iterrows():
        sym = row["stockSymbol"]
        sc = score_row(row, snipers.get(sym), float(accum.get(sym, 0.0)))
        rec = row.to_dict()
        rec.update(sc)
        rec["accum"] = float(accum.get(sym, 0.0))
        rec["sniper_signal"] = (snipers.get(sym) or {}).get("signal", "")
        rec["entry"] = (snipers.get(sym) or {}).get("entry", row["close"])
        rec["stop_loss"] = (snipers.get(sym) or {}).get("stop_loss", round(row["close"] * 0.97, 2))
        rec["target1"] = (snipers.get(sym) or {}).get("target1", round(row["close"] * 1.03, 2))
        rec["target2"] = (snipers.get(sym) or {}).get("target2", round(row["close"] * 1.05, 2))
        rec["rr"] = (snipers.get(sym) or {}).get("rr", 1.0)
        rec["confidence"] = (snipers.get(sym) or {}).get("confidence", int(sc["composite"]))
        rows.append(rec)
    return pd.DataFrame(rows)


# ── backtest: pick the most accurate model ────────────────────────────────────

MODELS = ("continuation", "smart_money", "sniper", "composite")


def backtest(daily: pd.DataFrame, data: pd.DataFrame, as_of: pd.Timestamp) -> dict:
    """Walk last BACKTEST_DAYS sessions that have a next-day return."""
    sessions = sorted(daily["businessDate"].unique())
    sessions = [s for s in sessions if s < as_of]
    sessions = sessions[-BACKTEST_DAYS:]
    stats = {m: {"hits": 0, "n": 0, "ret_sum": 0.0} for m in MODELS}

    for sess in sessions:
        scored = attach_scores(daily, data, sess)
        scored = scored[scored["eligible"] == True]  # noqa: E712
        if scored.empty or "next_ret" not in scored.columns:
            continue
        scored = scored.dropna(subset=["next_ret"])
        if scored.empty:
            continue
        for model in MODELS:
            picks = scored.nlargest(TOP_N, model)
            if picks.empty:
                continue
            rets = picks["next_ret"]
            stats[model]["n"] += len(rets)
            stats[model]["hits"] += int((rets > 0).sum())
            stats[model]["ret_sum"] += float(rets.sum())

    summary = {}
    for model, s in stats.items():
        n = s["n"] or 1
        summary[model] = {
            "hit_rate": round(s["hits"] / n * 100.0, 1) if s["n"] else 0.0,
            "avg_next_ret": round(s["ret_sum"] / n, 2) if s["n"] else 0.0,
            "samples": s["n"],
        }
    winner = max(
        MODELS,
        key=lambda m: (summary[m]["hit_rate"], summary[m]["avg_next_ret"], summary[m]["samples"]),
    )
    return {"models": summary, "winner": winner}


def select_picks(scored: pd.DataFrame, winner: str, n: int = TOP_N) -> pd.DataFrame:
    df = scored[scored["eligible"] == True].copy()  # noqa: E712
    if df.empty:
        return df
    sig = df.get("sniper_signal", pd.Series("", index=df.index)).astype(str)
    df = df[~sig.str.contains("SELL", na=False)]
    # Prefer constructive closes; keep genuine BUY signals even after a red day.
    buyish = sig.str.contains("BUY", na=False)
    constructive = (df["range_pos"] >= 0.40) | buyish
    gated = df[constructive]
    if len(gated) >= n:
        df = gated
    if winner == "sniper" and "buy_score" in df.columns:
        strong = df[df["buy_score"] >= 4]
        if len(strong) >= 2:
            df = strong
    sort_cols = [c for c in (winner, "composite", "range_pos", "turnover") if c in df.columns]
    return df.sort_values(sort_cols, ascending=False).head(n)


def fmt_rs(n: float) -> str:
    if pd.isna(n):
        return "—"
    if abs(n) >= 1e9:
        return f"Rs {n/1e9:.2f}B"
    if abs(n) >= 1e6:
        return f"Rs {n/1e6:.1f}M"
    return f"Rs {n:,.0f}"


def generate_report(session: date, daily: pd.DataFrame, scored: pd.DataFrame,
                    bt: dict, data: pd.DataFrame) -> str:
    sess = pd.Timestamp(session)
    day = daily[daily["businessDate"] == sess]
    mkt_to = float(day["turnover"].sum()) if not day.empty else 0.0
    mkt_ret = float(day["intraday_ret"].median()) if not day.empty else 0.0
    winner = bt["winner"]
    picks = select_picks(scored, winner)
    gainers = day.nlargest(8, "intraday_ret")
    losers = day.nsmallest(5, "intraday_ret")

    lines = [
        f"# NEPSE Post-Market Strategy — {session.isoformat()}",
        "",
        f"**Session turnover:** {fmt_rs(mkt_to)}  |  **Median stock move:** {mkt_ret:+.2f}%",
        f"**Active scrips:** {day['stockSymbol'].nunique() if not day.empty else 0}",
        f"**Selected model:** `{winner}` (highest historical accuracy)",
        "",
        "## Model accuracy (next-day, last ~15 sessions)",
        "",
        "| Model | Hit rate | Avg next-day return | Samples |",
        "|-------|----------|---------------------|---------|",
    ]
    for m, s in bt["models"].items():
        mark = " ← selected" if m == winner else ""
        lines.append(
            f"| {m}{mark} | {s['hit_rate']}% | {s['avg_next_ret']:+.2f}% | {s['samples']} |"
        )

    lines += [
        "",
        f"## Highest-conviction next-session plays (top {TOP_N} by {winner})",
        "",
        "| Symbol | Close | Intraday | VWAP Δ | Vol x | Score | Entry | SL | T1 | T2 | Signal |",
        "|--------|-------|----------|--------|-------|-------|-------|----|----|----|--------|",
    ]
    for _, r in picks.iterrows():
        lines.append(
            f"| **{r['stockSymbol']}** | Rs {r['close']:.1f} | {r['intraday_ret']:+.2f}% | "
            f"{r['close_vs_vwap']:+.2f}% | {r.get('vol_ratio', float('nan')):.2f}x | "
            f"{r[winner]:.0f} | Rs {r['entry']} | Rs {r['stop_loss']} | "
            f"Rs {r['target1']} | Rs {r['target2']} | {r.get('sniper_signal','')} |"
        )

    lines += [
        "",
        "## Today's realized leaders",
        "",
        "| Symbol | Close | Intraday | Turnover | Range pos | Close vs VWAP |",
        "|--------|-------|----------|----------|-----------|---------------|",
    ]
    for _, r in gainers.iterrows():
        if r["turnover"] < MIN_TURNOVER:
            continue
        lines.append(
            f"| {r['stockSymbol']} | Rs {r['close']:.1f} | {r['intraday_ret']:+.2f}% | "
            f"{fmt_rs(r['turnover'])} | {r['range_pos']:.2f} | {r['close_vs_vwap']:+.2f}% |"
        )

    lines += ["", "## Today's laggards (liquidity-filtered)", ""]
    lines.append("| Symbol | Close | Intraday | Turnover |")
    lines.append("|--------|-------|----------|----------|")
    for _, r in losers.iterrows():
        if r["turnover"] < MIN_TURNOVER:
            continue
        lines.append(
            f"| {r['stockSymbol']} | Rs {r['close']:.1f} | {r['intraday_ret']:+.2f}% | {fmt_rs(r['turnover'])} |"
        )

    wr = bt["models"][winner]
    cash_bias = wr["hit_rate"] < 50 and wr["avg_next_ret"] < 0

    lines += [
        "",
        "## Playbook",
        "",
        f"- Use **{winner}** rankings — it won the walk-forward accuracy test "
        f"({wr['hit_rate']}% hit-rate, {wr['avg_next_ret']:+.2f}% avg next-day).",
        "- Prefer names that closed in the upper half of the day's range **and** above VWAP.",
        "- Skip illiquid prints (< Rs 15M turnover or < 30 trades).",
        "- Stops: sniper ATR stop when available, otherwise ~3% below close.",
    ]
    if cash_bias:
        lines.append(
            "- **Cash bias:** the winning model is still negative-expectancy in this tape. "
            "Do not chase today's leaders (continuation hit-rate is the worst). "
            "Half-size only, or skip new entries."
        )
    else:
        lines.append("- Size down if the selected model's hit-rate is below 50%.")
    lines += [
        "",
        "*Auto-generated from NEPSE floorsheet data. Not financial advice.*",
        "",
    ]
    return "\n".join(lines)


def telegram_message(session: date, daily: pd.DataFrame, scored: pd.DataFrame,
                     bt: dict) -> str:
    sess = pd.Timestamp(session)
    day = daily[daily["businessDate"] == sess]
    mkt_to = float(day["turnover"].sum()) if not day.empty else 0.0
    mkt_ret = float(day["intraday_ret"].median()) if not day.empty else 0.0
    winner = bt["winner"]
    wr = bt["models"][winner]
    picks = select_picks(scored, winner)
    gainers = day[day["turnover"] >= MIN_TURNOVER].nlargest(5, "intraday_ret")

    bias = "BULLISH" if mkt_ret > 0.15 else ("BEARISH" if mkt_ret < -0.15 else "MIXED")
    msg = [
        f"NEPSE POST-MARKET  {session.isoformat()}",
        f"Turnover {fmt_rs(mkt_to)}  |  Median {mkt_ret:+.2f}%  |  {bias}",
        f"Model: {winner}  |  Hit-rate {wr['hit_rate']}%  |  Avg next-day {wr['avg_next_ret']:+.2f}%",
        "",
        f"NEXT-SESSION TOP {TOP_N}",
    ]
    if picks.empty:
        msg.append("  No liquid setups cleared the filter.")
    else:
        for i, (_, r) in enumerate(picks.iterrows(), 1):
            msg.append(
                f"  {i}. {r['stockSymbol']}  Close Rs {r['close']:.1f}  "
                f"{r['intraday_ret']:+.2f}%  Score {r[winner]:.0f}"
            )
            msg.append(
                f"     Entry {r['entry']}  SL {r['stop_loss']}  "
                f"T1 {r['target1']}  T2 {r['target2']}"
            )

    msg += ["", "TODAY'S LEADERS"]
    for _, r in gainers.iterrows():
        msg.append(
            f"  {r['stockSymbol']}  {r['intraday_ret']:+.2f}%  "
            f"Rs {r['close']:.1f}  {fmt_rs(r['turnover'])}"
        )

    if wr["hit_rate"] < 50 and wr["avg_next_ret"] < 0:
        msg.append("")
        msg.append("CASH BIAS: winning model is still negative-expectancy. Half-size or skip.")
    msg += ["", "Not financial advice. DYOR."]
    return "\n".join(msg)


def send_telegram(text: str, token: str, chat_id: str) -> bool:
    api = f"https://api.telegram.org/bot{token}/sendMessage"
    ok = True
    for i in range(0, len(text), 3900):
        chunk = text[i:i + 3900]
        payload = json.dumps({
            "chat_id": chat_id,
            "text": chunk,
        }).encode()
        req = urllib.request.Request(
            api, data=payload, method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                body = json.loads(resp.read().decode())
            if body.get("ok"):
                log("Telegram message sent.")
            else:
                log(f"Telegram error: {body}")
                ok = False
        except urllib.error.HTTPError as exc:
            log(f"Telegram HTTP {exc.code}: {exc.read()[:300]!r}")
            ok = False
        except Exception as exc:
            log(f"Telegram send failed: {exc}")
            ok = False
    return ok


# ── main ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="NEPSE 5pm post-market strategy")
    p.add_argument("data_dir", nargs="?", default=".")
    p.add_argument("--data-dir", dest="data_dir_flag", default=None)
    p.add_argument("--skip-fetch", action="store_true", help="Use CSVs already on disk")
    p.add_argument("--no-telegram", action="store_true")
    p.add_argument("--force", action="store_true", help="Run even on weekend / stale session")
    args = p.parse_args()
    if args.data_dir_flag:
        args.data_dir = args.data_dir_flag
    return args


def main() -> int:
    args = parse_args()
    data_dir = os.path.abspath(args.data_dir)
    today = npt_today()

    if not args.force and not is_nepse_trading_day(today):
        log(f"{today} is not a NEPSE trading day (weekend). Skipping.")
        return 0

    if not args.skip_fetch:
        rc = fetch_today_floorsheet(data_dir)
        if rc != 0:
            log(f"Floorsheet fetch exited {rc}; will try existing CSVs.")

    try:
        data = load_floorsheets(data_dir)
    except FileNotFoundError as exc:
        log(str(exc))
        return 1

    latest = data["businessDate"].dt.date.max()
    if latest != today and not args.force:
        log(f"No floorsheet for {today} (latest session {latest}). Holiday or incomplete publish — skip.")
        return 0

    session = latest
    log(f"Building features for session {session} ({len(data):,} prints) …")
    daily = add_history_features(build_daily(data))
    sess_ts = pd.Timestamp(session)
    snipers = snipers_as_of(daily, sess_ts)
    log(f"Sniper symbols: {len(snipers)}")

    scored = attach_scores(daily, data, sess_ts, snipers)
    bt = backtest(daily, data, sess_ts)
    log(f"Winning model: {bt['winner']}  {bt['models'][bt['winner']]}")

    report = generate_report(session, daily, scored, bt, data)
    out = os.path.join(data_dir, f"strategy_postmarket_{session.isoformat()}.md")
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(report)
    log(f"Saved {out}")
    print(report)

    token = os.getenv("TELEGRAM_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if args.no_telegram:
        log("Telegram disabled (--no-telegram).")
    elif not token or not chat_id:
        log("TELEGRAM_TOKEN / TELEGRAM_CHAT_ID not set — skip send.")
    else:
        send_telegram(telegram_message(session, daily, scored, bt), token, chat_id)

    return 0


if __name__ == "__main__":
    sys.exit(main())
