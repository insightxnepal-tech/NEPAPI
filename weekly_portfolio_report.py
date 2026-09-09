#!/usr/bin/env python3
"""
Weekly NEPSE portfolio + candle-scan report.

Uses unique floorsheet trading days (dedupes weekend/holiday copies) and
reconstructs candle ENTRY/EXIT activity from git history of candle_scan_latest.json.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

PORTFOLIO_PATH = Path(os.getenv("PORTFOLIO_FILE", "portfolio_data.json"))
STOCKMAP_PATH = Path(os.getenv("STOCKMAP_FILE", "stockmap.json"))
POSITIONS_PATH = Path(os.getenv("CANDLE_POSITIONS_FILE", "candle_positions.json"))
ARTIFACT_DIR = Path(os.getenv("ARTIFACT_DIR", "artifacts"))
REPORT_DIR = Path(os.getenv("WEEKLY_REPORT_DIR", "."))

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


def get_date_from_filename(filename: str) -> str | None:
    match = re.search(r"(\d{4})[_-](\d{2})[_-](\d{2})", os.path.basename(filename))
    return "-".join(match.groups()) if match else None


def unique_floorsheet_days() -> list[tuple[str, str, str]]:
    """Return [(date, path, md5), ...] newest-first, collapsing identical copies.

    Weekend/holiday jobs often republish the last trading day's CSV under a new
    filename. Keep the *oldest* date for each content hash so the label matches
    the actual session (and prefer businessDate inside the file when present).
    """
    import hashlib

    files = glob.glob("floorsheet*.csv")
    dated: list[tuple[str, str]] = []
    for f in files:
        dt = get_date_from_filename(f)
        if dt:
            dated.append((dt, f))
    dated.sort(key=lambda x: x[0])  # oldest → newest while grouping

    by_hash: dict[str, tuple[str, str]] = {}
    for dt, path in dated:
        h = hashlib.md5(Path(path).read_bytes()).hexdigest()
        # First time we see this hash = earliest filename date
        if h not in by_hash:
            # Prefer businessDate column when available
            session = dt
            try:
                sample = pd.read_csv(path, usecols=["businessDate"], nrows=5)
                if not sample.empty:
                    session = str(sample["businessDate"].iloc[0])[:10]
            except Exception:
                pass
            by_hash[h] = (session, path)

    days = [(dt, path, h) for h, (dt, path) in by_hash.items()]
    days.sort(key=lambda x: x[0], reverse=True)
    return days


def load_ltp(path: str) -> dict[str, float]:
    df = pd.read_csv(path)
    df = df.sort_values("contractId", ascending=False)
    return df.groupby("stockSymbol")["contractRate"].first().astype(float).to_dict()


def day_turnover(path: str) -> float:
    df = pd.read_csv(path, usecols=["contractAmount"])
    return float(df["contractAmount"].sum())


def portfolio_snapshot(portfolio: dict, ltp: dict[str, float]) -> dict:
    rows = []
    total_cost = total_value = 0.0
    for symbol, data in portfolio.items():
        qty = float(data["qty"])
        avg = float(data["rate"])
        price = float(ltp.get(symbol, 0) or 0)
        cost = qty * avg
        value = qty * price
        pl = value - cost
        pl_pct = (pl / cost * 100) if cost else 0.0
        total_cost += cost
        total_value += value
        rows.append(
            {
                "symbol": symbol,
                "qty": qty,
                "avg": avg,
                "ltp": price,
                "cost": cost,
                "value": value,
                "pl": pl,
                "pl_pct": pl_pct,
            }
        )
    total_pl = total_value - total_cost
    total_pl_pct = (total_pl / total_cost * 100) if total_cost else 0.0
    rows.sort(key=lambda r: r["pl_pct"], reverse=True)
    return {
        "rows": rows,
        "total_cost": total_cost,
        "total_value": total_value,
        "total_pl": total_pl,
        "total_pl_pct": total_pl_pct,
    }


def candle_events_from_git(start: str, end: str) -> list[dict]:
    """Pull ENTRY/EXIT lists from candle_scan_latest.json commits in [start, end]."""
    from datetime import datetime, timedelta

    try:
        end_plus = (
            datetime.strptime(end, "%Y-%m-%d") + timedelta(days=2)
        ).strftime("%Y-%m-%d")
    except ValueError:
        end_plus = end

    try:
        log = subprocess.check_output(
            [
                "git",
                "log",
                "--pretty=format:%H|%ad|%s",
                "--date=short",
                "--",
                "candle_scan_latest.json",
            ],
            text=True,
        )
    except subprocess.CalledProcessError:
        return []

    events: list[dict] = []
    for line in log.splitlines():
        if not line.strip():
            continue
        sha, day, _subj = line.split("|", 2)
        # Commits can land 1–2 mornings after the candle day.
        if day < start or day > end_plus:
            continue
        try:
            raw = subprocess.check_output(
                ["git", "show", f"{sha}:candle_scan_latest.json"], text=True
            )
            data = json.loads(raw)
        except (subprocess.CalledProcessError, json.JSONDecodeError):
            continue
        as_of = data.get("as_of") or day
        if as_of < start or as_of > end:
            continue
        entries = [e.get("symbol") for e in data.get("entries", []) if e.get("symbol")]
        exits = [
            e.get("signal", {}).get("symbol")
            for e in data.get("exits", [])
            if e.get("signal", {}).get("symbol")
        ]
        if not entries and not exits:
            continue
        events.append(
            {
                "as_of": as_of,
                "commit_date": day,
                "entries": entries,
                "exits": exits,
                "open": list(data.get("open_positions", {}).keys()),
            }
        )

    # Prefer unique as_of (latest commit for that candle day)
    by_as_of: dict[str, dict] = {}
    for ev in sorted(events, key=lambda e: (e["as_of"], e["commit_date"])):
        by_as_of[ev["as_of"]] = ev
    return [by_as_of[k] for k in sorted(by_as_of)]


def fmt_rs(n: float) -> str:
    return f"Rs {n:,.0f}"


def fmt_pct(n: float) -> str:
    return f"{n:+.2f}%"


def build_report(
    trading_days: int = 5,
    end_date: str | None = None,
) -> tuple[str, dict]:
    days = unique_floorsheet_days()
    if not days:
        raise SystemExit("No floorsheet CSV files found.")

    if end_date:
        days = [d for d in days if d[0] <= end_date]
    if len(days) < 2:
        raise SystemExit("Need at least 2 unique trading days for a weekly report.")

    week = list(reversed(days[:trading_days]))  # oldest → newest
    start_dt, start_path, _ = week[0]
    end_dt, end_path, _ = week[-1]
    week_dates = [d[0] for d in week]

    with open(PORTFOLIO_PATH) as f:
        portfolio = json.load(f)
    stock_map = {}
    if STOCKMAP_PATH.exists():
        with open(STOCKMAP_PATH) as f:
            stock_map = json.load(f)

    start_ltp = load_ltp(start_path)
    end_ltp = load_ltp(end_path)
    start_snap = portfolio_snapshot(portfolio, start_ltp)
    end_snap = portfolio_snapshot(portfolio, end_ltp)

    week_pl = end_snap["total_value"] - start_snap["total_value"]
    week_pl_pct = (
        (week_pl / start_snap["total_value"] * 100) if start_snap["total_value"] else 0.0
    )

    # Per-symbol weekly move (price change over the week)
    symbol_moves = []
    for row in end_snap["rows"]:
        sym = row["symbol"]
        s_px = float(start_ltp.get(sym, 0) or 0)
        e_px = row["ltp"]
        move = e_px - s_px
        move_pct = (move / s_px * 100) if s_px else 0.0
        week_value_delta = (e_px - s_px) * row["qty"]
        symbol_moves.append(
            {
                **row,
                "start_ltp": s_px,
                "move": move,
                "move_pct": move_pct,
                "week_value_delta": week_value_delta,
                "name": (stock_map.get(sym) or {}).get("name", ""),
                "sector": (stock_map.get(sym) or {}).get("sector", ""),
            }
        )
    symbol_moves.sort(key=lambda r: r["week_value_delta"], reverse=True)

    turnovers = [(dt, day_turnover(path)) for dt, path, _ in week]
    total_to = sum(t for _, t in turnovers)
    avg_to = total_to / len(turnovers) if turnovers else 0.0

    candle_events = candle_events_from_git(start_dt, end_dt)
    positions = {}
    if POSITIONS_PATH.exists():
        with open(POSITIONS_PATH) as f:
            positions = json.load(f)

    winners = [r for r in symbol_moves if r["week_value_delta"] > 0][:5]
    losers = [r for r in symbol_moves if r["week_value_delta"] < 0]
    losers = sorted(losers, key=lambda r: r["week_value_delta"])[:5]

    lines: list[str] = []
    lines.append(f"# 📊 NEPSE Weekly Portfolio Report")
    lines.append(
        f"**Week:** {start_dt} → {end_dt}  |  "
        f"**Trading days:** {len(week_dates)} ({', '.join(week_dates)})"
    )
    lines.append(f"**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Portfolio snapshot")
    lines.append("")
    lines.append("| | Start | End | Week Δ |")
    lines.append("|---|---:|---:|---:|")
    lines.append(
        f"| **Value** | {fmt_rs(start_snap['total_value'])} | "
        f"{fmt_rs(end_snap['total_value'])} | "
        f"{fmt_rs(week_pl)} ({fmt_pct(week_pl_pct)}) |"
    )
    lines.append(
        f"| **Unrealized P/L vs cost** | {fmt_pct(start_snap['total_pl_pct'])} | "
        f"{fmt_pct(end_snap['total_pl_pct'])} | "
        f"{fmt_rs(end_snap['total_pl'] - start_snap['total_pl'])} |"
    )
    lines.append(
        f"| **Cost basis** | {fmt_rs(end_snap['total_cost'])} | "
        f"{fmt_rs(end_snap['total_cost'])} | — |"
    )
    lines.append("")
    lines.append("## Top movers (by weekly P/L Rs)")
    lines.append("")
    lines.append("### Winners")
    if winners:
        lines.append("| Symbol | Start → End | Move | Week P/L |")
        lines.append("|--------|-------------|-----:|---------:|")
        for r in winners:
            lines.append(
                f"| **{r['symbol']}** | {r['start_ltp']:.2f} → {r['ltp']:.2f} | "
                f"{fmt_pct(r['move_pct'])} | {fmt_rs(r['week_value_delta'])} |"
            )
    else:
        lines.append("_No winners this week._")
    lines.append("")
    lines.append("### Losers")
    if losers:
        lines.append("| Symbol | Start → End | Move | Week P/L |")
        lines.append("|--------|-------------|-----:|---------:|")
        for r in losers:
            lines.append(
                f"| **{r['symbol']}** | {r['start_ltp']:.2f} → {r['ltp']:.2f} | "
                f"{fmt_pct(r['move_pct'])} | {fmt_rs(r['week_value_delta'])} |"
            )
    else:
        lines.append("_No losers this week._")
    lines.append("")
    lines.append("## Full portfolio (end of week)")
    lines.append("")
    lines.append("| Symbol | Qty | Avg | LTP | P/L % | P/L Rs | Week Δ |")
    lines.append("|--------|----:|----:|----:|------:|-------:|-------:|")
    for r in sorted(symbol_moves, key=lambda x: x["pl_pct"], reverse=True):
        ind = "🟢" if r["pl"] >= 0 else "🔴"
        lines.append(
            f"| {ind} {r['symbol']} | {int(r['qty'])} | {r['avg']:.2f} | "
            f"{r['ltp']:.2f} | {fmt_pct(r['pl_pct'])} | {fmt_rs(r['pl'])} | "
            f"{fmt_rs(r['week_value_delta'])} |"
        )
    lines.append("")
    lines.append("## Candle scanner (200/20 EMA)")
    lines.append("")
    if candle_events:
        lines.append("| As-of | ENTRY | EXIT | Open after |")
        lines.append("|-------|-------|------|------------|")
        for ev in candle_events:
            lines.append(
                f"| {ev['as_of']} | "
                f"{', '.join(ev['entries']) or '—'} | "
                f"{', '.join(ev['exits']) or '—'} | "
                f"{', '.join(ev['open']) or '—'} |"
            )
    else:
        lines.append("_No candle ENTRY/EXIT events found in git for this window._")
    lines.append("")
    if positions:
        lines.append("**Open candle positions now:**")
        for sym, pos in positions.items():
            lines.append(
                f"- `{sym}` entered {pos.get('entry_date')} @ {pos.get('entry_price')} "
                f"(RSI {pos.get('rsi')})"
            )
        lines.append("")
    lines.append("## Market turnover (floorsheet)")
    lines.append("")
    lines.append(f"Week total: **{fmt_rs(total_to)}**  |  Daily avg: **{fmt_rs(avg_to)}**")
    lines.append("")
    lines.append("| Date | Turnover |")
    lines.append("|------|---------:|")
    for dt, to in turnovers:
        lines.append(f"| {dt} | {fmt_rs(to)} |")
    lines.append("")
    lines.append("---")
    lines.append("_Not financial advice. Educational / research use only._")
    lines.append("")

    report = "\n".join(lines)
    meta = {
        "start": start_dt,
        "end": end_dt,
        "trading_days": week_dates,
        "week_pl": week_pl,
        "week_pl_pct": week_pl_pct,
        "end_value": end_snap["total_value"],
        "end_pl_pct": end_snap["total_pl_pct"],
        "candle_events": candle_events,
        "turnovers": turnovers,
        "symbol_moves": symbol_moves,
    }
    return report, meta


def telegram_summary(meta: dict, report_path: str) -> str:
    start, end = meta["start"], meta["end"]
    msg = (
        f"📊 *WEEKLY PORTFOLIO REPORT*\n"
        f"{start} → {end} ({len(meta['trading_days'])} sessions)\n\n"
        f"Week Δ: *{fmt_pct(meta['week_pl_pct'])}* ({fmt_rs(meta['week_pl'])})\n"
        f"Value: {fmt_rs(meta['end_value'])}  |  vs cost {fmt_pct(meta['end_pl_pct'])}\n\n"
    )
    moves = meta["symbol_moves"]
    top = sorted(moves, key=lambda r: r["week_value_delta"], reverse=True)[:3]
    bot = sorted(moves, key=lambda r: r["week_value_delta"])[:3]
    msg += "*Top:* " + ", ".join(
        f"{r['symbol']} {fmt_pct(r['move_pct'])}" for r in top
    )
    msg += "\n*Bottom:* " + ", ".join(
        f"{r['symbol']} {fmt_pct(r['move_pct'])}" for r in bot
    )
    entries = []
    exits = []
    for ev in meta["candle_events"]:
        entries.extend(ev["entries"])
        exits.extend(ev["exits"])
    if entries or exits:
        msg += f"\n\n🕯️ Entries: {', '.join(entries) or '—'}"
        msg += f"\n🕯️ Exits: {', '.join(exits) or '—'}"
    msg += f"\n\n_Full report: `{report_path}`_"
    return msg


def send_telegram(text: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram skipped (no TELEGRAM_TOKEN / TELEGRAM_CHAT_ID).")
        return False
    import urllib.request

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = json.dumps(
        {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown",
        }
    ).encode()
    req = urllib.request.Request(
        url, data=payload, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            body = json.loads(resp.read())
            ok = bool(body.get("ok"))
            print("Telegram ok" if ok else f"Telegram error: {body}")
            return ok
    except Exception as e:
        print(f"Telegram failed: {e}")
        return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Weekly NEPSE portfolio report")
    parser.add_argument(
        "--days",
        type=int,
        default=5,
        help="Number of unique trading days to include (default 5 ≈ 1 NEPSE week)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default="",
        help="End date YYYY-MM-DD (default: latest unique floorsheet day)",
    )
    parser.add_argument(
        "--no-telegram",
        action="store_true",
        help="Do not send Telegram summary",
    )
    parser.add_argument(
        "--out",
        type=str,
        default="",
        help="Output markdown path (default weekly_report_YYYY-MM-DD.md)",
    )
    args = parser.parse_args(argv)

    report, meta = build_report(
        trading_days=args.days,
        end_date=args.end or None,
    )
    out = args.out or str(REPORT_DIR / f"weekly_report_{meta['end']}.md")
    Path(out).write_text(report, encoding="utf-8")
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    artifact = ARTIFACT_DIR / f"weekly_report_{meta['end']}.md"
    artifact.write_text(report, encoding="utf-8")
    print(report)
    print(f"\n✅ Saved → {out}")
    print(f"✅ Artifact → {artifact}")

    if not args.no_telegram:
        send_telegram(telegram_summary(meta, out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
