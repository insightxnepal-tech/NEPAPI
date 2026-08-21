#!/usr/bin/env python3
"""
Daily SuperTrend scanner — TradingView-style ATR SuperTrend.

Defaults match TradingView: ATR length 10, multiplier 3.0.

ENTRY (latest daily candle):
  SuperTrend flips from bearish (-1) to bullish (+1).

EXIT (open positions only):
  SuperTrend flips from bullish (+1) to bearish (-1).

Telegram reports ENTRY FOUND / EXIT FOUND, plus names currently in uptrend.
Designed to run after NEPSE close (Sun-Thu).
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from typing import Optional

import pandas as pd

import candle_scanner as cs

# -- Strategy constants (TradingView SuperTrend defaults) ----------
ATR_LEN = int(os.getenv("SUPERTREND_ATR_LEN", "10"))
MULT = float(os.getenv("SUPERTREND_MULT", "3.0"))
MIN_HISTORY = max(50, ATR_LEN * 5)
SLEEP_BETWEEN_SYMBOLS = float(
    os.getenv("SUPERTREND_SLEEP", str(cs.SLEEP_BETWEEN_SYMBOLS))
)

POSITIONS_FILE = os.getenv("SUPERTREND_POSITIONS_FILE", "supertrend_positions.json")
LATEST_FILE = os.getenv("SUPERTREND_LATEST_FILE", "supertrend_scan_latest.json")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", cs.TELEGRAM_TOKEN)
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", cs.TELEGRAM_CHAT_ID)


@dataclass
class SuperTrendSignal:
    symbol: str
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    atr: float
    supertrend: float
    direction: int  # +1 bullish, -1 bearish
    prev_direction: int
    flipped_up: bool
    flipped_down: bool
    entry: bool
    exit_reasons: list = field(default_factory=list)

    @property
    def signal(self) -> str:
        if self.entry:
            return "ENTRY"
        if self.exit_reasons:
            return "EXIT"
        return "NONE"


# -- Indicators ----------------------------------------------------
def compute_supertrend(
    df: pd.DataFrame,
    atr_len: int = ATR_LEN,
    mult: float = MULT,
) -> pd.DataFrame:
    """
    Add atr, st_upper, st_lower, supertrend, st_dir to a daily OHLCV frame.

    Expected columns: businessDate, high, low, close, volume.
    Optional open — falls back to previous close.
    Semantics match TradingView ta.supertrend(factor, atrPeriod).
    """
    out = df.copy().sort_values("businessDate").reset_index(drop=True)
    high = out["high"].astype(float)
    low = out["low"].astype(float)
    close = out["close"].astype(float)

    if "open" not in out.columns:
        out["open"] = close.shift(1).fillna(close)
    else:
        out["open"] = out["open"].astype(float).fillna(close.shift(1)).fillna(close)

    prev_close = close.shift(1)
    tr = pd.concat(
        [high - low, (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    atr = cs._rma(tr, atr_len)
    out["atr"] = atr

    hl2 = (high + low) / 2.0
    basic_upper = hl2 + mult * atr
    basic_lower = hl2 - mult * atr

    final_upper = basic_upper.copy()
    final_lower = basic_lower.copy()
    for i in range(1, len(out)):
        if pd.isna(basic_upper.iloc[i]) or pd.isna(final_upper.iloc[i - 1]):
            continue
        if close.iloc[i - 1] <= final_upper.iloc[i - 1]:
            final_upper.iloc[i] = min(basic_upper.iloc[i], final_upper.iloc[i - 1])
        else:
            final_upper.iloc[i] = basic_upper.iloc[i]
        if close.iloc[i - 1] >= final_lower.iloc[i - 1]:
            final_lower.iloc[i] = max(basic_lower.iloc[i], final_lower.iloc[i - 1])
        else:
            final_lower.iloc[i] = basic_lower.iloc[i]

    direction = pd.Series(1, index=out.index, dtype=int)
    st = final_lower.copy()
    for i in range(1, len(out)):
        if pd.isna(final_upper.iloc[i]) or pd.isna(final_lower.iloc[i]):
            direction.iloc[i] = int(direction.iloc[i - 1])
            st.iloc[i] = st.iloc[i - 1]
            continue
        prev_dir = int(direction.iloc[i - 1])
        if prev_dir == -1 and close.iloc[i] > final_upper.iloc[i - 1]:
            direction.iloc[i] = 1
        elif prev_dir == 1 and close.iloc[i] < final_lower.iloc[i - 1]:
            direction.iloc[i] = -1
        else:
            direction.iloc[i] = prev_dir
        st.iloc[i] = (
            final_lower.iloc[i] if direction.iloc[i] == 1 else final_upper.iloc[i]
        )

    out["st_upper"] = final_upper
    out["st_lower"] = final_lower
    out["supertrend"] = st
    out["st_dir"] = direction
    return out


def evaluate_latest(
    df: pd.DataFrame, symbol: str = ""
) -> Optional[SuperTrendSignal]:
    """Build a SuperTrendSignal from the last bar of an indicator frame."""
    if df is None or len(df) < MIN_HISTORY:
        return None
    if "st_dir" not in df.columns:
        df = compute_supertrend(df)
    if pd.isna(df["supertrend"].iloc[-1]) or pd.isna(df["atr"].iloc[-1]):
        return None

    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else last
    direction = int(last["st_dir"])
    prev_direction = int(prev["st_dir"])
    flipped_up = direction == 1 and prev_direction == -1
    flipped_down = direction == -1 and prev_direction == 1

    exit_reasons: list[str] = []
    if flipped_down:
        exit_reasons.append(
            f"SuperTrend flipped bearish (ST {float(last['supertrend']):.2f})"
        )

    bdate = last["businessDate"]
    if hasattr(bdate, "strftime"):
        date_str = bdate.strftime("%Y-%m-%d")
    else:
        date_str = str(bdate)[:10]

    return SuperTrendSignal(
        symbol=symbol,
        date=date_str,
        open=round(float(last["open"]), 2),
        high=round(float(last["high"]), 2),
        low=round(float(last["low"]), 2),
        close=round(float(last["close"]), 2),
        volume=round(float(last["volume"]), 0),
        atr=round(float(last["atr"]), 2),
        supertrend=round(float(last["supertrend"]), 2),
        direction=direction,
        prev_direction=prev_direction,
        flipped_up=flipped_up,
        flipped_down=flipped_down,
        entry=bool(flipped_up),
        exit_reasons=exit_reasons,
    )


def apply_position_rules(signal: SuperTrendSignal, open_positions: dict) -> str:
    held = signal.symbol in open_positions
    if signal.entry and not held:
        return "ENTRY"
    if held and signal.exit_reasons and not signal.entry:
        return "EXIT"
    return "NONE"


def update_positions(
    positions: dict, signal: SuperTrendSignal, action: str
) -> dict:
    out = dict(positions)
    if action == "ENTRY":
        out[signal.symbol] = {
            "entry_date": signal.date,
            "entry_price": signal.close,
            "supertrend": signal.supertrend,
            "atr": signal.atr,
        }
    elif action == "EXIT" and signal.symbol in out:
        del out[signal.symbol]
    return out


def load_positions() -> dict:
    data = cs.load_json(POSITIONS_FILE, {})
    return data if isinstance(data, dict) else {}


def format_telegram(
    entries: list[SuperTrendSignal],
    exits: list[tuple[SuperTrendSignal, dict]],
    scanned: int,
    as_of: str,
    skipped: int = 0,
    uptrend: Optional[list[SuperTrendSignal]] = None,
) -> str:
    lines = [
        f"📡 *SuperTrend Scan — {as_of}*",
        f"_ATR {ATR_LEN} · multiplier {MULT}_",
        "",
    ]

    if entries:
        lines.append(f"🟢 *ENTRY FOUND ({len(entries)})*")
        lines.append("_SuperTrend flipped bullish_")
        lines.append("")
        for s in entries:
            lines.append(f"• *{s.symbol}* @ Rs {s.close:.2f}")
            lines.append(
                f"  ST {s.supertrend:.2f} | ATR {s.atr:.2f} | "
                f"O {s.open:.2f} H {s.high:.2f} L {s.low:.2f}"
            )
            lines.append("")
    else:
        lines.append("🟢 *ENTRY FOUND:* none")
        lines.append("")

    if exits:
        lines.append(f"🛑 *EXIT FOUND ({len(exits)})*")
        lines.append("")
        for s, pos in exits:
            entry_px = pos.get("entry_price")
            entry_dt = pos.get("entry_date", "?")
            pnl = ""
            if isinstance(entry_px, (int, float)) and entry_px:
                pct = (s.close - entry_px) / entry_px * 100
                pnl = f" | P/L {pct:+.1f}%"
            lines.append(f"• *{s.symbol}* @ Rs {s.close:.2f}{pnl}")
            lines.append(f"  Entered Rs {entry_px} on {entry_dt}")
            lines.append(f"  Reason: {'; '.join(s.exit_reasons)}")
            lines.append("")
    else:
        lines.append("🛑 *EXIT FOUND:* none")
        lines.append("")

    if uptrend:
        lines.append(f"📈 *In uptrend now ({len(uptrend)})*")
        preview = uptrend[:15]
        lines.append(", ".join(f"{s.symbol} ({s.close:.0f})" for s in preview))
        if len(uptrend) > 15:
            lines.append(f"_…and {len(uptrend) - 15} more_")
        lines.append("")

    lines.append(
        f"_Scanned {scanned} stocks ({skipped} skipped, need {MIN_HISTORY}+ days)._"
    )
    lines.append("_Not financial advice._")
    return "\n".join(lines).strip() + "\n"


def scan_ohlcv_map(
    ohlcv_by_symbol: dict[str, pd.DataFrame],
    positions: dict,
) -> tuple[
    list[SuperTrendSignal],
    list[tuple[SuperTrendSignal, dict]],
    dict,
    int,
    list[SuperTrendSignal],
]:
    entries: list[SuperTrendSignal] = []
    exits: list[tuple[SuperTrendSignal, dict]] = []
    uptrend: list[SuperTrendSignal] = []
    new_positions = dict(positions)
    skipped = 0

    for symbol, raw in ohlcv_by_symbol.items():
        if raw is None or len(raw) < MIN_HISTORY:
            skipped += 1
            continue
        try:
            indicated = raw if "st_dir" in raw.columns else compute_supertrend(raw)
            signal = evaluate_latest(indicated, symbol=symbol)
        except Exception as e:
            print(f"  {symbol}: evaluate error {e}")
            skipped += 1
            continue
        if signal is None:
            skipped += 1
            continue

        if signal.direction == 1:
            uptrend.append(signal)

        action = apply_position_rules(signal, new_positions)
        if action == "ENTRY":
            entries.append(signal)
            new_positions = update_positions(new_positions, signal, action)
            print(f"  🟢 ENTRY {symbol} @ {signal.close} ST {signal.supertrend}")
        elif action == "EXIT":
            pos = dict(new_positions.get(symbol, {}))
            exits.append((signal, pos))
            new_positions = update_positions(new_positions, signal, action)
            print(f"  🛑 EXIT  {symbol} @ {signal.close} {signal.exit_reasons}")
        else:
            side = "UP" if signal.direction == 1 else "DOWN"
            print(f"  · {symbol} {side} ST {signal.supertrend}")

    uptrend.sort(key=lambda s: s.symbol)
    return entries, exits, new_positions, skipped, uptrend


def run_scan(
    scan_all: bool = False,
    symbols: Optional[list[str]] = None,
    send: bool = True,
    persist: bool = True,
) -> dict:
    sys.setrecursionlimit(10000)
    from nepse import Nepse

    cs.patch_nepse_tls(Nepse)
    n = Nepse()
    n.setTLSVerification(False)

    positions = load_positions()
    universe = cs.resolve_symbols(n, scan_all=scan_all, only=symbols)
    for held in positions:
        if held not in universe:
            universe.append(held)

    print(
        f"📡 SuperTrend scan: {len(universe)} symbols, "
        f"{len(positions)} open positions (ATR {ATR_LEN}, mult {MULT})"
    )
    cid_map = n.getSecurityIDKeyMap() or {}

    ohlcv_by_symbol: dict[str, pd.DataFrame] = {}
    for i, symbol in enumerate(universe):
        if i and i % 10 == 0:
            print(f"    Progress: {i}/{len(universe)}")
        cid = cid_map.get(symbol)
        if not cid:
            print(f"  {symbol}: no security id")
            continue
        try:
            rows = cs.fetch_symbol_history(n, cid)
            if not rows:
                continue
            ohlcv_by_symbol[symbol] = cs.history_to_ohlcv(rows)
        except Exception as e:
            print(f"  {symbol}: history error {e}")
        time.sleep(SLEEP_BETWEEN_SYMBOLS)

    entries, exits, new_positions, skipped, uptrend = scan_ohlcv_map(
        ohlcv_by_symbol, positions
    )

    as_of = date.today().isoformat()
    if entries:
        as_of = entries[0].date
    elif exits:
        as_of = exits[0][0].date
    elif uptrend:
        as_of = uptrend[0].date

    msg = format_telegram(
        entries,
        exits,
        scanned=len(ohlcv_by_symbol),
        as_of=as_of,
        skipped=skipped,
        uptrend=uptrend,
    )
    print("\n" + msg)

    payload = {
        "as_of": as_of,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "atr_len": ATR_LEN,
        "multiplier": MULT,
        "scanned": len(ohlcv_by_symbol),
        "skipped": skipped,
        "entries": [asdict(s) for s in entries],
        "exits": [{"signal": asdict(s), "position": pos} for s, pos in exits],
        "uptrend": [asdict(s) for s in uptrend],
        "open_positions": new_positions,
        "message": msg,
    }

    telegram_ok = True
    if send:
        telegram_ok = cs.send_telegram(
            msg, token=TELEGRAM_TOKEN, chat_id=TELEGRAM_CHAT_ID
        )
    payload["telegram_ok"] = telegram_ok

    if persist:
        cs.save_json(POSITIONS_FILE, new_positions)
        cs.save_json(LATEST_FILE, payload)
        print(f"Saved {POSITIONS_FILE} and {LATEST_FILE}")

    return payload


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Daily SuperTrend (ATR) scanner for NEPSE"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Scan all ordinary NEPSE equities (default: portfolio only)",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default="",
        help="Comma-separated symbols to scan (overrides --all / portfolio)",
    )
    parser.add_argument(
        "--no-telegram",
        action="store_true",
        help="Do not send Telegram (print only)",
    )
    parser.add_argument(
        "--no-persist",
        action="store_true",
        help="Do not write position / latest JSON files",
    )
    args = parser.parse_args(argv)

    only = [s for s in args.symbols.split(",") if s.strip()] or None
    payload = run_scan(
        scan_all=args.all,
        symbols=only,
        send=not args.no_telegram,
        persist=not args.no_persist,
    )
    if not args.no_telegram and not payload.get("telegram_ok", False):
        print("Telegram delivery failed.")
        return 1
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"Fatal error: {e}")
        raise
