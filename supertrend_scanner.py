#!/usr/bin/env python3
"""
Daily Supertrend scanner (TradingView default: ATR 10, multiplier 3).

🟢 ENTRY FOUND — Supertrend flips from red (downtrend) to green (uptrend)
🛑 EXIT FOUND  — Supertrend flips from green (uptrend) to red (downtrend)

Sends a Telegram message after NEPSE close (Sun–Thu).
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

import candle_scanner as cs

ATR_PERIOD = 10
ATR_MULTIPLIER = 3.0
MIN_HISTORY = 40
LATEST_FILE = os.getenv("SUPERTREND_LATEST_FILE", "supertrend_scan_latest.json")


@dataclass
class SupertrendSignal:
    symbol: str
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    atr: float
    supertrend: float
    prev_supertrend: float
    direction: int          # +1 uptrend (green), -1 downtrend (red)
    prev_direction: int
    flipped_up: bool
    flipped_down: bool

    @property
    def signal(self) -> str:
        if self.flipped_up:
            return "ENTRY"
        if self.flipped_down:
            return "EXIT"
        return "BULL" if self.direction == 1 else "BEAR"


def load_floorsheet_ohlcv(path: str | Path) -> dict[str, dict]:
    """Build one daily OHLCV bar per symbol from a floorsheet CSV."""
    df = pd.read_csv(path)
    if df.empty:
        return {}
    df["contractRate"] = pd.to_numeric(df["contractRate"], errors="coerce")
    df["contractQuantity"] = pd.to_numeric(df["contractQuantity"], errors="coerce")
    if "tradeTime" in df.columns:
        df["tradeTime"] = pd.to_datetime(df["tradeTime"], errors="coerce")
        df = df.sort_values("tradeTime")
    else:
        df = df.sort_values("contractId")
    g = df.groupby("stockSymbol")
    ohlc = g["contractRate"].agg(open="first", high="max", low="min", close="last")
    vol = g["contractQuantity"].sum().rename("volume")
    bdate = str(df["businessDate"].iloc[0])[:10]
    out = ohlc.join(vol).reset_index()
    rows: dict[str, dict] = {}
    for _, r in out.iterrows():
        rows[str(r["stockSymbol"]).upper()] = {
            "businessDate": pd.Timestamp(bdate),
            "open": float(r["open"]),
            "high": float(r["high"]),
            "low": float(r["low"]),
            "close": float(r["close"]),
            "volume": float(r["volume"] or 0),
        }
    return rows


def apply_as_of(
    df: pd.DataFrame,
    as_of: date,
    overlay: Optional[dict] = None,
) -> pd.DataFrame:
    """Keep history before as_of and append the overlay bar for that day."""
    out = df.copy()
    out["businessDate"] = pd.to_datetime(out["businessDate"])
    cutoff = pd.Timestamp(as_of)
    out = out[out["businessDate"] < cutoff]
    if overlay:
        out = pd.concat([out, pd.DataFrame([overlay])], ignore_index=True)
    return out.sort_values("businessDate").drop_duplicates("businessDate", keep="last")


def last_bar_on(df: pd.DataFrame, as_of: date) -> bool:
    if df is None or df.empty:
        return False
    last = pd.to_datetime(df["businessDate"].iloc[-1]).date()
    return last == as_of


def compute_supertrend(
    df: pd.DataFrame,
    period: int = ATR_PERIOD,
    multiplier: float = ATR_MULTIPLIER,
) -> pd.DataFrame:
    """
    TradingView-style Supertrend.

    ATR is Wilder's RMA. Final bands ratchet. Direction +1 = green/uptrend
    (line below price), -1 = red/downtrend (line above price).
    """
    out = df.copy().sort_values("businessDate").reset_index(drop=True)
    high = out["high"].astype(float)
    low = out["low"].astype(float)
    close = out["close"].astype(float)
    if "open" not in out.columns:
        out["open"] = close.shift(1).fillna(close)
    else:
        out["open"] = out["open"].astype(float).fillna(close.shift(1)).fillna(close)

    tr = pd.concat(
        [high - low, (high - close.shift(1)).abs(), (low - close.shift(1)).abs()],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1.0 / period, adjust=False).mean()
    hl2 = (high + low) / 2.0
    basic_upper = hl2 + multiplier * atr
    basic_lower = hl2 - multiplier * atr

    n = len(out)
    final_upper = basic_upper.to_numpy(copy=True)
    final_lower = basic_lower.to_numpy(copy=True)
    close_np = close.to_numpy()

    for i in range(1, n):
        if basic_upper.iloc[i] < final_upper[i - 1] or close_np[i - 1] > final_upper[i - 1]:
            final_upper[i] = basic_upper.iloc[i]
        else:
            final_upper[i] = final_upper[i - 1]
        if basic_lower.iloc[i] > final_lower[i - 1] or close_np[i - 1] < final_lower[i - 1]:
            final_lower[i] = basic_lower.iloc[i]
        else:
            final_lower[i] = final_lower[i - 1]

    st = final_upper.copy()
    direction = [-1] * n
    for i in range(1, n):
        if st[i - 1] == final_upper[i - 1]:
            if close_np[i] <= final_upper[i]:
                st[i] = final_upper[i]
                direction[i] = -1
            else:
                st[i] = final_lower[i]
                direction[i] = 1
        else:
            if close_np[i] >= final_lower[i]:
                st[i] = final_lower[i]
                direction[i] = 1
            else:
                st[i] = final_upper[i]
                direction[i] = -1

    out["atr"] = atr
    out["supertrend"] = st
    out["st_direction"] = direction
    return out


def evaluate_latest(
    df: pd.DataFrame,
    symbol: str = "",
    period: int = ATR_PERIOD,
    multiplier: float = ATR_MULTIPLIER,
) -> Optional[SupertrendSignal]:
    if df is None or len(df) < MIN_HISTORY:
        return None
    needed = {"supertrend", "st_direction"}
    indicated = df if needed.issubset(df.columns) else compute_supertrend(df, period, multiplier)
    if len(indicated) < 2:
        return None

    last = indicated.iloc[-1]
    prev = indicated.iloc[-2]
    direction = int(last["st_direction"])
    prev_direction = int(prev["st_direction"])
    bdate = last["businessDate"]
    date_str = bdate.strftime("%Y-%m-%d") if hasattr(bdate, "strftime") else str(bdate)[:10]

    return SupertrendSignal(
        symbol=symbol,
        date=date_str,
        open=round(float(last["open"]), 2),
        high=round(float(last["high"]), 2),
        low=round(float(last["low"]), 2),
        close=round(float(last["close"]), 2),
        volume=round(float(last.get("volume", 0) or 0), 0),
        atr=round(float(last["atr"]), 2) if pd.notna(last["atr"]) else 0.0,
        supertrend=round(float(last["supertrend"]), 2),
        prev_supertrend=round(float(prev["supertrend"]), 2),
        direction=direction,
        prev_direction=prev_direction,
        flipped_up=prev_direction == -1 and direction == 1,
        flipped_down=prev_direction == 1 and direction == -1,
    )


def scan_ohlcv_map(
    ohlcv_by_symbol: dict[str, pd.DataFrame],
    period: int = ATR_PERIOD,
    multiplier: float = ATR_MULTIPLIER,
) -> tuple[list[SupertrendSignal], list[SupertrendSignal], list[SupertrendSignal], list[SupertrendSignal], int]:
    entries: list[SupertrendSignal] = []
    exits: list[SupertrendSignal] = []
    bulls: list[SupertrendSignal] = []
    bears: list[SupertrendSignal] = []
    skipped = 0

    for symbol, raw in ohlcv_by_symbol.items():
        if raw is None or len(raw) < MIN_HISTORY:
            skipped += 1
            continue
        raw = raw.sort_values("businessDate").reset_index(drop=True)
        try:
            signal = evaluate_latest(raw, symbol=symbol, period=period, multiplier=multiplier)
        except Exception as e:
            print(f"  {symbol}: evaluate error {e}")
            skipped += 1
            continue
        if signal is None:
            skipped += 1
            continue

        if signal.flipped_up:
            entries.append(signal)
            print(f"  🟢 ENTRY {symbol} @ {signal.close} ST {signal.supertrend}")
        elif signal.flipped_down:
            exits.append(signal)
            print(f"  🛑 EXIT  {symbol} @ {signal.close} ST {signal.supertrend}")
        elif signal.direction == 1:
            bulls.append(signal)
            print(f"  ▲ {symbol} uptrend @ {signal.close} ST {signal.supertrend}")
        else:
            bears.append(signal)
            print(f"  ▼ {symbol} downtrend @ {signal.close} ST {signal.supertrend}")

    return entries, exits, bulls, bears, skipped


def format_telegram(
    entries: list[SupertrendSignal],
    exits: list[SupertrendSignal],
    bulls: list[SupertrendSignal],
    bears: list[SupertrendSignal],
    scanned: int,
    as_of: str,
    skipped: int = 0,
    period: int = ATR_PERIOD,
    multiplier: float = ATR_MULTIPLIER,
) -> str:
    lines = [
        f"📈 *Supertrend Scan — {as_of}*",
        f"_ATR {period} · multiplier {multiplier:g}_",
        "",
    ]

    if entries:
        lines.append(f"🟢 *ENTRY FOUND ({len(entries)})* — flipped to uptrend")
        lines.append("")
        for s in entries:
            lines.append(f"• *{s.symbol}* @ Rs {s.close:.2f}")
            lines.append(f"  Supertrend {s.supertrend:.2f} | ATR {s.atr:.2f}")
            lines.append("")
    else:
        lines.append("🟢 *ENTRY FOUND:* none (no bullish flip today)")
        lines.append("")

    if exits:
        lines.append(f"🛑 *EXIT FOUND ({len(exits)})* — flipped to downtrend")
        lines.append("")
        for s in exits:
            lines.append(f"• *{s.symbol}* @ Rs {s.close:.2f}")
            lines.append(f"  Supertrend {s.supertrend:.2f} | ATR {s.atr:.2f}")
            lines.append("")
    else:
        lines.append("🛑 *EXIT FOUND:* none (no bearish flip today)")
        lines.append("")

    if bulls:
        names = ", ".join(s.symbol for s in bulls[:15])
        extra = f" +{len(bulls) - 15} more" if len(bulls) > 15 else ""
        lines.append(f"▲ *Still uptrend ({len(bulls)}):* {names}{extra}")
        lines.append("")
    if bears:
        names = ", ".join(s.symbol for s in bears[:10])
        extra = f" +{len(bears) - 10} more" if len(bears) > 10 else ""
        lines.append(f"▼ *Still downtrend ({len(bears)}):* {names}{extra}")
        lines.append("")

    lines.append(
        f"_Scanned {scanned} stocks ({skipped} skipped, need {MIN_HISTORY}+ days)._"
    )
    lines.append("_Not financial advice._")
    return "\n".join(lines).strip() + "\n"


def run_scan(
    scan_all: bool = False,
    symbols: Optional[list[str]] = None,
    send: bool = True,
    persist: bool = True,
    period: int = ATR_PERIOD,
    multiplier: float = ATR_MULTIPLIER,
    as_of: Optional[date] = None,
    floorsheet: Optional[str] = None,
) -> dict:
    sys.setrecursionlimit(10000)
    from nepse import Nepse

    cs.patch_nepse_tls(Nepse)
    n = Nepse()
    n.setTLSVerification(False)

    as_of = as_of or date.today()
    overlay_map: dict[str, dict] = {}
    sheet_path = floorsheet or f"floorsheet_{as_of.isoformat()}.csv"
    if Path(sheet_path).exists():
        overlay_map = load_floorsheet_ohlcv(sheet_path)
        print(f"  Overlaying {len(overlay_map)} floorsheet bars from {sheet_path}")

    universe = cs.resolve_symbols(n, scan_all=scan_all, only=symbols)
    if overlay_map and scan_all:
        extra = [s for s in overlay_map if s not in universe]
        universe.extend(extra)
    print(
        f"📈 Supertrend scan: {len(universe)} symbols "
        f"(ATR {period}, x{multiplier:g}, as-of {as_of.isoformat()})"
    )
    cid_map = n.getSecurityIDKeyMap() or {}

    ohlcv_by_symbol: dict[str, pd.DataFrame] = {}
    for i, symbol in enumerate(universe):
        if i and i % 10 == 0:
            print(f"    Progress: {i}/{len(universe)}", flush=True)
        cid = cid_map.get(symbol)
        if not cid:
            print(f"  {symbol}: no security id")
            continue
        try:
            rows = cs.fetch_symbol_history(n, cid)
            if not rows:
                continue
            hist = cs.history_to_ohlcv(rows)
            overlay = overlay_map.get(symbol)
            if overlay is not None:
                hist = apply_as_of(hist, as_of, overlay)
            else:
                hist["businessDate"] = pd.to_datetime(hist["businessDate"])
                hist = hist[hist["businessDate"] <= pd.Timestamp(as_of)]
            if hist.empty or not last_bar_on(hist, as_of):
                continue
            ohlcv_by_symbol[symbol] = hist
        except Exception as e:
            print(f"  {symbol}: history error {e}")
        time.sleep(cs.SLEEP_BETWEEN_SYMBOLS)

    entries, exits, bulls, bears, skipped = scan_ohlcv_map(
        ohlcv_by_symbol, period=period, multiplier=multiplier
    )
    as_of_label = as_of.isoformat()

    msg = format_telegram(
        entries,
        exits,
        bulls,
        bears,
        scanned=len(ohlcv_by_symbol),
        as_of=as_of_label,
        skipped=skipped,
        period=period,
        multiplier=multiplier,
    )
    print("\n" + msg)

    payload = {
        "as_of": as_of_label,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "period": period,
        "multiplier": multiplier,
        "scanned": len(ohlcv_by_symbol),
        "skipped": skipped,
        "entries": [asdict(s) for s in entries],
        "exits": [asdict(s) for s in exits],
        "bulls": [s.symbol for s in bulls],
        "bears": [s.symbol for s in bears],
        "message": msg,
    }

    telegram_ok = True
    if send:
        telegram_ok = cs.send_telegram(msg)
    payload["telegram_ok"] = telegram_ok

    if persist:
        cs.save_json(LATEST_FILE, payload)
        print(f"Saved {LATEST_FILE}")

    return payload


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Daily Supertrend scanner")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Scan all ordinary NEPSE equities (default: portfolio only)",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default="",
        help="Comma-separated symbols to scan",
    )
    parser.add_argument("--period", type=int, default=ATR_PERIOD, help="ATR period")
    parser.add_argument(
        "--multiplier", type=float, default=ATR_MULTIPLIER, help="ATR multiplier"
    )
    parser.add_argument("--no-telegram", action="store_true")
    parser.add_argument("--no-persist", action="store_true")
    parser.add_argument(
        "--as-of",
        type=str,
        default="",
        metavar="YYYY-MM-DD",
        help="Evaluate Supertrend on this business date (default: today)",
    )
    parser.add_argument(
        "--floorsheet",
        type=str,
        default="",
        help="Floorsheet CSV used to overlay that day's OHLCV",
    )
    args = parser.parse_args(argv)

    only = [s for s in args.symbols.split(",") if s.strip()] or None
    as_of_date = date.fromisoformat(args.as_of) if args.as_of else None
    payload = run_scan(
        scan_all=args.all,
        symbols=only,
        send=not args.no_telegram,
        persist=not args.no_persist,
        period=args.period,
        multiplier=args.multiplier,
        as_of=as_of_date,
        floorsheet=args.floorsheet or None,
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
