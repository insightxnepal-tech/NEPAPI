#!/usr/bin/env python3
"""
Daily candle scanner — TradingView-style 200 EMA / 20 EMA bounce.

ENTRY (all four must be true on the latest daily candle):
  1. Price is ABOVE the orange 200 EMA
  2. RSI dipped into the 38–48 zone (this candle or last few days)
  3. GREEN candle bounced off or closed near the blue 20 EMA
  4. Daily volume is higher than the 20-day volume MA

EXIT (open positions only — any one triggers):
  1. Close below the 20 EMA (bounce support lost)
  2. Close below the 200 EMA (uptrend broken)
  3. RSI >= 70 (overbought take-profit)

Sends a Telegram message when ENTRY FOUND or EXIT FOUND.
Designed to run daily after NEPSE close (Sun–Thu).
"""

from __future__ import annotations

import argparse
import json
import os
import ssl
import sys
import time
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import requests

# ── Strategy constants (match the manual candle loop) ─────────────
EMA_TREND = 200
EMA_BOUNCE = 20
VOL_MA = 20
RSI_LEN = 14
RSI_ZONE = (38.0, 48.0)
RSI_LOOKBACK = 3          # allow the RSI dip on this candle or the prior 2
RSI_DIP_MAX = 55.0        # still counts if RSI is just lifting out of the zone
RSI_OVERBOUGHT = 70.0
NEAR_PCT = 0.015          # 1.5% of price
NEAR_ATR_MULT = 0.5       # or 0.5 × ATR, whichever is larger
MIN_HISTORY = 200
HISTORY_CALENDAR_DAYS = 500
SLEEP_BETWEEN_SYMBOLS = 0.3

POSITIONS_FILE = os.getenv("CANDLE_POSITIONS_FILE", "candle_positions.json")
LATEST_FILE = os.getenv("CANDLE_LATEST_FILE", "candle_scan_latest.json")
PORTFOLIO_FILE = os.getenv("PORTFOLIO_FILE", "portfolio_data.json")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8618135314:AAHoDrHGP2sncP1HxEGLDj0OKtIpSLeuD0U")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "8563709547")


# ── Data classes ──────────────────────────────────────────────────
@dataclass
class CandleSignal:
    symbol: str
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    ema20: float
    ema200: float
    rsi: float
    vol_ma20: float
    atr: float
    above_200: bool
    rsi_dip: bool
    green_near_20: bool
    volume_ok: bool
    entry: bool
    exit_reasons: list = field(default_factory=list)

    @property
    def signal(self) -> str:
        if self.entry:
            return "ENTRY"
        if self.exit_reasons:
            return "EXIT"
        return "NONE"


# ── Indicators ────────────────────────────────────────────────────
def _rma(series: pd.Series, length: int) -> pd.Series:
    """Wilder moving average (TradingView RMA / RSI smoothing)."""
    return series.ewm(alpha=1.0 / length, adjust=False).mean()


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add ema20, ema200, rsi, vol_ma20, atr, open to a daily OHLCV frame.

    Expected columns: businessDate, high, low, close, volume.
    Optional: open. If missing, previous close is used as open.
    """
    out = df.copy().sort_values("businessDate").reset_index(drop=True)
    close = out["close"].astype(float)
    high = out["high"].astype(float)
    low = out["low"].astype(float)
    volume = out["volume"].astype(float)

    if "open" not in out.columns:
        out["open"] = close.shift(1).fillna(close)
    else:
        out["open"] = out["open"].astype(float).fillna(close.shift(1)).fillna(close)

    out["ema20"] = close.ewm(span=EMA_BOUNCE, adjust=False).mean()
    out["ema200"] = close.ewm(span=EMA_TREND, adjust=False).mean()

    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    rs = _rma(gain, RSI_LEN) / _rma(loss, RSI_LEN).replace(0, 1e-10)
    out["rsi"] = 100.0 - (100.0 / (1.0 + rs))

    tr = pd.concat(
        [high - low, (high - close.shift(1)).abs(), (low - close.shift(1)).abs()],
        axis=1,
    ).max(axis=1)
    out["atr"] = tr.rolling(RSI_LEN).mean()
    out["vol_ma20"] = volume.rolling(VOL_MA).mean()
    return out


def _near_distance(price: float, atr: float) -> float:
    atr_part = (atr if pd.notna(atr) and atr > 0 else 0.0) * NEAR_ATR_MULT
    return max(price * NEAR_PCT, atr_part, price * 0.005)


def evaluate_latest(df: pd.DataFrame, symbol: str = "") -> Optional[CandleSignal]:
    """Apply the manual candle loop to the last row of an indicator frame."""
    if df is None or len(df) < MIN_HISTORY:
        return None
    if df["ema200"].iloc[-1] != df["ema200"].iloc[-1]:  # NaN
        return None

    last = df.iloc[-1]
    lookback = min(RSI_LOOKBACK, len(df))
    recent_rsi = df["rsi"].iloc[-lookback:]

    price = float(last["close"])
    open_px = float(last["open"])
    high = float(last["high"])
    low = float(last["low"])
    ema20 = float(last["ema20"])
    ema200 = float(last["ema200"])
    rsi = float(last["rsi"])
    vol = float(last["volume"])
    vol_ma = float(last["vol_ma20"]) if pd.notna(last["vol_ma20"]) else 0.0
    atr = float(last["atr"]) if pd.notna(last["atr"]) else 0.0

    above_200 = price > ema200

    in_zone = (recent_rsi >= RSI_ZONE[0]) & (recent_rsi <= RSI_ZONE[1])
    rsi_dip = bool(in_zone.any()) and rsi <= RSI_DIP_MAX

    green = price > open_px
    dist = _near_distance(price, atr)
    bounced = (low <= ema20 + dist) and (price >= ema20 - dist)
    closed_near = abs(price - ema20) <= dist
    green_near_20 = bool(green and (bounced or closed_near))

    volume_ok = vol_ma > 0 and vol > vol_ma

    entry = bool(above_200 and rsi_dip and green_near_20 and volume_ok)

    exit_reasons: list[str] = []
    if price < ema20:
        exit_reasons.append(f"Close below 20 EMA ({ema20:.2f})")
    if price < ema200:
        exit_reasons.append(f"Close below 200 EMA ({ema200:.2f})")
    if rsi >= RSI_OVERBOUGHT:
        exit_reasons.append(f"RSI overbought ({rsi:.1f} >= {RSI_OVERBOUGHT:.0f})")

    bdate = last["businessDate"]
    if hasattr(bdate, "strftime"):
        date_str = bdate.strftime("%Y-%m-%d")
    else:
        date_str = str(bdate)[:10]

    return CandleSignal(
        symbol=symbol,
        date=date_str,
        open=round(open_px, 2),
        high=round(high, 2),
        low=round(low, 2),
        close=round(price, 2),
        volume=round(vol, 0),
        ema20=round(ema20, 2),
        ema200=round(ema200, 2),
        rsi=round(rsi, 1),
        vol_ma20=round(vol_ma, 0),
        atr=round(atr, 2),
        above_200=above_200,
        rsi_dip=rsi_dip,
        green_near_20=green_near_20,
        volume_ok=volume_ok,
        entry=entry,
        exit_reasons=exit_reasons,
    )


def apply_position_rules(
    signal: CandleSignal,
    open_positions: dict,
) -> str:
    """
    Decide what to notify.

    ENTRY only when the setup is new (not already in an open position).
    EXIT only for an existing open position (never on the same day as entry).
    """
    held = signal.symbol in open_positions
    if signal.entry and not held:
        return "ENTRY"
    if held and signal.exit_reasons and not signal.entry:
        return "EXIT"
    return "NONE"


def update_positions(
    positions: dict,
    signal: CandleSignal,
    action: str,
) -> dict:
    out = dict(positions)
    if action == "ENTRY":
        out[signal.symbol] = {
            "entry_date": signal.date,
            "entry_price": signal.close,
            "ema20": signal.ema20,
            "ema200": signal.ema200,
            "rsi": signal.rsi,
        }
    elif action == "EXIT" and signal.symbol in out:
        del out[signal.symbol]
    return out


# ── Persistence ───────────────────────────────────────────────────
def load_json(path: str, default):
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"Warning: could not read {path}: {e}")
    return default


def save_json(path: str, payload) -> None:
    with open(path, "w") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")


def load_positions() -> dict:
    data = load_json(POSITIONS_FILE, {})
    return data if isinstance(data, dict) else {}


def load_portfolio_symbols() -> list[str]:
    data = load_json(PORTFOLIO_FILE, {})
    if isinstance(data, dict):
        return [str(s).upper() for s in data.keys()]
    return []


def load_latest_scan() -> dict:
    return load_json(LATEST_FILE, {})


# ── Telegram ──────────────────────────────────────────────────────
def format_telegram(
    entries: list[CandleSignal],
    exits: list[tuple[CandleSignal, dict]],
    scanned: int,
    as_of: str,
    skipped: int = 0,
) -> str:
    lines = [f"🕯️ *Daily Candle Scan — {as_of}*", ""]

    if entries:
        lines.append(f"🟢 *ENTRY FOUND ({len(entries)})*")
        lines.append("_Price > 200 EMA · RSI 38–48 · green 20 EMA bounce · vol > MA20_")
        lines.append("")
        for s in entries:
            vol_x = (s.volume / s.vol_ma20) if s.vol_ma20 else 0
            lines.append(f"• *{s.symbol}* @ Rs {s.close:.2f}")
            lines.append(
                f"  200 EMA {s.ema200:.2f} | 20 EMA {s.ema20:.2f} | RSI {s.rsi:.1f}"
            )
            lines.append(
                f"  Vol {vol_x:.1f}× MA20 | O {s.open:.2f} H {s.high:.2f} L {s.low:.2f}"
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

    lines.append(f"_Scanned {scanned} stocks ({skipped} skipped, need {MIN_HISTORY}+ days)._")
    lines.append("_Not financial advice._")
    return "\n".join(lines).strip() + "\n"


def send_telegram(text: str, token: str = "", chat_id: str = "") -> bool:
    token = token or TELEGRAM_TOKEN
    chat_id = chat_id or TELEGRAM_CHAT_ID
    if not token or not chat_id:
        print("Telegram skipped: TELEGRAM_TOKEN or TELEGRAM_CHAT_ID not set.")
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    ok = True
    for i in range(0, len(text), 4000):
        chunk = text[i : i + 4000]
        try:
            res = requests.post(
                url,
                json={"chat_id": chat_id, "text": chunk, "parse_mode": "Markdown"},
                timeout=20,
            )
            print(f"Telegram status {res.status_code}: {res.text[:200]}")
            if res.status_code != 200:
                ok = False
        except Exception as e:
            print(f"Telegram send error: {e}")
            ok = False
    return ok


# ── NEPSE history fetch ───────────────────────────────────────────
def patch_nepse_tls(Nepse) -> None:
    """Same TLS workaround used by sniper_scanner.py."""

    def patched_request_get(self, url, include_authorization_headers=True):
        full_url = f"https://www.nepalstock.com{url}"
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json",
        }
        if include_authorization_headers:
            access_token = self.token_manager.getAccessToken()
            headers["Authorization"] = f"Salter {access_token}"
        ctx = ssl._create_unverified_context()
        req = urllib.request.Request(full_url, headers=headers)
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=30) as response:
                return json.loads(response.read().decode())
        except Exception as e:
            print(f"  NEPSE request error {url}: {e}")
            return {}

    Nepse.requestGETAPI = patched_request_get


def history_to_ohlcv(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    rename = {
        "closePrice": "close",
        "highPrice": "high",
        "lowPrice": "low",
        "openPrice": "open",
        "totalTradedQuantity": "volume",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    if "volume" not in df.columns and "totalTradedQuantity" in df.columns:
        df["volume"] = df["totalTradedQuantity"]
    needed = {"businessDate", "high", "low", "close", "volume"}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"history missing columns: {missing}")
    df["businessDate"] = pd.to_datetime(df["businessDate"])
    df = df.drop_duplicates("businessDate").sort_values("businessDate")
    return df


def fetch_symbol_history(n, cid: int) -> list[dict]:
    full_history: list[dict] = []
    end_date = date.today()
    start_date = end_date - timedelta(days=HISTORY_CALENDAR_DAYS)
    for page in range(0, 6):
        url = (
            f"/api/nots/market/history/security/{cid}"
            f"?&size=500&startDate={start_date}&endDate={end_date}&page={page}"
        )
        res = n.requestGETAPI(url)
        if not res or "content" not in res or not res["content"]:
            break
        full_history.extend(res["content"])
        if len(res["content"]) < 15:
            break
    return full_history


def _is_ordinary_equity(security: dict) -> bool:
    name = str(security.get("securityName") or security.get("companyName") or "")
    skip = ("Mutual Fund", "Debenture", "Bond", "Promoter", "Preference")
    return not any(s.lower() in name.lower() for s in skip)


def resolve_symbols(n, scan_all: bool, only: Optional[Iterable[str]]) -> list[str]:
    if only:
        return [s.strip().upper() for s in only if s.strip()]
    if scan_all:
        securities = n.getSecurityList() or []
        return [
            s["symbol"]
            for s in securities
            if s.get("symbol") and _is_ordinary_equity(s)
        ]
    return load_portfolio_symbols()


# ── Scan driver ───────────────────────────────────────────────────
def scan_ohlcv_map(
    ohlcv_by_symbol: dict[str, pd.DataFrame],
    positions: dict,
) -> tuple[list[CandleSignal], list[tuple[CandleSignal, dict]], dict, int]:
    entries: list[CandleSignal] = []
    exits: list[tuple[CandleSignal, dict]] = []
    new_positions = dict(positions)
    skipped = 0

    for symbol, raw in ohlcv_by_symbol.items():
        if raw is None or len(raw) < MIN_HISTORY:
            skipped += 1
            continue
        try:
            needed = {"ema20", "ema200", "rsi", "vol_ma20"}
            indicated = raw if needed.issubset(raw.columns) else compute_indicators(raw)
            signal = evaluate_latest(indicated, symbol=symbol)
        except Exception as e:
            print(f"  {symbol}: evaluate error {e}")
            skipped += 1
            continue
        if signal is None:
            skipped += 1
            continue

        action = apply_position_rules(signal, new_positions)
        if action == "ENTRY":
            entries.append(signal)
            new_positions = update_positions(new_positions, signal, action)
            print(f"  🟢 ENTRY {symbol} @ {signal.close} RSI {signal.rsi}")
        elif action == "EXIT":
            pos = dict(new_positions.get(symbol, {}))
            exits.append((signal, pos))
            new_positions = update_positions(new_positions, signal, action)
            print(f"  🛑 EXIT  {symbol} @ {signal.close} {signal.exit_reasons}")
        else:
            flags = []
            if not signal.above_200:
                flags.append("below200")
            if not signal.rsi_dip:
                flags.append("rsi")
            if not signal.green_near_20:
                flags.append("ema20")
            if not signal.volume_ok:
                flags.append("vol")
            print(f"  · {symbol} skip ({', '.join(flags) or 'held'})")

    return entries, exits, new_positions, skipped


def run_scan(
    scan_all: bool = False,
    symbols: Optional[list[str]] = None,
    send: bool = True,
    persist: bool = True,
) -> dict:
    sys.setrecursionlimit(10000)
    from nepse import Nepse

    patch_nepse_tls(Nepse)
    n = Nepse()
    n.setTLSVerification(False)

    positions = load_positions()
    universe = resolve_symbols(n, scan_all=scan_all, only=symbols)
    # Always re-check open positions even if they fell out of the universe.
    for held in positions:
        if held not in universe:
            universe.append(held)

    print(f"🕯️ Candle scan: {len(universe)} symbols, {len(positions)} open positions")
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
            rows = fetch_symbol_history(n, cid)
            if not rows:
                continue
            ohlcv_by_symbol[symbol] = history_to_ohlcv(rows)
        except Exception as e:
            print(f"  {symbol}: history error {e}")
        time.sleep(SLEEP_BETWEEN_SYMBOLS)

    entries, exits, new_positions, skipped = scan_ohlcv_map(ohlcv_by_symbol, positions)
    as_of = date.today().isoformat()
    if entries:
        as_of = entries[0].date
    elif exits:
        as_of = exits[0][0].date

    msg = format_telegram(
        entries, exits, scanned=len(ohlcv_by_symbol), as_of=as_of, skipped=skipped
    )
    print("\n" + msg)

    payload = {
        "as_of": as_of,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "scanned": len(ohlcv_by_symbol),
        "skipped": skipped,
        "entries": [asdict(s) for s in entries],
        "exits": [
            {"signal": asdict(s), "position": pos} for s, pos in exits
        ],
        "open_positions": new_positions,
        "message": msg,
    }

    telegram_ok = True
    if send:
        telegram_ok = send_telegram(msg)
    payload["telegram_ok"] = telegram_ok

    if persist:
        save_json(POSITIONS_FILE, new_positions)
        save_json(LATEST_FILE, payload)
        print(f"Saved {POSITIONS_FILE} and {LATEST_FILE}")

    return payload


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Daily 200/20 EMA candle scanner")
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
