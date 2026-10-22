#!/usr/bin/env python3
"""
Daily Supertrend scanner — classic ATR(10) × 3.

BUY:  Supertrend flips bullish on the latest completed daily bar (new signal only).
SELL: Open Supertrend BUY position flips bearish.

Sends Telegram when BUY or SELL is found.
Designed to run daily at 10:00 AM NPT on NEPSE trading days (Sun–Thu),
skipping Fri/Sat, Nepal holidays, and duplicate as_of scans.
"""

from __future__ import annotations

import argparse
import json
import os
import ssl
import sys
import time
import urllib.request
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import requests

# ── Strategy constants ────────────────────────────────────────────
ATR_LEN = 10
ST_MULT = 3.0
MIN_HISTORY = 30
HISTORY_CALENDAR_DAYS = 500
SLEEP_BETWEEN_SYMBOLS = 0.3
NPT = ZoneInfo("Asia/Kathmandu")

POSITIONS_FILE = os.getenv("SUPERTREND_POSITIONS_FILE", "supertrend_positions.json")
LATEST_FILE = os.getenv("SUPERTREND_LATEST_FILE", "supertrend_scan_latest.json")
HOLIDAYS_FILE = os.getenv("NEPSE_HOLIDAYS_FILE", "nepse_holidays.json")
PORTFOLIO_FILE = os.getenv("PORTFOLIO_FILE", "portfolio_data.json")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8618135314:AAHoDrHGP2sncP1HxEGLDj0OKtIpSLeuD0U")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "8563709547")


# ── Data classes ──────────────────────────────────────────────────
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
    direction: int  # 1 = bullish, -1 = bearish
    prev_direction: int
    buy_flip: bool
    sell_flip: bool

    @property
    def signal(self) -> str:
        if self.buy_flip:
            return "BUY"
        if self.sell_flip:
            return "SELL"
        return "NONE"


# ── Persistence helpers ───────────────────────────────────────────
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


def load_holidays() -> set[str]:
    data = load_json(HOLIDAYS_FILE, [])
    if isinstance(data, dict):
        data = data.get("holidays") or data.get("dates") or []
    if not isinstance(data, list):
        return set()
    return {str(d)[:10] for d in data}


# ── Trading-day / holiday gate ────────────────────────────────────
def today_npt() -> date:
    return datetime.now(NPT).date()


def is_weekend_npt(d: Optional[date] = None) -> bool:
    """NEPSE weekly off: Friday (4) and Saturday (5)."""
    d = d or today_npt()
    return d.weekday() in (4, 5)  # Fri, Sat


def parse_market_as_of(status) -> Optional[date]:
    if not status:
        return None
    if isinstance(status, dict):
        raw = status.get("asOf") or status.get("as_of")
    else:
        raw = getattr(status, "asOf", None)
    if not raw:
        return None
    try:
        return pd.to_datetime(raw).date()
    except Exception:
        return None


def market_is_open_flag(status) -> Optional[str]:
    if not status:
        return None
    if isinstance(status, dict):
        return str(status.get("isOpen") or status.get("is_open") or "")
    return str(getattr(status, "isOpen", "") or "")


def should_skip_for_calendar(
    force: bool = False,
    holidays: Optional[set[str]] = None,
    today: Optional[date] = None,
    market_status=None,
) -> tuple[bool, str]:
    """
    Early gate before expensive market-wide scan.
    Returns (skip, reason).
    """
    if force:
        return False, ""
    today = today or today_npt()
    if is_weekend_npt(today):
        return True, f"Weekend / NEPSE weekly holiday ({today.isoformat()})"

    holidays = holidays if holidays is not None else load_holidays()
    if today.isoformat() in holidays:
        return True, f"Nepal/NEPSE holiday ({today.isoformat()})"

    # NOTE: Do NOT treat market_status isOpen=CLOSED + stale asOf as a holiday.
    # This job runs at 10:00 AM NPT — before the open — so status is normally
    # CLOSED with asOf=previous session even on real trading days.
    # Mid-week holidays are covered by nepse_holidays.json + as_of candle dedupe.
    _ = market_status  # accepted for API compatibility / future use

    return False, ""


def already_scanned_as_of(as_of: str, force: bool = False) -> bool:
    if force or not as_of:
        return False
    latest = load_latest_scan()
    return str(latest.get("as_of") or "")[:10] == str(as_of)[:10]


# ── Indicators ────────────────────────────────────────────────────
def _rma(series: pd.Series, length: int) -> pd.Series:
    """Wilder moving average (TradingView RMA)."""
    return series.ewm(alpha=1.0 / length, adjust=False).mean()


def compute_supertrend(
    df: pd.DataFrame,
    period: int = ATR_LEN,
    multiplier: float = ST_MULT,
) -> pd.DataFrame:
    """
    TradingView-style Supertrend.

    Adds columns: atr, st_upper, st_lower, supertrend, st_dir
    st_dir: 1 = bullish (price above ST), -1 = bearish.
    """
    out = df.copy().sort_values("businessDate").reset_index(drop=True)
    high = out["high"].astype(float)
    low = out["low"].astype(float)
    close = out["close"].astype(float)

    if "open" not in out.columns:
        out["open"] = close.shift(1).fillna(close)
    else:
        out["open"] = out["open"].astype(float).fillna(close.shift(1)).fillna(close)

    if "volume" not in out.columns:
        out["volume"] = 0.0
    else:
        out["volume"] = out["volume"].astype(float).fillna(0.0)

    tr = pd.concat(
        [high - low, (high - close.shift(1)).abs(), (low - close.shift(1)).abs()],
        axis=1,
    ).max(axis=1)
    atr = _rma(tr, period)
    out["atr"] = atr

    hl2 = (high + low) / 2.0
    basic_upper = hl2 + multiplier * atr
    basic_lower = hl2 - multiplier * atr

    final_upper = basic_upper.copy()
    final_lower = basic_lower.copy()
    st_dir = pd.Series(1, index=out.index, dtype=int)
    st_line = pd.Series(index=out.index, dtype=float)

    for i in range(len(out)):
        if i == 0 or pd.isna(atr.iloc[i]):
            final_upper.iloc[i] = basic_upper.iloc[i]
            final_lower.iloc[i] = basic_lower.iloc[i]
            st_dir.iloc[i] = 1
            st_line.iloc[i] = final_lower.iloc[i]
            continue

        prev_close = close.iloc[i - 1]
        if basic_upper.iloc[i] < final_upper.iloc[i - 1] or prev_close > final_upper.iloc[i - 1]:
            final_upper.iloc[i] = basic_upper.iloc[i]
        else:
            final_upper.iloc[i] = final_upper.iloc[i - 1]

        if basic_lower.iloc[i] > final_lower.iloc[i - 1] or prev_close < final_lower.iloc[i - 1]:
            final_lower.iloc[i] = basic_lower.iloc[i]
        else:
            final_lower.iloc[i] = final_lower.iloc[i - 1]

        if st_dir.iloc[i - 1] == 1:
            if close.iloc[i] < final_lower.iloc[i]:
                st_dir.iloc[i] = -1
            else:
                st_dir.iloc[i] = 1
        else:
            if close.iloc[i] > final_upper.iloc[i]:
                st_dir.iloc[i] = 1
            else:
                st_dir.iloc[i] = -1

        st_line.iloc[i] = (
            final_lower.iloc[i] if st_dir.iloc[i] == 1 else final_upper.iloc[i]
        )

    out["st_upper"] = final_upper
    out["st_lower"] = final_lower
    out["supertrend"] = st_line
    out["st_dir"] = st_dir
    return out


def evaluate_latest(df: pd.DataFrame, symbol: str = "") -> Optional[SupertrendSignal]:
    """Read the latest bar for buy/sell flips."""
    if df is None or len(df) < MIN_HISTORY:
        return None
    needed = {"supertrend", "st_dir", "atr"}
    if not needed.issubset(df.columns):
        df = compute_supertrend(df)
    if len(df) < 2:
        return None
    if pd.isna(df["supertrend"].iloc[-1]) or pd.isna(df["atr"].iloc[-1]):
        return None

    last = df.iloc[-1]
    prev = df.iloc[-2]
    direction = int(last["st_dir"])
    prev_direction = int(prev["st_dir"])
    buy_flip = prev_direction == -1 and direction == 1
    sell_flip = prev_direction == 1 and direction == -1

    bdate = last["businessDate"]
    if hasattr(bdate, "strftime"):
        date_str = bdate.strftime("%Y-%m-%d")
    else:
        date_str = str(bdate)[:10]

    return SupertrendSignal(
        symbol=symbol,
        date=date_str,
        open=round(float(last["open"]), 2),
        high=round(float(last["high"]), 2),
        low=round(float(last["low"]), 2),
        close=round(float(last["close"]), 2),
        volume=round(float(last.get("volume", 0) or 0), 0),
        atr=round(float(last["atr"]), 2),
        supertrend=round(float(last["supertrend"]), 2),
        direction=direction,
        prev_direction=prev_direction,
        buy_flip=buy_flip,
        sell_flip=sell_flip,
    )


def apply_position_rules(signal: SupertrendSignal, open_positions: dict) -> str:
    """
    BUY only on a new bullish flip when not already held.
    SELL only for an existing open position on a bearish flip.
    """
    held = signal.symbol in open_positions
    if signal.buy_flip and not held:
        return "BUY"
    if held and signal.sell_flip:
        return "SELL"
    return "NONE"


def update_positions(
    positions: dict,
    signal: SupertrendSignal,
    action: str,
) -> dict:
    out = dict(positions)
    if action == "BUY":
        out[signal.symbol] = {
            "entry_date": signal.date,
            "entry_price": signal.close,
            "supertrend": signal.supertrend,
            "atr": signal.atr,
        }
    elif action == "SELL" and signal.symbol in out:
        del out[signal.symbol]
    return out


# ── Telegram ──────────────────────────────────────────────────────
def format_telegram(
    buys: list[SupertrendSignal],
    sells: list[tuple[SupertrendSignal, dict]],
    scanned: int,
    as_of: str,
    skipped: int = 0,
) -> str:
    lines = [
        f"📈 *Supertrend Scan — {as_of}*",
        f"_ATR {ATR_LEN} × {ST_MULT:g}_",
        "",
    ]

    if buys:
        lines.append(f"🟢 *BUY ({len(buys)})* — new bullish flip")
        lines.append("")
        for s in buys:
            lines.append(f"• *{s.symbol}* @ Rs {s.close:.2f}")
            lines.append(
                f"  ST {s.supertrend:.2f} | ATR {s.atr:.2f} | "
                f"O {s.open:.2f} H {s.high:.2f} L {s.low:.2f}"
            )
            lines.append("")
    else:
        lines.append("🟢 *BUY:* none")
        lines.append("")

    if sells:
        lines.append(f"🛑 *SELL ({len(sells)})* — bearish flip on open buy")
        lines.append("")
        for s, pos in sells:
            entry_px = pos.get("entry_price")
            entry_dt = pos.get("entry_date", "?")
            pnl = ""
            if isinstance(entry_px, (int, float)) and entry_px:
                pct = (s.close - entry_px) / entry_px * 100
                pnl = f" | P/L {pct:+.1f}%"
            lines.append(f"• *{s.symbol}* @ Rs {s.close:.2f}{pnl}")
            lines.append(f"  Entered Rs {entry_px} on {entry_dt}")
            lines.append(f"  ST {s.supertrend:.2f} | ATR {s.atr:.2f}")
            lines.append("")
    else:
        lines.append("🛑 *SELL:* none")
        lines.append("")

    lines.append(
        f"_Scanned {scanned} stocks ({skipped} skipped, need {MIN_HISTORY}+ days)._"
    )
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
    """Same TLS workaround used by candle_scanner.py / sniper_scanner.py."""

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
    needed = {"businessDate", "high", "low", "close"}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"history missing columns: {missing}")
    if "volume" not in df.columns:
        df["volume"] = 0.0
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


def fetch_market_status(n):
    try:
        return n.getMarketStatus()
    except Exception as e:
        print(f"Warning: getMarketStatus failed: {e}")
        return None


# ── Scan driver ───────────────────────────────────────────────────
def scan_ohlcv_map(
    ohlcv_by_symbol: dict[str, pd.DataFrame],
    positions: dict,
) -> tuple[list[SupertrendSignal], list[tuple[SupertrendSignal, dict]], dict, int]:
    buys: list[SupertrendSignal] = []
    sells: list[tuple[SupertrendSignal, dict]] = []
    new_positions = dict(positions)
    skipped = 0

    for symbol, raw in ohlcv_by_symbol.items():
        if raw is None or len(raw) < MIN_HISTORY:
            skipped += 1
            continue
        try:
            indicated = (
                raw
                if {"supertrend", "st_dir", "atr"}.issubset(raw.columns)
                else compute_supertrend(raw)
            )
            signal = evaluate_latest(indicated, symbol=symbol)
        except Exception as e:
            print(f"  {symbol}: evaluate error {e}")
            skipped += 1
            continue
        if signal is None:
            skipped += 1
            continue

        action = apply_position_rules(signal, new_positions)
        if action == "BUY":
            buys.append(signal)
            new_positions = update_positions(new_positions, signal, action)
            print(f"  🟢 BUY  {symbol} @ {signal.close} ST {signal.supertrend}")
        elif action == "SELL":
            pos = dict(new_positions.get(symbol, {}))
            sells.append((signal, pos))
            new_positions = update_positions(new_positions, signal, action)
            print(f"  🛑 SELL {symbol} @ {signal.close} ST {signal.supertrend}")
        else:
            trend = "UP" if signal.direction == 1 else "DOWN"
            held = "held" if symbol in new_positions else "flat"
            print(f"  · {symbol} {trend} ({held})")

    return buys, sells, new_positions, skipped


def _latest_as_of(ohlcv_by_symbol: dict[str, pd.DataFrame]) -> str:
    latest: Optional[pd.Timestamp] = None
    for raw in ohlcv_by_symbol.values():
        if raw is None or raw.empty or "businessDate" not in raw.columns:
            continue
        mx = pd.to_datetime(raw["businessDate"]).max()
        if latest is None or mx > latest:
            latest = mx
    if latest is None:
        return today_npt().isoformat()
    return latest.strftime("%Y-%m-%d")


def run_scan(
    scan_all: bool = False,
    symbols: Optional[list[str]] = None,
    send: bool = True,
    persist: bool = True,
    force: bool = False,
) -> dict:
    sys.setrecursionlimit(10000)

    skip, reason = should_skip_for_calendar(force=force)
    if skip:
        print(f"⏭ Skipping Supertrend scan: {reason}")
        return {
            "skipped": True,
            "reason": reason,
            "as_of": None,
            "buys": [],
            "sells": [],
            "telegram_ok": True,
        }

    from nepse import Nepse

    patch_nepse_tls(Nepse)
    n = Nepse()
    n.setTLSVerification(False)

    status = fetch_market_status(n)
    skip, reason = should_skip_for_calendar(
        force=force, market_status=status, today=today_npt()
    )
    if skip:
        print(f"⏭ Skipping Supertrend scan: {reason}")
        return {
            "skipped": True,
            "reason": reason,
            "as_of": None,
            "buys": [],
            "sells": [],
            "telegram_ok": True,
        }
    if status:
        print(f"Market status: {market_is_open_flag(status)} asOf={parse_market_as_of(status)}")

    positions = load_positions()
    universe = resolve_symbols(n, scan_all=scan_all, only=symbols)
    for held in positions:
        if held not in universe:
            universe.append(held)

    print(
        f"📈 Supertrend scan: {len(universe)} symbols, "
        f"{len(positions)} open positions (ATR{ATR_LEN}×{ST_MULT:g})"
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
            rows = fetch_symbol_history(n, cid)
            if not rows:
                continue
            ohlcv_by_symbol[symbol] = history_to_ohlcv(rows)
        except Exception as e:
            print(f"  {symbol}: history error {e}")
        time.sleep(SLEEP_BETWEEN_SYMBOLS)

    as_of = _latest_as_of(ohlcv_by_symbol)
    if already_scanned_as_of(as_of, force=force):
        reason = f"Already scanned as_of={as_of} (holiday or re-run)"
        print(f"⏭ Skipping Telegram / position update: {reason}")
        return {
            "skipped": True,
            "reason": reason,
            "as_of": as_of,
            "buys": [],
            "sells": [],
            "telegram_ok": True,
            "open_positions": positions,
        }

    buys, sells, new_positions, skipped = scan_ohlcv_map(ohlcv_by_symbol, positions)
    if buys:
        as_of = buys[0].date
    elif sells:
        as_of = sells[0][0].date

    msg = format_telegram(
        buys, sells, scanned=len(ohlcv_by_symbol), as_of=as_of, skipped=skipped
    )
    print("\n" + msg)

    payload = {
        "as_of": as_of,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "scanned": len(ohlcv_by_symbol),
        "skipped_symbols": skipped,
        "buys": [asdict(s) for s in buys],
        "sells": [{"signal": asdict(s), "position": pos} for s, pos in sells],
        "open_positions": new_positions,
        "message": msg,
        "skipped": False,
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
    parser = argparse.ArgumentParser(description="Daily Supertrend (ATR10×3) scanner")
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
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass weekend / holiday / as_of dedupe gates",
    )
    args = parser.parse_args(argv)

    only = [s for s in args.symbols.split(",") if s.strip()] or None
    payload = run_scan(
        scan_all=args.all,
        symbols=only,
        send=not args.no_telegram,
        persist=not args.no_persist,
        force=args.force,
    )
    if payload.get("skipped"):
        return 0
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
