#!/usr/bin/env python3
"""Compare Supertrend parameter sets and confluence filters on NEPSE floorsheets."""

from __future__ import annotations

from collections import defaultdict

import numpy as np
import pandas as pd

import supertrend_scanner as st

PARAM_SETS = [
    (7, 2.0),
    (7, 3.0),
    (10, 2.0),
    (10, 3.0),
    (10, 4.0),
    (11, 2.0),
    (14, 2.0),
    (14, 3.0),
]

HORIZONS = (1, 3, 5, 10)


def add_context(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    close = out["close"].astype(float)
    out["ema21"] = close.ewm(span=21, adjust=False).mean()
    out["ema50"] = close.ewm(span=min(50, max(len(out), 1)), adjust=False).mean()
    out["atr_pct"] = np.where(close > 0, out["atr"] / close * 100.0, 0.0)
    out["vol_ratio"] = np.where(out["vol_ma20"] > 0, out["volume"] / out["vol_ma20"], 0.0)
    out["green"] = out["close"] > out["open"]
    out["dist_pct"] = np.where(
        out["supertrend"] > 0,
        (out["close"] - out["supertrend"]) / out["supertrend"] * 100.0,
        0.0,
    )
    body = (out["close"] - out["open"]).abs()
    rng = (out["high"] - out["low"]).replace(0, np.nan)
    out["body_frac"] = (body / rng).fillna(0.0)
    return out


def fwd_returns(close: np.ndarray, i: int) -> dict[str, float]:
    out = {}
    px = close[i]
    if px <= 0:
        return {f"d{h}": np.nan for h in HORIZONS}
    for h in HORIZONS:
        j = i + h
        out[f"d{h}"] = (close[j] / px - 1.0) * 100.0 if j < len(close) else np.nan
    return out


def whipsaw(trend: np.ndarray, i: int, within: int = 3) -> bool:
    end = min(len(trend), i + 1 + within)
    return any(int(trend[j]) != int(trend[i]) for j in range(i + 1, end))


def collect_flips(indicated: pd.DataFrame, symbol: str, period: int, multiplier: float) -> list[dict]:
    trend = indicated["st_trend"].to_numpy()
    close = indicated["close"].to_numpy(dtype=float)
    rows = []
    for i in range(1, len(indicated)):
        if int(trend[i]) == int(trend[i - 1]):
            continue
        last = indicated.iloc[i]
        side = "BUY" if int(trend[i]) == 1 else "SELL"
        rec = {
            "symbol": symbol,
            "period": period,
            "multiplier": multiplier,
            "side": side,
            "date": str(last["businessDate"])[:10],
            "close": float(last["close"]),
            "rsi": float(last["rsi"]) if pd.notna(last["rsi"]) else np.nan,
            "vol_ratio": float(last["vol_ratio"]),
            "ema21": float(last["ema21"]),
            "ema50": float(last["ema50"]),
            "atr_pct": float(last["atr_pct"]),
            "dist_pct": float(last["dist_pct"]),
            "green": bool(last["green"]),
            "body_frac": float(last["body_frac"]),
            "above_ema21": float(last["close"]) > float(last["ema21"]),
            "above_ema50": float(last["close"]) > float(last["ema50"]),
            "whipsaw3": whipsaw(trend, i, 3),
            "bars_left": len(indicated) - 1 - i,
        }
        rec.update(fwd_returns(close, i))
        rows.append(rec)
    return rows


def collect_pullbacks(indicated: pd.DataFrame, symbol: str, period: int, multiplier: float) -> list[dict]:
    """Bullish trend, price tags Supertrend (within 1.5%) and closes green."""
    trend = indicated["st_trend"].to_numpy()
    close = indicated["close"].to_numpy(dtype=float)
    rows = []
    in_setup = False
    for i in range(21, len(indicated)):
        last = indicated.iloc[i]
        bull = int(trend[i]) == 1
        near = 0 <= float(last["dist_pct"]) <= 1.5
        ok = bull and near and bool(last["green"])
        if ok and not in_setup:
            rec = {
                "symbol": symbol,
                "period": period,
                "multiplier": multiplier,
                "side": "PULLBACK",
                "date": str(last["businessDate"])[:10],
                "close": float(last["close"]),
                "rsi": float(last["rsi"]) if pd.notna(last["rsi"]) else np.nan,
                "vol_ratio": float(last["vol_ratio"]),
                "above_ema21": float(last["close"]) > float(last["ema21"]),
                "above_ema50": float(last["close"]) > float(last["ema50"]),
                "green": bool(last["green"]),
                "body_frac": float(last["body_frac"]),
                "atr_pct": float(last["atr_pct"]),
                "dist_pct": float(last["dist_pct"]),
                "whipsaw3": whipsaw(trend, i, 3),
            }
            rec.update(fwd_returns(close, i))
            rows.append(rec)
            in_setup = True
        elif not near:
            in_setup = False
    return rows


def summarize(df: pd.DataFrame, label: str) -> dict:
    if df.empty:
        return {"setup": label, "n": 0}
    def avg(col):
        s = df[col].dropna()
        return round(float(s.mean()), 2) if len(s) else np.nan

    def win(col):
        s = df[col].dropna()
        return round(float((s > 0).mean() * 100), 1) if len(s) else np.nan

    row = {
        "setup": label,
        "n": int(len(df)),
        "whipsaw%": round(float(df["whipsaw3"].mean() * 100), 1) if "whipsaw3" in df else np.nan,
        "avg_d1": avg("d1"),
        "win_d1": win("d1"),
        "avg_d3": avg("d3"),
        "win_d3": win("d3"),
        "avg_d5": avg("d5"),
        "win_d5": win("d5"),
        "avg_d10": avg("d10"),
        "win_d10": win("d10"),
        "med_d5": round(float(df["d5"].dropna().median()), 2) if df["d5"].notna().any() else np.nan,
    }
    return row


def apply_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    out = df
    if filters.get("vol"):
        out = out[out["vol_ratio"] >= filters["vol"]]
    if filters.get("rsi"):
        lo, hi = filters["rsi"]
        out = out[out["rsi"].between(lo, hi)]
    if filters.get("ema21"):
        out = out[out["above_ema21"] == True]  # noqa: E712
    if filters.get("ema50"):
        out = out[out["above_ema50"] == True]  # noqa: E712
    if filters.get("green"):
        out = out[out["green"] == True]  # noqa: E712
    if filters.get("body"):
        out = out[out["body_frac"] >= filters["body"]]
    if filters.get("atr_pct"):
        out = out[out["atr_pct"] >= filters["atr_pct"]]
    return out


def main() -> int:
    ohlcv_map = st.load_ohlcv_floorsheet(".", None)
    stockmap = st.load_stockmap()
    flips = []
    pullbacks = []
    used = 0
    for symbol, raw in ohlcv_map.items():
        meta = stockmap.get(symbol, {}) if isinstance(stockmap.get(symbol), dict) else {}
        name = str(meta.get("name") or symbol)
        sector = str(meta.get("sector") or "")
        if not st._is_ordinary_equity_name(name, sector):
            continue
        if raw is None or len(raw) < 40:
            continue
        used += 1
        for period, mult in PARAM_SETS:
            indicated = add_context(st.compute_supertrend(raw, period, mult))
            flips.extend(collect_flips(indicated, symbol, period, mult))
            if (period, mult) == (10, 3.0):
                pullbacks.extend(collect_pullbacks(indicated, symbol, period, mult))

    fdf = pd.DataFrame(flips)
    pdf = pd.DataFrame(pullbacks)
    print(f"Universe: {used} ordinary equities")
    print(f"Flip events: {len(fdf)}  pullbacks(10,3): {len(pdf)}")

    param_rows = []
    for period, mult in PARAM_SETS:
        buys = fdf[(fdf.period == period) & (fdf.multiplier == mult) & (fdf.side == "BUY")]
        sells = fdf[(fdf.period == period) & (fdf.multiplier == mult) & (fdf.side == "SELL")]
        b = summarize(buys, f"BUY ({period},{mult:g})")
        s = summarize(sells, f"SELL ({period},{mult:g})")
        # For sells, negative stock return is the desired direction.
        b["sell_avg_d5"] = s["avg_d5"]
        b["sell_n"] = s["n"]
        b["sell_whipsaw%"] = s["whipsaw%"]
        param_rows.append(b)
        print(
            f"  ({period:>2},{mult:g})  BUY n={b['n']:<4} d5={b['avg_d5']:+6.2f}% win5={b['win_d5']}% "
            f"whip={b['whipsaw%']}% | SELL n={s['n']:<4} d5={s['avg_d5']:+6.2f}% whip={s['whipsaw%']}%"
        )

    param_df = pd.DataFrame(param_rows)

    base = fdf[(fdf.period == 10) & (fdf.multiplier == 3.0) & (fdf.side == "BUY")]
    filter_defs = [
        ("BUY (10,3) raw", {}),
        ("+ vol > 1.2x MA20", {"vol": 1.2}),
        ("+ RSI 40-65", {"rsi": (40, 65)}),
        ("+ close > EMA21", {"ema21": True}),
        ("+ close > EMA50", {"ema50": True}),
        ("+ green candle", {"green": True}),
        ("+ body >= 50% of range", {"body": 0.5}),
        ("+ ATR% >= 1.5", {"atr_pct": 1.5}),
        (
            "QUALITY: vol1.2 + EMA21 + green + RSI40-65",
            {"vol": 1.2, "ema21": True, "green": True, "rsi": (40, 65)},
        ),
        (
            "QUALITY tight: vol1.2 + EMA21 + green + body0.4 + ATR1.5",
            {"vol": 1.2, "ema21": True, "green": True, "body": 0.4, "atr_pct": 1.5},
        ),
        (
            "SWING: vol1.2 + EMA50 + RSI45-70 + green",
            {"vol": 1.2, "ema50": True, "rsi": (45, 70), "green": True},
        ),
    ]
    filter_rows = [summarize(apply_filters(base, spec), name) for name, spec in filter_defs]
    if not pdf.empty:
        filter_rows.append(summarize(pdf, "PULLBACK to ST (10,3) green + dist<=1.5%"))
        q_pb = pdf[(pdf["vol_ratio"] >= 1.2) & (pdf["above_ema21"] == True)]  # noqa: E712
        filter_rows.append(summarize(q_pb, "PULLBACK + vol1.2 + EMA21"))

    # Best combo across params with quality filter
    combo_rows = []
    quality = {"vol": 1.2, "ema21": True, "green": True, "rsi": (40, 65)}
    for period, mult in PARAM_SETS:
        buys = fdf[(fdf.period == period) & (fdf.multiplier == mult) & (fdf.side == "BUY")]
        combo_rows.append(summarize(apply_filters(buys, quality), f"Q BUY ({period},{mult:g})"))

    filt_df = pd.DataFrame(filter_rows)
    combo_df = pd.DataFrame(combo_rows)

    print("\n=== Filters on BUY (10,3) ===")
    print(filt_df.to_string(index=False))
    print("\n=== Quality filter across params ===")
    print(combo_df.to_string(index=False))

    out = "/opt/cursor/artifacts/supertrend_setup_backtest.xlsx"
    with pd.ExcelWriter(out, engine="openpyxl") as xw:
        param_df.to_excel(xw, sheet_name="Params BUY", index=False)
        filt_df.to_excel(xw, sheet_name="Filters on 10-3", index=False)
        combo_df.to_excel(xw, sheet_name="Quality by params", index=False)
    print(f"\nWrote {out}")
    param_df.to_csv("/opt/cursor/artifacts/supertrend_params.csv", index=False)
    filt_df.to_csv("/opt/cursor/artifacts/supertrend_filters.csv", index=False)
    combo_df.to_csv("/opt/cursor/artifacts/supertrend_quality_params.csv", index=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
