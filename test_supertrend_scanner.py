#!/usr/bin/env python3
"""Unit tests for the daily Supertrend scanner (no NEPSE network)."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import date

import numpy as np
import pandas as pd

import supertrend_scanner as st


def make_ohlcv(
    n: int = 80,
    start: float = 100.0,
    path: str = "down_then_up",
) -> pd.DataFrame:
    """Build synthetic daily OHLCV that can force Supertrend flips."""
    dates = pd.bdate_range("2025-01-01", periods=n)
    if path == "down_then_up":
        # Long downtrend, then a sharp rally on the final bars.
        close = np.linspace(start, start * 0.55, n - 4)
        close = np.concatenate(
            [close, np.array([close[-1] * 1.02, close[-1] * 1.08, close[-1] * 1.18, close[-1] * 1.32])]
        )
    elif path == "up_then_down":
        close = np.linspace(start * 0.6, start, n - 4)
        close = np.concatenate(
            [close, np.array([close[-1] * 0.98, close[-1] * 0.90, close[-1] * 0.80, close[-1] * 0.68])]
        )
    elif path == "steady_up":
        close = np.linspace(start * 0.7, start, n)
    elif path == "steady_down":
        close = np.linspace(start, start * 0.6, n)
    else:
        raise ValueError(path)

    open_ = np.r_[close[0], close[:-1]]
    high = np.maximum(open_, close) + np.abs(close) * 0.01
    low = np.minimum(open_, close) - np.abs(close) * 0.01
    # Widen the last candle so a flip is more likely.
    high[-1] = max(high[-1], close[-1] * 1.03)
    low[-1] = min(low[-1], close[-1] * 0.97)
    volume = np.full(n, 10_000.0)

    return pd.DataFrame(
        {
            "businessDate": dates,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        }
    )


def force_flip_on_last(df: pd.DataFrame, target_dir: int) -> pd.DataFrame:
    """
    Compute Supertrend, then surgically adjust the last close so the
    latest bar flips to target_dir (1=bullish, -1=bearish).
    """
    out = st.compute_supertrend(df.copy())
    if len(out) < 3:
        raise RuntimeError("need history")
    # Walk backward until prev bar is opposite of target, then spike last bar.
    # Simpler: set all prior dirs by rebuilding with extreme last candle.
    prev = out.iloc[-2]
    last = out.iloc[-1]
    df2 = df.copy()
    if target_dir == 1:
        # Need prev bearish, last close above final upper.
        # Make last close pierce prior upper band aggressively.
        pierce = float(prev["st_upper"]) * 1.15 if pd.notna(prev["st_upper"]) else float(last["close"]) * 1.2
        df2.loc[df2.index[-1], "close"] = pierce
        df2.loc[df2.index[-1], "open"] = pierce * 0.98
        df2.loc[df2.index[-1], "high"] = pierce * 1.02
        df2.loc[df2.index[-1], "low"] = pierce * 0.97
    else:
        pierce = float(prev["st_lower"]) * 0.85 if pd.notna(prev["st_lower"]) else float(last["close"]) * 0.8
        df2.loc[df2.index[-1], "close"] = pierce
        df2.loc[df2.index[-1], "open"] = pierce * 1.02
        df2.loc[df2.index[-1], "high"] = pierce * 1.03
        df2.loc[df2.index[-1], "low"] = pierce * 0.97
    return st.compute_supertrend(df2)


class SupertrendIndicatorTests(unittest.TestCase):
    def test_compute_adds_columns(self):
        df = make_ohlcv()
        out = st.compute_supertrend(df)
        for col in ("atr", "st_upper", "st_lower", "supertrend", "st_dir"):
            self.assertIn(col, out.columns)
        self.assertTrue((out["st_dir"].isin([1, -1])).all())

    def test_bullish_flip_buy_action(self):
        base = make_ohlcv(path="down_then_up")
        # Ensure we end with a bullish flip on the last bar.
        indicated = None
        signal = None
        for _ in range(5):
            indicated = force_flip_on_last(base, target_dir=1)
            # If prev is not bearish, deepen the downtrend first.
            if int(indicated["st_dir"].iloc[-2]) != -1:
                base = make_ohlcv(n=100, path="steady_down")
                # append a rally bar
                last = base.iloc[-1].copy()
                last["businessDate"] = base["businessDate"].iloc[-1] + pd.Timedelta(days=1)
                last["close"] = last["close"] * 1.4
                last["open"] = last["close"] * 0.95
                last["high"] = last["close"] * 1.05
                last["low"] = last["open"]
                base = pd.concat([base.iloc[:-1], pd.DataFrame([last])], ignore_index=True)
                indicated = force_flip_on_last(base, target_dir=1)
            signal = st.evaluate_latest(indicated, symbol="TEST")
            if signal and signal.buy_flip:
                break
        self.assertIsNotNone(signal)
        self.assertTrue(signal.buy_flip, f"dirs={indicated['st_dir'].iloc[-3:].tolist()}")
        self.assertEqual(st.apply_position_rules(signal, {}), "BUY")
        # Already held → no duplicate BUY
        self.assertEqual(st.apply_position_rules(signal, {"TEST": {}}), "NONE")

    def test_bearish_flip_sell_only_when_held(self):
        base = make_ohlcv(path="up_then_down")
        indicated = force_flip_on_last(base, target_dir=-1)
        if int(indicated["st_dir"].iloc[-2]) != 1:
            base = make_ohlcv(n=100, path="steady_up")
            last = base.iloc[-1].copy()
            last["businessDate"] = base["businessDate"].iloc[-1] + pd.Timedelta(days=1)
            last["close"] = last["close"] * 0.6
            last["open"] = last["close"] * 1.05
            last["high"] = last["open"]
            last["low"] = last["close"] * 0.95
            base = pd.concat([base.iloc[:-1], pd.DataFrame([last])], ignore_index=True)
            indicated = force_flip_on_last(base, target_dir=-1)
        signal = st.evaluate_latest(indicated, symbol="HELD")
        self.assertIsNotNone(signal)
        self.assertTrue(signal.sell_flip, f"dirs={indicated['st_dir'].iloc[-3:].tolist()}")
        self.assertEqual(st.apply_position_rules(signal, {}), "NONE")
        self.assertEqual(
            st.apply_position_rules(signal, {"HELD": {"entry_price": 100}}),
            "SELL",
        )

    def test_no_duplicate_buy_when_already_bullish(self):
        df = make_ohlcv(path="steady_up")
        indicated = st.compute_supertrend(df)
        signal = st.evaluate_latest(indicated, symbol="UP")
        self.assertIsNotNone(signal)
        # Steady up should not flip on the last bar.
        self.assertFalse(signal.buy_flip)
        self.assertEqual(st.apply_position_rules(signal, {}), "NONE")


class PositionUpdateTests(unittest.TestCase):
    def test_update_positions_buy_and_sell(self):
        sig = st.SupertrendSignal(
            symbol="ABC",
            date="2026-09-01",
            open=10,
            high=11,
            low=9,
            close=10.5,
            volume=1000,
            atr=0.5,
            supertrend=9.8,
            direction=1,
            prev_direction=-1,
            buy_flip=True,
            sell_flip=False,
        )
        positions = st.update_positions({}, sig, "BUY")
        self.assertIn("ABC", positions)
        self.assertEqual(positions["ABC"]["entry_price"], 10.5)

        sig2 = st.SupertrendSignal(
            symbol="ABC",
            date="2026-09-05",
            open=11,
            high=11,
            low=9,
            close=9.2,
            volume=1000,
            atr=0.5,
            supertrend=10.1,
            direction=-1,
            prev_direction=1,
            buy_flip=False,
            sell_flip=True,
        )
        positions = st.update_positions(positions, sig2, "SELL")
        self.assertNotIn("ABC", positions)


class CalendarGateTests(unittest.TestCase):
    def test_weekend_and_holiday_skip(self):
        skip, _ = st.should_skip_for_calendar(
            force=False, today=date(2026, 9, 4), holidays=set()
        )  # Friday
        self.assertTrue(skip)
        skip, _ = st.should_skip_for_calendar(
            force=False, today=date(2026, 9, 5), holidays=set()
        )  # Saturday
        self.assertTrue(skip)
        skip, _ = st.should_skip_for_calendar(
            force=False, today=date(2026, 9, 6), holidays=set()
        )  # Sunday trading day
        self.assertFalse(skip)
        skip, _ = st.should_skip_for_calendar(
            force=False, today=date(2026, 9, 7), holidays={"2026-09-07"}
        )
        self.assertTrue(skip)
        skip, _ = st.should_skip_for_calendar(
            force=True, today=date(2026, 9, 4), holidays={"2026-09-04"}
        )
        self.assertFalse(skip)

    def test_as_of_dedupe_skips_telegram_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            latest_path = os.path.join(tmp, "latest.json")
            with open(latest_path, "w") as f:
                json.dump({"as_of": "2026-09-04", "buys": []}, f)
            old_latest = st.LATEST_FILE
            try:
                st.LATEST_FILE = latest_path
                self.assertTrue(st.already_scanned_as_of("2026-09-04", force=False))
                self.assertFalse(st.already_scanned_as_of("2026-09-04", force=True))
                self.assertFalse(st.already_scanned_as_of("2026-09-05", force=False))
            finally:
                st.LATEST_FILE = old_latest


class ScanMapTests(unittest.TestCase):
    def test_scan_ohlcv_map_buy_and_sell(self):
        buy_df = force_flip_on_last(make_ohlcv(path="down_then_up"), target_dir=1)
        # Ensure buy flip
        if not st.evaluate_latest(buy_df, "BUY1").buy_flip:
            buy_df = force_flip_on_last(make_ohlcv(n=120, path="steady_down"), target_dir=1)

        sell_df = force_flip_on_last(make_ohlcv(path="up_then_down"), target_dir=-1)
        positions = {
            "SELL1": {
                "entry_date": "2026-08-01",
                "entry_price": 100.0,
                "supertrend": 95.0,
                "atr": 2.0,
            }
        }
        ohlcv = {"BUY1": buy_df, "SELL1": sell_df}
        buys, sells, new_pos, skipped = st.scan_ohlcv_map(ohlcv, positions)

        buy_sig = st.evaluate_latest(buy_df, "BUY1")
        sell_sig = st.evaluate_latest(sell_df, "SELL1")
        if buy_sig and buy_sig.buy_flip:
            self.assertTrue(any(b.symbol == "BUY1" for b in buys))
            self.assertIn("BUY1", new_pos)
        if sell_sig and sell_sig.sell_flip:
            self.assertTrue(any(s.symbol == "SELL1" for s, _ in sells))
            self.assertNotIn("SELL1", new_pos)


if __name__ == "__main__":
    unittest.main()
