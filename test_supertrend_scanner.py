#!/usr/bin/env python3
"""Unit tests for Supertrend scanner (no NEPSE network)."""

import unittest

import numpy as np
import pandas as pd

import supertrend_scanner as st


def make_ohlcv(n=80, start=100.0, crash_at=None, rally_at=None):
    """Synthetic daily bars. Optional crash/rally on the last stretch."""
    dates = pd.bdate_range("2025-01-01", periods=n)
    close = np.full(n, start, dtype=float)
    # Slow grind up so Supertrend can settle green.
    for i in range(1, n):
        close[i] = close[i - 1] * 1.008

    if crash_at is not None:
        close[crash_at:] = close[crash_at - 1] * np.linspace(0.98, 0.82, n - crash_at)
    if rally_at is not None:
        # First grind down so ST is red, then rally.
        close = np.full(n, start, dtype=float)
        for i in range(1, rally_at):
            close[i] = close[i - 1] * 0.992
        for i in range(rally_at, n):
            close[i] = close[i - 1] * 1.025

    high = close * 1.006
    low = close * 0.994
    open_ = np.r_[close[0], close[:-1]]
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


class TestSupertrendMath(unittest.TestCase):
    def test_columns_and_direction_values(self):
        df = make_ohlcv()
        out = st.compute_supertrend(df)
        for col in ("atr", "supertrend", "st_direction"):
            self.assertIn(col, out.columns)
        self.assertTrue(set(out["st_direction"].unique()).issubset({-1, 1}))
        self.assertTrue((out["atr"].iloc[15:] > 0).all())

    def test_uptrend_is_green_and_line_below_price(self):
        df = make_ohlcv(n=80)
        out = st.compute_supertrend(df)
        last = out.iloc[-1]
        self.assertEqual(int(last["st_direction"]), 1)
        self.assertLess(float(last["supertrend"]), float(last["close"]))

    def test_downtrend_is_red_and_line_above_price(self):
        df = make_ohlcv(n=80, crash_at=40)
        out = st.compute_supertrend(df)
        last = out.iloc[-1]
        self.assertEqual(int(last["st_direction"]), -1)
        self.assertGreater(float(last["supertrend"]), float(last["close"]))


class TestFlips(unittest.TestCase):
    def test_rally_flip_is_entry(self):
        df = make_ohlcv(n=90, rally_at=70)
        sig = st.evaluate_latest(df, "NABIL")
        self.assertIsNotNone(sig)
        # Last bars of a strong rally should be green; flip may be today or already in.
        self.assertEqual(sig.direction, 1)
        indicated = st.compute_supertrend(df)
        dirs = indicated["st_direction"].astype(int)
        self.assertTrue((dirs == -1).any())
        self.assertTrue((dirs == 1).any())

    def test_crash_flip_is_exit(self):
        df = make_ohlcv(n=90, crash_at=70)
        sig = st.evaluate_latest(df, "JBBL")
        self.assertIsNotNone(sig)
        self.assertEqual(sig.direction, -1)

    def test_scan_map_separates_entry_exit(self):
        up = make_ohlcv(n=90, rally_at=75)
        down = make_ohlcv(n=90, crash_at=75)
        # Force last-bar flip by splicing a known prior direction.
        up_st = st.compute_supertrend(up)
        down_st = st.compute_supertrend(down)
        up_st.loc[up_st.index[-2], "st_direction"] = -1
        up_st.loc[up_st.index[-1], "st_direction"] = 1
        down_st.loc[down_st.index[-2], "st_direction"] = 1
        down_st.loc[down_st.index[-1], "st_direction"] = -1
        entries, exits, bulls, bears, skipped = st.scan_ohlcv_map(
            {"API": up_st, "NRN": down_st}
        )
        self.assertEqual(skipped, 0)
        self.assertEqual([s.symbol for s in entries], ["API"])
        self.assertEqual([s.symbol for s in exits], ["NRN"])

    def test_too_short_returns_none(self):
        df = make_ohlcv(n=10)
        self.assertIsNone(st.evaluate_latest(df, "MEN"))

    def test_telegram_mentions_entry_and_exit(self):
        df = make_ohlcv()
        sig = st.evaluate_latest(df, "SBI")
        msg = st.format_telegram(
            [sig], [sig], [], [], scanned=2, as_of="2026-08-31"
        )
        self.assertIn("ENTRY FOUND", msg)
        self.assertIn("EXIT FOUND", msg)
        self.assertIn("SBI", msg)
        self.assertIn("ATR 10", msg)


if __name__ == "__main__":
    unittest.main()
