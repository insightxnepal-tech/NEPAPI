#!/usr/bin/env python3
"""Unit tests for the daily 200/20 EMA candle scanner (no NEPSE network)."""

import os
import tempfile
import unittest

import numpy as np
import pandas as pd

import candle_scanner as cs


def make_ohlcv(
    n=240,
    start_price=100.0,
    trend=0.15,
    bounce=True,
    rsi_zone=True,
    green=True,
    high_volume=True,
    below_200=False,
    far_from_20=False,
    red_below_20=False,
    overbought=False,
):
    """Build a synthetic daily series that can satisfy or break each rule."""
    rng = np.random.default_rng(42)
    dates = pd.bdate_range("2025-01-01", periods=n)
    # Slow uptrend so EMA200 sits below price, then flatten near the end.
    t = np.linspace(0, 1, n)
    close = start_price * (1 + trend * t)
    close = close + rng.normal(0, 0.15, n)
    if below_200:
        close[-40:] = close[-40:] * 0.82
    close = np.maximum(close, 1.0)

    # Recalculate after optional crash so EMAs are consistent.
    s = pd.Series(close)
    ema20 = s.ewm(span=20, adjust=False).mean()
    ema200 = s.ewm(span=200, adjust=False).mean()

    open_ = np.r_[close[0], close[:-1]]
    high = np.maximum(open_, close) + 0.4
    low = np.minimum(open_, close) - 0.4
    volume = np.full(n, 10_000.0)

    # Last candle: pin it to the desired setup.
    ema20_last = float(ema20.iloc[-2])  # approx; we set close near last ema20
    ema200_last = float(ema200.iloc[-1])

    if red_below_20:
        close[-1] = ema20_last * 0.97
        open_[-1] = close[-1] * 1.02
        high[-1] = open_[-1]
        low[-1] = close[-1] * 0.99
    elif far_from_20:
        close[-1] = ema20_last * 1.08
        open_[-1] = close[-1] * 0.99
        high[-1] = close[-1] * 1.01
        low[-1] = open_[-1]
    elif green:
        # Green candle that tags the 20 EMA and closes just above it.
        close[-1] = ema20_last * 1.004
        open_[-1] = ema20_last * 0.992
        low[-1] = ema20_last * 0.995
        high[-1] = close[-1] * 1.005
        if below_200:
            close[-1] = ema200_last * 0.97
            open_[-1] = close[-1] * 0.99
            low[-1] = close[-1] * 0.98
            high[-1] = open_[-1] * 1.01
    else:
        close[-1] = ema20_last * 1.002
        open_[-1] = ema20_last * 1.012
        high[-1] = open_[-1]
        low[-1] = ema20_last * 0.995

    if high_volume:
        volume[-1] = 25_000
    else:
        volume[-1] = 1_000

    df = pd.DataFrame(
        {
            "businessDate": dates,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        }
    )

    indicated = cs.compute_indicators(df)

    # Force RSI into / out of the 38–48 zone on the last few bars without
    # wrecking the last candle's OHLC relationship too much.
    if rsi_zone:
        indicated.loc[indicated.index[-3:], "rsi"] = [46.0, 42.0, 44.0]
    elif overbought:
        indicated.loc[indicated.index[-3:], "rsi"] = [68.0, 71.0, 73.0]
    else:
        indicated.loc[indicated.index[-3:], "rsi"] = [62.0, 61.0, 60.0]

    if bounce is False and not far_from_20 and not red_below_20:
        # Keep green but lift the whole candle well above 20 EMA.
        last = indicated.index[-1]
        indicated.loc[last, "open"] = indicated.loc[last, "ema20"] * 1.06
        indicated.loc[last, "low"] = indicated.loc[last, "ema20"] * 1.05
        indicated.loc[last, "close"] = indicated.loc[last, "ema20"] * 1.07
        indicated.loc[last, "high"] = indicated.loc[last, "close"] * 1.01

    return indicated


class TestIndicators(unittest.TestCase):
    def test_ema_and_rsi_columns(self):
        df = pd.DataFrame(
            {
                "businessDate": pd.bdate_range("2025-01-01", periods=220),
                "high": np.linspace(101, 121, 220),
                "low": np.linspace(99, 119, 220),
                "close": np.linspace(100, 120, 220),
                "volume": np.full(220, 5000),
            }
        )
        out = cs.compute_indicators(df)
        for col in ("ema20", "ema200", "rsi", "vol_ma20", "atr", "open"):
            self.assertIn(col, out.columns)
        self.assertGreater(out["ema200"].iloc[-1], out["ema200"].iloc[50])
        self.assertTrue((out["rsi"].iloc[20:] >= 0).all())
        self.assertTrue((out["rsi"].iloc[20:] <= 100).all())

    def test_open_falls_back_to_previous_close(self):
        df = pd.DataFrame(
            {
                "businessDate": pd.bdate_range("2025-01-01", periods=5),
                "high": [11, 12, 13, 14, 15],
                "low": [9, 10, 11, 12, 13],
                "close": [10, 11, 12, 13, 14],
                "volume": [100] * 5,
            }
        )
        out = cs.compute_indicators(df)
        self.assertAlmostEqual(out["open"].iloc[1], 10)
        self.assertAlmostEqual(out["open"].iloc[-1], 13)


class TestEntryRules(unittest.TestCase):
    def test_all_four_yes_is_entry(self):
        df = make_ohlcv()
        sig = cs.evaluate_latest(df, "NABIL")
        self.assertIsNotNone(sig)
        self.assertTrue(sig.above_200, "price should be above 200 EMA")
        self.assertTrue(sig.rsi_dip, "RSI should be in 38–48")
        self.assertTrue(sig.green_near_20, "green candle should be near 20 EMA")
        self.assertTrue(sig.volume_ok, "volume should exceed MA20")
        self.assertTrue(sig.entry)
        self.assertEqual(sig.signal, "ENTRY")

    def test_below_200_ema_is_ignored(self):
        df = make_ohlcv(below_200=True)
        sig = cs.evaluate_latest(df, "NABIL")
        self.assertFalse(sig.above_200)
        self.assertFalse(sig.entry)

    def test_rsi_lookback_still_counts_as_dip(self):
        df = make_ohlcv()
        # Latest RSI just left the zone; prior bar was inside 38–48.
        df.loc[df.index[-3:], "rsi"] = [41.0, 46.0, 48.2]
        sig = cs.evaluate_latest(df, "API")
        self.assertTrue(sig.rsi_dip)
        self.assertTrue(sig.entry)

    def test_rsi_not_in_zone_is_ignored(self):
        df = make_ohlcv(rsi_zone=False)
        sig = cs.evaluate_latest(df, "NABIL")
        self.assertFalse(sig.rsi_dip)
        self.assertFalse(sig.entry)

    def test_red_candle_is_ignored(self):
        df = make_ohlcv(green=False)
        sig = cs.evaluate_latest(df, "NABIL")
        self.assertFalse(sig.green_near_20)
        self.assertFalse(sig.entry)

    def test_candle_far_from_20_ema_is_ignored(self):
        df = make_ohlcv(far_from_20=True, bounce=False)
        sig = cs.evaluate_latest(df, "NABIL")
        self.assertFalse(sig.green_near_20)
        self.assertFalse(sig.entry)

    def test_low_volume_is_ignored(self):
        df = make_ohlcv(high_volume=False)
        sig = cs.evaluate_latest(df, "NABIL")
        self.assertFalse(sig.volume_ok)
        self.assertFalse(sig.entry)

    def test_too_short_history_returns_none(self):
        df = make_ohlcv(n=50)
        self.assertIsNone(cs.evaluate_latest(df, "NABIL"))


class TestExitAndPositions(unittest.TestCase):
    def test_close_below_20_is_exit_for_open_position(self):
        df = make_ohlcv(red_below_20=True, rsi_zone=False)
        sig = cs.evaluate_latest(df, "JBBL")
        self.assertFalse(sig.entry)
        self.assertTrue(any("20 EMA" in r for r in sig.exit_reasons))
        action = cs.apply_position_rules(sig, {"JBBL": {"entry_price": 400}})
        self.assertEqual(action, "EXIT")

    def test_no_exit_without_open_position(self):
        df = make_ohlcv(red_below_20=True, rsi_zone=False)
        sig = cs.evaluate_latest(df, "JBBL")
        action = cs.apply_position_rules(sig, {})
        self.assertEqual(action, "NONE")

    def test_duplicate_entry_not_refired(self):
        df = make_ohlcv()
        sig = cs.evaluate_latest(df, "NABIL")
        self.assertTrue(sig.entry)
        action = cs.apply_position_rules(sig, {"NABIL": {"entry_price": sig.close}})
        self.assertEqual(action, "NONE")

    def test_rsi_overbought_exits_open_position(self):
        df = make_ohlcv(rsi_zone=False, overbought=True, far_from_20=True)
        # Keep price above both EMAs so the only exit reason is RSI.
        sig = cs.evaluate_latest(df, "SBI")
        self.assertTrue(any("overbought" in r.lower() for r in sig.exit_reasons))
        action = cs.apply_position_rules(sig, {"SBI": {"entry_price": 300}})
        self.assertEqual(action, "EXIT")

    def test_update_positions_add_and_remove(self):
        df = make_ohlcv()
        sig = cs.evaluate_latest(df, "GBIME")
        pos = cs.update_positions({}, sig, "ENTRY")
        self.assertIn("GBIME", pos)
        self.assertEqual(pos["GBIME"]["entry_price"], sig.close)
        pos = cs.update_positions(pos, sig, "EXIT")
        self.assertNotIn("GBIME", pos)

    def test_scan_map_emits_entry_then_exit(self):
        entry_df = make_ohlcv()
        exit_df = make_ohlcv(red_below_20=True, rsi_zone=False)
        entries, exits, pos, skipped = cs.scan_ohlcv_map({"HDL": entry_df}, {})
        self.assertEqual(skipped, 0)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].symbol, "HDL")
        self.assertIn("HDL", pos)

        entries2, exits2, pos2, _ = cs.scan_ohlcv_map({"HDL": exit_df}, pos)
        self.assertEqual(len(entries2), 0)
        self.assertEqual(len(exits2), 1)
        self.assertNotIn("HDL", pos2)


class TestTelegramFormatting(unittest.TestCase):
    def test_message_contains_entry_and_exit_headers(self):
        df = make_ohlcv()
        entry = cs.evaluate_latest(df, "NABIL")
        exit_sig = cs.evaluate_latest(make_ohlcv(red_below_20=True, rsi_zone=False), "JBBL")
        msg = cs.format_telegram(
            [entry],
            [(exit_sig, {"entry_price": 400.0, "entry_date": "2026-07-01"})],
            scanned=2,
            as_of="2026-08-16",
        )
        self.assertIn("ENTRY FOUND", msg)
        self.assertIn("EXIT FOUND", msg)
        self.assertIn("NABIL", msg)
        self.assertIn("JBBL", msg)
        self.assertIn("P/L", msg)

    def test_empty_scan_still_mentions_entry_and_exit(self):
        msg = cs.format_telegram([], [], scanned=10, as_of="2026-08-16", skipped=2)
        self.assertIn("ENTRY FOUND:* none", msg)
        self.assertIn("EXIT FOUND:* none", msg)


class TestHistoryParser(unittest.TestCase):
    def test_renames_nepse_fields(self):
        rows = [
            {
                "businessDate": "2026-08-14",
                "closePrice": 100,
                "highPrice": 102,
                "lowPrice": 99,
                "openPrice": 98,
                "totalTradedQuantity": 5000,
            },
            {
                "businessDate": "2026-08-15",
                "closePrice": 101,
                "highPrice": 103,
                "lowPrice": 100,
                "openPrice": 100,
                "totalTradedQuantity": 6000,
            },
        ]
        df = cs.history_to_ohlcv(rows)
        self.assertEqual(list(df["close"]), [100, 101])
        self.assertEqual(list(df["volume"]), [5000, 6000])
        self.assertIn("open", df.columns)


class TestJsonRoundtrip(unittest.TestCase):
    def test_load_save_positions(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "pos.json")
            old = cs.POSITIONS_FILE
            cs.POSITIONS_FILE = path
            try:
                cs.save_json(path, {"API": {"entry_price": 1}})
                loaded = cs.load_positions()
                self.assertEqual(loaded["API"]["entry_price"], 1)
            finally:
                cs.POSITIONS_FILE = old


if __name__ == "__main__":
    unittest.main()
