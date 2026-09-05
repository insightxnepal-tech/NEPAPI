#!/usr/bin/env python3
"""Unit tests for the SuperTrend scanner (no NEPSE network)."""

from __future__ import annotations

import os
import tempfile
import unittest

import numpy as np
import pandas as pd

import supertrend_scanner as st


def make_ohlcv(
    n: int = 120,
    start: float = 100.0,
    trend: float = 0.4,
    flip_up: bool = False,
    flip_down: bool = False,
) -> pd.DataFrame:
    """
    Build a synthetic daily series.

    Default: steady uptrend (bullish SuperTrend).
    flip_up: long downtrend then a sharp rally on the last bar.
    flip_down: long uptrend then a sharp selloff on the last bar.
    """
    dates = pd.bdate_range("2025-01-01", periods=n)
    t = np.linspace(0, 1, n)

    if flip_up:
        close = start * (1 - 0.35 * t)
        # Last bar: hard rally through prior upper band.
        close[-1] = close[-2] * 1.18
    elif flip_down:
        close = start * (1 + trend * t)
        close[-1] = close[-2] * 0.82
    else:
        close = start * (1 + trend * t)

    open_ = np.r_[close[0], close[:-1]]
    if flip_up:
        open_[-1] = close[-2]
    if flip_down:
        open_[-1] = close[-2]

    high = np.maximum(open_, close) + 0.6
    low = np.minimum(open_, close) - 0.6
    if flip_up:
        high[-1] = close[-1] + 0.5
        low[-1] = open_[-1] - 0.2
    if flip_down:
        high[-1] = open_[-1] + 0.2
        low[-1] = close[-1] - 0.5

    return pd.DataFrame(
        {
            "businessDate": dates,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": np.full(n, 5000.0),
        }
    )


class TestSuperTrendIndicator(unittest.TestCase):
    def test_columns_present(self):
        df = make_ohlcv()
        out = st.compute_supertrend(df)
        for col in ("atr", "st_upper", "st_lower", "supertrend", "st_dir"):
            self.assertIn(col, out.columns)
        self.assertTrue((out["st_dir"].iloc[20:].isin([-1, 1])).all())

    def test_uptrend_is_bullish(self):
        df = make_ohlcv(trend=0.5)
        out = st.compute_supertrend(df)
        self.assertEqual(int(out["st_dir"].iloc[-1]), 1)
        self.assertLess(float(out["supertrend"].iloc[-1]), float(out["close"].iloc[-1]))

    def test_downtrend_is_bearish(self):
        df = make_ohlcv(trend=-0.4)
        out = st.compute_supertrend(df)
        self.assertEqual(int(out["st_dir"].iloc[-1]), -1)
        self.assertGreater(float(out["supertrend"].iloc[-1]), float(out["close"].iloc[-1]))


class TestSignals(unittest.TestCase):
    def test_bullish_flip_is_entry(self):
        df = make_ohlcv(flip_up=True)
        out = st.compute_supertrend(df)
        # Force a clean flip on the last two bars for a deterministic unit test.
        out.loc[out.index[-2], "st_dir"] = -1
        out.loc[out.index[-1], "st_dir"] = 1
        sig = st.evaluate_latest(out, "NABIL")
        self.assertIsNotNone(sig)
        self.assertTrue(sig.flipped_up)
        self.assertTrue(sig.entry)
        self.assertEqual(sig.signal, "ENTRY")

    def test_bearish_flip_sets_exit_reason(self):
        df = make_ohlcv(flip_down=True)
        out = st.compute_supertrend(df)
        out.loc[out.index[-2], "st_dir"] = 1
        out.loc[out.index[-1], "st_dir"] = -1
        sig = st.evaluate_latest(out, "NABIL")
        self.assertIsNotNone(sig)
        self.assertTrue(sig.flipped_down)
        self.assertFalse(sig.entry)
        self.assertTrue(sig.exit_reasons)
        self.assertEqual(sig.signal, "EXIT")

    def test_short_history_returns_none(self):
        df = make_ohlcv(n=20)
        self.assertIsNone(st.evaluate_latest(df, "NABIL"))


class TestPositions(unittest.TestCase):
    def test_entry_then_exit(self):
        entry_df = make_ohlcv(flip_up=True)
        entry_ind = st.compute_supertrend(entry_df)
        entry_ind.loc[entry_ind.index[-2], "st_dir"] = -1
        entry_ind.loc[entry_ind.index[-1], "st_dir"] = 1
        entry_sig = st.evaluate_latest(entry_ind, "HDL")
        self.assertEqual(st.apply_position_rules(entry_sig, {}), "ENTRY")
        pos = st.update_positions({}, entry_sig, "ENTRY")
        self.assertIn("HDL", pos)

        exit_df = make_ohlcv(flip_down=True)
        exit_ind = st.compute_supertrend(exit_df)
        exit_ind.loc[exit_ind.index[-2], "st_dir"] = 1
        exit_ind.loc[exit_ind.index[-1], "st_dir"] = -1
        exit_sig = st.evaluate_latest(exit_ind, "HDL")
        self.assertEqual(st.apply_position_rules(exit_sig, pos), "EXIT")
        pos2 = st.update_positions(pos, exit_sig, "EXIT")
        self.assertNotIn("HDL", pos2)

    def test_no_duplicate_entry(self):
        df = make_ohlcv(flip_up=True)
        ind = st.compute_supertrend(df)
        ind.loc[ind.index[-2], "st_dir"] = -1
        ind.loc[ind.index[-1], "st_dir"] = 1
        sig = st.evaluate_latest(ind, "NABIL")
        action = st.apply_position_rules(sig, {"NABIL": {"entry_price": 100}})
        self.assertEqual(action, "NONE")

    def test_no_exit_without_position(self):
        df = make_ohlcv(flip_down=True)
        ind = st.compute_supertrend(df)
        ind.loc[ind.index[-2], "st_dir"] = 1
        ind.loc[ind.index[-1], "st_dir"] = -1
        sig = st.evaluate_latest(ind, "NABIL")
        self.assertEqual(st.apply_position_rules(sig, {}), "NONE")

    def test_scan_map_emits_entry_and_exit(self):
        entry_df = make_ohlcv(flip_up=True)
        entry_ind = st.compute_supertrend(entry_df)
        entry_ind.loc[entry_ind.index[-2], "st_dir"] = -1
        entry_ind.loc[entry_ind.index[-1], "st_dir"] = 1

        entries, exits, pos, skipped, uptrend = st.scan_ohlcv_map(
            {"HDL": entry_ind}, {}
        )
        self.assertEqual(skipped, 0)
        self.assertEqual(len(entries), 1)
        self.assertIn("HDL", pos)
        self.assertTrue(any(s.symbol == "HDL" for s in uptrend))

        exit_df = make_ohlcv(flip_down=True)
        exit_ind = st.compute_supertrend(exit_df)
        exit_ind.loc[exit_ind.index[-2], "st_dir"] = 1
        exit_ind.loc[exit_ind.index[-1], "st_dir"] = -1
        entries2, exits2, pos2, _, _ = st.scan_ohlcv_map({"HDL": exit_ind}, pos)
        self.assertEqual(len(entries2), 0)
        self.assertEqual(len(exits2), 1)
        self.assertNotIn("HDL", pos2)


class TestTelegram(unittest.TestCase):
    def test_message_headers(self):
        df = make_ohlcv(flip_up=True)
        ind = st.compute_supertrend(df)
        ind.loc[ind.index[-2], "st_dir"] = -1
        ind.loc[ind.index[-1], "st_dir"] = 1
        entry = st.evaluate_latest(ind, "NABIL")

        exit_df = make_ohlcv(flip_down=True)
        exit_ind = st.compute_supertrend(exit_df)
        exit_ind.loc[exit_ind.index[-2], "st_dir"] = 1
        exit_ind.loc[exit_ind.index[-1], "st_dir"] = -1
        exit_sig = st.evaluate_latest(exit_ind, "JBBL")

        msg = st.format_telegram(
            [entry],
            [(exit_sig, {"entry_price": 400.0, "entry_date": "2026-07-01"})],
            scanned=2,
            as_of="2026-08-21",
        )
        self.assertIn("ENTRY FOUND", msg)
        self.assertIn("EXIT FOUND", msg)
        self.assertIn("NABIL", msg)
        self.assertIn("JBBL", msg)
        self.assertIn("SuperTrend", msg)

    def test_empty_scan_mentions_entry_and_exit(self):
        msg = st.format_telegram([], [], scanned=10, as_of="2026-08-21", skipped=2)
        self.assertIn("ENTRY FOUND:* none", msg)
        self.assertIn("EXIT FOUND:* none", msg)


class TestLiveBar(unittest.TestCase):
    def _live_row(self, ltp=110.0, date="2026-08-24"):
        return {
            "symbol": "NABIL",
            "openPrice": 105.0,
            "highPrice": 112.0,
            "lowPrice": 104.0,
            "lastTradedPrice": ltp,
            "totalTradeQuantity": 4200,
            "lastUpdatedDateTime": f"{date} 12:30:00.000000",
        }

    def test_appends_new_bar(self):
        df = make_ohlcv(n=60)
        before = len(df)
        out = st.append_live_bar(df, self._live_row())
        self.assertEqual(len(out), before + 1)
        last = out.iloc[-1]
        self.assertEqual(float(last["close"]), 110.0)
        self.assertEqual(float(last["high"]), 112.0)
        self.assertEqual(float(last["volume"]), 4200.0)
        self.assertEqual(str(last["businessDate"])[:10], "2026-08-24")

    def test_skips_if_date_already_present(self):
        df = make_ohlcv(n=60)
        last_date = str(df["businessDate"].iloc[-1])[:10]
        out = st.append_live_bar(df, self._live_row(date=last_date))
        self.assertEqual(len(out), len(df))

    def test_skips_if_no_trades(self):
        df = make_ohlcv(n=60)
        out = st.append_live_bar(df, self._live_row(ltp=0))
        self.assertEqual(len(out), len(df))

    def test_live_bar_feeds_supertrend(self):
        df = make_ohlcv(n=80)
        out = st.append_live_bar(df, self._live_row())
        ind = st.compute_supertrend(out)
        self.assertFalse(pd.isna(ind["supertrend"].iloc[-1]))


class TestPersistence(unittest.TestCase):
    def test_load_save_positions(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "pos.json")
            old = st.POSITIONS_FILE
            st.POSITIONS_FILE = path
            try:
                import candle_scanner as cs

                cs.save_json(path, {"API": {"entry_price": 1}})
                loaded = st.load_positions()
                self.assertEqual(loaded["API"]["entry_price"], 1)
            finally:
                st.POSITIONS_FILE = old


if __name__ == "__main__":
    unittest.main()
