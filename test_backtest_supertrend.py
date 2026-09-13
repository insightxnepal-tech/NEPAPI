#!/usr/bin/env python3
"""Unit tests for the SuperTrend backtester (no NEPSE network)."""

from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

import backtest_supertrend as bt
import supertrend_scanner as st


def make_wave_ohlcv(n: int = 300, period: int = 60) -> pd.DataFrame:
    """Big slow sine wave — guarantees several SuperTrend flips."""
    dates = pd.bdate_range("2024-01-01", periods=n)
    t = np.arange(n)
    close = 100 + 30 * np.sin(2 * np.pi * t / period)
    open_ = np.r_[close[0], close[:-1]]
    return pd.DataFrame(
        {
            "businessDate": dates,
            "open": open_,
            "high": np.maximum(open_, close) + 0.5,
            "low": np.minimum(open_, close) - 0.5,
            "close": close,
            "volume": np.full(n, 1000.0),
        }
    )


def make_trend_ohlcv(n: int = 200, trend: float = 0.5) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-01", periods=n)
    t = np.linspace(0, 1, n)
    close = 100 * (1 + trend * t)
    open_ = np.r_[close[0], close[:-1]]
    return pd.DataFrame(
        {
            "businessDate": dates,
            "open": open_,
            "high": np.maximum(open_, close) + 0.5,
            "low": np.minimum(open_, close) - 0.5,
            "close": close,
            "volume": np.full(n, 1000.0),
        }
    )


class TestBacktestFrame(unittest.TestCase):
    def test_wave_produces_closed_trades(self):
        df = make_wave_ohlcv()
        trades = bt.backtest_frame(df, "WAVE")
        self.assertGreater(len(trades), 1)
        closed = [t for t in trades if not t.open]
        self.assertGreater(len(closed), 0)
        for t in closed:
            self.assertIsNotNone(t.exit_date)
            self.assertGreater(t.holding_days, 0)
            expected = (t.exit_price - t.entry_price) / t.entry_price * 100
            self.assertAlmostEqual(t.return_pct, expected, places=1)

    def test_steady_uptrend_keeps_one_open_trade(self):
        df = make_trend_ohlcv(trend=0.6)
        trades = bt.backtest_frame(df, "UP")
        # A pure uptrend never flips bearish: at most one open trade, no closed.
        self.assertLessEqual(len(trades), 1)
        for t in trades:
            self.assertTrue(t.open)
            self.assertIsNone(t.exit_date)

    def test_short_history_returns_no_trades(self):
        df = make_trend_ohlcv(n=30)
        self.assertEqual(bt.backtest_frame(df, "SHORT"), [])

    def test_entry_exit_ordering(self):
        df = make_wave_ohlcv()
        trades = bt.backtest_frame(df, "WAVE")
        for t in trades:
            if t.exit_date:
                self.assertLess(t.entry_date, t.exit_date)


class TestStats(unittest.TestCase):
    def test_symbol_stats_fields(self):
        df = make_wave_ohlcv()
        ind = st.compute_supertrend(df)
        trades = bt.backtest_frame(ind, "WAVE")
        s = bt.symbol_stats(ind, trades)
        for key in (
            "trades",
            "closed",
            "win_rate",
            "strategy_return_pct",
            "buy_hold_return_pct",
        ):
            self.assertIn(key, s)
        self.assertEqual(s["trades"], len(trades))

    def test_aggregate_stats(self):
        df = make_wave_ohlcv()
        ind = st.compute_supertrend(df)
        trades = bt.backtest_frame(ind, "WAVE")
        per_symbol = {"WAVE": bt.symbol_stats(ind, trades)}
        agg = bt.aggregate_stats(trades, per_symbol)
        self.assertEqual(agg["symbols"], 1)
        self.assertEqual(agg["total_trades"], len(trades))
        closed = [t for t in trades if not t.open]
        self.assertEqual(agg["closed_trades"], len(closed))
        if closed:
            wins = [t for t in closed if t.return_pct > 0]
            self.assertAlmostEqual(
                agg["win_rate"], round(len(wins) / len(closed) * 100, 1)
            )

    def test_report_renders(self):
        df = make_wave_ohlcv()
        ind = st.compute_supertrend(df)
        trades = bt.backtest_frame(ind, "WAVE")
        per_symbol = {"WAVE": bt.symbol_stats(ind, trades)}
        result = {
            "generated_at": "2026-08-23T00:00:00Z",
            "atr_len": st.ATR_LEN,
            "multiplier": st.MULT,
            "calendar_days": 730,
            "skipped": 0,
            "aggregate": bt.aggregate_stats(trades, per_symbol),
            "per_symbol": per_symbol,
            "trades": [],
        }
        report = bt.format_report(result)
        self.assertIn("SuperTrend Backtest Report", report)
        self.assertIn("WAVE", report)
        self.assertIn("Win rate", report)


if __name__ == "__main__":
    unittest.main()
