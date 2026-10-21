#!/usr/bin/env python3
"""Calendar helpers and pick-quality gates for the 5pm NEPSE trigger."""

from datetime import date

import pandas as pd

from nepse_calendar import is_nepse_trading_day, is_weekend_closed
from postmarket_strategy import select_picks


def test_legacy_week_sun_thu():
    # 2026-04-02 Thursday open; Friday/Saturday closed under the old week.
    assert is_nepse_trading_day(date(2026, 4, 2)) is True
    assert is_weekend_closed(date(2026, 4, 3)) is True
    assert is_weekend_closed(date(2026, 4, 4)) is True
    assert is_nepse_trading_day(date(2026, 4, 5)) is True  # Sunday, legacy week


def test_new_week_mon_fri():
    assert is_weekend_closed(date(2026, 4, 11)) is True   # Saturday
    assert is_weekend_closed(date(2026, 4, 12)) is True   # Sunday
    assert is_nepse_trading_day(date(2026, 4, 13)) is True  # Monday
    assert is_nepse_trading_day(date(2026, 8, 13)) is True  # Thursday
    assert is_nepse_trading_day(date(2026, 8, 14)) is True  # Friday
    assert is_nepse_trading_day(date(2026, 8, 15)) is False  # Saturday


def test_select_picks_drops_sells_and_dumps():
    df = pd.DataFrame([
        {"stockSymbol": "AAA", "eligible": True, "sniper": 90, "composite": 80,
         "range_pos": 0.9, "turnover": 50e6, "sniper_signal": "SNIPER BUY", "buy_score": 5},
        {"stockSymbol": "BBB", "eligible": True, "sniper": 95, "composite": 40,
         "range_pos": 0.1, "turnover": 80e6, "sniper_signal": "SELL / EXIT", "buy_score": 1},
        {"stockSymbol": "CCC", "eligible": True, "sniper": 70, "composite": 60,
         "range_pos": 0.7, "turnover": 20e6, "sniper_signal": "NEAR BUY", "buy_score": 4},
    ])
    picks = select_picks(df, "sniper", n=5)
    assert list(picks["stockSymbol"]) == ["AAA", "CCC"]


if __name__ == "__main__":
    test_legacy_week_sun_thu()
    test_new_week_mon_fri()
    test_select_picks_drops_sells_and_dumps()
    print("calendar tests passed")
