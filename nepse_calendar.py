#!/usr/bin/env python3
"""NEPSE session calendar.

Trading week changed on 2026-04-06 from Sun–Thu to Mon–Fri (Sat+Sun closed).
Public holidays are NOT hardcoded — an empty or stale floorsheet is the
authoritative holiday signal.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

# NPT is UTC+05:45
NPT = timezone(timedelta(hours=5, minutes=45))
WEEKDAY_CHANGE = date(2026, 4, 6)


def npt_now() -> datetime:
    return datetime.now(NPT)


def npt_today() -> date:
    return npt_now().date()


def is_weekend_closed(d: date) -> bool:
    """True when NEPSE is closed for the weekly holiday."""
    wd = d.weekday()  # Mon=0 … Sun=6
    if d >= WEEKDAY_CHANGE:
        return wd >= 5  # Saturday, Sunday
    return wd in (4, 5)  # Friday, Saturday (legacy week)


def is_nepse_trading_day(d: date | None = None) -> bool:
    """Calendar check only (weekends). Holidays are detected from floorsheet data."""
    if d is None:
        d = npt_today()
    return not is_weekend_closed(d)


def session_close_npt() -> time:
    """Regular board close is 15:00 NPT; 17:00 is the post-market analysis slot."""
    return time(15, 0)


def is_after_close(now: datetime | None = None) -> bool:
    now = now or npt_now()
    return now.timetz().replace(tzinfo=None) >= session_close_npt()
