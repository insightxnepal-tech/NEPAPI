#!/usr/bin/env python3
"""Unit tests for the daily floorsheet fetcher (no NEPSE network)."""

import asyncio
import os
import subprocess
import sys
import tempfile
import unittest

import fetch_today


class FakeNepse:
    """Stands in for AsyncNepse: replays a scripted sequence of pages/errors."""

    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = 0

    def setTLSVerification(self, verify):
        pass

    async def getFloorSheet(self, show_progress=False):
        outcome = self.outcomes[self.calls]
        self.calls += 1
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def row(business_date, contract_id="1"):
    return {"contractId": contract_id, "stockSymbol": "NABIL", "businessDate": business_date}


class SessionDateTests(unittest.TestCase):
    def test_uses_business_date_not_local_date(self):
        # A Friday session fetched on Saturday must still be named for Friday.
        rows = [row("2026-09-18"), row("2026-09-18", "2")]
        self.assertEqual(fetch_today.session_date(rows), "2026-09-18")

    def test_picks_latest_session_and_trims_timestamps(self):
        rows = [row("2026-09-16"), row("2026-09-17T14:59:59.990075")]
        self.assertEqual(fetch_today.session_date(rows), "2026-09-17")

    def test_falls_back_to_today_without_business_date(self):
        from datetime import date

        rows = [{"contractId": "1", "businessDate": None}]
        self.assertEqual(fetch_today.session_date(rows), date.today().strftime("%Y-%m-%d"))


class FetchRetryTests(unittest.TestCase):
    def _run(self, fake):
        original = fetch_today.AsyncNepse
        fetch_today.AsyncNepse = lambda: fake
        try:
            return asyncio.run(fetch_today.fetch_floorsheet())
        finally:
            fetch_today.AsyncNepse = original

    def test_retries_a_transient_failure(self):
        fake = FakeNepse([TimeoutError(""), [row("2026-09-17")]])
        self.assertEqual(self._run(fake), [row("2026-09-17")])
        self.assertEqual(fake.calls, 2)

    def test_raises_after_all_attempts_fail(self):
        fake = FakeNepse([TimeoutError("")] * fetch_today.FETCH_ATTEMPTS)
        with self.assertRaises(TimeoutError):
            self._run(fake)
        self.assertEqual(fake.calls, fetch_today.FETCH_ATTEMPTS)


class ExitCodeTests(unittest.TestCase):
    def test_failed_fetch_exits_nonzero_and_writes_nothing(self):
        """A silent exit 0 let the workflow commit nothing and still report success."""
        script = (
            "import fetch_today\n"
            "fetch_today.fetch_floorsheet = None\n"
            "async def boom():\n"
            "    raise TimeoutError('')\n"
            "fetch_today.fetch_floorsheet = boom\n"
            "import asyncio, sys\n"
            "try:\n"
            "    asyncio.run(fetch_today.main())\n"
            "except Exception as e:\n"
            "    print(f'Error: {type(e).__name__}: {e}')\n"
            "    sys.exit(1)\n"
        )
        with tempfile.TemporaryDirectory() as tmp:
            env = dict(os.environ, PYTHONPATH=os.path.dirname(os.path.abspath(__file__)))
            proc = subprocess.run(
                [sys.executable, "-c", script], cwd=tmp, env=env, capture_output=True, text=True
            )
            self.assertEqual(proc.returncode, 1, proc.stderr)
            self.assertIn("Error: TimeoutError", proc.stdout)
            self.assertEqual(os.listdir(tmp), [])


if __name__ == "__main__":
    unittest.main()
