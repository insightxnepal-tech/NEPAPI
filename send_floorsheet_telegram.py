#!/usr/bin/env python3
"""
Send the latest NEPSE floorsheet CSV to Telegram.

Used by the daily floorsheet GitHub Action after fetch_today.py.
Requires TELEGRAM_TOKEN and TELEGRAM_CHAT_ID from the environment
(GitHub Actions secrets). There is no hardcoded bot token.

  python send_floorsheet_telegram.py
  python send_floorsheet_telegram.py --csv floorsheet_2026-08-14.csv
  python send_floorsheet_telegram.py --dry-run
  python send_floorsheet_telegram.py --check-auth
"""

from __future__ import annotations

import argparse
import csv
import json
import mimetypes
import os
import re
import sys
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Optional

DATA_DIR = os.getenv("DATA_DIR", ".")
DATED_CSV_RE = re.compile(r"^floorsheet_(\d{4}-\d{2}-\d{2})\.csv$")
TELEGRAM_DOC_MAX_BYTES = 50 * 1024 * 1024
SEND_TIMEOUT_SEC = 180


def get_token() -> str:
    return (os.getenv("TELEGRAM_TOKEN") or "").strip()


def get_chat_id() -> str:
    return (os.getenv("TELEGRAM_CHAT_ID") or "").strip()


def telegram_api(token: Optional[str] = None) -> str:
    token = (token or get_token()).strip()
    if not token:
        raise ValueError("TELEGRAM_TOKEN is not set")
    return f"https://api.telegram.org/bot{token}"


def find_latest_floorsheet(data_dir: str = DATA_DIR) -> Optional[Path]:
    """Return the newest floorsheet_YYYY-MM-DD.csv, ignoring generic copies."""
    latest_date = None
    latest_path = None
    for path in Path(data_dir).glob("floorsheet_*.csv"):
        if "dividend" in path.name:
            continue
        match = DATED_CSV_RE.match(path.name)
        if not match:
            continue
        file_date = match.group(1)
        if latest_date is None or file_date > latest_date:
            latest_date = file_date
            latest_path = path
    return latest_path


def summarize_csv(csv_path: Path) -> dict:
    """Compute a short summary from a floorsheet CSV."""
    trades = 0
    turnover = 0.0
    shares = 0.0
    symbols = set()
    business_dates = set()

    with open(csv_path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            trades += 1
            try:
                turnover += float(row.get("contractAmount") or 0)
            except (TypeError, ValueError):
                pass
            try:
                shares += float(row.get("contractQuantity") or 0)
            except (TypeError, ValueError):
                pass
            symbol = (row.get("stockSymbol") or "").strip()
            if symbol:
                symbols.add(symbol)
            bdate = (row.get("businessDate") or "").strip()
            if bdate:
                business_dates.add(bdate)

    return {
        "trades": trades,
        "turnover": turnover,
        "shares": shares,
        "stocks": len(symbols),
        "business_dates": sorted(business_dates),
        "label": Path(csv_path).stem.replace("floorsheet_", ""),
    }


def format_summary_message(summary: dict, filename: str) -> str:
    dates = summary["business_dates"]
    session = dates[0] if len(dates) == 1 else ", ".join(dates) or summary["label"]
    return (
        f"📊 *NEPSE Floorsheet — {session}*\n\n"
        f"• Trades: `{summary['trades']:,}`\n"
        f"• Turnover: `Rs {summary['turnover'] / 1e9:.2f}B`\n"
        f"• Shares: `{summary['shares']:,.0f}`\n"
        f"• Stocks: `{summary['stocks']}`\n\n"
        f"📎 `{filename}`"
    )


def format_telegram_http_error(exc: urllib.error.HTTPError) -> str:
    body = exc.read().decode("utf-8", errors="replace")
    if exc.code == 401:
        return (
            "❌ Telegram HTTP 401 Unauthorized. TELEGRAM_TOKEN is invalid or revoked.\n"
            "Set a current bot token from @BotFather as TELEGRAM_TOKEN. "
            "GitHub Actions must use secrets.TELEGRAM_TOKEN (do not hardcode a token)."
        )
    return f"❌ Telegram HTTP {exc.code}: {body}"


def _telegram_json(method: str, payload: Optional[dict] = None, timeout: int = 30) -> dict:
    url = f"{telegram_api()}/{method}"
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"} if data is not None else {},
        method="POST" if data is not None else "GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read())


def get_me() -> dict:
    return _telegram_json("getMe", payload=None)


def send_message(chat_id: str, text: str) -> dict:
    return _telegram_json(
        "sendMessage",
        {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
    )


def send_document(chat_id: str, filepath: Path, caption: str = "") -> dict:
    mime = mimetypes.guess_type(str(filepath))[0] or "text/csv"
    fname = filepath.name
    file_data = filepath.read_bytes()
    if len(file_data) > TELEGRAM_DOC_MAX_BYTES:
        raise ValueError(
            f"{fname} is {len(file_data):,} bytes; Telegram limit is 50MB"
        )

    boundary = uuid.uuid4().hex
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="chat_id"\r\n\r\n'
        f"{chat_id}\r\n"
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="caption"\r\n\r\n'
        f"{caption}\r\n"
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="document"; filename="{fname}"\r\n'
        f"Content-Type: {mime}\r\n\r\n"
    ).encode() + file_data + f"\r\n--{boundary}--\r\n".encode()

    req = urllib.request.Request(
        f"{telegram_api()}/sendDocument",
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    with urllib.request.urlopen(req, timeout=SEND_TIMEOUT_SEC) as response:
        return json.loads(response.read())


def require_credentials() -> Optional[str]:
    """Return an error message if Telegram credentials are missing."""
    if not get_token():
        return (
            "❌ TELEGRAM_TOKEN is not set.\n"
            "Export it or add GitHub Actions secret TELEGRAM_TOKEN."
        )
    if not get_chat_id():
        return (
            "❌ TELEGRAM_CHAT_ID is not set.\n"
            "Export it or add GitHub Actions secret TELEGRAM_CHAT_ID."
        )
    return None


def check_auth() -> int:
    err = require_credentials()
    if err:
        print(err)
        return 1
    try:
        resp = get_me()
    except urllib.error.HTTPError as exc:
        print(format_telegram_http_error(exc))
        return 1
    except Exception as exc:
        print(f"❌ Telegram getMe failed: {exc}")
        return 1
    if not resp.get("ok"):
        print(f"❌ Telegram getMe failed: {resp}")
        return 1
    username = (resp.get("result") or {}).get("username") or "unknown"
    print(f"Telegram auth ok (@{username})")
    return 0


def send_floorsheet(csv_path: Path, chat_id: Optional[str] = None, dry_run: bool = False) -> int:
    if not csv_path.exists():
        print(f"❌ Floorsheet not found: {csv_path}")
        return 1

    chat_id = (chat_id or get_chat_id()).strip()
    summary = summarize_csv(csv_path)
    message = format_summary_message(summary, csv_path.name)
    caption = f"NEPSE Floorsheet {summary['label']}"
    size_mb = csv_path.stat().st_size / (1024 * 1024)

    print(f"CSV: {csv_path} ({size_mb:.1f} MB, {summary['trades']:,} trades)")
    if dry_run:
        print("--- dry-run message ---")
        print(message)
        print(f"Would send document with caption: {caption}")
        return 0

    err = require_credentials()
    if err:
        print(err)
        return 1
    if not chat_id:
        print("❌ TELEGRAM_CHAT_ID is not set.")
        return 1

    try:
        auth_rc = check_auth()
        if auth_rc != 0:
            return auth_rc

        msg_resp = send_message(chat_id, message)
        if not msg_resp.get("ok"):
            print(f"❌ Telegram sendMessage failed: {msg_resp}")
            return 1
        print("Sent summary message.")

        doc_resp = send_document(chat_id, csv_path, caption=caption)
        if not doc_resp.get("ok"):
            print(f"❌ Telegram sendDocument failed: {doc_resp}")
            return 1
        print(f"Sent CSV to Telegram chat {chat_id}.")
        return 0
    except urllib.error.HTTPError as exc:
        print(format_telegram_http_error(exc))
        return 1
    except Exception as exc:
        print(f"❌ Failed to send floorsheet: {exc}")
        return 1


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send latest floorsheet CSV to Telegram")
    parser.add_argument("--csv", metavar="PATH", help="CSV file to send (default: latest dated floorsheet)")
    parser.add_argument("--data-dir", default=DATA_DIR, help="Directory to search for floorsheet_YYYY-MM-DD.csv")
    parser.add_argument("--dry-run", action="store_true", help="Print summary without calling Telegram")
    parser.add_argument("--check-auth", action="store_true", help="Validate TELEGRAM_TOKEN via getMe and exit")
    return parser.parse_args(argv)


def main(argv: Optional[list] = None) -> int:
    args = parse_args(argv)
    if args.check_auth:
        return check_auth()
    if args.csv:
        csv_path = Path(args.csv)
    else:
        csv_path = find_latest_floorsheet(args.data_dir)
        if csv_path is None:
            print(f"❌ No floorsheet_YYYY-MM-DD.csv found in {args.data_dir}")
            return 1
    return send_floorsheet(csv_path, dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
