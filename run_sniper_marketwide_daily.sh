#!/bin/zsh
set -euo pipefail

# Runs market-wide sniper scan and sends BUY picks to Telegram.
cd "/Users/sanishtamang/NEPAPI"

PYTHON_BIN="/Library/Frameworks/Python.framework/Versions/3.14/bin/python3"
"$PYTHON_BIN" "sniper_scanner.py" --all

