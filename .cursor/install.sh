#!/usr/bin/env bash
# Idempotent dependency setup for the NEPSE API Cloud Agent environment.
# Creates a project-local virtualenv (.venv) and installs Python dependencies.
set -euo pipefail

# Run from the repository root regardless of where the script is invoked.
cd "$(dirname "$0")/.."

# The default image ships python3.12 but not the venv/ensurepip module, so
# install it once if it is missing. This is a fast no-op on prebuilt snapshots.
if ! python3 -c "import ensurepip" >/dev/null 2>&1; then
  sudo apt-get update -y
  sudo apt-get install -y python3-venv
fi

# Create the virtualenv only if it does not already exist.
if [ ! -x ".venv/bin/python" ]; then
  python3 -m venv .venv
fi

.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt

echo "NEPSE API environment ready. Activate with: source .venv/bin/activate"
