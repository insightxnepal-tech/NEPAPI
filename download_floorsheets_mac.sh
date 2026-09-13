#!/bin/bash
# Run this on your Mac to download floorsheet CSVs into Downloads/floorsheet
set -euo pipefail
DEST="/Users/sanishtamang/Downloads/floorsheet"
TMP="$(mktemp -d /tmp/nepapi-floorsheet.XXXXXX)"
REPO="https://github.com/insightxnepal-tech/NEPAPI.git"
BRANCH="cursor/fetch-missing-floorsheets-ba04"

mkdir -p "$DEST"
echo "Cloning $BRANCH …"
git clone --depth 1 --branch "$BRANCH" "$REPO" "$TMP/repo"
echo "Copying CSV files → $DEST"
cp -f "$TMP/repo"/floorsheet_20*.csv "$DEST/"
cp -f "$TMP/repo"/floorsheet.csv "$DEST/" 2>/dev/null || true
COUNT=$(ls "$DEST"/floorsheet_20*.csv 2>/dev/null | wc -l | tr -d ' ')
rm -rf "$TMP"
echo "Done. $COUNT floorsheet CSV files saved to $DEST"
open "$DEST"
