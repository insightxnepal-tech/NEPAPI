#!/bin/bash
# Download NEPSE floorsheet Excel dashboard to your Mac Downloads/floorsheet folder
set -euo pipefail
DEST_DIR="/Users/sanishtamang/Downloads/floorsheet"
DEST="$DEST_DIR/NEPSE_Floorsheet_Dashboard.xlsx"
BRANCH="cursor/fetch-missing-floorsheets-ba04"
REPO="insightxnepal-tech/NEPAPI"
URL="https://raw.githubusercontent.com/${REPO}/${BRANCH}/NEPSE_Floorsheet_Dashboard.xlsx"

mkdir -p "$DEST_DIR"
echo "Downloading Excel dashboard …"
curl -fL "$URL" -o "$DEST"
echo "Saved → $DEST ($(du -h "$DEST" | cut -f1))"
open "$DEST_DIR"
open "$DEST"
