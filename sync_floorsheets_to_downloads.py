#!/usr/bin/env python3
"""Copy all repo floorsheet_*.csv files into the Downloads floorsheet folder.

Default destination:
  /Users/sanishtamang/Downloads/floorsheet

Override with:
  python sync_floorsheets_to_downloads.py --dest /path/to/folder
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def default_dest() -> Path:
    preferred = Path("/Users/sanishtamang/Downloads/floorsheet")
    if preferred.parent.exists() or preferred.exists():
        return preferred
    return Path.home() / "Downloads" / "floorsheet"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dest",
        type=Path,
        default=None,
        help="Destination folder (default: /Users/sanishtamang/Downloads/floorsheet)",
    )
    parser.add_argument(
        "--src",
        type=Path,
        default=Path(__file__).resolve().parent,
        help="Source folder containing floorsheet_*.csv (default: repo root)",
    )
    args = parser.parse_args()

    src: Path = args.src
    dest: Path = args.dest or default_dest()
    dest.mkdir(parents=True, exist_ok=True)

    files = sorted(src.glob("floorsheet_20*.csv"))
    if Path(src / "floorsheet.csv").exists():
        files.append(src / "floorsheet.csv")

    if not files:
        print(f"No floorsheet CSV files found in {src}")
        return 1

    for path in files:
        target = dest / path.name
        shutil.copy2(path, target)
        print(f"copied {path.name} → {target}")

    print(f"\nDone. {len(files)} file(s) saved to {dest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
