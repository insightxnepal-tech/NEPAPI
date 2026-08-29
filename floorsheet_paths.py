#!/usr/bin/env python3
"""Local Downloads folder for NEPSE floorsheet CSV/XLSX copies."""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Iterable, List, Optional

# User's Mac Downloads folder. Override with FLOORSHEET_DIR when needed.
DEFAULT_DOWNLOAD_DIR = Path("/Users/sanishtamang/Downloads/floorsheet")


def floorsheet_download_dir() -> Path:
    env = os.getenv("FLOORSHEET_DIR", "").strip()
    if env:
        return Path(env).expanduser()
    return DEFAULT_DOWNLOAD_DIR


def candidate_download_dirs() -> List[Path]:
    """Preferred Downloads path, then ~/Downloads/floorsheet as a fallback."""
    dirs: List[Path] = []
    for path in (floorsheet_download_dir(), Path.home() / "Downloads" / "floorsheet"):
        resolved = path.expanduser()
        if resolved not in dirs:
            dirs.append(resolved)
    return dirs


def ensure_download_dir() -> Optional[Path]:
    """Create and return the first writable Downloads folder, else None."""
    for dest_dir in candidate_download_dirs():
        try:
            dest_dir.mkdir(parents=True, exist_ok=True)
            probe = dest_dir / ".floorsheet_write_test"
            probe.write_text("ok")
            probe.unlink()
            return dest_dir
        except OSError:
            continue
    return None


def copy_floorsheet_to_downloads(
    sources: Iterable[Path],
    log=print,
) -> List[Path]:
    """
    Copy floorsheet files into /Users/sanishtamang/Downloads/floorsheet
    (and ~/Downloads/floorsheet if that path is different and writable).

    Never raises: a missing Mac path on CI must not fail the fetch.
    """
    saved: List[Path] = []
    files = [Path(src) for src in sources if src and Path(src).exists()]
    if not files:
        return saved

    for dest_dir in candidate_download_dirs():
        try:
            dest_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            log(f"Downloads folder not writable ({dest_dir}): {exc}")
            continue
        for src in files:
            dest = dest_dir / src.name
            try:
                if dest.exists() and dest.resolve() == src.resolve():
                    saved.append(dest)
                    log(f"Downloads copy → {dest}")
                    continue
                shutil.copy2(src, dest)
                saved.append(dest)
                log(f"Downloads copy → {dest}")
            except OSError as exc:
                log(f"Could not copy {src.name} to {dest_dir}: {exc}")
    return saved
