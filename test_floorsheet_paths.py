#!/usr/bin/env python3
"""Tests for floorsheet Downloads-folder copies."""

import os
import tempfile
import unittest
from pathlib import Path

import floorsheet_paths as fp


class TestFloorsheetDownloadDir(unittest.TestCase):
    def test_default_is_sanishtamang_downloads(self):
        old = os.environ.pop("FLOORSHEET_DIR", None)
        try:
            self.assertEqual(
                fp.floorsheet_download_dir(),
                Path("/Users/sanishtamang/Downloads/floorsheet"),
            )
        finally:
            if old is not None:
                os.environ["FLOORSHEET_DIR"] = old

    def test_env_override(self):
        with tempfile.TemporaryDirectory() as tmp:
            os.environ["FLOORSHEET_DIR"] = tmp
            try:
                self.assertEqual(fp.floorsheet_download_dir(), Path(tmp))
            finally:
                os.environ.pop("FLOORSHEET_DIR", None)

    def test_copy_writes_csv_into_override_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            src_dir = Path(tmp) / "src"
            dest_dir = Path(tmp) / "Downloads" / "floorsheet"
            src_dir.mkdir()
            src = src_dir / "floorsheet_2026-08-17.csv"
            src.write_text("contractId,businessDate\n1,2026-08-17\n")
            os.environ["FLOORSHEET_DIR"] = str(dest_dir)
            try:
                saved = fp.copy_floorsheet_to_downloads([src], log=lambda *_: None)
            finally:
                os.environ.pop("FLOORSHEET_DIR", None)
            dest = dest_dir / "floorsheet_2026-08-17.csv"
            self.assertTrue(dest.exists())
            self.assertIn(dest, saved)
            self.assertIn("2026-08-17", dest.read_text())

    def test_same_path_copy_does_not_raise(self):
        with tempfile.TemporaryDirectory() as tmp:
            dest_dir = Path(tmp) / "floorsheet"
            dest_dir.mkdir()
            src = dest_dir / "floorsheet_2026-08-17.csv"
            src.write_text("ok")
            os.environ["FLOORSHEET_DIR"] = str(dest_dir)
            try:
                saved = fp.copy_floorsheet_to_downloads([src], log=lambda *_: None)
            finally:
                os.environ.pop("FLOORSHEET_DIR", None)
            self.assertIn(src, saved)


if __name__ == "__main__":
    unittest.main()
