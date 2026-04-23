# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Retention/cleanup policies for snapshot directories."""

from __future__ import annotations

import subprocess
from pathlib import Path

from .util import log


def apply_retention_days(dir_path: Path, retain_days: int, latest_name: str) -> None:
    if retain_days <= 0:
        return
    if not dir_path.exists():
        return
    # Use find for efficiency (no Python directory walks needed)
    # -mtime +N matches strictly greater than N days. We want older than retain_days, so +retain_days.
    # Exclude the latest symlink by name.
    try:
        subprocess.run(
            [
                "find",
                str(dir_path),
                "-type",
                "f",
                "-name",
                "*.jpg",
                "!",
                "-name",
                latest_name,
                "-mtime",
                f"+{retain_days}",
                "-delete",
            ],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except OSError as e:
        log("WARNING", f"Retention (days) failed for {dir_path}: {e}")


def apply_retention_count(dir_path: Path, retain_count: int, latest_name: str) -> None:
    if retain_count <= 0:
        return
    if not dir_path.exists():
        return

    # Count retention requires listing/sorting. Do it only on the retention interval.
    files: list[tuple[float, Path]] = []
    try:
        for p in dir_path.iterdir():
            if not p.is_file():
                continue
            if p.suffix.lower() != ".jpg":
                continue
            if p.name == latest_name:
                continue
            try:
                files.append((p.stat().st_mtime, p))
            except FileNotFoundError:
                pass
        files.sort(key=lambda t: t[0], reverse=True)
        for _, p in files[retain_count:]:
            try:
                p.unlink(missing_ok=True)
            except OSError:
                pass
    except OSError as e:
        log("WARNING", f"Retention (count) failed for {dir_path}: {e}")
