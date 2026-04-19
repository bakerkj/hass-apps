# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Logging + small format helpers — leaf module, no project dependencies."""

from __future__ import annotations

import time
from pathlib import Path

# A message is printed when its rank >= the configured log level's rank.
# Higher rank = more severe: DEBUG(0) < INFO(1) < WARNING(2) < ERROR(3).
LOG_LEVEL_RANK = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3}
_log_level = "INFO"


def set_log_level(level: str) -> None:
    """Set the module-global log level.  Only ``main()`` should call this."""
    global _log_level
    _log_level = level


def log(level: str, msg: str) -> None:
    if LOG_LEVEL_RANK.get(level, 1) >= LOG_LEVEL_RANK.get(_log_level, 1):
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"{ts} [{level}] {msg}", flush=True)


def _display_path(filepath: Path) -> str:
    """Format a Frigate recording path as a compact timestamp.

    Frigate stores recordings as
    ``<recordings_dir>/YYYY-MM-DD/HH/<camera>/MM.SS.mp4``.
    Returns ``YYYYMMDDHHmmss`` (e.g. ``20260418130103``).  The camera is
    already shown as a log prefix.  Falls back to the bare filename if
    the path is shorter than expected.
    """
    parts = filepath.parts
    if len(parts) >= 4:
        date = parts[-4].replace("-", "")  # YYYY-MM-DD → YYYYMMDD
        hour = parts[-3]  # HH
        mm = filepath.stem.split(".")[0] if "." in filepath.stem else filepath.stem
        ss = filepath.stem.split(".")[1] if "." in filepath.stem else "00"
        return f"{date} {hour}:{mm}:{ss}"
    return filepath.name


def _fmt(n: int | float | None, width: int = 0) -> str:
    """Human-readable byte size string, optionally right-justified to *width*."""
    if n is None:
        s = "N/A"
    else:
        n = float(n)
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if n < 1024:
                s = f"{n:.1f}{unit}"
                break
            n /= 1024
        else:
            s = f"{n:.1f}PB"
    return s.rjust(width) if width else s
