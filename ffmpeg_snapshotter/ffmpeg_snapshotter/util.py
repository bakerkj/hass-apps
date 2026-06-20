# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Small utilities used across the snapshotter package."""

import os
import re
import time
from pathlib import Path


def log(level: str, msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} [{level}] {msg}", flush=True)


def redact_url(url: str) -> str:
    return re.sub(r"(?<=://)[^@]+@", "***:***@", url)


def ensure_media_path(output_dir: str) -> Path:
    p = Path(output_dir)
    if not str(p).startswith("/media/"):
        p = Path("/media") / p
    return p


def set_latest_symlink(target: Path, latest_path: Path) -> None:
    # Best-effort symlink update. If symlinks aren't supported on /media, we log once per attempt.
    try:
        tmp_link = latest_path.with_suffix(".jpg.tmp")
        if tmp_link.exists() or tmp_link.is_symlink():
            tmp_link.unlink()
        rel = os.path.relpath(str(target), str(latest_path.parent))
        tmp_link.symlink_to(rel)
        tmp_link.replace(latest_path)
    except OSError as e:
        log("WARNING", f"Failed to update symlink {latest_path} -> {target}: {e}")
