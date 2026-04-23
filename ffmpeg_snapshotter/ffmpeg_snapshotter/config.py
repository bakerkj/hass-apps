# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Per-stream configuration dataclass."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class StreamCfg:
    name: str
    url: str
    interval_seconds: int
    output_dir: Path
    filename_format: str
    date_dir_format: str
    latest_name: str
    retain_count: int
    retain_days: int
    extra_input_args: str
    extra_output_args: str
