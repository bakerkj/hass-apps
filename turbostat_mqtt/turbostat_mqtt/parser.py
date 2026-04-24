# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Streaming turbostat parser + subprocess helper."""

from __future__ import annotations

import re
import subprocess


def start_turbostat(interval_s: float) -> subprocess.Popen:
    cmd = [
        "turbostat",
        "--Summary",
        "--quiet",
        "--enable",
        "all",
        "--interval",
        str(interval_s),
    ]
    return subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        bufsize=1,
        universal_newlines=True,
    )


class TurbostatParser:
    def __init__(self) -> None:
        self.header: list[str] | None = None
        self.num_re = re.compile(r"^[-+]?\d+(?:\.\d+)?$")

    def reset(self) -> None:
        self.header = None

    def parse_line(self, raw_line: str) -> tuple[list[str], dict[str, str], str] | None:
        line = raw_line.rstrip("\n")
        if not line.strip():
            return None

        parts = re.split(r"\s+", line.strip())

        def is_number(s: str) -> bool:
            return self.num_re.match(s) is not None

        if self.header is None:
            if all((not is_number(p)) for p in parts):
                self.header = parts
            return None

        if all((not is_number(p)) for p in parts):
            self.header = parts
            return None

        if len(parts) != len(self.header):
            return None

        values = dict(zip(self.header, parts))
        return self.header, values, line
