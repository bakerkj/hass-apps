# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Streaming turbostat parser + subprocess helper."""

import asyncio
import re

from .metadata import COLUMN_RENAMES


async def start_turbostat(interval_s: float) -> asyncio.subprocess.Process:
    """Spawn turbostat with its stdout as an asyncio stream.

    Bytes, not text: the caller decodes with a replacement policy, so a stray
    non-UTF-8 byte cannot raise out of the read and kill the sampler.
    """
    return await asyncio.create_subprocess_exec(
        "turbostat",
        "--Summary",
        "--quiet",
        "--enable",
        "all",
        "--interval",
        str(interval_s),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )


class TurbostatParser:
    def __init__(self) -> None:
        self.header: list[str] | None = None
        # Verbatim turbostat header (pre-alias) kept so consumers needing to
        # zip names against the raw turbostat line can recover the original
        # column names without a reverse-map.
        self.original_header: list[str] | None = None
        # Per-position scale factor: COLUMN_RENAMES scale for columns whose
        # source name/unit drifted; 1.0 for everything else. Lives next to
        # the alias so we can't double-scale when the source emits the
        # historical name natively.
        self._scales: list[float] = []
        self.num_re = re.compile(r"^[-+]?\d+(?:\.\d+)?$")

    def reset(self) -> None:
        self.header = None
        self.original_header = None
        self._scales = []

    def _resolve_header(
        self, parts: list[str]
    ) -> tuple[list[str], list[str], list[float]]:
        canonical: list[str] = []
        original: list[str] = []
        scales: list[float] = []
        for p in parts:
            rename = COLUMN_RENAMES.get(p)
            original.append(p)
            if rename is None:
                canonical.append(p)
                scales.append(1.0)
            else:
                new_name, scale = rename
                canonical.append(new_name)
                scales.append(scale)
        return canonical, original, scales

    def parse_line(self, raw_line: str) -> tuple[list[str], dict[str, str], str] | None:
        line = raw_line.rstrip("\n")
        if not line.strip():
            return None

        parts = re.split(r"\s+", line.strip())

        def is_number(s: str) -> bool:
            return self.num_re.match(s) is not None

        if self.header is None:
            if all((not is_number(p)) for p in parts):
                self.header, self.original_header, self._scales = self._resolve_header(
                    parts
                )
            return None

        if all((not is_number(p)) for p in parts):
            self.header, self.original_header, self._scales = self._resolve_header(
                parts
            )
            return None

        if len(parts) != len(self.header):
            return None

        scaled: list[str] = []
        for i, p in enumerate(parts):
            scale = self._scales[i]
            if scale == 1.0:
                scaled.append(p)
                continue
            # round to 6 decimals to suppress binary-float noise from
            # multiplication (e.g. 50495 * 0.001 → 50.495000000000005)
            # before the value lands in MQTT JSON.
            try:
                scaled.append(str(round(float(p) * scale, 6)))
            except ValueError:
                scaled.append(p)

        values = dict(zip(self.header, scaled))
        return self.header, values, line
