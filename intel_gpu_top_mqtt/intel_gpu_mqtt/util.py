# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Utility helpers: JSON stream parsing, safe coercion, engine-field dig."""

import json
from typing import Any


def extract_latest_json_object(buf: str) -> tuple[dict | None, str]:
    """Parse the latest complete dict object from intel_gpu_top -J streaming output."""
    s = buf.lstrip()
    if s.startswith("["):
        s = s[1:]

    dec = json.JSONDecoder()
    i = 0
    last = None
    last_end = None

    def skip(j: int) -> int:
        while j < len(s) and s[j] in " \r\n\t,":
            j += 1
        return j

    i = skip(i)
    while i < len(s):
        try:
            obj, end = dec.raw_decode(s, i)
        except json.JSONDecodeError:
            break
        if isinstance(obj, dict):
            last = obj
            last_end = end
        i = skip(end)

    if last is None or last_end is None:
        return None, buf[-200_000:]

    remaining = s[last_end:]
    return last, remaining[-200_000:]


def safe_float(x: Any) -> float | None:
    if isinstance(x, (int, float)):
        return float(x)
    return None


def dig(d: dict[str, Any], path: list[str]) -> Any:
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
        if cur is None:
            return None
    return cur


def find_engine_field(
    raw: dict[str, Any], engine_name: str, field: str
) -> float | None:
    engines = raw.get("engines")
    if isinstance(engines, dict):
        for k, v in engines.items():
            if k.lower() == engine_name.lower() and isinstance(v, dict):
                return safe_float(v.get(field))
    return None
