# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Small utilities shared across the turbostat_mqtt package."""

import re
import time


def log(level: str, msg: str, min_level: str = "INFO") -> None:
    order = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}
    if order.get(level, 20) < order.get(min_level, 20):
        return
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"{ts} [{level}] {msg}", flush=True)


def sanitize_key(k: str) -> str:
    k = k.strip()
    # cpuidle counters end in - or + to distinguish the two governor
    # misprediction directions from the base entry count (turbostat.8:
    # ``-`` = should have been shallower, ``+`` = should have been deeper).
    # Preserve the distinction as suffixes before the generic -/+ scrubbing
    # below folds them all into a single ``_``, which would otherwise
    # silently overwrite two of every three variants sharing the same key.
    if k.endswith("-"):
        k = k[:-1] + "_shallow"
    elif k.endswith("+"):
        k = k[:-1] + "_deep"
    k = k.replace("%", "_pct")
    k = k.replace("/", "_per_")
    k = k.replace("-", "_")
    k = re.sub(r"[^A-Za-z0-9_]+", "_", k)
    k = re.sub(r"_+", "_", k).strip("_")
    return k.lower()
