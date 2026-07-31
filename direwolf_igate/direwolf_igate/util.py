# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Small utilities shared across the direwolf_igate package."""

import time


def log(level: str, msg: str, min_level: str = "INFO") -> None:
    order = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}
    if order.get(level, 20) < order.get(min_level, 20):
        return
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"{ts} [{level}] {msg}", flush=True)
