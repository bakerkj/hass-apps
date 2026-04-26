# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""``CompressorContext`` — shared state passed to workers and service loops."""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass, field

from .config import Config
from .throttle import RateLimiter


@dataclass
class CompressorContext:
    """Shared, per-daemon state passed to every compression worker and loop.

    All threads share a single ``compress_db`` SQLite connection with
    Frigate ATTACHed read-write as ``frigate`` for the life of the daemon,
    so cross-DB reads and writes go through one connection (and one page
    cache).  ``compress_db_lock`` groups multi-statement transactions so
    they don't interleave with other threads' commits under sqlite3's
    autocommit-per-statement default.
    """

    cfg: Config
    compress_db: sqlite3.Connection
    compress_db_lock: threading.Lock = field(default_factory=threading.Lock)
    rate_limiter: RateLimiter = field(default_factory=RateLimiter)
