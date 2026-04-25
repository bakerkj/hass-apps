# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""``CompressorContext`` — shared state passed to workers and service loops."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field

from .config import Config
from .throttle import RateLimiter


@dataclass
class CompressorContext:
    """Shared, per-daemon state passed to every compression worker.

    Each thread opens its own SQLite connection to the compress DB (WAL mode
    handles concurrency).  ``compress_db`` is only set in tests for convenience.
    """

    cfg: Config
    frigate_ro: sqlite3.Connection
    frigate_rw: sqlite3.Connection
    compress_db: sqlite3.Connection | None = None
    # Persistent read-only compress-db connection used by ``get_eligible_recordings``
    # on the main loop thread.  Opened once at daemon startup so the
    # per-iteration eligibility query reuses a warm page cache instead of
    # paying the schema read cost every 60 s.  ``None`` in tests —
    # eligibility then falls back to opening a transient connection.
    eligibility_ro: sqlite3.Connection | None = None
    rate_limiter: RateLimiter = field(default_factory=RateLimiter)
