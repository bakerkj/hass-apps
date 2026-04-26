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

    All threads share a single ``compress_db`` SQLite connection — Frigate is
    ATTACHed to it (read-write) as ``frigate`` for the life of the daemon, so
    cross-DB queries AND writes (workers updating ``segment_size``,
    housekeeping retrying failed segment updates) go through the same
    connection.  No separate ``frigate_rw`` connection needed.

    ``compress_db_lock`` serialises access: SQLite serialises individual
    statements internally, but the lock is needed to group multi-statement
    transactions (probe-loop batch insert + commit, worker SELECT/UPDATE
    pairs) so they don't interleave with other threads' commits in
    sqlite3's autocommit-per-statement default.  Frigate writes piggy-back
    on this same lock — the per-compression frigate UPDATE is a single
    statement so contention is microseconds.

    A single shared connection means a single shared page cache — the
    writer-invalidates-reader-cache amplification that motivated multiple
    per-thread caches no longer applies, and total cache memory drops from
    ~7 × 64–128 MB across per-thread connections to one 128 MB pool.
    """

    cfg: Config
    compress_db: sqlite3.Connection
    compress_db_lock: threading.Lock = field(default_factory=threading.Lock)
    rate_limiter: RateLimiter = field(default_factory=RateLimiter)
