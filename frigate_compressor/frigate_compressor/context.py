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
    ATTACHed to it read-only as ``frigate`` for the life of the daemon, so
    cross-DB queries (eligibility, probe loop, stats, housekeeping) don't
    need to ATTACH/DETACH per call.  ``compress_db_lock`` serialises access:
    SQLite serialises individual statements internally, but the lock is
    needed to group multi-statement transactions (probe-loop batch insert
    + commit, worker SELECT/UPDATE pairs) so they don't interleave with
    other threads' commits in sqlite3's autocommit-per-statement default.

    A single shared connection means a single shared page cache — the
    writer-invalidates-reader-cache amplification that motivated multiple
    per-thread caches no longer applies, and total cache memory drops from
    ~7 × 64–128 MB across per-thread connections to one 128 MB pool.

    ``frigate_rw`` is the only direct rw connection to Frigate (used to
    update ``recordings.segment_size``); reads of Frigate go through the
    attached ``frigate`` schema on ``compress_db``.
    """

    cfg: Config
    compress_db: sqlite3.Connection
    frigate_rw: sqlite3.Connection
    compress_db_lock: threading.Lock = field(default_factory=threading.Lock)
    frigate_rw_lock: threading.Lock = field(default_factory=threading.Lock)
    rate_limiter: RateLimiter = field(default_factory=RateLimiter)
