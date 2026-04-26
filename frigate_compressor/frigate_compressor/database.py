# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Compress DB schema, status constants, connection helpers, ``_record``."""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

from .config import Config
from .util import log

# Compress DB status values — use these constants everywhere instead of
# bare string literals so a typo becomes a NameError, not a silent data bug.
STATUS_OK = "ok"
STATUS_ERROR = "error"
STATUS_SEGMENT_UPDATE_FAILED = "segment_update_failed"

SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    recording_id    TEXT    PRIMARY KEY,
    camera          TEXT    NOT NULL,
    path            TEXT    NOT NULL,
    recording_type  TEXT,
    -- start_time is denormalised from Frigate's recordings so eligibility
    -- and backlog queries can range-scan on (camera, start_time) without
    -- joining against Frigate's DB per-row.  Populated by the probe loop
    -- and backfilled for any pre-existing rows on startup.
    start_time      REAL,

    -- Original probe (from camera, filled by probe loop)
    codec           TEXT,
    width           INTEGER,
    height          INTEGER,
    fps             REAL,
    bitrate         INTEGER,
    duration_sec    REAL,
    file_size       INTEGER,
    scanned_at      TEXT,

    -- Tier 1 compression
    t1_encoder      TEXT,
    t1_width        INTEGER,
    t1_height       INTEGER,
    t1_fps          REAL,
    t1_bitrate      INTEGER,
    t1_file_size    INTEGER,
    t1_encode_sec   REAL,
    t1_status       TEXT,
    t1_error_msg    TEXT,
    t1_compressed_at TEXT,

    -- Tier 2 compression
    t2_encoder      TEXT,
    t2_width        INTEGER,
    t2_height       INTEGER,
    t2_fps          REAL,
    t2_bitrate      INTEGER,
    t2_file_size    INTEGER,
    t2_encode_sec   REAL,
    t2_status       TEXT,
    t2_error_msg    TEXT,
    t2_compressed_at TEXT
);

-- Backlog-existence + eligibility partial indexes are defined below in
-- ``START_TIME_INDEXES`` (they all reference start_time, which was
-- added in a later schema upgrade).  An earlier iteration also kept
-- (camera, recording_id) variants for the MQTT backlog queries and a
-- full ``(camera)`` index — those have been dropped since the (camera,
-- start_time) ``_pending_age`` partials serve every query that filters
-- by camera AND mean every files-row INSERT/UPDATE has fewer indexes
-- to maintain.
"""


# Indexes that reference ``start_time``.  Kept separate from SCHEMA so we
# can run the ALTER TABLE migration that adds the column (see
# ``_migrate_add_columns``) before creating these indexes — otherwise the
# CREATE INDEX statements would fail on upgrade with "no such column".
#
# Eligibility indexes: same pending filters as the backlog ones above,
# but keyed on (camera, start_time) so the planner can do a range scan
# for "camera X, start_time < cutoff" within the pending-only subset.
# Benchmarked at ~4000× faster than the previous recordings-driven
# LEFT JOIN.  Requires start_time to be populated on files — see
# ``backfill_files_start_time``.
START_TIME_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_files_t1_pending_age ON files(camera, start_time)
  WHERE t1_status IS NULL
     OR t1_status NOT IN ('ok', 'segment_update_failed');

CREATE INDEX IF NOT EXISTS idx_files_t2_pending_age ON files(camera, start_time)
  WHERE t1_status IN ('ok', 'segment_update_failed')
    AND (t2_status IS NULL
         OR t2_status NOT IN ('ok', 'segment_update_failed'));

-- Partial indexes for the weekly housekeeping pass.  Each scans rows in a
-- rare status; without these, housekeeping has to SCAN the full files
-- table (sub-second per query but bursty I/O).  Maintenance cost is near
-- zero — almost no row update transitions in/out of these states.
-- Deferred (alongside the start_time indexes) because the t*_compressed_at
-- columns may need an ALTER TABLE on upgrade from a vintage schema.
CREATE INDEX IF NOT EXISTS idx_files_seg_retry ON files(recording_id)
  WHERE t1_status = 'segment_update_failed'
     OR t2_status = 'segment_update_failed';

CREATE INDEX IF NOT EXISTS idx_files_t1_error ON files(t1_compressed_at)
  WHERE t1_status = 'error';

CREATE INDEX IF NOT EXISTS idx_files_t2_error ON files(t2_compressed_at)
  WHERE t2_status = 'error';
"""


def _migrate_add_columns(conn: sqlite3.Connection) -> None:
    """Add columns that newer schemas introduced via ``ALTER TABLE``.

    SQLite's ``CREATE TABLE IF NOT EXISTS`` is a no-op when the table
    already exists — it does NOT add columns mentioned in the new schema.
    This function bridges that gap for upgrades from older versions.

    Idempotent: only ALTERs columns that are actually missing.
    """
    cols = {row[1] for row in conn.execute("PRAGMA table_info(files)").fetchall()}
    if "start_time" not in cols:
        conn.execute("ALTER TABLE files ADD COLUMN start_time REAL")
        conn.commit()
        log("INFO", "Migrated: added files.start_time column")
    # The compressed_at columns are referenced by partial indexes for the
    # housekeeping recent-errors view.  Vintage schemas predating these
    # columns must get them via ALTER before the indexes can be created.
    for col in ("t1_compressed_at", "t2_compressed_at"):
        if col not in cols:
            conn.execute(f"ALTER TABLE files ADD COLUMN {col} TEXT")
            conn.commit()
            log("INFO", f"Migrated: added files.{col} column")


# Indexes from earlier development iterations that turned out to be
# unhelpful — drop them on startup so leftover installs don't carry
# their ~50 MB write overhead forever.  Safe to remove this list once
# we're confident nobody has them anymore.
_OBSOLETE_INDEXES = (
    "idx_files_t1_t2_status",
    "idx_files_t2_status",
    # ``idx_files_t{1,2}_pending`` were (camera, recording_id) partial
    # indexes used by an earlier shape of the MQTT backlog queries.  The
    # current code drives backlog checks from the (camera, start_time)
    # ``_pending_age`` variants, which serve those queries equally well
    # AND the eligibility query.  Keeping the redundant pair around just
    # doubled the partial-index maintenance cost on every status change.
    "idx_files_t1_pending",
    "idx_files_t2_pending",
    # ``idx_files_camera`` was a full index on (camera).  No code path
    # uses it: every WHERE camera=? query in the daemon either takes the
    # PK-by-recording_id path or has an ``INDEXED BY idx_files_*_pending_age``
    # hint that selects a partial index.  Each row INSERT was paying for
    # one extra B-tree write to maintain it.
    "idx_files_camera",
)


def _drop_obsolete_indexes(conn: sqlite3.Connection) -> None:
    """Drop indexes from previous versions that are no longer useful."""
    existing = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()
    }
    for name in _OBSOLETE_INDEXES:
        if name in existing:
            conn.execute(f"DROP INDEX IF EXISTS {name}")
            log("INFO", f"Dropped obsolete index {name}")
    conn.commit()


def vacuum_compress_db(conn: sqlite3.Connection) -> None:
    """Run VACUUM on the compress DB to reclaim free pages and defragment.

    Called by ``app.py`` at the very end of the upgrade path — after both
    schema migration (in ``open_compress_db``) and the row-rewriting
    backfill of ``files.start_time``.  Running it once at the end is
    cheaper than running it twice and reclaims fragmentation from both.

    VACUUM is expensive (full DB rewrite, exclusive lock) so we never
    run it on every startup; SQLite reuses freed pages for new inserts,
    so the file reaches a natural steady state without periodic
    vacuuming.

    VACUUM cannot run inside a transaction, so we commit any pending
    work and disable the sqlite3 module's implicit-transaction wrapping
    around the call.
    """
    free = conn.execute("PRAGMA freelist_count").fetchone()[0]
    total = conn.execute("PRAGMA page_count").fetchone()[0]
    log("INFO", f"Running VACUUM after upgrade ({free}/{total} pages free)")
    conn.commit()
    old_isolation = conn.isolation_level
    conn.isolation_level = None
    try:
        conn.execute("VACUUM")
    finally:
        conn.isolation_level = old_isolation


# Materialised aggregate table + triggers.
#
# ``files_stats`` keeps a per-(camera, rtype) rollup of file counts and
# bytes in each tier, maintained transactionally by triggers on the
# ``files`` table.  The MQTT publisher reads this table directly (a
# handful of rows) instead of re-aggregating 800K+ rows every minute.
#
# Tier is derived from status:
#   tier 2 = t2_status in (ok, segment_update_failed)
#   tier 1 = t1_status in (ok, segment_update_failed) AND not tier 2
#   tier 0 = neither
#
# Bytes for a file's current tier come from the matching size column:
#   tier 0 → file_size   (original probe)
#   tier 1 → t1_file_size
#   tier 2 → t2_file_size
#
# Correctness contract: the three triggers together keep files_stats in
# sync with ``files``.  A full rebuild is a straightforward GROUP BY
# aggregation over ``files`` — see ``_backfill_files_stats`` and
# ``verify_files_stats``.  If a trigger bug ever causes drift, the
# audit hook detects it on startup and rebuilds from scratch.
FILES_STATS_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS files_stats (
    camera       TEXT    NOT NULL,
    rtype        TEXT    NOT NULL,
    files_count  INTEGER NOT NULL DEFAULT 0,
    tier0_bytes  INTEGER NOT NULL DEFAULT 0,
    tier1_bytes  INTEGER NOT NULL DEFAULT 0,
    tier2_bytes  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (camera, rtype)
);

CREATE TRIGGER IF NOT EXISTS files_stats_after_insert
AFTER INSERT ON files
FOR EACH ROW
BEGIN
    INSERT INTO files_stats
        (camera, rtype, files_count, tier0_bytes, tier1_bytes, tier2_bytes)
    VALUES (
        NEW.camera,
        COALESCE(NEW.recording_type, 'continuous'),
        1,
        CASE
            WHEN NEW.t2_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                THEN 0
            WHEN NEW.t1_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                THEN 0
            ELSE COALESCE(NEW.file_size, 0)
        END,
        CASE
            WHEN NEW.t2_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                THEN 0
            WHEN NEW.t1_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                THEN COALESCE(NEW.t1_file_size, 0)
            ELSE 0
        END,
        CASE
            WHEN NEW.t2_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                THEN COALESCE(NEW.t2_file_size, 0)
            ELSE 0
        END
    )
    ON CONFLICT(camera, rtype) DO UPDATE SET
        files_count = files_count + 1,
        tier0_bytes = tier0_bytes + excluded.tier0_bytes,
        tier1_bytes = tier1_bytes + excluded.tier1_bytes,
        tier2_bytes = tier2_bytes + excluded.tier2_bytes;
END;

CREATE TRIGGER IF NOT EXISTS files_stats_after_delete
AFTER DELETE ON files
FOR EACH ROW
BEGIN
    UPDATE files_stats SET
        files_count = files_count - 1,
        tier0_bytes = tier0_bytes - (
            CASE
                WHEN OLD.t2_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                    THEN 0
                WHEN OLD.t1_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                    THEN 0
                ELSE COALESCE(OLD.file_size, 0)
            END
        ),
        tier1_bytes = tier1_bytes - (
            CASE
                WHEN OLD.t2_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                    THEN 0
                WHEN OLD.t1_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                    THEN COALESCE(OLD.t1_file_size, 0)
                ELSE 0
            END
        ),
        tier2_bytes = tier2_bytes - (
            CASE
                WHEN OLD.t2_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                    THEN COALESCE(OLD.t2_file_size, 0)
                ELSE 0
            END
        )
    WHERE camera = OLD.camera
      AND rtype = COALESCE(OLD.recording_type, 'continuous');
END;

-- UPDATE uses the "subtract from OLD bucket, add to NEW bucket" pattern.
-- When OLD and NEW share the same (camera, rtype) bucket, counts are a
-- net zero and bytes adjust by the delta between OLD and NEW contributions.
-- When they differ (tier transition or rtype change) the row correctly
-- moves.
CREATE TRIGGER IF NOT EXISTS files_stats_after_update
AFTER UPDATE ON files
FOR EACH ROW
BEGIN
    UPDATE files_stats SET
        files_count = files_count - 1,
        tier0_bytes = tier0_bytes - (
            CASE
                WHEN OLD.t2_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                    THEN 0
                WHEN OLD.t1_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                    THEN 0
                ELSE COALESCE(OLD.file_size, 0)
            END
        ),
        tier1_bytes = tier1_bytes - (
            CASE
                WHEN OLD.t2_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                    THEN 0
                WHEN OLD.t1_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                    THEN COALESCE(OLD.t1_file_size, 0)
                ELSE 0
            END
        ),
        tier2_bytes = tier2_bytes - (
            CASE
                WHEN OLD.t2_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                    THEN COALESCE(OLD.t2_file_size, 0)
                ELSE 0
            END
        )
    WHERE camera = OLD.camera
      AND rtype = COALESCE(OLD.recording_type, 'continuous');

    INSERT INTO files_stats
        (camera, rtype, files_count, tier0_bytes, tier1_bytes, tier2_bytes)
    VALUES (
        NEW.camera,
        COALESCE(NEW.recording_type, 'continuous'),
        1,
        CASE
            WHEN NEW.t2_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                THEN 0
            WHEN NEW.t1_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                THEN 0
            ELSE COALESCE(NEW.file_size, 0)
        END,
        CASE
            WHEN NEW.t2_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                THEN 0
            WHEN NEW.t1_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                THEN COALESCE(NEW.t1_file_size, 0)
            ELSE 0
        END,
        CASE
            WHEN NEW.t2_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                THEN COALESCE(NEW.t2_file_size, 0)
            ELSE 0
        END
    )
    ON CONFLICT(camera, rtype) DO UPDATE SET
        files_count = files_count + 1,
        tier0_bytes = tier0_bytes + excluded.tier0_bytes,
        tier1_bytes = tier1_bytes + excluded.tier1_bytes,
        tier2_bytes = tier2_bytes + excluded.tier2_bytes;
END;
"""


# Reused in the backfill + verify queries below; keeping the CASE logic
# in one place avoids drift with the trigger bodies.
_FILES_STATS_SELECT = f"""
SELECT
    camera,
    COALESCE(recording_type, 'continuous') AS rtype,
    COUNT(*) AS files_count,
    SUM(CASE
            WHEN t2_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}') THEN 0
            WHEN t1_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}') THEN 0
            ELSE COALESCE(file_size, 0)
        END) AS tier0_bytes,
    SUM(CASE
            WHEN t2_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}') THEN 0
            WHEN t1_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                THEN COALESCE(t1_file_size, 0)
            ELSE 0
        END) AS tier1_bytes,
    SUM(CASE
            WHEN t2_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                THEN COALESCE(t2_file_size, 0)
            ELSE 0
        END) AS tier2_bytes
FROM files
GROUP BY camera, COALESCE(recording_type, 'continuous')
"""


def backfill_files_start_time(conn: sqlite3.Connection, cfg: Config) -> int:
    """Fill in ``files.start_time`` from Frigate's recordings for any row
    that's missing it.

    Needed after upgrading to the schema that carries start_time on
    ``files``.  Called once at startup — for fresh rows the probe loop
    writes start_time inline.  Returns the number of rows updated.
    """
    _attach_frigate(conn, cfg, "frigate_backfill")
    try:
        cur = conn.execute(
            "UPDATE files SET start_time = ("
            "  SELECT start_time FROM frigate_backfill.recordings r"
            "  WHERE r.id = files.recording_id"
            ") WHERE start_time IS NULL"
        )
        n = cur.rowcount
        conn.commit()
        return n
    finally:
        conn.execute("DETACH DATABASE frigate_backfill")


def _backfill_files_stats(conn: sqlite3.Connection) -> None:
    """One-shot populate ``files_stats`` from the current ``files`` rows.

    Called on first install, after a trigger-bug audit fails, or by
    tests.  Safe to re-run — fully replaces the table contents.
    """
    conn.executescript("DELETE FROM files_stats;")
    conn.execute(
        "INSERT INTO files_stats "
        "(camera, rtype, files_count, tier0_bytes, tier1_bytes, tier2_bytes) "
        + _FILES_STATS_SELECT
    )
    conn.commit()


_ZERO_BUCKET = (0, 0, 0, 0)


def verify_files_stats(conn: sqlite3.Connection) -> bool:
    """Compare ``files_stats`` against a fresh aggregation of ``files``.

    Returns True if they agree, False otherwise.  Intended as an
    occasional audit (e.g. at startup); logs a warning + returns False
    if drift is detected.  Call ``_backfill_files_stats`` to recover.

    All-zero rows in ``files_stats`` (a bucket that once had files but
    was drained by deletes or rtype changes) compare equal to "no such
    row" in the fresh aggregation — triggers never prune zeroed rows,
    and that's OK semantically since no query cares about them.
    """

    def _nonzero(rows):
        return {k: v for k, v in rows if v != _ZERO_BUCKET}

    expected = _nonzero(
        ((row[0], row[1]), (row[2], row[3], row[4], row[5]))
        for row in conn.execute(_FILES_STATS_SELECT)
    )
    observed = _nonzero(
        ((row[0], row[1]), (row[2], row[3], row[4], row[5]))
        for row in conn.execute(
            "SELECT camera, rtype, files_count, tier0_bytes, tier1_bytes, tier2_bytes"
            " FROM files_stats"
        )
    )
    if expected == observed:
        return True
    diff_keys = set(expected.keys()) ^ set(observed.keys())
    log(
        "WARNING",
        f"files_stats drift detected: {len(expected)} aggregated buckets, "
        f"{len(observed)} in table, {len(diff_keys)} keys differ",
    )
    return False


VIEWS = f"""
CREATE VIEW IF NOT EXISTS savings_by_camera AS
SELECT
    camera,
    COUNT(CASE WHEN t1_status = '{STATUS_OK}' THEN 1 END)                    AS t1_files,
    COUNT(CASE WHEN t2_status = '{STATUS_OK}' THEN 1 END)                    AS t2_files,
    SUM(CASE WHEN t1_status = '{STATUS_OK}' THEN file_size END)              AS t1_bytes_before,
    SUM(CASE WHEN t1_status = '{STATUS_OK}' THEN t1_file_size END)           AS t1_bytes_after,
    SUM(CASE WHEN t2_status = '{STATUS_OK}'
             THEN COALESCE(t1_file_size, file_size) END)                     AS t2_bytes_before,
    SUM(CASE WHEN t2_status = '{STATUS_OK}' THEN t2_file_size END)           AS t2_bytes_after
FROM files
WHERE file_size > 0
GROUP BY camera;

CREATE VIEW IF NOT EXISTS recent_errors AS
SELECT camera, path, recording_type,
       t1_compressed_at, t1_error_msg,
       t2_compressed_at, t2_error_msg
FROM files
WHERE (t1_status = '{STATUS_ERROR}' AND t1_compressed_at >= datetime('now', '-7 days'))
   OR (t2_status = '{STATUS_ERROR}' AND t2_compressed_at >= datetime('now', '-7 days'))
ORDER BY COALESCE(t2_compressed_at, t1_compressed_at) DESC;
"""


def _migrate_to_files_table(conn: sqlite3.Connection) -> None:
    """Migrate data from the old compressed_files/probed_files tables to files."""
    tables = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if "compressed_files" not in tables:
        return  # already migrated or fresh DB

    conn.executescript(
        """
        INSERT OR IGNORE INTO files
            (recording_id, camera, path, recording_type, file_size,
             t1_encoder, t1_file_size, t1_encode_sec, t1_status,
             t1_error_msg, t1_compressed_at)
        SELECT recording_id, camera, path, recording_type, size_before,
               encoder, size_after, duration_sec, status,
               error_msg, last_attempted_at
        FROM compressed_files
        WHERE tier = 1;

        INSERT OR IGNORE INTO files
            (recording_id, camera, path, recording_type, file_size,
             t2_encoder, t2_file_size, t2_encode_sec, t2_status,
             t2_error_msg, t2_compressed_at)
        SELECT recording_id, camera, path, recording_type, size_before,
               encoder, size_after, duration_sec, status,
               error_msg, last_attempted_at
        FROM compressed_files
        WHERE tier = 2;

        DROP TABLE IF EXISTS compressed_files;
        DROP TABLE IF EXISTS probed_files;
        DROP VIEW IF EXISTS savings_by_camera;
        DROP VIEW IF EXISTS recent_errors;
        """
    )
    conn.commit()
    log("INFO", "Migrated compressed_files → files table")


def open_compress_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(f"file:{path}", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    # synchronous=NORMAL is SQLite's recommended setting for WAL mode:
    # fsync only on checkpoint instead of every commit.  On power loss
    # we may lose the last couple of status updates, but the DB stays
    # intact — and our writes are idempotent re-compressions anyway, so
    # "lose last update → re-compress one file next startup" is the
    # worst case.
    conn.execute("PRAGMA synchronous=NORMAL")
    # cache_size=-131072 → up to 128 MB per connection.  Comfortably
    # fits the steady-state hot set (partial indexes + top of PK/camera
    # indexes + recent rows ≈ 45 MB) with ample headroom for DB growth.
    # Lazy — only allocated as pages get touched, so idle connections
    # cost nothing.  Applied on every long-lived rw connection (worker
    # threads, probe loop, housekeeping).
    conn.execute("PRAGMA cache_size=-131072")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.executescript(SCHEMA)
    _migrate_to_files_table(conn)
    # Add columns added in newer schemas via ALTER TABLE for upgrades from
    # earlier versions (CREATE TABLE IF NOT EXISTS won't add new columns
    # to an existing table).  Must run BEFORE indexes that reference the
    # new columns.  VACUUM (if needed) is run by ``app.py`` at the end of
    # the full upgrade — including ``backfill_files_start_time`` which
    # rewrites every row to populate the new column — so we don't run it
    # here.
    _migrate_add_columns(conn)
    _drop_obsolete_indexes(conn)
    # Indexes that reference newly-added columns — safe to create now.
    conn.executescript(START_TIME_INDEXES)
    # Views are created after migration so they reference the final table.
    conn.executescript(VIEWS)
    # Materialised aggregate table + triggers.  Triggers are installed
    # BEFORE backfill so the audit can compare current-state rebuilds
    # cleanly; backfill itself INSERTs into files_stats directly (not
    # via triggers on files), so the order is safe either way.
    conn.executescript(FILES_STATS_SCHEMA)
    # Backfill if empty (first install, or migration from pre-stats schema)
    # or if the table is out of sync with the raw files (audit failure
    # recovery).
    n_stats = conn.execute("SELECT COUNT(*) FROM files_stats").fetchone()[0]
    n_files = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    if n_stats == 0 and n_files > 0:
        log("INFO", f"files_stats is empty but {n_files} files present — backfilling")
        _backfill_files_stats(conn)
    elif n_files > 0 and not verify_files_stats(conn):
        log("WARNING", "files_stats audit failed — rebuilding")
        _backfill_files_stats(conn)
    return conn


# Columns we read from or write to in Frigate's recordings table.
_REQUIRED_FRIGATE_COLUMNS: frozenset[str] = frozenset(
    {"id", "camera", "path", "start_time", "motion", "objects", "segment_size"}
)


def check_frigate_schema(conn: sqlite3.Connection) -> None:
    """Verify that Frigate's recordings table has all expected columns.

    Raises RuntimeError with a descriptive message if the table is absent or
    any required column is missing.  Call this once at startup before entering
    the main loop so that schema drift is caught immediately rather than
    silently producing wrong results hours later.
    """
    rows = conn.execute("PRAGMA table_info(recordings)").fetchall()
    _validate_frigate_columns(rows)


def check_frigate_schema_attached(conn: sqlite3.Connection) -> None:
    """Same as ``check_frigate_schema`` but checks the attached ``frigate``
    schema on a compress.db connection — used at startup so we don't have
    to open a separate frigate connection just for the schema check.
    """
    rows = conn.execute("PRAGMA frigate.table_info(recordings)").fetchall()
    _validate_frigate_columns(rows)


def _validate_frigate_columns(rows) -> None:
    if not rows:
        raise RuntimeError(
            "Frigate DB does not contain a 'recordings' table. "
            "Is this the right database file?"
        )
    present = {row["name"] for row in rows}
    missing = _REQUIRED_FRIGATE_COLUMNS - present
    if missing:
        raise RuntimeError(
            f"Frigate DB schema drift detected — missing column(s): "
            f"{', '.join(sorted(missing))}. "
            f"Check whether Frigate was upgraded and review the column list in "
            f"_REQUIRED_FRIGATE_COLUMNS."
        )


def _recording_type(motion: int | None, objects: int | None) -> str:
    """Classify a recording by its motion/objects counts. Priority: object > motion > continuous."""
    if objects:
        return "object"
    if motion:
        return "motion"
    return "continuous"


def _record(
    conn: sqlite3.Connection,
    *,
    recording_id: str,
    camera: str,
    path: str,
    tier: int,
    recording_type: str,
    encoder: str,
    size_before: int | None,
    size_after: int | None,
    duration_sec: float | None,
    status: str,
    error_msg: str | None = None,
) -> None:
    now = time.strftime("%Y-%m-%dT%H:%M:%S")
    t = f"t{tier}"
    conn.execute(
        f"""
        INSERT INTO files
            (recording_id, camera, path, recording_type, file_size,
             {t}_encoder, {t}_file_size, {t}_encode_sec,
             {t}_compressed_at, {t}_status, {t}_error_msg)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(recording_id) DO UPDATE SET
            recording_type     = excluded.recording_type,
            file_size          = COALESCE(files.file_size, excluded.file_size),
            {t}_encoder        = excluded.{t}_encoder,
            {t}_file_size      = excluded.{t}_file_size,
            {t}_encode_sec     = excluded.{t}_encode_sec,
            {t}_compressed_at  = excluded.{t}_compressed_at,
            {t}_status         = excluded.{t}_status,
            {t}_error_msg      = excluded.{t}_error_msg
        """,
        (
            recording_id,
            camera,
            path,
            recording_type,
            size_before,
            encoder,
            size_after,
            duration_sec,
            now,
            status,
            error_msg,
        ),
    )
    conn.commit()


def _attach_frigate(conn: sqlite3.Connection, cfg: Config, alias: str) -> None:
    """ATTACH the Frigate DB to ``conn`` read-write under the given alias.

    Bumps the attached schema's cache to 64 MB — SQLite's page cache is
    *per-attached-file*, not per-connection, so the main-db cache_size we
    set elsewhere does NOT apply to Frigate here.  64 MB comfortably fits
    the recordings PK index top levels + the (camera, start_time) index,
    which is what eligibility joins, stats aggregates, and housekeeping
    prune all walk.
    """
    db_path = str(cfg.frigate_db).replace('"', "")
    conn.execute(f'ATTACH DATABASE "file:{db_path}" AS {alias}')
    conn.execute(f"PRAGMA {alias}.cache_size=-65536")
    conn.execute(f"PRAGMA {alias}.busy_timeout=10000")
