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
# Tier-2 only: encoded ahead of its activation date (sibling .t2 file on disk),
# waiting to be swapped in at the day-30 boundary. Set when tier2.source="direct".
STATUS_DIRECT = "direct"

SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    recording_id    TEXT    PRIMARY KEY,
    camera          TEXT    NOT NULL,
    path            TEXT    NOT NULL,
    recording_type  TEXT,
    -- start_time is denormalised from Frigate's recordings so eligibility
    -- and backlog queries can range-scan on (camera, start_time) without
    -- joining against Frigate's DB per-row.
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

-- Eligibility partials: keyed on (camera, start_time) so the planner can
-- range-scan "camera X, start_time < cutoff" within the pending-only
-- subset.  ~4000× faster than the recordings-driven LEFT JOIN it
-- replaced.
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
CREATE INDEX IF NOT EXISTS idx_files_seg_retry ON files(recording_id)
  WHERE t1_status = 'segment_update_failed'
     OR t2_status = 'segment_update_failed';

CREATE INDEX IF NOT EXISTS idx_files_t1_error ON files(t1_compressed_at)
  WHERE t1_status = 'error';

CREATE INDEX IF NOT EXISTS idx_files_t2_error ON files(t2_compressed_at)
  WHERE t2_status = 'error';
"""


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


def _tier_case(prefix: str, tier: int) -> str:
    """CASE expression that bucketises one ``files`` row's bytes into
    the given tier (0, 1, or 2).  ``prefix`` is the SQL row alias prefix
    — ``"NEW."`` / ``"OLD."`` in triggers, ``""`` in plain SELECTs.

    Single source of truth for the tier classification — trigger bodies
    and the rebuild SELECT funnel through here so they can't drift.
    """
    if tier == 0:
        # not yet compressed → original file_size
        return (
            f"CASE WHEN {prefix}t2_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}') THEN 0"
            f" WHEN {prefix}t1_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}') THEN 0"
            f" ELSE COALESCE({prefix}file_size, 0) END"
        )
    if tier == 1:
        # t1 done, t2 not done → t1_file_size
        return (
            f"CASE WHEN {prefix}t2_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}') THEN 0"
            f" WHEN {prefix}t1_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}') THEN COALESCE({prefix}t1_file_size, 0)"
            f" ELSE 0 END"
        )
    if tier == 2:
        # t2 done → t2_file_size
        return (
            f"CASE WHEN {prefix}t2_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}') THEN COALESCE({prefix}t2_file_size, 0)"
            f" ELSE 0 END"
        )
    raise ValueError(f"unknown tier: {tier}")


# Reused by the INSERT trigger and the UPDATE trigger's NEW half.
# ON CONFLICT adds the new row's tier-buckets to whatever already exists.
_INSERT_OR_BUMP_FROM_NEW = f"""
    INSERT INTO files_stats
        (camera, rtype, files_count, tier0_bytes, tier1_bytes, tier2_bytes)
    VALUES (
        NEW.camera, COALESCE(NEW.recording_type, 'continuous'), 1,
        {_tier_case("NEW.", 0)},
        {_tier_case("NEW.", 1)},
        {_tier_case("NEW.", 2)}
    )
    ON CONFLICT(camera, rtype) DO UPDATE SET
        files_count = files_count + 1,
        tier0_bytes = tier0_bytes + excluded.tier0_bytes,
        tier1_bytes = tier1_bytes + excluded.tier1_bytes,
        tier2_bytes = tier2_bytes + excluded.tier2_bytes;
"""


# Reused by the DELETE trigger and the UPDATE trigger's OLD half.
# Subtracts the deleted/updated row's tier-buckets from its bucket row.
_SUBTRACT_OLD = f"""
    UPDATE files_stats SET
        files_count = files_count - 1,
        tier0_bytes = tier0_bytes - ({_tier_case("OLD.", 0)}),
        tier1_bytes = tier1_bytes - ({_tier_case("OLD.", 1)}),
        tier2_bytes = tier2_bytes - ({_tier_case("OLD.", 2)})
    WHERE camera = OLD.camera
      AND rtype = COALESCE(OLD.recording_type, 'continuous');
"""


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
{_INSERT_OR_BUMP_FROM_NEW}
END;

CREATE TRIGGER IF NOT EXISTS files_stats_after_delete
AFTER DELETE ON files
FOR EACH ROW
BEGIN
{_SUBTRACT_OLD}
END;

-- UPDATE uses the "subtract from OLD bucket, add to NEW bucket" pattern.
-- When OLD and NEW share the same (camera, rtype) bucket, counts net to
-- zero and bytes adjust by the delta between OLD and NEW contributions.
-- When they differ (tier transition or rtype change) the row correctly
-- moves.
CREATE TRIGGER IF NOT EXISTS files_stats_after_update
AFTER UPDATE ON files
FOR EACH ROW
BEGIN
{_SUBTRACT_OLD}
{_INSERT_OR_BUMP_FROM_NEW}
END;
"""


_FILES_STATS_SELECT = f"""
SELECT
    camera,
    COALESCE(recording_type, 'continuous') AS rtype,
    COUNT(*) AS files_count,
    SUM({_tier_case("", 0)}) AS tier0_bytes,
    SUM({_tier_case("", 1)}) AS tier1_bytes,
    SUM({_tier_case("", 2)}) AS tier2_bytes
FROM files
GROUP BY camera, COALESCE(recording_type, 'continuous')
"""


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
    conn.execute("PRAGMA cache_size=-262144")  # up to 256 MB
    conn.execute("PRAGMA busy_timeout=10000")
    # Bound the per-index work that ``PRAGMA optimize`` (run from
    # housekeeping) does on this long-running connection — without a
    # cap, ANALYZE can full-scan the ``files`` table.  10k samples per
    # index is well above SQLite's 100–1000 recommended range, picked
    # because the partial indexes on ``files`` filter very skewed
    # distributions (per-camera, per-status) and benefit from a denser
    # sample; total cost is ~10k × index_count rows once per
    # housekeeping pass, still trivial.
    conn.execute("PRAGMA analysis_limit=10000")
    conn.executescript(SCHEMA)
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
    """ATTACH the Frigate DB to ``conn`` read-write under the given alias."""
    db_path = str(cfg.frigate_db).replace('"', "")
    conn.execute(f'ATTACH DATABASE "file:{db_path}" AS {alias}')
    conn.execute(f"PRAGMA {alias}.cache_size=-131072")  # up to 128 MB
    conn.execute(f"PRAGMA {alias}.busy_timeout=10000")
