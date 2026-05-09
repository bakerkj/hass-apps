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
# waiting to be swapped in at tier2.min_days. Set when tier2.source="direct".
STATUS_DIRECT = "direct"
# Terminal failure: encode failed ``_MAX_ATTEMPTS`` times in a row.
# Excluded from eligibility forever — operator must reset to NULL via
# SQL to retry.  Distinguished from STATUS_ERROR so an in-flight
# transient failure doesn't get confused with a permanent one.
STATUS_GIVE_UP = "give_up"

# Retry policy.  ``_MAX_ATTEMPTS`` consecutive failures flips the row
# to STATUS_GIVE_UP.  Between failures, the row is excluded from
# eligibility until ``now >= t{tier}_next_retry_at``.  Backoff is
# exponential: delay = ``_BACKOFF_BASE_SEC * 2^(attempts-1)`` capped
# at ``_BACKOFF_MAX_SEC``.  Schedule for attempts 1..10:
#   1m, 2m, 4m, 8m, 16m, 32m, 1h04, 2h08, 4h16, 8h32 — total ≈ 17h.
_MAX_ATTEMPTS = 10
_BACKOFF_BASE_SEC = 60.0
_BACKOFF_MAX_SEC = 8.5 * 3600  # 8h30m

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
    -- Retry bookkeeping (consecutive failures + earliest next attempt).
    -- Reset to (0, NULL) on success.  See ``_record`` and the eligibility
    -- query for how these gate retry timing and the give_up cap.
    t1_attempts      INTEGER NOT NULL DEFAULT 0,
    t1_next_retry_at TEXT,

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
    t2_compressed_at TEXT,
    t2_attempts      INTEGER NOT NULL DEFAULT 0,
    t2_next_retry_at TEXT
);

-- Eligibility partials: keyed on (camera, start_time) so the planner can
-- range-scan "camera X, start_time < cutoff" within the pending-only
-- subset.  ~4000× faster than the recordings-driven LEFT JOIN it
-- replaced.  ``give_up`` excluded so terminally-failed rows don't
-- consume index space.
CREATE INDEX IF NOT EXISTS idx_files_t1_pending_age ON files(camera, start_time)
  WHERE t1_status IS NULL
     OR t1_status NOT IN ('ok', 'segment_update_failed', 'give_up');

CREATE INDEX IF NOT EXISTS idx_files_t2_pending_age ON files(camera, start_time)
  WHERE t1_status IN ('ok', 'segment_update_failed')
    AND (t2_status IS NULL
         OR t2_status NOT IN ('ok', 'segment_update_failed', 'give_up'));

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


def _pre_encoded_case(prefix: str) -> str:
    """CASE expression for a row's pre-encoded tier-2 bytes.

    A row with ``t2_status='direct'`` has a pre-encoded tier-2 ``.t2.mp4``
    sibling file on disk alongside its primary tier-1 file, awaiting the
    swap at ``tier2.min_days``.  ``t2_file_size`` is set on those rows
    (compress_direct fills it when the file is encoded), so we can sum
    it here for a per-camera "extra disk parked on tier-1 rows" counter.
    Same prefix convention as :func:`_tier_case`.
    """
    return (
        f"CASE WHEN {prefix}t2_status = '{STATUS_DIRECT}'"
        f" THEN COALESCE({prefix}t2_file_size, 0) ELSE 0 END"
    )


# Reused by the INSERT trigger and the UPDATE trigger's NEW half.
# ON CONFLICT adds the new row's tier-buckets to whatever already exists.
_INSERT_OR_BUMP_FROM_NEW = f"""
    INSERT INTO files_stats
        (camera, rtype, files_count,
         tier0_bytes, tier1_bytes, tier2_bytes, tier2_pre_encoded_bytes)
    VALUES (
        NEW.camera, COALESCE(NEW.recording_type, 'continuous'), 1,
        {_tier_case("NEW.", 0)},
        {_tier_case("NEW.", 1)},
        {_tier_case("NEW.", 2)},
        {_pre_encoded_case("NEW.")}
    )
    ON CONFLICT(camera, rtype) DO UPDATE SET
        files_count         = files_count + 1,
        tier0_bytes         = tier0_bytes + excluded.tier0_bytes,
        tier1_bytes         = tier1_bytes + excluded.tier1_bytes,
        tier2_bytes         = tier2_bytes + excluded.tier2_bytes,
        tier2_pre_encoded_bytes = tier2_pre_encoded_bytes + excluded.tier2_pre_encoded_bytes;
"""


# Reused by the DELETE trigger and the UPDATE trigger's OLD half.
# Subtracts the deleted/updated row's tier-buckets from its bucket row.
_SUBTRACT_OLD = f"""
    UPDATE files_stats SET
        files_count         = files_count - 1,
        tier0_bytes         = tier0_bytes - ({_tier_case("OLD.", 0)}),
        tier1_bytes         = tier1_bytes - ({_tier_case("OLD.", 1)}),
        tier2_bytes         = tier2_bytes - ({_tier_case("OLD.", 2)}),
        tier2_pre_encoded_bytes = tier2_pre_encoded_bytes - ({_pre_encoded_case("OLD.")})
    WHERE camera = OLD.camera
      AND rtype = COALESCE(OLD.recording_type, 'continuous');
"""


FILES_STATS_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS files_stats (
    camera               TEXT    NOT NULL,
    rtype                TEXT    NOT NULL,
    files_count          INTEGER NOT NULL DEFAULT 0,
    tier0_bytes          INTEGER NOT NULL DEFAULT 0,
    tier1_bytes          INTEGER NOT NULL DEFAULT 0,
    tier2_bytes          INTEGER NOT NULL DEFAULT 0,
    -- Bytes of pre-encoded tier-2 files (``.t2.mp4``) parked alongside
    -- tier-1 primaries — populated when ``t2_status='direct'``.
    -- Independent of tier1_bytes / tier2_bytes: a row contributes to
    -- tier1_bytes for its primary file AND tier2_pre_encoded_bytes for
    -- its pre-encoded tier-2 file simultaneously.
    tier2_pre_encoded_bytes  INTEGER NOT NULL DEFAULT 0,
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
-- WHEN clause skips the trigger when the UPDATE doesn't change any
-- column that affects ``files_stats`` — probe-loop UPDATEs touch
-- ``codec``/``bitrate``/``scanned_at``/``next_retry_at`` etc. which
-- don't move the row's tier-bucket, so firing the subtract/add shuffle
-- on every probe write was wasted I/O.  Bucket movement is driven
-- entirely by ``recording_type`` + the t1/t2 status pair + the
-- t1/t2 file_size pair (see ``_tier_case`` / ``_pre_encoded_case``).
CREATE TRIGGER IF NOT EXISTS files_stats_after_update
AFTER UPDATE ON files
FOR EACH ROW
WHEN OLD.recording_type IS NOT NEW.recording_type
  OR OLD.t1_status      IS NOT NEW.t1_status
  OR OLD.t2_status      IS NOT NEW.t2_status
  OR OLD.t1_file_size   IS NOT NEW.t1_file_size
  OR OLD.t2_file_size   IS NOT NEW.t2_file_size
  OR OLD.file_size      IS NOT NEW.file_size
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
    SUM({_tier_case("", 2)}) AS tier2_bytes,
    SUM({_pre_encoded_case("")}) AS tier2_pre_encoded_bytes
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
        "(camera, rtype, files_count,"
        " tier0_bytes, tier1_bytes, tier2_bytes, tier2_pre_encoded_bytes) "
        + _FILES_STATS_SELECT
    )
    conn.commit()


_ZERO_BUCKET = (0, 0, 0, 0, 0)


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
        ((row[0], row[1]), (row[2], row[3], row[4], row[5], row[6]))
        for row in conn.execute(_FILES_STATS_SELECT)
    )
    observed = _nonzero(
        ((row[0], row[1]), (row[2], row[3], row[4], row[5], row[6]))
        for row in conn.execute(
            "SELECT camera, rtype, files_count,"
            " tier0_bytes, tier1_bytes, tier2_bytes, tier2_pre_encoded_bytes"
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


_FILES_STATS_TRIGGERS = (
    "files_stats_after_insert",
    "files_stats_after_delete",
    "files_stats_after_update",
)


def _migrate_files(conn: sqlite3.Connection) -> None:
    """Bring an existing ``files`` schema up to the current version.

    Adds retry-bookkeeping columns (``t{1,2}_attempts`` and
    ``t{1,2}_next_retry_at``) if they're missing — SQLite's ``CREATE
    TABLE IF NOT EXISTS`` is a no-op when the table exists, so new
    columns require an explicit ``ALTER TABLE``.

    Drops the eligibility partial indexes unconditionally so the
    subsequent ``CREATE INDEX IF NOT EXISTS`` (in ``SCHEMA``) installs
    fresh indexes whose WHERE clauses exclude ``give_up`` rows.  ``IF
    NOT EXISTS`` would otherwise leave the old WHERE clause in place.

    Idempotent — safe to run on a fresh DB (table doesn't exist yet,
    nothing to migrate).
    """
    has_table = (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='files'"
        ).fetchone()
        is not None
    )
    if not has_table:
        return

    cols = {row["name"] for row in conn.execute("PRAGMA table_info(files)").fetchall()}
    for col_name, col_type in (
        ("t1_attempts", "INTEGER NOT NULL DEFAULT 0"),
        ("t1_next_retry_at", "TEXT"),
        ("t2_attempts", "INTEGER NOT NULL DEFAULT 0"),
        ("t2_next_retry_at", "TEXT"),
    ):
        if col_name not in cols:
            conn.execute(f"ALTER TABLE files ADD COLUMN {col_name} {col_type}")

    # WHERE clause changed: drop so SCHEMA's CREATE INDEX IF NOT EXISTS
    # builds the new one.
    conn.execute("DROP INDEX IF EXISTS idx_files_t1_pending_age")
    conn.execute("DROP INDEX IF EXISTS idx_files_t2_pending_age")
    conn.commit()


def _migrate_files_stats(conn: sqlite3.Connection) -> None:
    """Bring an existing ``files_stats`` schema up to the current version.

    Two migrations:

    * Add the ``tier2_pre_encoded_bytes`` column if it's missing.  This is
      a non-destructive ``ALTER TABLE`` — existing rows get the column
      default (0) and the post-install audit (``verify_files_stats``)
      then triggers a backfill that fills it from ``files``.

    * Drop the three trigger definitions unconditionally so the
      subsequent ``CREATE TRIGGER`` (in ``FILES_STATS_SCHEMA``) installs
      bodies that match the current column set.  ``CREATE TRIGGER IF
      NOT EXISTS`` would otherwise leave stale trigger bodies in place
      after an upgrade and they would silently never write the new
      column.

    Idempotent — safe to run on a fresh DB (table doesn't exist yet,
    nothing to migrate).
    """
    has_table = (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='files_stats'"
        ).fetchone()
        is not None
    )
    if has_table:
        cols = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(files_stats)").fetchall()
        }
        if "tier2_pre_encoded_bytes" not in cols:
            log("INFO", "Migrating files_stats: adding tier2_pre_encoded_bytes column")
            conn.execute(
                "ALTER TABLE files_stats"
                " ADD COLUMN tier2_pre_encoded_bytes INTEGER NOT NULL DEFAULT 0"
            )
    for trig in _FILES_STATS_TRIGGERS:
        conn.execute(f"DROP TRIGGER IF EXISTS {trig}")
    conn.commit()


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
    # Migrate ``files`` schema BEFORE running ``SCHEMA``: an existing
    # ``files`` table won't pick up the new retry-bookkeeping columns
    # via ``CREATE TABLE IF NOT EXISTS`` (it's a no-op when the table
    # exists).  Drop the eligibility partial indexes too — their WHERE
    # clauses changed to exclude ``give_up``, and ``CREATE INDEX IF NOT
    # EXISTS`` won't replace an existing index with a different
    # definition.  Both will be re-created by ``SCHEMA``.
    _migrate_files(conn)
    conn.executescript(SCHEMA)
    conn.executescript(VIEWS)
    # Migrate ``files_stats`` schema before re-installing triggers — on
    # an upgrade the old triggers reference the old column set and would
    # silently leave the new column at 0 forever.  Drop & recreate so
    # the trigger bodies are always in lockstep with the current schema.
    _migrate_files_stats(conn)
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
    commit: bool = True,
) -> None:
    """Record a tier compression result.

    Success paths (``status == STATUS_OK``) reset the retry counter and
    clear ``next_retry_at`` so a subsequent failure starts the backoff
    schedule from attempt 1.  Non-success paths leave both fields alone
    here — failure bookkeeping (increment + backoff) goes through
    ``_record_failure`` instead, which writes them in the same UPDATE
    that flips status to error / give_up.

    ``commit=False`` lets the caller bundle this write with other
    statements on the same connection so they all commit (or none
    do).  Caller is responsible for ``conn.commit()`` after the bundle.
    """
    now = time.strftime("%Y-%m-%dT%H:%M:%S")
    t = f"t{tier}"
    # ``segment_update_failed`` and ``direct`` both mean "encode worked"
    # — only ``error`` and ``give_up`` are real failures, and those
    # don't go through this path (they route via ``_record_failure``).
    is_success = status in (STATUS_OK, STATUS_DIRECT, STATUS_SEGMENT_UPDATE_FAILED)
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
            {", " + t + "_attempts = 0, " + t + "_next_retry_at = NULL" if is_success else ""}
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
    if commit:
        conn.commit()


def _record_failure(
    conn: sqlite3.Connection,
    *,
    recording_id: str,
    camera: str,
    path: str,
    tier: int,
    recording_type: str,
    encoder: str,
    size_after: int | None = None,
    duration_sec: float | None = None,
    error_msg: str | None = None,
    commit: bool = True,
) -> tuple[int, str]:
    """Atomically increment ``t{tier}_attempts`` and pick a status.

    Returns ``(new_attempts, status)`` where ``status`` is
    ``STATUS_GIVE_UP`` if the cap has been hit and ``STATUS_ERROR``
    otherwise.  Computes ``next_retry_at`` from the new attempt count
    (``_BACKOFF_BASE_SEC * 2^(attempts-1)`` capped at
    ``_BACKOFF_MAX_SEC``); ``next_retry_at`` is left NULL when the
    status is ``give_up`` since the row is no longer eligible for
    retry.

    ``size_after`` and ``duration_sec`` are stored when the caller has
    them (e.g. timeout failures know how long ffmpeg ran) — useful for
    post-mortem.  All bookkeeping happens in one UPDATE so concurrent
    encode failures on the same row (which shouldn't happen — workers
    don't share rows — but is defensive) can't lose increments.
    """
    now_epoch = time.time()
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(now_epoch))
    t = f"t{tier}"

    # Read current attempt count.  Row may not exist (first failure on
    # a never-recorded recording_id), in which case current_attempts=0.
    row = conn.execute(
        f"SELECT COALESCE({t}_attempts, 0) AS n FROM files WHERE recording_id = ?",
        (recording_id,),
    ).fetchone()
    current_attempts = row["n"] if row is not None else 0
    new_attempts = current_attempts + 1

    if new_attempts >= _MAX_ATTEMPTS:
        status = STATUS_GIVE_UP
        next_retry_iso: str | None = None
    else:
        status = STATUS_ERROR
        delay_sec = min(
            _BACKOFF_BASE_SEC * (2 ** (new_attempts - 1)),
            _BACKOFF_MAX_SEC,
        )
        next_retry_iso = time.strftime(
            "%Y-%m-%dT%H:%M:%S", time.localtime(now_epoch + delay_sec)
        )

    conn.execute(
        f"""
        INSERT INTO files
            (recording_id, camera, path, recording_type,
             {t}_encoder, {t}_file_size, {t}_encode_sec,
             {t}_compressed_at, {t}_status, {t}_error_msg,
             {t}_attempts, {t}_next_retry_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(recording_id) DO UPDATE SET
            recording_type     = excluded.recording_type,
            {t}_encoder        = excluded.{t}_encoder,
            {t}_file_size      = excluded.{t}_file_size,
            {t}_encode_sec     = excluded.{t}_encode_sec,
            {t}_compressed_at  = excluded.{t}_compressed_at,
            {t}_status         = excluded.{t}_status,
            {t}_error_msg      = excluded.{t}_error_msg,
            {t}_attempts       = excluded.{t}_attempts,
            {t}_next_retry_at  = excluded.{t}_next_retry_at
        """,
        (
            recording_id,
            camera,
            path,
            recording_type,
            encoder,
            size_after,
            duration_sec,
            now_iso,
            status,
            error_msg,
            new_attempts,
            next_retry_iso,
        ),
    )
    if commit:
        conn.commit()
    return new_attempts, status


def _attach_frigate(conn: sqlite3.Connection, cfg: Config, alias: str) -> None:
    """ATTACH the Frigate DB to ``conn`` read-write under the given alias.

    The database expression is bound as a SQL parameter so a path
    containing ``?`` (which would otherwise inject SQLite URI options
    like ``mode=ro`` and quietly change open semantics) is passed as a
    plain string — no URI parsing on this path.  ``alias`` is a
    constant ("frigate") chosen by the caller, never operator input,
    so it's safely interpolated.
    """
    conn.execute(
        f"ATTACH DATABASE ? AS {alias}",
        (str(cfg.frigate_db),),
    )
    conn.execute(f"PRAGMA {alias}.cache_size=-131072")  # up to 128 MB
    conn.execute(f"PRAGMA {alias}.busy_timeout=10000")
