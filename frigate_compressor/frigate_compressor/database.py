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

CREATE INDEX IF NOT EXISTS idx_files_camera ON files(camera);
"""

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
    conn.execute("PRAGMA busy_timeout=10000")
    conn.executescript(SCHEMA)
    _migrate_to_files_table(conn)
    # Views are created after migration so they reference the final table.
    conn.executescript(VIEWS)
    return conn


def open_frigate_db(path: Path) -> sqlite3.Connection:
    """Open Frigate's DB read-only (WAL-safe)."""
    conn = sqlite3.connect(
        f"file:{path}?mode=ro",
        uri=True,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def open_frigate_db_rw(path: Path) -> sqlite3.Connection:
    """Separate RW connection used only for segment_size updates."""
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
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


def _attach_frigate_ro(conn: sqlite3.Connection, cfg: Config, alias: str) -> None:
    """ATTACH the Frigate DB to ``conn`` read-only under the given alias.

    Centralizes the path escaping so any future change to the URI format
    happens in one place rather than four.
    """
    db_path = str(cfg.frigate_db).replace('"', "")
    conn.execute(f'ATTACH DATABASE "file:{db_path}?mode=ro" AS {alias}')
