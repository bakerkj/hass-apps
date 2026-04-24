# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for compress DB schema, the _record upsert, migration, and the _fmt utility."""

from __future__ import annotations

import sqlite3

import pytest

import frigate_compressor as fc

from fc_helpers import _open_compress_db


# ═══════════════════════════════════════════════════════════════════════════════
# DB schema
# ═══════════════════════════════════════════════════════════════════════════════


def test_open_compress_db_creates_table(tmp_path):
    conn = _open_compress_db(tmp_path)
    tables = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    assert "files" in tables
    conn.close()


def test_open_compress_db_creates_views(tmp_path):
    conn = _open_compress_db(tmp_path)
    views = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='view'"
        ).fetchall()
    }
    assert "savings_by_camera" in views
    assert "recent_errors" in views
    conn.close()


def test_open_compress_db_idempotent(tmp_path):
    conn1 = _open_compress_db(tmp_path)
    conn1.close()
    conn2 = _open_compress_db(tmp_path)
    conn2.close()


def test_schema_has_tier_columns(tmp_path):
    conn = _open_compress_db(tmp_path)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(files)").fetchall()}
    assert "recording_type" in cols
    assert "t1_status" in cols
    assert "t2_status" in cols
    assert "codec" in cols
    assert "scanned_at" in cols
    conn.close()


def test_open_frigate_db_readonly_enforced(tmp_path):
    frigate_db = tmp_path / "frigate.db"
    setup = sqlite3.connect(str(frigate_db))
    setup.execute("CREATE TABLE recordings (id TEXT PRIMARY KEY)")
    setup.commit()
    setup.close()

    ro = fc.open_frigate_db(frigate_db)
    with pytest.raises(sqlite3.OperationalError):
        ro.execute("INSERT INTO recordings (id) VALUES ('x')")
    ro.close()


# ═══════════════════════════════════════════════════════════════════════════════
# check_frigate_schema
# ═══════════════════════════════════════════════════════════════════════════════


def _conn_with_schema(tmp_path, ddl: str) -> sqlite3.Connection:
    """Open an in-memory-ish SQLite connection with the given DDL applied."""
    conn = sqlite3.connect(str(tmp_path / "test.db"))
    conn.row_factory = sqlite3.Row
    conn.executescript(ddl)
    return conn


def test_check_frigate_schema_passes_with_all_columns(tmp_path):
    conn = _conn_with_schema(
        tmp_path,
        "CREATE TABLE recordings ("
        "  id TEXT, camera TEXT, path TEXT, start_time REAL,"
        "  motion INTEGER, objects INTEGER, segment_size REAL"
        ");",
    )
    fc.check_frigate_schema(conn)  # must not raise
    conn.close()


def test_check_frigate_schema_passes_with_extra_columns(tmp_path):
    """Extra columns Frigate may add in the future must not cause a failure."""
    conn = _conn_with_schema(
        tmp_path,
        "CREATE TABLE recordings ("
        "  id TEXT, camera TEXT, path TEXT, start_time REAL,"
        "  motion INTEGER, objects INTEGER, segment_size REAL,"
        "  some_new_column TEXT"
        ");",
    )
    fc.check_frigate_schema(conn)  # must not raise
    conn.close()


def test_check_frigate_schema_raises_when_table_absent(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "empty.db"))
    conn.row_factory = sqlite3.Row
    with pytest.raises(RuntimeError, match="does not contain a 'recordings' table"):
        fc.check_frigate_schema(conn)
    conn.close()


def test_check_frigate_schema_raises_on_missing_column(tmp_path):
    conn = _conn_with_schema(
        tmp_path,
        # segment_size deliberately omitted
        "CREATE TABLE recordings ("
        "  id TEXT, camera TEXT, path TEXT, start_time REAL,"
        "  motion INTEGER, objects INTEGER"
        ");",
    )
    with pytest.raises(RuntimeError, match="segment_size"):
        fc.check_frigate_schema(conn)
    conn.close()


def test_check_frigate_schema_raises_on_multiple_missing_columns(tmp_path):
    conn = _conn_with_schema(
        tmp_path,
        "CREATE TABLE recordings (id TEXT);",
    )
    with pytest.raises(RuntimeError, match="schema drift"):
        fc.check_frigate_schema(conn)
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# _record
# ═══════════════════════════════════════════════════════════════════════════════


def test_record_inserts_tier1_row(tmp_path):
    conn = _open_compress_db(tmp_path)
    fc._record(
        conn,
        recording_id="abc123",
        camera="front",
        path="/media/front/a.mp4",
        tier=1,
        recording_type="motion",
        encoder="cpu",
        size_before=1000,
        size_after=500,
        duration_sec=1.5,
        status=fc.STATUS_OK,
    )
    row = conn.execute("SELECT * FROM files WHERE recording_id='abc123'").fetchone()
    assert row is not None
    assert row["t1_status"] == fc.STATUS_OK
    assert row["recording_type"] == "motion"
    assert row["file_size"] == 1000
    assert row["t1_file_size"] == 500
    assert row["t2_status"] is None
    conn.close()


def test_record_inserts_tier2_row(tmp_path):
    conn = _open_compress_db(tmp_path)
    fc._record(
        conn,
        recording_id="abc123",
        camera="front",
        path="/media/front/a.mp4",
        tier=2,
        recording_type="continuous",
        encoder="qsv",
        size_before=500,
        size_after=200,
        duration_sec=1.0,
        status=fc.STATUS_OK,
    )
    row = conn.execute("SELECT * FROM files WHERE recording_id='abc123'").fetchone()
    assert row is not None
    assert row["t2_status"] == fc.STATUS_OK
    assert row["t2_file_size"] == 200
    assert row["t1_status"] is None
    conn.close()


def test_record_tier1_then_tier2_preserves_both(tmp_path):
    conn = _open_compress_db(tmp_path)
    # Tier 1
    fc._record(
        conn,
        recording_id="abc123",
        camera="front",
        path="/media/a.mp4",
        tier=1,
        recording_type="motion",
        encoder="cpu",
        size_before=1000,
        size_after=500,
        duration_sec=1.5,
        status=fc.STATUS_OK,
    )
    # Tier 2
    fc._record(
        conn,
        recording_id="abc123",
        camera="front",
        path="/media/a.mp4",
        tier=2,
        recording_type="motion",
        encoder="cpu",
        size_before=500,
        size_after=200,
        duration_sec=1.0,
        status=fc.STATUS_OK,
    )
    rows = conn.execute("SELECT * FROM files WHERE recording_id='abc123'").fetchall()
    assert len(rows) == 1
    row = rows[0]
    assert row["file_size"] == 1000  # original size preserved
    assert row["t1_status"] == fc.STATUS_OK
    assert row["t1_file_size"] == 500
    assert row["t2_status"] == fc.STATUS_OK
    assert row["t2_file_size"] == 200
    conn.close()


def test_record_upserts_same_tier_on_retry(tmp_path):
    conn = _open_compress_db(tmp_path)
    fc._record(
        conn,
        recording_id="abc123",
        camera="front",
        path="/media/a.mp4",
        tier=1,
        recording_type="motion",
        encoder="cpu",
        size_before=1000,
        size_after=None,
        duration_sec=None,
        status=fc.STATUS_ERROR,
        error_msg="timeout",
    )
    fc._record(
        conn,
        recording_id="abc123",
        camera="front",
        path="/media/a.mp4",
        tier=1,
        recording_type="motion",
        encoder="cpu",
        size_before=1000,
        size_after=400,
        duration_sec=2.0,
        status=fc.STATUS_OK,
    )
    rows = conn.execute("SELECT * FROM files WHERE recording_id='abc123'").fetchall()
    assert len(rows) == 1
    assert rows[0]["t1_status"] == fc.STATUS_OK
    conn.close()


def test_record_error_with_message(tmp_path):
    conn = _open_compress_db(tmp_path)
    fc._record(
        conn,
        recording_id="xyz",
        camera="back",
        path="/media/b.mp4",
        tier=2,
        recording_type="continuous",
        encoder="qsv",
        size_before=2000,
        size_after=None,
        duration_sec=None,
        status=fc.STATUS_ERROR,
        error_msg="file missing",
    )
    row = conn.execute("SELECT * FROM files WHERE recording_id='xyz'").fetchone()
    assert row["t2_error_msg"] == "file missing"
    assert row["recording_type"] == "continuous"
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# Migration
# ═══════════════════════════════════════════════════════════════════════════════


def test_migration_from_old_schema(tmp_path):
    """Create old compressed_files with data, open DB, verify migration."""
    db_path = tmp_path / "compress.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE compressed_files (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            recording_id    TEXT    NOT NULL UNIQUE,
            camera          TEXT    NOT NULL,
            path            TEXT    NOT NULL,
            tier            INTEGER NOT NULL,
            recording_type  TEXT    NOT NULL,
            encoder         TEXT    NOT NULL,
            size_before     INTEGER,
            size_after      INTEGER,
            duration_sec    REAL,
            last_attempted_at TEXT  NOT NULL,
            status          TEXT    NOT NULL,
            error_msg       TEXT
        );
        INSERT INTO compressed_files
            (recording_id, camera, path, tier, recording_type, encoder,
             size_before, size_after, duration_sec, last_attempted_at, status)
        VALUES ('r1', 'cam1', '/a.mp4', 1, 'motion', 'cpu',
                1000, 500, 1.5, '2026-01-01T00:00:00', 'ok');
        INSERT INTO compressed_files
            (recording_id, camera, path, tier, recording_type, encoder,
             size_before, size_after, duration_sec, last_attempted_at, status)
        VALUES ('r2', 'cam1', '/b.mp4', 2, 'continuous', 'qsv',
                2000, 800, 2.0, '2026-01-02T00:00:00', 'ok');
        """
    )
    conn.commit()
    conn.close()

    # Re-open through open_compress_db which triggers migration
    conn = fc.open_compress_db(db_path)

    # Old table should be gone
    tables = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    assert "compressed_files" not in tables
    assert "files" in tables

    # Data should be migrated
    r1 = conn.execute("SELECT * FROM files WHERE recording_id='r1'").fetchone()
    assert r1 is not None
    assert r1["file_size"] == 1000
    assert r1["t1_status"] == "ok"
    assert r1["t1_file_size"] == 500

    r2 = conn.execute("SELECT * FROM files WHERE recording_id='r2'").fetchone()
    assert r2 is not None
    assert r2["t2_status"] == "ok"
    assert r2["t2_file_size"] == 800

    conn.close()


def test_migration_idempotent(tmp_path):
    """Opening an already-migrated DB should not error."""
    conn1 = _open_compress_db(tmp_path)
    conn1.close()
    conn2 = _open_compress_db(tmp_path)
    conn2.close()


# ═══════════════════════════════════════════════════════════════════════════════
# _fmt
# ═══════════════════════════════════════════════════════════════════════════════


def test_fmt_bytes():
    assert fc._fmt(512) == "512.0B"


def test_fmt_kilobytes():
    assert fc._fmt(2048) == "2.0KB"


def test_fmt_megabytes():
    assert fc._fmt(1024 * 1024) == "1.0MB"


def test_fmt_gigabytes():
    assert fc._fmt(1024**3) == "1.0GB"


def test_fmt_none():
    assert fc._fmt(None) == "N/A"


def test_fmt_float():
    assert fc._fmt(1536.0) == "1.5KB"


# ═══════════════════════════════════════════════════════════════════════════════
# files_stats materialised aggregate + triggers
# ═══════════════════════════════════════════════════════════════════════════════


def _stats_rows(conn: sqlite3.Connection) -> dict:
    """Read files_stats into a dict keyed by (camera, rtype)."""
    return {
        (r[0], r[1]): {
            "files_count": r[2],
            "tier0_bytes": r[3],
            "tier1_bytes": r[4],
            "tier2_bytes": r[5],
        }
        for r in conn.execute(
            "SELECT camera, rtype, files_count, tier0_bytes, tier1_bytes, tier2_bytes"
            " FROM files_stats"
        )
    }


def _verify(conn: sqlite3.Connection) -> None:
    """Assert the trigger-maintained stats match a fresh aggregation."""
    assert fc.verify_files_stats(conn), "files_stats drifted from files"


def test_files_stats_insert_tier0(tmp_path):
    """INSERT of an unprobed-looking row bumps tier0_bytes + files_count."""
    conn = _open_compress_db(tmp_path)
    conn.execute(
        "INSERT INTO files (recording_id, camera, path, recording_type, file_size)"
        " VALUES ('r1', 'cam', '/p', 'motion', 1000)"
    )
    conn.commit()
    s = _stats_rows(conn)
    assert s[("cam", "motion")] == {
        "files_count": 1,
        "tier0_bytes": 1000,
        "tier1_bytes": 0,
        "tier2_bytes": 0,
    }
    _verify(conn)


def test_files_stats_default_rtype_is_continuous(tmp_path):
    """A row inserted with NULL recording_type is bucketed as continuous."""
    conn = _open_compress_db(tmp_path)
    conn.execute(
        "INSERT INTO files (recording_id, camera, path, file_size)"
        " VALUES ('r1', 'cam', '/p', 500)"
    )
    conn.commit()
    s = _stats_rows(conn)
    assert s[("cam", "continuous")]["files_count"] == 1
    assert s[("cam", "continuous")]["tier0_bytes"] == 500
    _verify(conn)


def test_files_stats_update_tier0_to_tier1(tmp_path):
    """Promoting a row to tier-1 moves bytes from tier0 to tier1."""
    conn = _open_compress_db(tmp_path)
    conn.execute(
        "INSERT INTO files (recording_id, camera, path, recording_type, file_size)"
        " VALUES ('r1', 'cam', '/p', 'continuous', 1000)"
    )
    conn.execute(
        "UPDATE files SET t1_status = 'ok', t1_file_size = 400"
        " WHERE recording_id = 'r1'"
    )
    conn.commit()
    s = _stats_rows(conn)
    assert s[("cam", "continuous")] == {
        "files_count": 1,
        "tier0_bytes": 0,
        "tier1_bytes": 400,
        "tier2_bytes": 0,
    }
    _verify(conn)


def test_files_stats_update_tier1_to_tier2(tmp_path):
    """Promoting tier-1 to tier-2 moves bytes from tier1 to tier2."""
    conn = _open_compress_db(tmp_path)
    conn.execute(
        "INSERT INTO files (recording_id, camera, path, recording_type, file_size,"
        " t1_status, t1_file_size)"
        " VALUES ('r1', 'cam', '/p', 'motion', 1000, 'ok', 400)"
    )
    conn.execute(
        "UPDATE files SET t2_status = 'ok', t2_file_size = 100"
        " WHERE recording_id = 'r1'"
    )
    conn.commit()
    s = _stats_rows(conn)
    assert s[("cam", "motion")] == {
        "files_count": 1,
        "tier0_bytes": 0,
        "tier1_bytes": 0,
        "tier2_bytes": 100,
    }
    _verify(conn)


def test_files_stats_update_segment_update_failed_is_compressed(tmp_path):
    """'segment_update_failed' is treated the same as 'ok' for bucketing."""
    conn = _open_compress_db(tmp_path)
    conn.execute(
        "INSERT INTO files (recording_id, camera, path, recording_type, file_size,"
        " t1_status, t1_file_size)"
        " VALUES ('r1', 'cam', '/p', 'continuous', 1000, 'segment_update_failed', 300)"
    )
    conn.commit()
    s = _stats_rows(conn)
    assert s[("cam", "continuous")]["tier1_bytes"] == 300
    assert s[("cam", "continuous")]["tier0_bytes"] == 0
    _verify(conn)


def test_files_stats_update_error_stays_tier0(tmp_path):
    """status='error' doesn't count as compressed — stays in tier0."""
    conn = _open_compress_db(tmp_path)
    conn.execute(
        "INSERT INTO files (recording_id, camera, path, recording_type, file_size)"
        " VALUES ('r1', 'cam', '/p', 'motion', 1000)"
    )
    conn.execute(
        "UPDATE files SET t1_status = 'error', t1_file_size = 0"
        " WHERE recording_id = 'r1'"
    )
    conn.commit()
    s = _stats_rows(conn)
    assert s[("cam", "motion")]["tier0_bytes"] == 1000
    assert s[("cam", "motion")]["tier1_bytes"] == 0
    _verify(conn)


def test_files_stats_update_rtype_rebuckets(tmp_path):
    """Changing recording_type moves all of the file's bytes to the new bucket."""
    conn = _open_compress_db(tmp_path)
    conn.execute(
        "INSERT INTO files (recording_id, camera, path, recording_type, file_size)"
        " VALUES ('r1', 'cam', '/p', 'continuous', 1000)"
    )
    conn.execute("UPDATE files SET recording_type = 'motion' WHERE recording_id = 'r1'")
    conn.commit()
    s = _stats_rows(conn)
    assert ("cam", "continuous") in s
    assert s[("cam", "continuous")]["files_count"] == 0
    assert s[("cam", "continuous")]["tier0_bytes"] == 0
    assert s[("cam", "motion")]["files_count"] == 1
    assert s[("cam", "motion")]["tier0_bytes"] == 1000
    _verify(conn)


def test_files_stats_delete(tmp_path):
    """Deleting a row decrements its bucket."""
    conn = _open_compress_db(tmp_path)
    conn.execute(
        "INSERT INTO files (recording_id, camera, path, recording_type, file_size,"
        " t1_status, t1_file_size)"
        " VALUES ('r1', 'cam', '/p', 'motion', 1000, 'ok', 300)"
    )
    conn.execute("DELETE FROM files WHERE recording_id = 'r1'")
    conn.commit()
    s = _stats_rows(conn)
    # Row stays in stats but counts drop to zero.
    if ("cam", "motion") in s:
        assert s[("cam", "motion")]["files_count"] == 0
        assert s[("cam", "motion")]["tier0_bytes"] == 0
        assert s[("cam", "motion")]["tier1_bytes"] == 0
    _verify(conn)


def test_files_stats_multiple_cameras_and_tiers(tmp_path):
    """End-to-end: mix of cameras, rtypes, and tier states agree with aggregation."""
    conn = _open_compress_db(tmp_path)
    conn.executescript(
        """
        INSERT INTO files (recording_id, camera, path, recording_type, file_size)
        VALUES
          ('a', 'front', '/a', 'continuous', 100),
          ('b', 'front', '/b', 'motion',     200),
          ('c', 'back',  '/c', 'object',     300);
        UPDATE files SET t1_status = 'ok', t1_file_size = 50 WHERE recording_id = 'b';
        UPDATE files SET t1_status = 'ok', t1_file_size = 100,
                         t2_status = 'ok', t2_file_size = 40
            WHERE recording_id = 'c';
        """
    )
    conn.commit()
    _verify(conn)
    s = _stats_rows(conn)
    # front/continuous: 100 @ tier0
    assert s[("front", "continuous")] == {
        "files_count": 1,
        "tier0_bytes": 100,
        "tier1_bytes": 0,
        "tier2_bytes": 0,
    }
    # front/motion: 50 @ tier1
    assert s[("front", "motion")]["tier1_bytes"] == 50
    # back/object: 40 @ tier2
    assert s[("back", "object")]["tier2_bytes"] == 40


def test_files_stats_backfill_from_existing_files(tmp_path):
    """If a DB pre-dates the triggers, reopening it populates files_stats."""
    # First open: install schema + triggers.  Insert rows with triggers active.
    conn = _open_compress_db(tmp_path)
    conn.execute(
        "INSERT INTO files (recording_id, camera, path, recording_type, file_size)"
        " VALUES ('r1', 'cam', '/p', 'motion', 700)"
    )
    conn.commit()
    # Simulate a DB that pre-dates the stats table by emptying it.
    conn.execute("DELETE FROM files_stats")
    conn.commit()
    conn.close()

    # Reopening should notice stats is empty and rebuild from files.
    conn = _open_compress_db(tmp_path)
    s = _stats_rows(conn)
    assert s[("cam", "motion")]["files_count"] == 1
    assert s[("cam", "motion")]["tier0_bytes"] == 700
    _verify(conn)


def test_files_stats_audit_rebuilds_on_drift(tmp_path):
    """If files_stats drifts out of sync, reopening detects it and rebuilds."""
    conn = _open_compress_db(tmp_path)
    conn.execute(
        "INSERT INTO files (recording_id, camera, path, recording_type, file_size)"
        " VALUES ('r1', 'cam', '/p', 'continuous', 500)"
    )
    conn.commit()
    # Manually corrupt files_stats to simulate trigger drift.
    conn.execute("UPDATE files_stats SET tier0_bytes = 9999 WHERE camera = 'cam'")
    conn.commit()
    conn.close()

    conn = _open_compress_db(tmp_path)
    s = _stats_rows(conn)
    assert s[("cam", "continuous")]["tier0_bytes"] == 500  # rebuilt, not 9999
    _verify(conn)
