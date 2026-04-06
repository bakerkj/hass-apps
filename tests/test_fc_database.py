# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for compress DB schema, the _record upsert, and the _fmt utility."""

from __future__ import annotations

import sqlite3
import threading

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
    assert "compressed_files" in tables
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


def test_schema_has_recording_type_column(tmp_path):
    conn = _open_compress_db(tmp_path)
    cols = {
        r[1] for r in conn.execute("PRAGMA table_info(compressed_files)").fetchall()
    }
    assert "recording_type" in cols
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


def test_record_inserts_row(tmp_path):
    conn = _open_compress_db(tmp_path)
    lock = threading.Lock()
    fc._record(
        conn,
        lock,
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
    row = conn.execute(
        "SELECT * FROM compressed_files WHERE recording_id='abc123'"
    ).fetchone()
    assert row is not None
    assert row["status"] == fc.STATUS_OK
    assert row["recording_type"] == "motion"
    assert row["size_before"] == 1000
    assert row["size_after"] == 500
    conn.close()


def test_record_upserts_on_conflict(tmp_path):
    conn = _open_compress_db(tmp_path)
    lock = threading.Lock()
    fc._record(
        conn,
        lock,
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
        lock,
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
    rows = conn.execute(
        "SELECT * FROM compressed_files WHERE recording_id='abc123'"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["status"] == fc.STATUS_OK
    conn.close()


def test_record_error_with_message(tmp_path):
    conn = _open_compress_db(tmp_path)
    lock = threading.Lock()
    fc._record(
        conn,
        lock,
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
    row = conn.execute(
        "SELECT * FROM compressed_files WHERE recording_id='xyz'"
    ).fetchone()
    assert row["error_msg"] == "file missing"
    assert row["recording_type"] == "continuous"
    conn.close()


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
