# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Shared test helpers for frigate_compressor tests."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import frigate_compressor as fc


def _make_options(tmp_path: Path, **overrides) -> Path:
    """Write a minimal options.json and return its path."""
    opts = {
        "encoder": "cpu",
        "max_parallel_jobs": 1,
        "housekeeping_interval_days": 7,
        "frigate_db": str(tmp_path / "frigate.db"),
        "recordings_dir": str(tmp_path / "recordings"),
        "compress_db": str(tmp_path / "compress.db"),
        "log_level": "DEBUG",
        "dry_run": False,
        "tier1": {
            "min_days": 7,
            "continuous": {
                "quality": 28,
                "scale_mode": "none",
                "scale_value": "",
                "fps_mode": "none",
                "fps_value": 1.0,
            },
            "motion": {
                "quality": 26,
                "scale_mode": "halve",
                "scale_value": "",
                "fps_mode": "none",
                "fps_value": 1.0,
            },
            "object": {
                "quality": 22,
                "scale_mode": "none",
                "scale_value": "",
                "fps_mode": "none",
                "fps_value": 1.0,
            },
        },
        "tier2": {
            "min_days": 30,
            "continuous": {
                "quality": 34,
                "scale_mode": "halve",
                "scale_value": "",
                "fps_mode": "cap",
                "fps_value": 4.0,
            },
            "motion": {
                "quality": 30,
                "scale_mode": "halve",
                "scale_value": "",
                "fps_mode": "cap",
                "fps_value": 8.0,
            },
            "object": {
                "quality": 26,
                "scale_mode": "halve",
                "scale_value": "",
                "fps_mode": "cap",
                "fps_value": 8.0,
            },
        },
        "camera_overrides": [],
    }
    opts.update(overrides)
    p = tmp_path / "options.json"
    p.write_text(json.dumps(opts))
    return p


def _make_config(tmp_path: Path, **overrides) -> fc.Config:
    return fc.load_config(str(_make_options(tmp_path, **overrides)))


def _open_compress_db(tmp_path: Path) -> sqlite3.Connection:
    return fc.open_compress_db(tmp_path / "compress.db")


def _make_frigate_db(path: Path) -> sqlite3.Connection:
    """Create a minimal Frigate-style SQLite DB at *path*."""
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE recordings (
            id           TEXT PRIMARY KEY,
            camera       TEXT NOT NULL,
            path         TEXT,
            start_time   REAL NOT NULL,
            end_time     REAL,
            motion       INTEGER,
            objects      INTEGER,
            segment_size REAL
        )
        """
    )
    conn.commit()
    return conn


def _insert_recording(conn, rid, camera, path, start_time, motion=None, objects=None):
    conn.execute(
        "INSERT INTO recordings (id, camera, path, start_time, motion, objects)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (rid, camera, path, start_time, motion, objects),
    )
    conn.commit()
