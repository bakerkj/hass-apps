# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Shared test helpers for frigate_compressor tests."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import yaml

import frigate_compressor as fc


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


def _make_options(tmp_path: Path, **overrides) -> Path:
    """Write options.json + config.yaml and return the options.json path.

    Keyword arguments that start with ``yaml_`` are routed to the YAML
    config file:

    * ``yaml_defaults`` → ``defaults:`` block in config.yaml
    * ``yaml_cameras`` → ``cameras:`` block in config.yaml
      Defaults to ``{"cam": {}}`` (one camera with all defaults) so that
      tests which don't care about cameras still get a usable config.

    All other keyword arguments become top-level keys in options.json.
    """
    frigate_db = Path(overrides.pop("frigate_db", str(tmp_path / "frigate.db")))
    recordings_dir = Path(overrides.pop("recordings_dir", str(tmp_path / "recordings")))

    # Ensure a valid Frigate DB exists (not just a touched file — load_config
    # calls _discover_cameras which queries the recordings table).
    if not frigate_db.exists():
        db = _make_frigate_db(frigate_db)
        db.close()

    if not recordings_dir.exists():
        recordings_dir.mkdir(parents=True)

    # YAML config
    yaml_defaults = overrides.pop("yaml_defaults", None)
    yaml_cameras = overrides.pop("yaml_cameras", {"cam": {}})
    yaml_path = tmp_path / "config.yaml"
    yaml_cfg: dict = {}
    if yaml_defaults is not None:
        yaml_cfg["defaults"] = yaml_defaults
    if yaml_cameras is not None:
        yaml_cfg["cameras"] = yaml_cameras
    yaml_path.write_text(yaml.dump(yaml_cfg, default_flow_style=False))

    # Options JSON (HAOS settings only)
    opts: dict = {
        "encoder": "cpu",
        "max_parallel_jobs": 1,
        "housekeeping_interval_days": 7,
        "frigate_db": str(frigate_db),
        "recordings_dir": str(recordings_dir),
        "compress_db": str(tmp_path / "compress.db"),
        "config_path": str(yaml_path),
        "log_level": "DEBUG",
    }
    opts.update(overrides)
    p = tmp_path / "options.json"
    p.write_text(json.dumps(opts))
    return p


def _make_config(tmp_path: Path, **overrides) -> fc.Config:
    return fc.load_config(str(_make_options(tmp_path, **overrides)))


def _open_compress_db(tmp_path: Path) -> sqlite3.Connection:
    return fc.open_compress_db(tmp_path / "compress.db")
