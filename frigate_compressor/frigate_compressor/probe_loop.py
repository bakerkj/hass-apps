# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Background ffprobe loop — fills missing metadata for new Frigate recordings."""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

from .config import Config
from .context import CompressorContext
from .database import _attach_frigate_ro
from .ffmpeg import _probe
from .util import log

PROBE_SLEEP_SEC = 10.0

_PROBE_BATCH_SIZE = 5000


def _get_unprobed_recordings(cfg: Config, conn: sqlite3.Connection) -> list[dict]:
    """Return a batch of Frigate recordings not yet probed.

    Limited to ``_PROBE_BATCH_SIZE`` rows per call to keep memory bounded.
    """
    _attach_frigate_ro(conn, cfg, "frigate_probe")
    try:
        rows = conn.execute(
            """
            SELECT r.id, r.camera, r.path
            FROM   frigate_probe.recordings r
            LEFT JOIN files f ON f.recording_id = r.id
            WHERE  f.recording_id IS NULL
               OR  f.scanned_at IS NULL
            ORDER BY r.start_time ASC
            LIMIT ?
            """,
            (_PROBE_BATCH_SIZE,),
        ).fetchall()
    finally:
        conn.execute("DETACH DATABASE frigate_probe")

    return [
        {"recording_id": row["id"], "camera": row["camera"], "path": row["path"]}
        for row in rows
    ]


def _store_probe(
    conn: sqlite3.Connection,
    recording_id: str,
    camera: str,
    path: str,
    info: dict,
) -> None:
    """Insert or update probe results in the files table."""
    now = time.strftime("%Y-%m-%dT%H:%M:%S")
    conn.execute(
        """
        INSERT INTO files
            (recording_id, camera, path, codec, width, height,
             fps, bitrate, duration_sec, file_size, scanned_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(recording_id) DO UPDATE SET
            codec       = excluded.codec,
            width       = excluded.width,
            height      = excluded.height,
            fps         = excluded.fps,
            bitrate     = excluded.bitrate,
            duration_sec = excluded.duration_sec,
            file_size   = excluded.file_size,
            scanned_at   = excluded.scanned_at
        """,
        (
            recording_id,
            camera,
            path,
            info.get("codec"),
            info.get("width"),
            info.get("height"),
            info.get("fps"),
            info.get("bitrate"),
            info.get("duration_sec"),
            info.get("file_size"),
            now,
        ),
    )
    conn.commit()


def run_probe_loop(ctx: CompressorContext, stopping: threading.Event) -> None:
    """Continuously probe unprobed Frigate recordings.

    Each cycle fetches a batch of recordings not yet probed in ``files``,
    runs ``_probe`` on each, and stores the results.  Sleeps
    ``PROBE_SLEEP_SEC`` when fully caught up.

    Opens its own read-write connection to the compress DB so it never
    contends with the compression loop.  WAL mode allows
    concurrent readers and a single writer with busy_timeout handling
    contention.
    """
    probe_conn = sqlite3.connect(
        f"file:{ctx.cfg.compress_db}", uri=True, check_same_thread=False
    )
    probe_conn.row_factory = sqlite3.Row
    probe_conn.execute("PRAGMA journal_mode=WAL")
    probe_conn.execute("PRAGMA busy_timeout=10000")

    try:
        while not stopping.is_set():
            try:
                unprobed = _get_unprobed_recordings(ctx.cfg, probe_conn)
            except Exception as e:
                log("ERROR", f"Probe loop: failed to query unprobed recordings: {e}")
                stopping.wait(timeout=PROBE_SLEEP_SEC)
                continue

            if not unprobed:
                stopping.wait(timeout=PROBE_SLEEP_SEC)
                continue

            log("DEBUG", f"Probing {len(unprobed)} recording(s)")
            probed = 0
            for rec in unprobed:
                if stopping.is_set():
                    break
                info = _probe(Path(rec["path"]))
                if info is not None:
                    _store_probe(
                        probe_conn,
                        rec["recording_id"],
                        rec["camera"],
                        rec["path"],
                        info,
                    )
                    probed += 1
                else:
                    log(
                        "DEBUG",
                        f"[{rec['camera']}] Probe failed, skipping: {rec['path']}",
                    )
            if probed:
                log("DEBUG", f"Probed {probed} recording(s)")
    finally:
        probe_conn.close()
