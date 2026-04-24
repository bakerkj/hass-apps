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
from .database import _attach_frigate_ro, _recording_type
from .ffmpeg import _probe
from .util import log

PROBE_SLEEP_SEC = 60.0

_PROBE_BATCH_SIZE = 5000

# Incremental-scan knobs (see ``run_probe_loop``):
#  * ``_PROBE_SAFETY_WINDOW_SEC``: incremental queries scan recordings whose
#    ``start_time`` is >= (cursor - this), giving out-of-order arrivals or
#    transient errors a chance to be caught on the next poll.
#  * ``_PROBE_FULL_RESCAN_SEC``: every this long we force a full table scan
#    as a belt-and-suspenders backstop to catch anything older than the
#    safety window.
_PROBE_SAFETY_WINDOW_SEC = 15.0 * 60.0
_PROBE_FULL_RESCAN_SEC = 24.0 * 60.0 * 60.0


def _get_unprobed_recordings(
    cfg: Config,
    conn: sqlite3.Connection,
    cursor: float | None = None,
) -> list[dict]:
    """Return a batch of Frigate recordings not yet probed.

    When ``cursor`` is ``None``, scans the full ``recordings`` table (used
    on startup and every ``_PROBE_FULL_RESCAN_SEC``).  When ``cursor`` is
    a Unix timestamp, scans only recordings with ``start_time >= cursor -
    _PROBE_SAFETY_WINDOW_SEC`` — a bounded window around the newest thing
    we've already seen.  Capped at ``_PROBE_BATCH_SIZE`` rows per call.
    """
    _attach_frigate_ro(conn, cfg, "frigate_probe")
    try:
        if cursor is None:
            rows = conn.execute(
                """
                SELECT r.id, r.camera, r.path, r.start_time,
                       r.motion, r.objects
                FROM   frigate_probe.recordings r
                LEFT JOIN files f ON f.recording_id = r.id
                WHERE  f.recording_id IS NULL
                   OR  f.scanned_at IS NULL
                ORDER BY r.start_time ASC
                LIMIT ?
                """,
                (_PROBE_BATCH_SIZE,),
            ).fetchall()
        else:
            floor = cursor - _PROBE_SAFETY_WINDOW_SEC
            rows = conn.execute(
                """
                SELECT r.id, r.camera, r.path, r.start_time,
                       r.motion, r.objects
                FROM   frigate_probe.recordings r
                LEFT JOIN files f ON f.recording_id = r.id
                WHERE  r.start_time >= ?
                  AND (f.recording_id IS NULL OR f.scanned_at IS NULL)
                ORDER BY r.start_time ASC
                LIMIT ?
                """,
                (floor, _PROBE_BATCH_SIZE),
            ).fetchall()
    finally:
        conn.execute("DETACH DATABASE frigate_probe")

    return [
        {
            "recording_id": row["id"],
            "camera": row["camera"],
            "path": row["path"],
            "start_time": float(row["start_time"]),
            "recording_type": _recording_type(row["motion"], row["objects"]),
        }
        for row in rows
    ]


def _max_recording_start_time(cfg: Config, conn: sqlite3.Connection) -> float | None:
    """``MAX(start_time)`` across Frigate's recordings table.

    Used to seed the cursor when a full scan finds nothing to probe so
    subsequent iterations can run the cheap incremental query.
    """
    _attach_frigate_ro(conn, cfg, "frigate_max_st")
    try:
        row = conn.execute(
            "SELECT MAX(start_time) AS mx FROM frigate_max_st.recordings"
        ).fetchone()
    finally:
        conn.execute("DETACH DATABASE frigate_max_st")
    return float(row["mx"]) if row and row["mx"] is not None else None


def _store_probe(
    conn: sqlite3.Connection,
    recording_id: str,
    camera: str,
    path: str,
    info: dict,
    recording_type: str | None = None,
) -> None:
    """Insert or update probe results in the files table.

    ``recording_type`` is written on insert so the ``files_stats``
    triggers can bucket the row correctly.  If ``None`` (caller from
    older code paths that don't classify), the trigger falls back to
    'continuous' and the bucket will self-correct on the next status
    update that includes a valid recording_type.
    """
    now = time.strftime("%Y-%m-%dT%H:%M:%S")
    conn.execute(
        """
        INSERT INTO files
            (recording_id, camera, path, recording_type,
             codec, width, height,
             fps, bitrate, duration_sec, file_size, scanned_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(recording_id) DO UPDATE SET
            recording_type = COALESCE(excluded.recording_type, files.recording_type),
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
            recording_type,
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

    The probe query uses an in-memory ``start_time`` cursor to avoid
    re-scanning the full recordings table every poll.  On startup (or
    every ``_PROBE_FULL_RESCAN_SEC``) a full scan catches any backlog;
    steady-state polls only look at the last ``_PROBE_SAFETY_WINDOW_SEC``
    worth of recordings, which keeps the per-poll cost tiny even as
    the table grows to hundreds of thousands of rows.

    Opens its own read-write connection to the compress DB so it never
    contends with the compression loop.
    """
    probe_conn = sqlite3.connect(
        f"file:{ctx.cfg.compress_db}", uri=True, check_same_thread=False
    )
    probe_conn.row_factory = sqlite3.Row
    probe_conn.execute("PRAGMA journal_mode=WAL")
    probe_conn.execute("PRAGMA busy_timeout=10000")

    # Cursor state (in-memory only): ``None`` means the next iteration
    # does a full scan.  Set on startup and every ``_PROBE_FULL_RESCAN_SEC``.
    cursor: float | None = None
    last_full_scan: float = 0.0

    try:
        while not stopping.is_set():
            now_mono = time.monotonic()
            # Periodic belt-and-suspenders full rescan.
            if last_full_scan and (now_mono - last_full_scan) >= _PROBE_FULL_RESCAN_SEC:
                log("INFO", "Probe loop: periodic full rescan (cursor reset)")
                cursor = None

            was_full_scan = cursor is None
            try:
                unprobed = _get_unprobed_recordings(ctx.cfg, probe_conn, cursor)
            except Exception as e:
                log("ERROR", f"Probe loop: failed to query unprobed recordings: {e}")
                stopping.wait(timeout=PROBE_SLEEP_SEC)
                continue

            if was_full_scan:
                last_full_scan = now_mono

            if not unprobed:
                # Caught up.  If we just did a full scan with no results,
                # seed the cursor from MAX(start_time) so the next iteration
                # can run the cheap incremental query.
                if was_full_scan:
                    try:
                        mx = _max_recording_start_time(ctx.cfg, probe_conn)
                    except Exception as e:
                        log("WARNING", f"Probe loop: MAX(start_time) failed: {e}")
                        mx = None
                    if mx is not None:
                        cursor = mx
                stopping.wait(timeout=PROBE_SLEEP_SEC)
                continue

            log("DEBUG", f"Probing {len(unprobed)} recording(s)")
            probed = 0
            max_observed = cursor if cursor is not None else 0.0
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
                        recording_type=rec.get("recording_type"),
                    )
                    probed += 1
                else:
                    log(
                        "DEBUG",
                        f"[{rec['camera']}] Probe failed, skipping: {rec['path']}",
                    )
                if rec["start_time"] > max_observed:
                    max_observed = rec["start_time"]

            # Cursor advancement:
            #  * If a full scan returned a FULL batch (==LIMIT rows), there's
            #    likely more backlog — keep cursor=None so the next iteration
            #    re-scans full to drain it.
            #  * Otherwise, advance cursor to the newest start_time we just
            #    observed so the next iteration runs the bounded incremental
            #    query.
            if was_full_scan and len(unprobed) >= _PROBE_BATCH_SIZE:
                cursor = None
            else:
                cursor = max_observed

            if probed:
                log("DEBUG", f"Probed {probed} recording(s)")
    finally:
        probe_conn.close()
