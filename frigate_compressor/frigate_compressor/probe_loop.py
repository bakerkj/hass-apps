# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Background ffprobe loop — fills missing metadata for new Frigate recordings."""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

from .context import CompressorContext
from .database import _recording_type
from .ffmpeg import _probe
from .throttle import RateLimiter
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
    conn: sqlite3.Connection,
    cursor: float | None = None,
) -> list[dict]:
    """Return a batch of Frigate recordings not yet probed.

    When ``cursor`` is ``None``, scans the full ``recordings`` table (used
    on startup and every ``_PROBE_FULL_RESCAN_SEC``).  When ``cursor`` is
    a Unix timestamp, scans only recordings with ``start_time >= cursor -
    _PROBE_SAFETY_WINDOW_SEC`` — a bounded window around the newest thing
    we've already seen.  Capped at ``_PROBE_BATCH_SIZE`` rows per call.

    The connection must have Frigate's recordings attached as ``frigate``
    (the daemon attaches it once at startup; tests must do the same).
    """
    if cursor is None:
        rows = conn.execute(
            """
            SELECT r.id, r.camera, r.path, r.start_time,
                   r.motion, r.objects
            FROM   frigate.recordings r
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
            FROM   frigate.recordings r
            LEFT JOIN files f ON f.recording_id = r.id
            WHERE  r.start_time >= ?
              AND (f.recording_id IS NULL OR f.scanned_at IS NULL)
            ORDER BY r.start_time ASC
            LIMIT ?
            """,
            (floor, _PROBE_BATCH_SIZE),
        ).fetchall()

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


def _max_recording_start_time(conn: sqlite3.Connection) -> float | None:
    """``MAX(start_time)`` across Frigate's recordings table.

    Used to seed the cursor when a full scan finds nothing to probe so
    subsequent iterations can run the cheap incremental query.  The
    connection must have Frigate attached as ``frigate``.
    """
    row = conn.execute(
        "SELECT MAX(start_time) AS mx FROM frigate.recordings"
    ).fetchone()
    return float(row["mx"]) if row and row["mx"] is not None else None


def _store_probe(
    conn: sqlite3.Connection,
    recording_id: str,
    camera: str,
    path: str,
    info: dict,
    recording_type: str | None = None,
    start_time: float | None = None,
) -> None:
    """Insert or update probe results in the files table.

    ``recording_type`` is written on insert so the ``files_stats``
    triggers can bucket the row correctly.  ``start_time`` is
    denormalised onto files so eligibility/backlog queries can
    range-scan on (camera, start_time) without joining Frigate per-row.
    Both default to ``None`` for callers that don't have them — the
    ON CONFLICT path preserves existing values in that case.

    Does NOT commit — the caller batches commits across the per-poll
    batch.  Per-row commits caused two problems: (a) every commit added
    a WAL frame and a fsync-equivalent metadata write, multiplying wchar
    by ~250×; (b) every commit advanced the WAL snapshot, invalidating
    the eligibility connection's page cache so the next eligibility
    query paid the full cold cost.  One commit per poll fixes both.
    """
    now = time.strftime("%Y-%m-%dT%H:%M:%S")
    conn.execute(
        """
        INSERT INTO files
            (recording_id, camera, path, recording_type, start_time,
             codec, width, height,
             fps, bitrate, duration_sec, file_size, scanned_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(recording_id) DO UPDATE SET
            recording_type = COALESCE(excluded.recording_type, files.recording_type),
            start_time  = COALESCE(excluded.start_time, files.start_time),
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
            start_time,
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


def run_probe_loop(ctx: CompressorContext, stopping: threading.Event) -> None:
    """Continuously probe unprobed Frigate recordings.

    The probe query uses an in-memory ``start_time`` cursor to avoid
    re-scanning the full recordings table every poll.  On startup (or
    every ``_PROBE_FULL_RESCAN_SEC``) a full scan catches any backlog;
    steady-state polls only look at the last ``_PROBE_SAFETY_WINDOW_SEC``
    worth of recordings, which keeps the per-poll cost tiny even as
    the table grows to hundreds of thousands of rows.

    Uses ``ctx.compress_db`` (with Frigate attached as ``frigate``) under
    ``ctx.compress_db_lock``.  ``_probe`` (ffprobe) runs outside the lock
    so it can't block other DB ops; the lock is acquired only around the
    quick query / batched insert / commit operations.
    """
    compress_db = ctx.compress_db
    compress_db_lock = ctx.compress_db_lock

    # Cursor state (in-memory only): ``None`` means the next iteration
    # does a full scan.  Set on startup and every ``_PROBE_FULL_RESCAN_SEC``.
    cursor: float | None = None
    last_full_scan: float = 0.0

    while not stopping.is_set():
        now_mono = time.monotonic()
        # Periodic belt-and-suspenders full rescan.
        if last_full_scan and (now_mono - last_full_scan) >= _PROBE_FULL_RESCAN_SEC:
            log("INFO", "Probe loop: periodic full rescan (cursor reset)")
            cursor = None

        was_full_scan = cursor is None
        try:
            with compress_db_lock:
                unprobed = _get_unprobed_recordings(compress_db, cursor)
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
                    with compress_db_lock:
                        mx = _max_recording_start_time(compress_db)
                except Exception as e:
                    log("WARNING", f"Probe loop: MAX(start_time) failed: {e}")
                    mx = None
                if mx is not None:
                    cursor = mx
            log("DEBUG", "Probe loop idle — caught up")
            stopping.wait(timeout=PROBE_SLEEP_SEC)
            continue

        log("DEBUG", f"Probing {len(unprobed)} recording(s)")
        probed = 0
        max_observed = cursor if cursor is not None else 0.0
        # Pace probes evenly across ``PROBE_SLEEP_SEC`` using the same
        # deadline-based ``RateLimiter`` the compression loop uses (see
        # ``throttle.py`` / ``_pace_then_compress`` in ``app.py``).
        # ``acquire`` runs BEFORE each probe and waits up to the next
        # deadline — ffprobe time is absorbed into the interval rather
        # than added to it, so the batch lands at exactly
        # ``len(unprobed) * (60s / len(unprobed)) = 60s`` regardless of
        # per-probe duration.  A naive post-probe sleep would push the
        # batch over 60s by roughly ``len * probe_time``, drifting the
        # loop slower than Frigate's arrival rate.  Local instance —
        # not ``ctx.rate_limiter`` — so workers' target is untouched.
        limiter = RateLimiter()
        limiter.set_target(len(unprobed))
        # ffprobe (a subprocess) runs OUTSIDE the lock so other threads
        # can use compress_db while we wait on file IO.  We collect the
        # probe results, then take the lock once for the bulk INSERT +
        # commit.  Batching the commit (rather than per-row) avoids both
        # WAL frame amplification and the cache-invalidation cycle that
        # used to force eligibility queries to re-read on every commit.
        probe_results: list[tuple[dict, dict]] = []
        for rec in unprobed:
            if stopping.is_set():
                break
            limiter.acquire(stopping)
            info = _probe(Path(rec["path"]))
            if info is not None:
                probe_results.append((rec, info))
            else:
                log(
                    "DEBUG",
                    f"[{rec['camera']}] Probe failed, skipping: {rec['path']}",
                )
            if rec["start_time"] > max_observed:
                max_observed = rec["start_time"]

        try:
            with compress_db_lock:
                try:
                    for rec, info in probe_results:
                        _store_probe(
                            compress_db,
                            rec["recording_id"],
                            rec["camera"],
                            rec["path"],
                            info,
                            recording_type=rec.get("recording_type"),
                            start_time=rec.get("start_time"),
                        )
                        probed += 1
                    compress_db.commit()
                except Exception:
                    compress_db.rollback()
                    raise
        except Exception as e:
            log("ERROR", f"Probe loop: store batch failed: {e}")

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
            log("INFO", f"Probed {probed} recording(s)")
