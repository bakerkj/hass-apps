# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Per-recording compression worker (``compress_one``)."""

from __future__ import annotations

import sqlite3
import subprocess
import threading
import time
from pathlib import Path

from .config import Config, TypeSettings
from .context import CompressorContext
from .database import (
    STATUS_ERROR,
    STATUS_OK,
    STATUS_SEGMENT_UPDATE_FAILED,
    _record,
)
from .ffmpeg import (
    FFMPEG_STDERR_MAX_LEN,
    FFMPEG_TIMEOUT_SEC,
    _TEMP_PREFIX,
    _probe,
    build_ffmpeg_cmd,
)
from .util import _display_path, _fmt, log


# Per-worker-thread connection cache.  Populated by
# ``init_worker_connections`` (the ``ThreadPoolExecutor`` initializer in
# app.py).  ``compress_one`` falls back to per-call opens if the cache
# isn't populated, which is what tests and one-shot callers get.
_local = threading.local()


def init_worker_connections(cfg: Config) -> None:
    """``ThreadPoolExecutor`` initializer — open one set of DB connections
    per worker thread, kept alive for the daemon's lifetime.

    Replaces the prior "open + close 3 connections per file" pattern
    (~210 opens/min at full tilt) with one open per thread at startup.
    Each open reads the schema, WAL header, and several pages of
    sqlite_master, so amortising it across the thread's lifetime makes
    a meaningful dent in steady-state IO.
    """
    _local.compress_db = sqlite3.connect(
        f"file:{cfg.compress_db}", uri=True, check_same_thread=False
    )
    _local.compress_db.row_factory = sqlite3.Row
    _local.compress_db.execute("PRAGMA journal_mode=WAL")
    _local.compress_db.execute("PRAGMA synchronous=NORMAL")
    _local.compress_db.execute("PRAGMA cache_size=-196608")
    _local.compress_db.execute("PRAGMA busy_timeout=10000")
    _local.frigate_ro = sqlite3.connect(
        f"file:{cfg.frigate_db}?mode=ro", uri=True, check_same_thread=False
    )
    _local.frigate_ro.row_factory = sqlite3.Row
    _local.frigate_rw = sqlite3.connect(str(cfg.frigate_db), check_same_thread=False)
    _local.frigate_rw.row_factory = sqlite3.Row
    _local.frigate_rw.execute("PRAGMA busy_timeout=10000")


def compress_one(
    recording_id: str,
    path: str,
    camera: str,
    tier: int,
    recording_type: str,
    encoder: str,
    ctx: CompressorContext,
) -> bool:
    cfg = ctx.cfg
    compress_db = getattr(_local, "compress_db", None)
    if compress_db is not None:
        # Production path: the worker thread's pool initializer has
        # already opened these connections.  Reuse them.
        return _compress_one_inner(
            recording_id,
            path,
            camera,
            tier,
            recording_type,
            encoder,
            cfg,
            compress_db,
            _local.frigate_ro,
            _local.frigate_rw,
        )

    # Fallback: no thread-local connections (tests, ad-hoc callers).
    # Open + close per call so caller doesn't have to manage lifetime.
    compress_db = sqlite3.connect(
        f"file:{cfg.compress_db}", uri=True, check_same_thread=False
    )
    compress_db.row_factory = sqlite3.Row
    compress_db.execute("PRAGMA journal_mode=WAL")
    compress_db.execute("PRAGMA synchronous=NORMAL")
    compress_db.execute("PRAGMA cache_size=-196608")
    compress_db.execute("PRAGMA busy_timeout=10000")
    frigate_ro = sqlite3.connect(
        f"file:{cfg.frigate_db}?mode=ro", uri=True, check_same_thread=False
    )
    frigate_ro.row_factory = sqlite3.Row
    frigate_rw = sqlite3.connect(str(cfg.frigate_db), check_same_thread=False)
    frigate_rw.row_factory = sqlite3.Row
    frigate_rw.execute("PRAGMA busy_timeout=10000")
    try:
        return _compress_one_inner(
            recording_id,
            path,
            camera,
            tier,
            recording_type,
            encoder,
            cfg,
            compress_db,
            frigate_ro,
            frigate_rw,
        )
    finally:
        compress_db.close()
        frigate_ro.close()
        frigate_rw.close()


def _compress_one_inner(
    recording_id: str,
    path: str,
    camera: str,
    tier: int,
    recording_type: str,
    encoder: str,
    cfg: Config,
    compress_db: sqlite3.Connection,
    frigate_ro: sqlite3.Connection,
    frigate_rw: sqlite3.Connection,
) -> bool:

    # Resolve per-camera settings.
    cam_cfg = cfg.cameras.get(camera)
    if cam_cfg is None:
        log("WARNING", f"[{camera}] Not in camera config, skipping")
        return False
    tier_cfg = cam_cfg.tier1 if tier == 1 else cam_cfg.tier2
    ts: TypeSettings | None = getattr(tier_cfg, recording_type, None)
    if ts is None:
        log(
            "WARNING",
            f"[{camera}] No resolved settings for tier{tier}/{recording_type}, skipping",
        )
        return False
    if not ts.enabled:
        log(
            "DEBUG",
            f"[{camera}] Compression disabled for tier{tier}/{recording_type}, skipping",
        )
        return True
    dry_run = cam_cfg.dry_run

    def rec(
        *,
        size_before: int | None,
        size_after: int | None,
        duration_sec: float | None,
        status: str,
        error_msg: str | None = None,
    ) -> None:
        _record(
            compress_db,
            recording_id=recording_id,
            camera=camera,
            path=path,
            tier=tier,
            recording_type=recording_type,
            encoder=encoder,
            size_before=size_before,
            size_after=size_after,
            duration_sec=duration_sec,
            status=status,
            error_msg=error_msg,
        )

    filepath = Path(path)

    if not filepath.exists():
        log("WARNING", f"[{camera}] File missing, skipping: {path}")
        rec(
            size_before=None,
            size_after=None,
            duration_sec=None,
            status=STATUS_ERROR,
            error_msg="file missing",
        )
        return False

    size_before = filepath.stat().st_size

    # Require probe data before compressing.
    probe_row = compress_db.execute(
        "SELECT width, height, fps FROM files WHERE recording_id = ?",
        (recording_id,),
    ).fetchone()
    if not probe_row or not probe_row["width"] or not probe_row["height"]:
        log("DEBUG", f"[{camera}] Not yet probed, skipping: {_display_path(filepath)}")
        return False

    src_info = f"{probe_row['width']}x{probe_row['height']}"
    src_info = f"{src_info:<10}"

    # Format target settings for log messages.
    tgt_res = ""
    if ts.scale_mode != "none" and ts.scale_value:
        tgt_res = ts.scale_value.replace(":", "x") + " "
    elif ts.scale_mode == "halve":
        tgt_res = "halve "
    tgt_info = f"→{tgt_res}q{ts.quality}"
    tgt_info = f"{tgt_info:<14}"

    # Temp file is named .tmp.{recording_id}.mp4 — unique per job, easy to
    # identify as a temp file by housekeeping without affecting other jobs.
    tmpfile = filepath.parent / f"{_TEMP_PREFIX}{recording_id}.mp4"
    cmd = build_ffmpeg_cmd(filepath, tmpfile, encoder, ts)

    log("DEBUG", f"[{camera}]   cmd: {' '.join(cmd)}")

    if dry_run:
        # Dry-run does no work, so the post-success summary line never runs.
        # Emit a single self-contained INFO line here instead.
        log(
            "INFO",
            f"[{camera:<{cfg.cam_name_width}}] DRY RUN t{tier}:{recording_type[:3]}  "
            f"{_display_path(filepath)}  {src_info}{tgt_info}"
            f"{_fmt(size_before, 10)}",
        )
        return True

    t_start = time.monotonic()
    try:
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=FFMPEG_TIMEOUT_SEC
            )
        except subprocess.TimeoutExpired:
            duration = time.monotonic() - t_start
            rec(
                size_before=size_before,
                size_after=None,
                duration_sec=duration,
                status=STATUS_ERROR,
                error_msg=f"timeout after {FFMPEG_TIMEOUT_SEC}s",
            )
            log(
                "WARNING",
                f"[{camera}] ffmpeg timeout after {duration:.1f}s "
                f"(limit {FFMPEG_TIMEOUT_SEC}s): {_display_path(filepath)}",
            )
            return False
        except Exception as e:
            duration = time.monotonic() - t_start
            rec(
                size_before=size_before,
                size_after=None,
                duration_sec=duration,
                status=STATUS_ERROR,
                error_msg=f"ffmpeg exception: {e}",
            )
            log(
                "ERROR",
                f"[{camera}] ffmpeg raised unexpected exception after {duration:.1f}s: {e}",
            )
            return False

        duration = time.monotonic() - t_start

        if result.returncode != 0:
            err = (result.stderr or "")[:FFMPEG_STDERR_MAX_LEN].strip()
            rec(
                size_before=size_before,
                size_after=None,
                duration_sec=duration,
                status=STATUS_ERROR,
                error_msg=err,
            )
            log(
                "WARNING",
                f"[{camera}] ffmpeg failed after {duration:.1f}s "
                f"(rc={result.returncode}): {_display_path(filepath)}",
            )
            if err:
                log("DEBUG", f"[{camera}]   stderr: {err}")
            return False

        if not tmpfile.exists():
            rec(
                size_before=size_before,
                size_after=None,
                duration_sec=duration,
                status=STATUS_ERROR,
                error_msg="output missing",
            )
            log(
                "WARNING",
                f"[{camera}] output missing after encode ({duration:.1f}s): "
                f"{_display_path(filepath)}",
            )
            return False

        size_after = tmpfile.stat().st_size

        # Sanity: for very small output (<3% of original), run ffprobe
        # to verify the output is a valid video with matching duration.
        if size_after * 100 < size_before * 3:
            out_info = _probe(tmpfile)
            src_info_full = _probe(filepath)
            if out_info is None:
                rec(
                    size_before=size_before,
                    size_after=size_after,
                    duration_sec=duration,
                    status=STATUS_ERROR,
                    error_msg="output too small and ffprobe failed",
                )
                log(
                    "WARNING",
                    f"[{camera}] output small and invalid after {duration:.1f}s — "
                    f"keeping original: {_display_path(filepath)}",
                )
                return False
            # Verify duration matches within 1 second.
            if (
                src_info_full is not None
                and src_info_full.get("duration_sec")
                and out_info.get("duration_sec")
                and abs(src_info_full["duration_sec"] - out_info["duration_sec"]) > 1.0
            ):
                rec(
                    size_before=size_before,
                    size_after=size_after,
                    duration_sec=duration,
                    status=STATUS_ERROR,
                    error_msg=f"output too small and duration mismatch "
                    f"({src_info_full['duration_sec']:.1f}s vs {out_info['duration_sec']:.1f}s)",
                )
                log(
                    "WARNING",
                    f"[{camera}] output small and duration mismatch "
                    f"({src_info_full['duration_sec']:.1f}s vs {out_info['duration_sec']:.1f}s) — "
                    f"keeping original: {_display_path(filepath)}",
                )
                return False
            log(
                "DEBUG",
                f"[{camera}] output small ({size_after * 100 // size_before}% of "
                f"original) but valid ({out_info.get('duration_sec', '?')}s): "
                f"{_display_path(filepath)}",
            )

        # Safety: verify the original still exists and hasn't been modified.
        # Frigate may delete recordings during its own retention cleanup while
        # we were encoding.  If the file changed, the encode is based on stale
        # data.
        if not filepath.exists():
            rec(
                size_before=size_before,
                size_after=None,
                duration_sec=duration,
                status=STATUS_ERROR,
                error_msg="original deleted by Frigate during compression",
            )
            log(
                "WARNING",
                f"[{camera}] original deleted during compression ({duration:.1f}s) — "
                f"discarding output: {_display_path(filepath)}",
            )
            return False

        current_size = filepath.stat().st_size
        if current_size != size_before:
            rec(
                size_before=size_before,
                size_after=None,
                duration_sec=duration,
                status=STATUS_ERROR,
                error_msg=f"original changed during compression ({size_before}→{current_size} bytes)",
            )
            log(
                "WARNING",
                f"[{camera}] original changed during compression ({duration:.1f}s) — "
                f"discarding output: {_display_path(filepath)}",
            )
            return False

        # Safety: confirm Frigate still has this recording in its DB.
        # Closes the race where Frigate removes the DB row (and possibly the
        # file) between the checks above and the atomic replace below.
        # Without this, we could create an orphan on disk that Frigate never
        # cleans up.
        db_row = frigate_ro.execute(
            "SELECT id FROM recordings WHERE id = ?", (recording_id,)
        ).fetchone()
        if db_row is None:
            rec(
                size_before=size_before,
                size_after=None,
                duration_sec=duration,
                status=STATUS_ERROR,
                error_msg="recording removed from Frigate DB during compression",
            )
            log(
                "WARNING",
                f"[{camera}] recording removed from Frigate DB during compression "
                f"({duration:.1f}s) — discarding output to prevent orphan: "
                f"{_display_path(filepath)}",
            )
            return False

        # Atomically replace original.
        log(
            "DEBUG",
            f"[{camera}] Replacing original with compressed output: "
            f"{_display_path(filepath)}",
        )
        try:
            tmpfile.replace(filepath)
        except Exception as e:
            rec(
                size_before=size_before,
                size_after=size_after,
                duration_sec=duration,
                status=STATUS_ERROR,
                error_msg=f"replace failed: {e}",
            )
            log(
                "ERROR",
                f"[{camera}] failed to replace original after {duration:.1f}s: {e}",
            )
            return False
    finally:
        # Ensure temp file is always cleaned up, even on thread cancellation.
        tmpfile.unlink(missing_ok=True)

    pct = ((size_before - size_after) / size_before * 100) if size_before else 0.0
    log(
        "INFO",
        f"[{camera:<{cfg.cam_name_width}}] t{tier}:{recording_type[:3]}  "
        f"{_display_path(filepath)}  {src_info}{tgt_info}"
        f"{_fmt(size_before, 10)}→{_fmt(size_after, 10)}  {pct:>3.0f}%  {duration:>5.1f}s",
    )

    # Update segment_size in Frigate's DB (MB, float).
    # If this fails we record status='segment_update_failed' so housekeeping
    # can retry; the file itself is already safely replaced.
    new_size_mb = size_after / (1024 * 1024)
    log(
        "DEBUG",
        f"[{camera}] Updating Frigate segment_size to {new_size_mb:.3f}MB for {recording_id}",
    )
    seg_status = STATUS_OK
    seg_error: str | None = None
    try:
        frigate_rw.execute(
            "UPDATE recordings SET segment_size = ? WHERE id = ?",
            (new_size_mb, recording_id),
        )
        frigate_rw.commit()
    except Exception as e:
        seg_status = STATUS_SEGMENT_UPDATE_FAILED
        seg_error = str(e)
        log(
            "WARNING",
            f"[{camera}] failed to update Frigate segment_size — will retry at housekeeping: {e}",
        )

    rec(
        size_before=size_before,
        size_after=size_after,
        duration_sec=duration,
        status=seg_status,
        error_msg=seg_error,
    )
    return True
