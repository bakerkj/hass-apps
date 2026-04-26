# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Per-recording compression worker (``compress_one``)."""

from __future__ import annotations

import subprocess
import time
from pathlib import Path

from .config import TypeSettings
from .context import CompressorContext
from .database import (
    STATUS_ERROR,
    STATUS_OK,
    STATUS_SEGMENT_UPDATE_FAILED,
    _record,
)
from .detached_subprocess import DetachedResult, run_detached
from .ffmpeg import (
    FFMPEG_STDERR_MAX_LEN,
    FFMPEG_TIMEOUT_SEC,
    _TEMP_PREFIX,
    _probe,
    build_ffmpeg_cmd,
)
from .util import _display_path, _fmt, log


def compress_one(
    recording_id: str,
    path: str,
    camera: str,
    tier: int,
    recording_type: str,
    encoder: str,
    ctx: CompressorContext,
    probe_data: dict | None = None,
) -> bool:
    """Compress one recording end-to-end using the shared connections on ctx.

    All DB access goes through ``ctx.compress_db`` (writes + reads of files,
    plus reads + writes of frigate's recordings via the attached ``frigate``
    schema) serialised by ``ctx.compress_db_lock``.

    ``probe_data``: when the caller already has the row's ``width`` /
    ``height`` / ``fps`` (e.g. from ``get_eligible_recordings``), pass them
    in so the worker skips its pre-flight SELECT.  Avoiding that SELECT
    matters at production compression rates.  Tests and one-shot callers
    leave ``probe_data=None`` and the worker falls back to fetching it.
    """
    return _compress_one_inner(
        recording_id,
        path,
        camera,
        tier,
        recording_type,
        encoder,
        ctx,
        probe_data,
    )


def _compress_one_inner(
    recording_id: str,
    path: str,
    camera: str,
    tier: int,
    recording_type: str,
    encoder: str,
    ctx: CompressorContext,
    probe_data: dict | None = None,
) -> bool:
    cfg = ctx.cfg
    compress_db = ctx.compress_db
    compress_db_lock = ctx.compress_db_lock

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
        with compress_db_lock:
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

    def fail(error_msg: str, *, size_after: int | None = None) -> bool:
        """Record a STATUS_ERROR row with the current size_before/duration
        and return False — saves the seven-line ``rec(...)`` boilerplate at
        every post-probe failure branch."""
        rec(
            size_before=size_before,
            size_after=size_after,
            duration_sec=duration,
            status=STATUS_ERROR,
            error_msg=error_msg,
        )
        return False

    # Require probe data before compressing.  If the caller passed it in
    # (production fast path — eligibility query already fetched these
    # columns), skip the SELECT.  Tests and ad-hoc callers fall back to
    # a per-file SELECT.
    if probe_data is None:
        with compress_db_lock:
            row = compress_db.execute(
                "SELECT width, height, fps FROM files WHERE recording_id = ?",
                (recording_id,),
            ).fetchone()
        probe_data = (
            {"width": row["width"], "height": row["height"], "fps": row["fps"]}
            if row is not None
            else None
        )
    if not probe_data or not probe_data.get("width") or not probe_data.get("height"):
        log("DEBUG", f"[{camera}] Not yet probed, skipping: {_display_path(filepath)}")
        return False

    src_info = f"{probe_data['width']}x{probe_data['height']}"
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
            result: DetachedResult | subprocess.CompletedProcess[str]
            if cfg.detached_ffmpeg:
                # Spawn ffmpeg as an init-orphan so its IO accounting is
                # reaped by PID 1 (s6-svscan in the addon container) rather
                # than wait4()'d by this daemon.  See ``detached_subprocess.py``.
                result = run_detached(
                    cmd,
                    timeout=FFMPEG_TIMEOUT_SEC,
                    stderr_max_len=FFMPEG_STDERR_MAX_LEN,
                )
            else:
                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=FFMPEG_TIMEOUT_SEC
                )
        except subprocess.TimeoutExpired:
            duration = time.monotonic() - t_start
            log(
                "WARNING",
                f"[{camera}] ffmpeg timeout after {duration:.1f}s "
                f"(limit {FFMPEG_TIMEOUT_SEC}s): {_display_path(filepath)}",
            )
            return fail(f"timeout after {FFMPEG_TIMEOUT_SEC}s")
        except Exception as e:
            duration = time.monotonic() - t_start
            log(
                "ERROR",
                f"[{camera}] ffmpeg raised unexpected exception after {duration:.1f}s: {e}",
            )
            return fail(f"ffmpeg exception: {e}")

        duration = time.monotonic() - t_start

        if result.returncode != 0:
            err = (result.stderr or "")[:FFMPEG_STDERR_MAX_LEN].strip()
            log(
                "WARNING",
                f"[{camera}] ffmpeg failed after {duration:.1f}s "
                f"(rc={result.returncode}): {_display_path(filepath)}",
            )
            if err:
                log("DEBUG", f"[{camera}]   stderr: {err}")
            return fail(err)

        if not tmpfile.exists():
            log(
                "WARNING",
                f"[{camera}] output missing after encode ({duration:.1f}s): "
                f"{_display_path(filepath)}",
            )
            return fail("output missing")

        size_after = tmpfile.stat().st_size

        # Sanity: for very small output (<3% of original), run ffprobe
        # to verify the output is a valid video with matching duration.
        if size_after * 100 < size_before * 3:
            out_info = _probe(tmpfile)
            src_info_full = _probe(filepath)
            if out_info is None:
                log(
                    "WARNING",
                    f"[{camera}] output small and invalid after {duration:.1f}s — "
                    f"keeping original: {_display_path(filepath)}",
                )
                return fail(
                    "output too small and ffprobe failed", size_after=size_after
                )
            # Verify duration matches within 1 second.
            if (
                src_info_full is not None
                and src_info_full.get("duration_sec")
                and out_info.get("duration_sec")
                and abs(src_info_full["duration_sec"] - out_info["duration_sec"]) > 1.0
            ):
                mismatch = (
                    f"({src_info_full['duration_sec']:.1f}s vs"
                    f" {out_info['duration_sec']:.1f}s)"
                )
                log(
                    "WARNING",
                    f"[{camera}] output small and duration mismatch {mismatch} — "
                    f"keeping original: {_display_path(filepath)}",
                )
                return fail(
                    f"output too small and duration mismatch {mismatch}",
                    size_after=size_after,
                )
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
            log(
                "WARNING",
                f"[{camera}] original deleted during compression ({duration:.1f}s) — "
                f"discarding output: {_display_path(filepath)}",
            )
            return fail("original deleted by Frigate during compression")

        current_size = filepath.stat().st_size
        if current_size != size_before:
            log(
                "WARNING",
                f"[{camera}] original changed during compression ({duration:.1f}s) — "
                f"discarding output: {_display_path(filepath)}",
            )
            return fail(
                f"original changed during compression"
                f" ({size_before}→{current_size} bytes)"
            )

        # Safety: confirm Frigate still has this recording in its DB.
        # Closes the race where Frigate removes the DB row (and possibly the
        # file) between the checks above and the atomic replace below.
        # Without this, we could create an orphan on disk that Frigate never
        # cleans up.  Read via the attached ``frigate`` schema on
        # compress_db so it shares the daemon's single connection.
        with compress_db_lock:
            db_row = compress_db.execute(
                "SELECT id FROM frigate.recordings WHERE id = ?", (recording_id,)
            ).fetchone()
        if db_row is None:
            log(
                "WARNING",
                f"[{camera}] recording removed from Frigate DB during compression "
                f"({duration:.1f}s) — discarding output to prevent orphan: "
                f"{_display_path(filepath)}",
            )
            return fail("recording removed from Frigate DB during compression")

        # Atomically replace original.
        log(
            "DEBUG",
            f"[{camera}] Replacing original with compressed output: "
            f"{_display_path(filepath)}",
        )
        try:
            tmpfile.replace(filepath)
        except Exception as e:
            log(
                "ERROR",
                f"[{camera}] failed to replace original after {duration:.1f}s: {e}",
            )
            return fail(f"replace failed: {e}", size_after=size_after)
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

    # Update segment_size in Frigate's DB (MB, float) via the attached
    # ``frigate`` schema on compress_db.  Goes through the same connection
    # + lock as the compress.db UPDATE below, so frigate's segment_size
    # and our t1/t2_status flip commit atomically.  If this fails we
    # record ``segment_update_failed`` so housekeeping can retry; the
    # file itself is already safely replaced.
    new_size_mb = size_after / (1024 * 1024)
    log(
        "DEBUG",
        f"[{camera}] Updating Frigate segment_size to {new_size_mb:.3f}MB for {recording_id}",
    )
    seg_status = STATUS_OK
    seg_error: str | None = None
    try:
        with compress_db_lock:
            compress_db.execute(
                "UPDATE frigate.recordings SET segment_size = ? WHERE id = ?",
                (new_size_mb, recording_id),
            )
            compress_db.commit()
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
