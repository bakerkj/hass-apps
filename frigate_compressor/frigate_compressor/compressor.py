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
    STATUS_DIRECT,
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
    build_direct_ffmpeg_cmd,
    build_ffmpeg_cmd,
)
from .paths import sibling_path
from .util import _display_path, _fmt, log


def _format_src_info(probe_data: dict | None) -> str:
    """``WIDTHxHEIGHT`` left-padded to 10 — same shape across all log paths."""
    if not probe_data or not probe_data.get("width") or not probe_data.get("height"):
        return f"{'?x?':<10}"
    return f"{probe_data['width']}x{probe_data['height']:<10}"


def _format_tgt_info(ts: TypeSettings) -> str:
    """``→<scale> q<quality>`` left-padded to 14 — matches the chained log shape."""
    tgt_res = ""
    if ts.scale_mode != "none" and ts.scale_value:
        tgt_res = ts.scale_value.replace(":", "x") + " "
    elif ts.scale_mode == "halve":
        tgt_res = "halve "
    return f"{f'→{tgt_res}q{ts.quality}':<14}"


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

    src_info = _format_src_info(probe_data)
    tgt_info = _format_tgt_info(ts)

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
        f"{_fmt(size_before, 10)}→{_fmt(size_after, 10)}  {pct:>3.0f}%  "
        f"{'':>6}  {duration:>5.1f}s",
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


def compress_direct(
    recording_id: str,
    path: str,
    camera: str,
    recording_type: str,
    encoder: str,
    ctx: CompressorContext,
    probe_data: dict | None = None,
) -> bool:
    """Encode tier-1 + tier-2 from native source in one ffmpeg pass.

    Used when a segment reaches ``tier1.min_days`` and
    ``cam.tier2.source == "direct"`` and both tier-1 and tier-2 are
    enabled for ``recording_type``.  Tier-1 replaces the native file at
    the primary path (matching ``compress_one`` behavior); tier-2 lands
    at the sibling ``.t2.mp4`` path and stays there until the tier-2
    swap renames it onto the primary path.

    Falls back to ``compress_one(tier=1)`` when tier-2 is not enabled for
    the recording_type — a single encode is the right thing for that case.
    """
    cfg = ctx.cfg
    compress_db = ctx.compress_db
    compress_db_lock = ctx.compress_db_lock

    cam_cfg = cfg.cameras.get(camera)
    if cam_cfg is None:
        log("WARNING", f"[{camera}] Not in camera config, skipping")
        return False
    t1_ts = getattr(cam_cfg.tier1, recording_type, None)
    t2_ts = getattr(cam_cfg.tier2, recording_type, None)
    if t1_ts is None or t2_ts is None:
        log(
            "WARNING",
            f"[{camera}] No resolved settings for {recording_type}, skipping",
        )
        return False
    if not t1_ts.enabled:
        log("DEBUG", f"[{camera}] tier-1 disabled for {recording_type}, skipping")
        return True
    if not t2_ts.enabled:
        # No tier-2 work for this rtype — fall back to a normal tier-1 encode.
        return compress_one(
            recording_id, path, camera, 1, recording_type, encoder, ctx, probe_data
        )
    dry_run = cam_cfg.dry_run

    def rec(
        *,
        tier: int,
        status: str,
        size: int | None,
        duration: float | None,
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
                size_before=None,
                size_after=size,
                duration_sec=duration,
                status=status,
                error_msg=error_msg,
            )

    filepath = Path(path)

    if not filepath.exists():
        log("WARNING", f"[{camera}] File missing, skipping: {path}")
        rec(
            tier=1,
            status=STATUS_ERROR,
            size=None,
            duration=None,
            error_msg="file missing",
        )
        return False

    size_before = filepath.stat().st_size

    duration: float | None = None

    def fail_both(error_msg: str) -> bool:
        """Mark both tiers as errored and return False — the native file
        is left in place (atomic-replace hasn't happened yet), so this is
        recoverable: the chained eligibility query will pick the row back
        up at ``tier2.min_days`` and try again."""
        rec(
            tier=1,
            status=STATUS_ERROR,
            size=None,
            duration=duration,
            error_msg=error_msg,
        )
        rec(
            tier=2,
            status=STATUS_ERROR,
            size=None,
            duration=duration,
            error_msg=error_msg,
        )
        return False

    # Probe data check (mirrors compress_one).
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

    src_info = _format_src_info(probe_data)
    t1_tgt_info = _format_tgt_info(t1_ts)
    t2_tgt_info = _format_tgt_info(t2_ts)

    # Distinct temp filenames for the two outputs (compress_one uses a
    # single .tmp.{rid}.mp4 per job; here we suffix with .t1/.t2).
    t1_tmp = filepath.parent / f"{_TEMP_PREFIX}{recording_id}.t1.mp4"
    t2_tmp = filepath.parent / f"{_TEMP_PREFIX}{recording_id}.t2.mp4"
    cmd = build_direct_ffmpeg_cmd(filepath, t1_tmp, t2_tmp, encoder, t1_ts, t2_ts)

    log("DEBUG", f"[{camera}]   cmd: {' '.join(cmd)}")

    if dry_run:
        # Two lines mirroring chained DRY RUN — same input file, two outputs.
        cam_prefix = f"[{camera:<{cfg.cam_name_width}}]"
        ts = _display_path(filepath)
        log(
            "INFO",
            f"{cam_prefix} DRY RUN t1:{recording_type[:3]}  {ts}  "
            f"{src_info}{t1_tgt_info}{_fmt(size_before, 10)}",
        )
        log(
            "INFO",
            f"{cam_prefix} DRY RUN t2:{recording_type[:3]}  {ts}  "
            f"{src_info}{t2_tgt_info}{_fmt(size_before, 10)}",
        )
        return True

    t_start = time.monotonic()
    try:
        try:
            result: DetachedResult | subprocess.CompletedProcess[str]
            if cfg.detached_ffmpeg:
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
                f"[{camera}] direct ffmpeg timeout after {duration:.1f}s "
                f"(limit {FFMPEG_TIMEOUT_SEC}s): {_display_path(filepath)}",
            )
            return fail_both(f"timeout after {FFMPEG_TIMEOUT_SEC}s")
        except Exception as e:
            duration = time.monotonic() - t_start
            log(
                "ERROR",
                f"[{camera}] direct ffmpeg raised unexpected exception "
                f"after {duration:.1f}s: {e}",
            )
            return fail_both(f"ffmpeg exception: {e}")

        duration = time.monotonic() - t_start

        if result.returncode != 0:
            err = (result.stderr or "")[:FFMPEG_STDERR_MAX_LEN].strip()
            log(
                "WARNING",
                f"[{camera}] direct ffmpeg failed after {duration:.1f}s "
                f"(rc={result.returncode}): {_display_path(filepath)}",
            )
            if err:
                log("DEBUG", f"[{camera}]   stderr: {err}")
            return fail_both(err)

        if not t1_tmp.exists() or not t2_tmp.exists():
            log(
                "WARNING",
                f"[{camera}] direct outputs missing after encode "
                f"({duration:.1f}s): {_display_path(filepath)}",
            )
            return fail_both("output missing")

        t1_size = t1_tmp.stat().st_size
        t2_size = t2_tmp.stat().st_size

        # Same safety checks as compress_one: original still on disk and
        # unchanged, recording still in Frigate's DB.  If anything has
        # shifted under us, throw both outputs away rather than create
        # an inconsistent state.
        if not filepath.exists():
            log(
                "WARNING",
                f"[{camera}] original deleted during direct encode "
                f"({duration:.1f}s) — discarding outputs: {_display_path(filepath)}",
            )
            return fail_both("original deleted by Frigate during compression")

        current_size = filepath.stat().st_size
        if current_size != size_before:
            log(
                "WARNING",
                f"[{camera}] original changed during direct encode "
                f"({duration:.1f}s) — discarding outputs: {_display_path(filepath)}",
            )
            return fail_both(
                f"original changed during compression"
                f" ({size_before}→{current_size} bytes)"
            )

        with compress_db_lock:
            db_row = compress_db.execute(
                "SELECT id FROM frigate.recordings WHERE id = ?", (recording_id,)
            ).fetchone()
        if db_row is None:
            log(
                "WARNING",
                f"[{camera}] recording removed from Frigate DB during direct encode "
                f"({duration:.1f}s) — discarding outputs: {_display_path(filepath)}",
            )
            return fail_both("recording removed from Frigate DB during compression")

        # Atomic replace: tier-1 → primary path, tier-2 → sibling path.
        # If the tier-1 rename fails, both temps are kept (cleaned up in
        # finally).  If the tier-2 rename fails after tier-1 succeeded,
        # we still get a valid tier-1 file in place but mark t2 as errored
        # so the chained query picks it up at ``tier2.min_days``.
        sib = sibling_path(filepath)
        try:
            t1_tmp.replace(filepath)
        except Exception as e:
            log(
                "ERROR",
                f"[{camera}] failed to replace original after direct encode "
                f"({duration:.1f}s): {e}",
            )
            return fail_both(f"replace failed: {e}")

        t2_replace_failed: str | None = None
        try:
            t2_tmp.replace(sib)
        except Exception as e:
            t2_replace_failed = str(e)
            log(
                "WARNING",
                f"[{camera}] tier-2 sibling rename failed after tier-1 "
                f"replace succeeded: {e}",
            )
    finally:
        # Always clean up temp files (replace() consumes them, so this is
        # only a no-op on the success path).
        t1_tmp.unlink(missing_ok=True)
        t2_tmp.unlink(missing_ok=True)

    # Two chained-style lines: t1 carries the full encode duration; t2
    # shows ``direct`` in the duration column to flag that it's a sibling
    # of a direct encode rather than its own encode pass.  Sizes mirror
    # the chained semantics: t1 line is original→t1, t2 line is t1→t2.
    pct_t1 = ((size_before - t1_size) / size_before * 100) if size_before else 0.0
    pct_t2 = ((t1_size - t2_size) / t1_size * 100) if t1_size else 0.0
    cam_prefix = f"[{camera:<{cfg.cam_name_width}}]"
    ts = _display_path(filepath)
    log(
        "INFO",
        f"{cam_prefix} t1:{recording_type[:3]}  {ts}  "
        f"{src_info}{t1_tgt_info}"
        f"{_fmt(size_before, 10)}→{_fmt(t1_size, 10)}  {pct_t1:>3.0f}%  "
        f"{'':>6}  {duration:>5.1f}s",
    )
    log(
        "INFO",
        f"{cam_prefix} t2:{recording_type[:3]}  {ts}  "
        f"{src_info}{t2_tgt_info}"
        f"{_fmt(t1_size, 10)}→{_fmt(t2_size, 10)}  {pct_t2:>3.0f}%  "
        f"{'direct':>6}  {0.0:>5.1f}s",
    )

    # DB updates: tier-1 ok, tier-2 'direct' (or 'error' if sibling rename
    # failed).  Both row updates + the segment_size UPDATE happen on the
    # same connection under the same lock — observers see consistent state.
    new_size_mb = t1_size / (1024 * 1024)
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
            f"[{camera}] failed to update Frigate segment_size "
            f"— will retry at housekeeping: {e}",
        )

    rec(tier=1, status=seg_status, size=t1_size, duration=duration, error_msg=seg_error)
    if t2_replace_failed is None:
        rec(tier=2, status=STATUS_DIRECT, size=t2_size, duration=duration)
    else:
        rec(
            tier=2,
            status=STATUS_ERROR,
            size=t2_size,
            duration=duration,
            error_msg=f"sibling rename failed: {t2_replace_failed}",
        )
    return True


def swap_t2(
    recording_id: str,
    path: str,
    camera: str,
    recording_type: str,
    encoder: str,
    ctx: CompressorContext,
) -> bool:
    """Swap a sibling tier-2 file into the primary path.

    Used when ``t2_status='direct'`` AND the segment has reached
    ``tier2.min_days``.  The sibling ``.t2.mp4`` was encoded at the
    tier-1 boundary by ``compress_direct``; this function atomically
    renames it onto the primary path (replacing the tier-1 file) and
    updates Frigate's ``segment_size`` to match.

    Falls back to ``compress_one(tier=2)`` (chained re-encode of the
    existing tier-1 file) if the sibling is missing — disk corruption,
    admin deletion, or a partial direct-encode that didn't actually
    produce the sibling will all hit this path.
    """
    cfg = ctx.cfg
    compress_db = ctx.compress_db
    compress_db_lock = ctx.compress_db_lock

    cam_cfg = cfg.cameras.get(camera)
    if cam_cfg is None:
        log("WARNING", f"[{camera}] Not in camera config, skipping swap")
        return False

    filepath = Path(path)
    sib = sibling_path(filepath)

    def rec(*, status: str, size: int | None, error_msg: str | None = None) -> None:
        with compress_db_lock:
            _record(
                compress_db,
                recording_id=recording_id,
                camera=camera,
                path=path,
                tier=2,
                recording_type=recording_type,
                encoder=encoder,
                size_before=None,
                size_after=size,
                duration_sec=None,
                status=status,
                error_msg=error_msg,
            )

    # Sibling missing → fall back to chained encode of the current primary
    # (tier-1 file).  This produces an "ok" tier-2 row at slightly worse
    # quality than direct, but keeps the archive complete.
    if not sib.is_file():
        log(
            "WARNING",
            f"[{camera}] sibling missing for swap, falling back to chained: "
            f"{_display_path(filepath)}",
        )
        return compress_one(recording_id, path, camera, 2, recording_type, encoder, ctx)

    # Primary missing → Frigate retired the segment between direct-encode
    # and our swap.  Clean up the orphan sibling and mark error so the
    # housekeeping prune step can drop the row cleanly.
    if not filepath.exists():
        log(
            "WARNING",
            f"[{camera}] primary missing for swap, deleting orphan sibling: "
            f"{_display_path(sib)}",
        )
        try:
            sib.unlink(missing_ok=True)
        except OSError as e:
            log("WARNING", f"[{camera}] sibling unlink failed: {e}")
        rec(status=STATUS_ERROR, size=None, error_msg="primary missing at swap time")
        return False

    # Verify the recording is still in Frigate's DB before we replace the
    # file (same race the encode workers guard against).
    with compress_db_lock:
        db_row = compress_db.execute(
            "SELECT id FROM frigate.recordings WHERE id = ?", (recording_id,)
        ).fetchone()
    if db_row is None:
        log(
            "WARNING",
            f"[{camera}] recording removed from Frigate DB before swap, "
            f"deleting orphan sibling: {_display_path(sib)}",
        )
        try:
            sib.unlink(missing_ok=True)
        except OSError:
            pass
        rec(
            status=STATUS_ERROR,
            size=None,
            error_msg="recording removed from Frigate DB before swap",
        )
        return False

    # Atomic rename: sibling → primary path.  After this point, the primary
    # path holds tier-2 content and the sibling no longer exists.  Stat
    # the primary (= tier-1 file) BEFORE the rename so we can log the
    # tier-1→tier-2 size delta in the same shape as a chained ``t2:`` line.
    try:
        t1_size = filepath.stat().st_size
        t2_size = sib.stat().st_size
        sib.replace(filepath)
    except Exception as e:
        log(
            "ERROR",
            f"[{camera}] sibling→primary rename failed: {e}",
        )
        rec(status=STATUS_ERROR, size=None, error_msg=f"swap rename failed: {e}")
        return False

    # Source dims + target settings for the log line — same shape as the
    # chained ``t2:`` line.  ``swap`` in the duration column flags this
    # as a rename rather than its own encode pass.  Both lookups are
    # tolerant of NULL/missing data — log formatting falls back to
    # ``?x?`` / ``→?`` rather than failing the swap.
    with compress_db_lock:
        probe_row = compress_db.execute(
            "SELECT width, height FROM files WHERE recording_id = ?",
            (recording_id,),
        ).fetchone()
    src_info = _format_src_info(
        {"width": probe_row["width"], "height": probe_row["height"]}
        if probe_row is not None
        else None
    )
    t2_ts = getattr(cam_cfg.tier2, recording_type, None)
    tgt_info = _format_tgt_info(t2_ts) if t2_ts is not None else f"{'→?':<14}"
    pct = ((t1_size - t2_size) / t1_size * 100) if t1_size else 0.0
    log(
        "INFO",
        f"[{camera:<{cfg.cam_name_width}}] t2:{recording_type[:3]}  "
        f"{_display_path(filepath)}  {src_info}{tgt_info}"
        f"{_fmt(t1_size, 10)}→{_fmt(t2_size, 10)}  {pct:>3.0f}%  "
        f"{'swap':>6}  {0.0:>5.1f}s",
    )

    # Update Frigate's segment_size to the tier-2 file's size.
    new_size_mb = t2_size / (1024 * 1024)
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
            f"[{camera}] segment_size update after swap failed "
            f"— will retry at housekeeping: {e}",
        )

    rec(status=seg_status, size=t2_size, error_msg=seg_error)
    return True
