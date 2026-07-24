# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Periodic housekeeping: temp cleanup, segment retries, prune, summary logs."""

import sqlite3
import threading
from pathlib import Path

from .config import Config
from .context import CompressorContext
from .database import (
    STATUS_DIRECT,
    STATUS_OK,
    STATUS_SEGMENT_UPDATE_FAILED,
)
from .ffmpeg import _TEMP_GLOB
from .paths import delete_sibling
from .util import _display_path, _fmt, log


def run_housekeeping(ctx: CompressorContext) -> None:
    """Top-level housekeeping entrypoint.

    Runs on the main loop thread once per ``housekeeping_interval_days``,
    using the shared ``ctx.compress_db`` (with Frigate attached as
    ``frigate``).  Each subroutine acquires the lock around its work —
    pieces are independent enough that holding the lock for the whole
    pass would needlessly block the probe loop and workers for a
    noticeable stretch.
    """
    _run_housekeeping_inner(
        ctx.cfg,
        ctx.compress_db,
        ctx.compress_db_lock,
    )


def _hk_remove_temp_files(cfg: Config) -> None:
    """Remove leftover temp files from crashed compression runs."""
    temp_files = list(Path(cfg.recordings_dir).rglob(_TEMP_GLOB))
    for tmp in temp_files:
        if cfg.all_dry_run:
            log("INFO", f"DRY RUN: Would remove leftover temp file: {tmp}")
        else:
            log("WARNING", f"Removing leftover temp file: {tmp}")
            tmp.unlink(missing_ok=True)
    if not temp_files:
        log("DEBUG", "No leftover temp files found")


def _hk_retry_segment_updates(
    cfg: Config,
    compress_db: sqlite3.Connection,
    compress_db_lock: threading.Lock,
) -> None:
    """Retry segment_size updates that failed during prior compress runs."""
    with compress_db_lock:
        pending_seg = compress_db.execute(
            """SELECT recording_id, camera, path
               FROM files
               WHERE t1_status = ? OR t2_status = ?""",
            (STATUS_SEGMENT_UPDATE_FAILED, STATUS_SEGMENT_UPDATE_FAILED),
        ).fetchall()

    retried = promoted = 0
    for row in pending_seg:
        retried += 1
        fpath = Path(row["path"])
        if not fpath.exists():
            log(
                "DEBUG",
                f"[{row['camera']}] segment_update_failed file no longer on disk, skipping: {_display_path(fpath)}",
            )
            continue
        actual_size_mb = fpath.stat().st_size / (1024 * 1024)
        if cfg.all_dry_run:
            log(
                "INFO",
                f"[{row['camera']}] DRY RUN: would retry segment_size update"
                f" ({actual_size_mb:.3f}MB): {_display_path(fpath)}",
            )
            continue
        try:
            log(
                "DEBUG",
                f"[{row['camera']}] Retrying segment_size update ({actual_size_mb:.3f}MB): {_display_path(fpath)}",
            )
            with compress_db_lock:
                # Both writes happen on the same connection in the same
                # transaction — if either fails the whole row stays
                # ``segment_update_failed`` for the next run.
                compress_db.execute(
                    "UPDATE frigate.recordings SET segment_size = ? WHERE id = ?",
                    (actual_size_mb, row["recording_id"]),
                )
                compress_db.execute(
                    """UPDATE files SET
                         t1_status = CASE WHEN t1_status = ? THEN ? ELSE t1_status END,
                         t1_error_msg = CASE WHEN t1_status = ? THEN NULL ELSE t1_error_msg END,
                         t2_status = CASE WHEN t2_status = ? THEN ? ELSE t2_status END,
                         t2_error_msg = CASE WHEN t2_status = ? THEN NULL ELSE t2_error_msg END
                       WHERE recording_id = ?""",
                    (
                        STATUS_SEGMENT_UPDATE_FAILED,
                        STATUS_OK,
                        STATUS_SEGMENT_UPDATE_FAILED,
                        STATUS_SEGMENT_UPDATE_FAILED,
                        STATUS_OK,
                        STATUS_SEGMENT_UPDATE_FAILED,
                        row["recording_id"],
                    ),
                )
                compress_db.commit()
            promoted += 1
            log(
                "INFO",
                f"[{row['camera']}] retried segment_size update — ok: {_display_path(fpath)}",
            )
        except sqlite3.Error as e:
            log(
                "WARNING",
                f"[{row['camera']}] segment_size retry failed again: {e}",
            )

    if retried:
        log(
            "INFO",
            f"segment_size retries: {retried} attempted, {promoted} promoted to ok",
        )
    else:
        log("DEBUG", "No pending segment_size retries")


def _hk_cleanup_orphan_t2_siblings(
    cfg: Config,
    compress_db: sqlite3.Connection,
    compress_db_lock: threading.Lock,
) -> None:
    """Delete sibling tier-2 files for rows whose recording is gone from Frigate.

    Frigate's retention deletes the primary ``.mp4`` segment but doesn't
    know about our sibling ``.t2.mp4`` (it's not in its recordings table).
    We need to clean those up before :func:`_hk_prune_orphaned` removes
    the compress-db row.
    """
    with compress_db_lock:
        rows = compress_db.execute(
            """
            SELECT recording_id, camera, path FROM files
            WHERE t2_status = ?
              AND recording_id NOT IN (SELECT id FROM frigate.recordings)
            """,
            (STATUS_DIRECT,),
        ).fetchall()
    if not rows:
        log("DEBUG", "No orphan tier-2 sibling files to clean up")
        return
    deleted = 0
    for row in rows:
        primary = Path(row["path"])
        if cfg.all_dry_run:
            log(
                "INFO",
                f"[{row['camera']}] DRY RUN: would delete orphan sibling for "
                f"{_display_path(primary)}",
            )
            continue
        try:
            if delete_sibling(primary):
                deleted += 1
                log(
                    "DEBUG",
                    f"[{row['camera']}] deleted orphan sibling for "
                    f"{_display_path(primary)}",
                )
        except OSError as e:
            log(
                "WARNING",
                f"[{row['camera']}] failed to delete orphan sibling for "
                f"{_display_path(primary)}: {e}",
            )
    if deleted:
        log("INFO", f"Deleted {deleted} orphan tier-2 sibling files")


def _hk_prune_orphaned(
    cfg: Config,
    compress_db: sqlite3.Connection,
    compress_db_lock: threading.Lock,
) -> None:
    """Drop compress_db rows whose recording is no longer in Frigate's DB.

    Uses the persistent ``frigate`` schema attached at startup — no
    per-call ATTACH/DETACH.
    """
    pruned = 0
    with compress_db_lock:
        if cfg.all_dry_run:
            pruned = compress_db.execute(
                """
                SELECT COUNT(*) FROM files
                WHERE recording_id NOT IN (
                    SELECT id FROM frigate.recordings
                )
                """
            ).fetchone()[0]
        else:
            log("INFO", "Pruning orphaned compress DB entries")
            cursor = compress_db.execute(
                """
                DELETE FROM files
                WHERE recording_id NOT IN (
                    SELECT id FROM frigate.recordings
                )
                """
            )
            pruned = cursor.rowcount
            compress_db.commit()
    if pruned:
        prefix = "DRY RUN: Would prune" if cfg.all_dry_run else "Pruned"
        log("INFO", f"{prefix} {pruned} orphaned DB entries")
    else:
        log("DEBUG", "No orphaned DB entries")


def _hk_log_savings_summary(
    compress_db: sqlite3.Connection,
    compress_db_lock: threading.Lock,
) -> None:
    """Print the storage-savings table by camera."""
    with compress_db_lock:
        rows = compress_db.execute(
            "SELECT * FROM savings_by_camera ORDER BY camera"
        ).fetchall()
    if not rows:
        log("INFO", "No compression data yet")
        return
    log("INFO", "── Storage savings by camera")
    log(
        "INFO",
        f"  {'Camera':<20} {'T1':>5} {'T1 Before':>10} {'T1 After':>10}"
        f" {'T2':>5} {'T2 Before':>10} {'T2 After':>10}",
    )
    log(
        "INFO",
        f"  {'-' * 20} {'-' * 5} {'-' * 10} {'-' * 10} {'-' * 5} {'-' * 10} {'-' * 10}",
    )
    for r in rows:
        log(
            "INFO",
            f"  {r['camera']:<20}"
            f" {r['t1_files'] or 0:>5}"
            f" {_fmt(r['t1_bytes_before']):>10}"
            f" {_fmt(r['t1_bytes_after']):>10}"
            f" {r['t2_files'] or 0:>5}"
            f" {_fmt(r['t2_bytes_before']):>10}"
            f" {_fmt(r['t2_bytes_after']):>10}",
        )


def _hk_log_recent_errors(
    compress_db: sqlite3.Connection,
    compress_db_lock: threading.Lock,
) -> None:
    """Print the most recent compression errors (last 7 days, up to 20)."""
    with compress_db_lock:
        errors = compress_db.execute("SELECT * FROM recent_errors LIMIT 20").fetchall()
    if not errors:
        return
    log("WARNING", "── Recent errors (last 7 days)")
    for err in errors:
        ts = err["t2_compressed_at"] or err["t1_compressed_at"]
        msg = err["t2_error_msg"] or err["t1_error_msg"]
        log("WARNING", f"  [{ts}] {err['camera']} | {msg}")


def _hk_sqlite_maintenance(
    compress_db: sqlite3.Connection,
    compress_db_lock: threading.Lock,
) -> None:
    """Refresh planner stats and truncate the WAL.

    ``PRAGMA optimize`` runs ANALYZE only on tables/indexes that have
    changed enough to benefit; the ``analysis_limit`` set in
    ``open_compress_db`` keeps each ANALYZE bounded.

    ``PRAGMA wal_checkpoint(TRUNCATE)`` flushes the WAL into the main
    DB and shrinks the ``-wal`` file to zero bytes.  Without this the
    WAL is bounded by autocheckpoint but never shrinks below its
    high-water mark, so the prune burst can leave it sitting fat.
    """
    with compress_db_lock:
        compress_db.execute("PRAGMA optimize")
        row = compress_db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    if row is not None:
        busy, log_pages, ckpt_pages = row[0], row[1], row[2]
        log(
            "DEBUG",
            f"SQLite maintenance: wal_checkpoint busy={busy} "
            f"log_pages={log_pages} ckpt_pages={ckpt_pages}",
        )


def _run_housekeeping_inner(
    cfg: Config,
    compress_db: sqlite3.Connection,
    compress_db_lock: threading.Lock,
) -> None:
    log("INFO", "── Housekeeping starting")
    _hk_remove_temp_files(cfg)
    _hk_retry_segment_updates(cfg, compress_db, compress_db_lock)
    _hk_cleanup_orphan_t2_siblings(cfg, compress_db, compress_db_lock)
    _hk_prune_orphaned(cfg, compress_db, compress_db_lock)
    _hk_log_savings_summary(compress_db, compress_db_lock)
    _hk_log_recent_errors(compress_db, compress_db_lock)
    _hk_sqlite_maintenance(compress_db, compress_db_lock)
    log("INFO", "── Housekeeping complete")
