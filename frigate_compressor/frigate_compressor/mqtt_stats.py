# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Snapshot the compress DB into ``FrigateStats`` for MQTT publishing.

Pure DB → dataclass aggregation, no MQTT/paho dependency.  Read by the
publisher in ``mqtt.py`` and by ``benchmarks/io_repro.py``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from .context import CompressorContext
from .database import (
    STATUS_OK,
    STATUS_SEGMENT_UPDATE_FAILED,
)


@dataclass
class CameraStats:
    """Per-camera storage breakdown.  All byte values are bytes (not MB)."""

    total_bytes: int
    total_files: int
    continuous_bytes: int
    motion_bytes: int
    object_bytes: int
    tier0_bytes: int  # not yet compressed
    tier1_bytes: int
    tier2_bytes: int
    # Bytes of pre-encoded tier-2 files (``.t2.mp4``) parked alongside
    # tier-1 primaries — populated when ``t2_status='direct'``.  Folded
    # into ``total_bytes`` so disk-usage totals match ``du``; exposed
    # separately so dashboards can show how much disk will be reclaimed
    # when the next swap pass runs.
    tier2_pre_encoded_bytes: int
    oldest_age_days: float | None  # None when the camera has no recordings
    # Fresh bytes written by this camera in the last ``rate_window_seconds``
    # divided by the window — i.e. how fast the camera is generating video.
    # Unlike ``tier0_bytes_rate`` this is not a pool delta; it's a true
    # write rate derived from ``recordings.start_time`` and is never negative.
    recording_bytes_rate: float
    # Compression-backlog health.  ``tier1_backlog_error`` is True when
    # the oldest recording that is *eligible* for tier-1 promotion (age
    # past the camera's tier1.min_days) has been waiting more than
    # ``mqtt.backlog_timeout_seconds``; tier2 analogously for tier-1→2.
    # Both are False for tiers that are disabled for the camera.
    tier1_backlog_error: bool
    tier2_backlog_error: bool


@dataclass
class FrigateStats:
    """Top-level snapshot of Frigate's recording allocation."""

    total_bytes: int
    total_files: int
    oldest_age_days: float | None
    tier0_bytes: int
    tier1_bytes: int
    tier2_bytes: int
    tier2_pre_encoded_bytes: int
    cameras: dict[str, CameraStats]


# Bytes per MB used to convert Frigate's segment_size column (stored as MB,
# float) to whole bytes for the MQTT byte sensors.
_MB_BYTES = 1024 * 1024


def collect_frigate_stats(ctx: CompressorContext) -> FrigateStats:
    """Snapshot the compress DB's per-camera rollup for MQTT publishing.

    The bulk of the data comes from the materialised ``files_stats`` table,
    which is maintained transactionally by triggers on ``files``.  Instead
    of aggregating 800K+ rows per publish, we read a handful of rows here.
    Three supplementary queries fill in the pieces ``files_stats`` can't
    carry cheaply:

      * recent recording-bytes rate (needs ``recordings.start_time``)
      * oldest start_time per camera (can't maintain MIN under DELETE)
      * per-tier backlog existence (per-camera EXISTS with partial indexes)

    Runs on the shared ``ctx.compress_db`` connection (with Frigate attached
    as ``frigate``) under ``ctx.compress_db_lock``.  The whole snapshot is
    one critical section so all the queries see the same transaction
    snapshot — important for the rate-window arithmetic that ties the
    ``files_stats`` totals to a particular point-in-time.
    """
    cfg = ctx.cfg
    conn = ctx.compress_db
    now = time.time()
    rate_window = float(cfg.mqtt.rate_window_seconds)
    rate_cutoff = now - rate_window

    with ctx.compress_db_lock:
        # Per (camera, rtype) rollup from the materialised stats table.
        # files_stats carries tier0/tier1/tier2 bytes so each row is one
        # rtype bucket for a camera — the publisher fans it out into
        # CameraStats fields.
        stats_rows = conn.execute(
            """
            SELECT camera, rtype, files_count,
                   tier0_bytes, tier1_bytes, tier2_bytes, tier2_pre_encoded_bytes
            FROM files_stats
            """
        ).fetchall()
        # Fresh bytes per camera within the rate window.  ``INDEXED BY
        # recordings_start_time`` forces a range scan on the small recent
        # window (~minutes of rows) instead of the planner's default of
        # scanning ``recordings_camera`` over all 800K+ rows and
        # post-filtering on start_time.  Filtered on start_time so
        # in-flight segments (NULL segment_size) contribute 0 until
        # finalised — a small under-count that self-corrects on the
        # next publish.
        recent_rows = conn.execute(
            f"""
            SELECT
                camera                                          AS camera,
                SUM(COALESCE(segment_size, 0) * {_MB_BYTES})    AS bytes
            FROM frigate.recordings INDEXED BY recordings_start_time
            WHERE start_time >= ?
            GROUP BY camera
            """,
            (rate_cutoff,),
        ).fetchall()
        # Oldest start_time per camera.  One indexed seek per camera
        # against ``recordings_camera_start_time_end_time`` —
        # ``GROUP BY camera`` would force a full SCAN of the 800K+ row
        # covering index because the planner can't skip-scan MIN over a
        # DESC-ordered column.  We probe configured cameras plus any seen
        # in the rollup or rate-window queries; that's the same set the
        # publisher will end up reporting on downstream.
        oldest_by_camera: dict[str, float | None] = {}
        target_cameras = (
            set(cfg.cameras)
            | {row["camera"] for row in stats_rows}
            | {row["camera"] for row in recent_rows}
        )
        for cam_name in target_cameras:
            row = conn.execute(
                "SELECT MIN(start_time) FROM frigate.recordings WHERE camera = ?",
                (cam_name,),
            ).fetchone()
            if row is not None and row[0] is not None:
                oldest_by_camera[cam_name] = row[0]
        # Per-camera backlog existence checks.  The Python side only needs
        # to know whether *any* eligible recording is still pending past
        # the backlog timeout — an EXISTS question.
        #
        # Driven from ``files`` via the partial index
        # ``idx_files_t{1,2}_pending_age`` (camera, start_time WHERE
        # <pending>).  Filtering on ``f.start_time`` (denormalised from
        # Frigate at probe time) lets the planner do a 2-column range
        # seek (camera=X AND start_time<threshold) and short-circuit on
        # ``LIMIT 1``.  An earlier shape filtered start_time via EXISTS
        # against ``frigate.recordings`` — that walked every pending row
        # for the camera (~70K) before LIMIT could fire, because the
        # start_time predicate wasn't pushed into the index.
        #
        # Semantic: a recording must have a files row (i.e. have been
        # probed) for its backlog to be visible here.  Not-yet-probed
        # recordings are excluded — they're the probe loop's problem,
        # not the compressor's.
        backlog_timeout = float(cfg.mqtt.backlog_timeout_seconds)
        backlog_errors: dict[str, tuple[bool, bool]] = {}
        for cam_name, cam_cfg in cfg.cameras.items():
            if not cam_cfg.enabled:
                backlog_errors[cam_name] = (False, False)
                continue
            t1_err = False
            t2_err = False
            if cam_cfg.tier1.enabled:
                threshold = now - cam_cfg.tier1.min_days * 86400.0 - backlog_timeout
                row = conn.execute(
                    f"""
                    SELECT 1
                    FROM files f
                    WHERE f.camera = ?
                      AND f.start_time < ?
                      AND (f.t1_status IS NULL
                           OR f.t1_status NOT IN
                              ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}'))
                    LIMIT 1
                    """,
                    (cam_name, threshold),
                ).fetchone()
                t1_err = row is not None
            if cam_cfg.tier2.enabled:
                threshold = now - cam_cfg.tier2.min_days * 86400.0 - backlog_timeout
                row = conn.execute(
                    f"""
                    SELECT 1
                    FROM files f
                    WHERE f.camera = ?
                      AND f.start_time < ?
                      AND f.t1_status IN
                          ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                      AND (f.t2_status IS NULL
                           OR f.t2_status NOT IN
                              ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}'))
                    LIMIT 1
                    """,
                    (cam_name, threshold),
                ).fetchone()
                t2_err = row is not None
            backlog_errors[cam_name] = (t1_err, t2_err)

    recording_rate: dict[str, float] = {
        r["camera"]: float(r["bytes"] or 0) / rate_window for r in recent_rows
    }

    cameras: dict[str, dict] = {}
    top_total_bytes = 0
    top_total_files = 0
    top_tier_bytes = {0: 0, 1: 0, 2: 0}
    top_pre_encoded_bytes = 0
    top_oldest: float | None = None

    def _empty_cam() -> dict:
        return {
            "total_bytes": 0,
            "total_files": 0,
            "continuous_bytes": 0,
            "motion_bytes": 0,
            "object_bytes": 0,
            "tier0_bytes": 0,
            "tier1_bytes": 0,
            "tier2_bytes": 0,
            "tier2_pre_encoded_bytes": 0,
            "oldest": None,
        }

    for row in stats_rows:
        cam = row["camera"]
        rtype = row["rtype"]
        files = int(row["files_count"] or 0)
        tier0 = int(row["tier0_bytes"] or 0)
        tier1 = int(row["tier1_bytes"] or 0)
        tier2 = int(row["tier2_bytes"] or 0)
        pre_encoded = int(row["tier2_pre_encoded_bytes"] or 0)
        # ``total_bytes`` (and the per-rtype rollups it fans into) is the
        # actual disk footprint, so the pre-encoded files are part of it
        # — that way dashboards line up with ``du`` instead of
        # under-reporting by the pre-encoded pool size during the
        # (tier1.min_days, tier2.min_days) window.  ``tier1_bytes`` stays
        # primary-file-only so the per-tier breakdown still says where
        # each row's *primary* file lives.
        bucket_bytes = tier0 + tier1 + tier2 + pre_encoded

        c = cameras.setdefault(cam, _empty_cam())
        c["total_bytes"] += bucket_bytes
        c["total_files"] += files
        if rtype in ("continuous", "motion", "object"):
            c[f"{rtype}_bytes"] += bucket_bytes
        else:
            # Defensive: unexpected rtype goes to the continuous bucket so
            # nothing silently disappears from the top-level total.
            c["continuous_bytes"] += bucket_bytes
        c["tier0_bytes"] += tier0
        c["tier1_bytes"] += tier1
        c["tier2_bytes"] += tier2
        c["tier2_pre_encoded_bytes"] += pre_encoded

        top_total_bytes += bucket_bytes
        top_total_files += files
        top_tier_bytes[0] += tier0
        top_tier_bytes[1] += tier1
        top_tier_bytes[2] += tier2
        top_pre_encoded_bytes += pre_encoded

    # Stitch in the oldest start_time per camera (separate query).
    for cam_name, oldest in oldest_by_camera.items():
        c = cameras.setdefault(cam_name, _empty_cam())
        if oldest is not None:
            if c["oldest"] is None or oldest < c["oldest"]:
                c["oldest"] = oldest
            if top_oldest is None or oldest < top_oldest:
                top_oldest = oldest

    def _age(t: float | None) -> float | None:
        return (now - float(t)) / 86400.0 if t is not None else None

    cam_stats: dict[str, CameraStats] = {}
    for cam, c in cameras.items():
        t1_err, t2_err = backlog_errors.get(cam, (False, False))
        cam_stats[cam] = CameraStats(
            total_bytes=c["total_bytes"],
            total_files=c["total_files"],
            continuous_bytes=c["continuous_bytes"],
            motion_bytes=c["motion_bytes"],
            object_bytes=c["object_bytes"],
            tier0_bytes=c["tier0_bytes"],
            tier1_bytes=c["tier1_bytes"],
            tier2_bytes=c["tier2_bytes"],
            tier2_pre_encoded_bytes=c["tier2_pre_encoded_bytes"],
            oldest_age_days=_age(c["oldest"]),
            recording_bytes_rate=recording_rate.get(cam, 0.0),
            tier1_backlog_error=t1_err,
            tier2_backlog_error=t2_err,
        )

    return FrigateStats(
        total_bytes=top_total_bytes,
        total_files=top_total_files,
        oldest_age_days=_age(top_oldest),
        tier0_bytes=top_tier_bytes[0],
        tier1_bytes=top_tier_bytes[1],
        tier2_bytes=top_tier_bytes[2],
        tier2_pre_encoded_bytes=top_pre_encoded_bytes,
        cameras=cam_stats,
    )
