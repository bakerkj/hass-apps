# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for collect_frigate_stats, RateTracker, and MqttPublisher."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path

import frigate_compressor as fc

from fc_helpers import _make_config, _make_frigate_db, _open_compress_db


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

# Convenience: 1 MB in bytes (matches fc._MB_BYTES).
_MB = 1024 * 1024


def _insert_rec(
    conn: sqlite3.Connection,
    rid: str,
    camera: str,
    start_time: float,
    *,
    motion: int = 0,
    objects: int = 0,
    segment_size_mb: float = 1.0,
) -> None:
    """Insert a recording row with a populated segment_size."""
    conn.execute(
        "INSERT INTO recordings"
        " (id, camera, path, start_time, motion, objects, segment_size)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            rid,
            camera,
            f"/media/{camera}/{rid}.mp4",
            start_time,
            motion,
            objects,
            segment_size_mb,
        ),
    )
    conn.commit()


def _make_stats_ctx(
    tmp_path: Path, **cfg_overrides
) -> tuple[fc.CompressorContext, sqlite3.Connection]:
    """Build a CompressorContext suitable for collect_frigate_stats tests.

    Returns (ctx, frigate_writer) — keep the writer connection so the test
    can insert rows after the ctx is built.  Caller is responsible for
    closing both via _close_stats_ctx.  Extra kwargs are forwarded to
    ``_make_config`` (e.g. ``yaml_cameras=...``).
    """
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)  # writer for the test
    compress_conn = _open_compress_db(tmp_path)
    cfg = _make_config(tmp_path, frigate_db=str(frigate_db), **cfg_overrides)

    fc._attach_frigate(compress_conn, cfg, "frigate")

    ctx = fc.CompressorContext(
        cfg=cfg,
        compress_db=compress_conn,
    )
    return ctx, frigate_conn


def _close_stats_ctx(ctx: fc.CompressorContext, writer: sqlite3.Connection) -> None:
    ctx.compress_db.close()
    writer.close()


def _record_compressed(
    ctx: fc.CompressorContext,
    rid: str,
    camera: str,
    tier: int,
    *,
    status: str | None = None,
    size_before: int = 10 * _MB,
    size_after: int = 5 * _MB,
) -> None:
    """Insert a row into compress DB so collect_frigate_stats sees it as compressed.

    ``size_before`` / ``size_after`` default to 10 MB / 5 MB for tests
    that only care about the tier bucketing; tests asserting specific
    byte totals can override — stats now read post-compression bytes
    from ``t1_file_size`` / ``t2_file_size`` (via ``files_stats``), not
    Frigate's ``segment_size``.
    """
    fc._record(
        ctx.compress_db,
        recording_id=rid,
        camera=camera,
        path=f"/media/{camera}/{rid}.mp4",
        tier=tier,
        recording_type="motion",
        encoder="cpu",
        size_before=size_before,
        size_after=size_after,
        duration_sec=1.0,
        status=status or fc.STATUS_OK,
    )


def _record_probed(
    ctx: fc.CompressorContext,
    rid: str,
    camera: str,
    *,
    file_size: int = 0,
    recording_type: str | None = None,
    start_time: float | None = None,
) -> None:
    """Insert a probed-but-not-compressed row into the compress DB.

    Matches what the probe loop writes: ``scanned_at`` set, ``file_size``
    populated, ``recording_type`` set based on motion/objects classification,
    ``start_time`` denormalised from Frigate.  ``files_stats`` triggers
    rely on ``recording_type`` + ``file_size`` to bucket the row correctly,
    and the eligibility/backlog queries range-scan on ``start_time``, so
    tests simulating a probed recording need to pass these to mirror the
    real probe flow.
    """
    ctx.compress_db.execute(
        "INSERT OR IGNORE INTO files"
        " (recording_id, camera, path, recording_type, file_size,"
        "  start_time, scanned_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            rid,
            camera,
            f"/media/{camera}/{rid}.mp4",
            recording_type,
            file_size,
            start_time,
            "2026-01-01T00:00:00",
        ),
    )
    ctx.compress_db.commit()


def _rtype_for(motion: int, objects: int) -> str:
    """Mirror of ``_recording_type`` — local helper to avoid cross-file deps."""
    if objects:
        return "object"
    if motion:
        return "motion"
    return "continuous"


def _insert_probed(
    writer: sqlite3.Connection,
    ctx: fc.CompressorContext,
    rid: str,
    camera: str,
    start_time: float,
    *,
    motion: int = 0,
    objects: int = 0,
    segment_size_mb: float = 1.0,
) -> None:
    """Insert recording into Frigate DB AND a matching probed files row.

    This is what happens in production: Frigate writes the recording,
    the probe loop quickly catches up and writes a files row.  Stats
    are computed from ``files_stats`` (the materialised aggregate), so
    tests need the files row for bytes to show up in the rollup.
    """
    _insert_rec(
        writer,
        rid,
        camera,
        start_time,
        motion=motion,
        objects=objects,
        segment_size_mb=segment_size_mb,
    )
    _record_probed(
        ctx,
        rid,
        camera,
        file_size=int(segment_size_mb * _MB),
        recording_type=_rtype_for(motion, objects),
        start_time=start_time,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# collect_frigate_stats
# ═══════════════════════════════════════════════════════════════════════════════


def test_collect_stats_empty_db(tmp_path):
    ctx, writer = _make_stats_ctx(tmp_path)
    try:
        stats = fc.collect_frigate_stats(ctx)
        assert stats.total_bytes == 0
        assert stats.total_files == 0
        assert stats.tier0_bytes == 0
        assert stats.tier1_bytes == 0
        assert stats.tier2_bytes == 0
        assert stats.oldest_age_days is None
        assert stats.cameras == {}
    finally:
        _close_stats_ctx(ctx, writer)


def test_collect_stats_single_camera_uncompressed(tmp_path):
    ctx, writer = _make_stats_ctx(tmp_path)
    try:
        # 3 files: continuous (10 MB), motion (20 MB), object (30 MB) = 60 MB
        _insert_probed(
            writer, ctx, "r1", "front", time.time() - 86400, segment_size_mb=10
        )
        _insert_probed(
            writer,
            ctx,
            "r2",
            "front",
            time.time() - 86400,
            motion=5,
            segment_size_mb=20,
        )
        _insert_probed(
            writer,
            ctx,
            "r3",
            "front",
            time.time() - 86400,
            motion=5,
            objects=2,
            segment_size_mb=30,
        )
        stats = fc.collect_frigate_stats(ctx)
        assert stats.total_files == 3
        assert stats.total_bytes == 60 * _MB
        # All uncompressed → tier 0
        assert stats.tier0_bytes == 60 * _MB
        assert stats.tier1_bytes == 0
        assert stats.tier2_bytes == 0
        # Single camera
        assert set(stats.cameras.keys()) == {"front"}
        cs = stats.cameras["front"]
        assert cs.total_files == 3
        assert cs.total_bytes == 60 * _MB
        assert cs.continuous_bytes == 10 * _MB
        assert cs.motion_bytes == 20 * _MB
        assert cs.object_bytes == 30 * _MB
        assert cs.tier0_bytes == 60 * _MB
        assert cs.oldest_age_days is not None
        assert 0.9 < cs.oldest_age_days < 1.1
    finally:
        _close_stats_ctx(ctx, writer)


def test_collect_stats_recording_type_priority(tmp_path):
    """objects > motion > continuous; objects=0 motion>0 → motion."""
    ctx, writer = _make_stats_ctx(tmp_path)
    try:
        _insert_probed(writer, ctx, "a", "cam", time.time(), motion=0, objects=0)
        _insert_probed(writer, ctx, "b", "cam", time.time(), motion=3, objects=0)
        _insert_probed(writer, ctx, "c", "cam", time.time(), motion=3, objects=2)
        # Edge case: object>0 with motion=0 (still classified as object)
        _insert_probed(writer, ctx, "d", "cam", time.time(), motion=0, objects=1)
        stats = fc.collect_frigate_stats(ctx)
        cs = stats.cameras["cam"]
        assert cs.continuous_bytes == 1 * _MB
        assert cs.motion_bytes == 1 * _MB
        assert cs.object_bytes == 2 * _MB
    finally:
        _close_stats_ctx(ctx, writer)


def test_collect_stats_compressed_tier_split(tmp_path):
    ctx, writer = _make_stats_ctx(tmp_path)
    try:
        # Probe all four.  file_size on the probed row is what tier-0 uses;
        # _record_compressed overrides size_after to set the post-compression
        # bytes stored as t1_file_size / t2_file_size.
        _insert_probed(writer, ctx, "u1", "cam", time.time(), segment_size_mb=10)
        _insert_probed(writer, ctx, "t1a", "cam", time.time(), segment_size_mb=10)
        _insert_probed(writer, ctx, "t1b", "cam", time.time(), segment_size_mb=10)
        _insert_probed(writer, ctx, "t2a", "cam", time.time(), segment_size_mb=10)
        # Promote two to tier 1 (size_after=5 MB each), one to tier 2
        # (size_after=20 MB) so the totals match the expectation below.
        _record_compressed(ctx, "t1a", "cam", tier=1, size_after=5 * _MB)
        _record_compressed(ctx, "t1b", "cam", tier=1, size_after=5 * _MB)
        _record_compressed(ctx, "t2a", "cam", tier=2, size_after=20 * _MB)
        stats = fc.collect_frigate_stats(ctx)
        # 10 (tier0) + 5+5 (tier1) + 20 (tier2)
        assert stats.total_bytes == (10 + 5 + 5 + 20) * _MB
        assert stats.tier0_bytes == 10 * _MB
        assert stats.tier1_bytes == 10 * _MB
        assert stats.tier2_bytes == 20 * _MB
        cs = stats.cameras["cam"]
        assert cs.tier0_bytes == 10 * _MB
        assert cs.tier1_bytes == 10 * _MB
        assert cs.tier2_bytes == 20 * _MB
    finally:
        _close_stats_ctx(ctx, writer)


def test_collect_stats_direct_row_contributes_sibling_bytes(tmp_path):
    """A row in t2_status='direct' has a pre-encoded sibling .t2.mp4 on disk.
    Its bytes show up as ``tier2_pre_encoded_bytes`` and are folded into
    ``total_bytes`` so dashboards reflect the true on-disk footprint."""
    ctx, writer = _make_stats_ctx(tmp_path)
    try:
        _insert_probed(writer, ctx, "d", "cam", time.time(), segment_size_mb=10)
        # Land tier-1 at 5 MB primary and the sibling at 2 MB.
        _record_compressed(ctx, "d", "cam", tier=1, size_after=5 * _MB)
        _record_compressed(
            ctx,
            "d",
            "cam",
            tier=2,
            status=fc.STATUS_DIRECT,
            size_after=2 * _MB,
        )
        stats = fc.collect_frigate_stats(ctx)
        # Primary file is tier-1 (5 MB), sibling is 2 MB, no tier-2 yet.
        assert stats.tier1_bytes == 5 * _MB
        assert stats.tier2_bytes == 0
        assert stats.tier2_pre_encoded_bytes == 2 * _MB
        # total_bytes includes the sibling so it matches actual disk usage.
        assert stats.total_bytes == (5 + 2) * _MB
        cs = stats.cameras["cam"]
        assert cs.tier2_pre_encoded_bytes == 2 * _MB
        assert cs.total_bytes == (5 + 2) * _MB
    finally:
        _close_stats_ctx(ctx, writer)


def test_collect_stats_segment_update_failed_counts_as_compressed(tmp_path):
    """A row whose status is segment_update_failed has been compressed on disk
    even though Frigate's segment_size update failed — it must still be
    bucketed by its tier, not as tier 0."""
    ctx, writer = _make_stats_ctx(tmp_path)
    try:
        _insert_probed(writer, ctx, "x", "cam", time.time(), segment_size_mb=10)
        _record_compressed(
            ctx,
            "x",
            "cam",
            tier=2,
            status=fc.STATUS_SEGMENT_UPDATE_FAILED,
            size_after=8 * _MB,
        )
        stats = fc.collect_frigate_stats(ctx)
        assert stats.tier0_bytes == 0
        assert stats.tier2_bytes == 8 * _MB
    finally:
        _close_stats_ctx(ctx, writer)


def test_collect_stats_error_status_does_not_count_as_compressed(tmp_path):
    """status=error means we never produced an output — the row is still on
    disk in its original form, so it belongs in tier 0."""
    ctx, writer = _make_stats_ctx(tmp_path)
    try:
        _insert_probed(writer, ctx, "x", "cam", time.time(), segment_size_mb=4)
        _record_compressed(ctx, "x", "cam", tier=1, status=fc.STATUS_ERROR)
        stats = fc.collect_frigate_stats(ctx)
        assert stats.tier0_bytes == 4 * _MB
        assert stats.tier1_bytes == 0
    finally:
        _close_stats_ctx(ctx, writer)


def test_collect_stats_null_file_size_treated_as_zero(tmp_path):
    """A probed row with NULL file_size must not crash the aggregate and
    should contribute zero bytes."""
    ctx, writer = _make_stats_ctx(tmp_path)
    try:
        writer.execute(
            "INSERT INTO recordings (id, camera, path, start_time, motion, objects)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            ("nullrow", "cam", "/x", time.time(), 0, 0),
        )
        writer.commit()
        # Probe the row but leave file_size NULL.
        ctx.compress_db.execute(
            "INSERT INTO files (recording_id, camera, path, recording_type, scanned_at)"
            " VALUES (?, ?, ?, ?, ?)",
            ("nullrow", "cam", "/x", "continuous", "2026-01-01T00:00:00"),
        )
        ctx.compress_db.commit()
        stats = fc.collect_frigate_stats(ctx)
        assert stats.total_files == 1
        assert stats.total_bytes == 0
    finally:
        _close_stats_ctx(ctx, writer)


def test_collect_stats_multi_camera(tmp_path):
    ctx, writer = _make_stats_ctx(tmp_path)
    try:
        _insert_probed(
            writer, ctx, "a1", "front", time.time() - 86400, segment_size_mb=10
        )
        _insert_probed(
            writer, ctx, "b1", "back", time.time() - 2 * 86400, segment_size_mb=20
        )
        stats = fc.collect_frigate_stats(ctx)
        assert set(stats.cameras.keys()) == {"front", "back"}
        assert stats.cameras["front"].total_bytes == 10 * _MB
        assert stats.cameras["back"].total_bytes == 20 * _MB
        # Top-level oldest = oldest of any camera
        assert stats.oldest_age_days is not None
        assert 1.9 < stats.oldest_age_days < 2.1
    finally:
        _close_stats_ctx(ctx, writer)


# ═══════════════════════════════════════════════════════════════════════════════
# recording_bytes_rate (windowed camera write rate)
# ═══════════════════════════════════════════════════════════════════════════════


def test_recording_rate_counts_only_window(tmp_path):
    """Bytes inside rate_window_seconds count; older bytes are ignored."""
    ctx, writer = _make_stats_ctx(tmp_path)
    window = float(ctx.cfg.mqtt.rate_window_seconds)
    try:
        now = time.time()
        # 30 MB in window, 100 MB outside — only the 30 MB should count.
        _insert_rec(writer, "in1", "cam", now - 10, segment_size_mb=10)
        _insert_rec(writer, "in2", "cam", now - 60, segment_size_mb=20)
        _insert_rec(writer, "old", "cam", now - window - 60, segment_size_mb=100)
        stats = fc.collect_frigate_stats(ctx)
        cs = stats.cameras["cam"]
        assert cs.recording_bytes_rate == (30 * _MB) / window
    finally:
        _close_stats_ctx(ctx, writer)


def test_recording_rate_zero_when_no_recent_activity(tmp_path):
    """A camera with only old recordings has a zero write rate, not None."""
    ctx, writer = _make_stats_ctx(tmp_path)
    window = float(ctx.cfg.mqtt.rate_window_seconds)
    try:
        _insert_rec(writer, "old", "cam", time.time() - window - 3600)
        cs = fc.collect_frigate_stats(ctx).cameras["cam"]
        assert cs.recording_bytes_rate == 0.0
    finally:
        _close_stats_ctx(ctx, writer)


def test_recording_rate_per_camera_isolated(tmp_path):
    """Activity on one camera must not leak into another's rate."""
    ctx, writer = _make_stats_ctx(tmp_path, yaml_cameras={"busy": {}, "idle": {}})
    window = float(ctx.cfg.mqtt.rate_window_seconds)
    try:
        now = time.time()
        _insert_rec(writer, "live", "busy", now - 5, segment_size_mb=60)
        _insert_rec(writer, "old", "idle", now - window - 60, segment_size_mb=60)
        cams = fc.collect_frigate_stats(ctx).cameras
        assert cams["busy"].recording_bytes_rate == (60 * _MB) / window
        assert cams["idle"].recording_bytes_rate == 0.0
    finally:
        _close_stats_ctx(ctx, writer)


# ═══════════════════════════════════════════════════════════════════════════════
# tier1_backlog_error / tier2_backlog_error
# ═══════════════════════════════════════════════════════════════════════════════

# Defaults baked into tests/fc_helpers.py yaml_defaults: tier1.min_days=8,
# tier2.min_days=30, both enabled.  The default backlog timeout is 3600s.
_T1_MIN_DAYS = 8
_T2_MIN_DAYS = 30


def test_backlog_ok_when_no_recordings(tmp_path):
    ctx, writer = _make_stats_ctx(tmp_path)
    try:
        # Insert one for camera bookkeeping only (no pending work).
        _insert_rec(writer, "r1", "cam", time.time() - 3600)
        # Mark it as tier2 OK so neither tier has anything pending.
        _record_compressed(ctx, "r1", "cam", tier=1)
        _record_compressed(ctx, "r1", "cam", tier=2)
        cs = fc.collect_frigate_stats(ctx).cameras["cam"]
        assert cs.tier1_backlog_error is False
        assert cs.tier2_backlog_error is False
    finally:
        _close_stats_ctx(ctx, writer)


def test_backlog_ok_when_too_young_to_be_eligible(tmp_path):
    """A fresh recording isn't eligible yet → not a backlog, even if pending."""
    ctx, writer = _make_stats_ctx(tmp_path)
    try:
        # 1 day old: well under tier1.min_days=8.
        _insert_rec(writer, "r1", "cam", time.time() - 86400)
        cs = fc.collect_frigate_stats(ctx).cameras["cam"]
        assert cs.tier1_backlog_error is False
        assert cs.tier2_backlog_error is False
    finally:
        _close_stats_ctx(ctx, writer)


def test_backlog_ok_when_eligible_but_within_timeout(tmp_path):
    """Eligible + pending, but only slightly past cutoff → still OK."""
    ctx, writer = _make_stats_ctx(tmp_path)
    try:
        # 60s past the tier1 eligibility cutoff — well inside 3600s timeout.
        _insert_rec(writer, "r1", "cam", time.time() - _T1_MIN_DAYS * 86400 - 60)
        cs = fc.collect_frigate_stats(ctx).cameras["cam"]
        assert cs.tier1_backlog_error is False
    finally:
        _close_stats_ctx(ctx, writer)


def test_tier1_backlog_flags_when_past_timeout(tmp_path):
    """Eligible + pending + older than backlog_timeout → tier1_backlog_error ON."""
    ctx, writer = _make_stats_ctx(tmp_path)
    try:
        # 2 hours past the tier1 eligibility cutoff (default timeout: 1 hour).
        st = time.time() - _T1_MIN_DAYS * 86400 - 7200
        _insert_rec(writer, "r1", "cam", st)
        _record_probed(ctx, "r1", "cam", start_time=st)
        cs = fc.collect_frigate_stats(ctx).cameras["cam"]
        assert cs.tier1_backlog_error is True
        # tier2 doesn't consider r1 — r1 hasn't been promoted to tier1 yet.
        assert cs.tier2_backlog_error is False
    finally:
        _close_stats_ctx(ctx, writer)


def test_tier2_backlog_requires_tier1_compressed(tmp_path):
    """A recording only qualifies for tier2 backlog once tier1 is done."""
    ctx, writer = _make_stats_ctx(tmp_path)
    try:
        now = time.time()
        # Old enough for tier2 eligibility and past the 1-hour timeout.
        st = now - _T2_MIN_DAYS * 86400 - 7200
        _insert_rec(writer, "r1", "cam", st)
        _record_probed(ctx, "r1", "cam", start_time=st)
        # Without compression record → still tier0, tier2 doesn't flag.
        cs_before = fc.collect_frigate_stats(ctx).cameras["cam"]
        assert cs_before.tier1_backlog_error is True
        assert cs_before.tier2_backlog_error is False
        # Promote to tier1 → now tier2 pending & past timeout.
        _record_compressed(ctx, "r1", "cam", tier=1)
        cs_after = fc.collect_frigate_stats(ctx).cameras["cam"]
        assert cs_after.tier1_backlog_error is False
        assert cs_after.tier2_backlog_error is True
    finally:
        _close_stats_ctx(ctx, writer)


def test_backlog_respects_disabled_tier(tmp_path):
    """A disabled tier on a camera never reports a backlog even if files pending."""
    ctx, writer = _make_stats_ctx(tmp_path)
    try:
        # Force tier1 off for cam.  Mutate the resolved config in place.
        ctx.cfg.cameras["cam"].tier1.enabled = False
        _insert_rec(writer, "r1", "cam", time.time() - _T1_MIN_DAYS * 86400 - 7200)
        _record_probed(ctx, "r1", "cam")
        cs = fc.collect_frigate_stats(ctx).cameras["cam"]
        assert cs.tier1_backlog_error is False
    finally:
        _close_stats_ctx(ctx, writer)


def test_backlog_unknown_camera_is_ok(tmp_path):
    """A recording from a camera not in the resolved config does not alert."""
    ctx, writer = _make_stats_ctx(tmp_path)
    try:
        _insert_rec(writer, "r1", "ghost", time.time() - _T2_MIN_DAYS * 86400 - 7200)
        _record_probed(ctx, "r1", "ghost")
        cs = fc.collect_frigate_stats(ctx).cameras["ghost"]
        assert cs.tier1_backlog_error is False
        assert cs.tier2_backlog_error is False
    finally:
        _close_stats_ctx(ctx, writer)


def test_backlog_ignores_unprobed_recordings(tmp_path):
    """A recording that exists in Frigate but hasn't been probed yet (no
    files row) does NOT count as backlog — probe catch-up is the probe
    loop's responsibility, not the compression health sensor's."""
    ctx, writer = _make_stats_ctx(tmp_path)
    try:
        # Recording is old enough + past the timeout, but no files row.
        _insert_rec(writer, "r1", "cam", time.time() - _T1_MIN_DAYS * 86400 - 7200)
        # Intentionally do NOT call _record_probed.
        cs = fc.collect_frigate_stats(ctx).cameras["cam"]
        assert cs.tier1_backlog_error is False
        assert cs.tier2_backlog_error is False
    finally:
        _close_stats_ctx(ctx, writer)


def test_backlog_respects_custom_timeout(tmp_path):
    """Changing mqtt.backlog_timeout_seconds shifts the threshold."""
    ctx, writer = _make_stats_ctx(tmp_path)
    try:
        # 120s past the tier1 eligibility cutoff.
        st = time.time() - _T1_MIN_DAYS * 86400 - 120
        _insert_rec(writer, "r1", "cam", st)
        _record_probed(ctx, "r1", "cam", start_time=st)
        # With a 60s timeout the 120s-past-cutoff file becomes a backlog.
        ctx.cfg.mqtt.backlog_timeout_seconds = 60
        cs = fc.collect_frigate_stats(ctx).cameras["cam"]
        assert cs.tier1_backlog_error is True
    finally:
        _close_stats_ctx(ctx, writer)


# ═══════════════════════════════════════════════════════════════════════════════
# RateTracker
# ═══════════════════════════════════════════════════════════════════════════════


def test_rate_tracker_returns_none_on_first_sample():
    rt = fc.RateTracker(window_seconds=300)
    assert rt.update("k", 100, now=1000) is None


def test_rate_tracker_simple_delta():
    rt = fc.RateTracker(window_seconds=300)
    rt.update("k", 0, now=0)
    # 100 units gained over 60s → 100/60 ≈ 1.6667 B/s
    rate = rt.update("k", 100, now=60)
    assert rate is not None
    assert abs(rate - (100.0 / 60.0)) < 1e-9


def test_rate_tracker_signed_negative():
    rt = fc.RateTracker(window_seconds=300)
    rt.update("k", 1000, now=0)
    rate = rt.update("k", 700, now=60)
    assert rate is not None
    assert rate < 0
    # Lost 300 units in 60s → -5.0 B/s
    assert abs(rate - (-5.0)) < 1e-9


def test_rate_tracker_window_drops_old_samples():
    """Samples older than the window must be dropped before computing the rate."""
    rt = fc.RateTracker(window_seconds=300)
    rt.update("k", 0, now=0)  # will fall outside window
    rt.update("k", 600, now=400)  # 400 > 300 → previous sample dropped
    # After dropping, only one sample remains → return None
    # (we just appended (400, 600); previous (0, 0) is < 400-300=100)
    rate = rt.update("k", 700, now=460)
    # Now samples = [(400, 600), (460, 700)]; rate = 100/60 ≈ 1.6667 B/s
    assert rate is not None
    assert abs(rate - (100.0 / 60.0)) < 1e-9


def test_rate_tracker_keys_independent():
    rt = fc.RateTracker(window_seconds=300)
    rt.update("a", 0, now=0)
    rt.update("b", 1000, now=0)
    ra = rt.update("a", 60, now=60)
    rb = rt.update("b", 940, now=60)
    assert ra is not None and ra > 0
    assert rb is not None and rb < 0


def test_rate_tracker_zero_dt_returns_none():
    rt = fc.RateTracker(window_seconds=300)
    rt.update("k", 0, now=100)
    # Same timestamp → dt=0 → None
    assert rt.update("k", 50, now=100) is None


# ═══════════════════════════════════════════════════════════════════════════════
# MqttPublisher (uses paho stub from conftest.py)
# ═══════════════════════════════════════════════════════════════════════════════


class _RecordingClient:
    """Stub paho client that records every publish for assertions."""

    def __init__(self, *args, **kwargs):
        self.publishes: list[tuple[str, str, bool]] = []  # (topic, payload, retain)
        self.on_connect = None
        self.on_disconnect = None
        self.on_message = None

    def publish(self, topic, payload="", qos=0, retain=False):
        self.publishes.append((topic, payload, retain))
        m = type("R", (), {})()
        m.rc = 0
        m.mid = 0
        return m

    def connect(self, *args, **kwargs):
        pass

    def loop_start(self):
        pass

    def loop_stop(self):
        pass

    def disconnect(self):
        pass

    def username_pw_set(self, *args, **kwargs):
        pass

    def will_set(self, *args, **kwargs):
        pass

    def reconnect_delay_set(self, *args, **kwargs):
        pass

    def subscribe(self, *args, **kwargs):
        pass


def _build_publisher(
    tmp_path: Path,
    monkeypatch,
    insert_rows: bool = True,
) -> tuple[
    fc.MqttPublisher, fc.CompressorContext, sqlite3.Connection, _RecordingClient
]:
    ctx, writer = _make_stats_ctx(tmp_path)

    if insert_rows:
        _insert_rec(writer, "u1", "front", time.time(), segment_size_mb=10)
        _insert_rec(writer, "u2", "front", time.time(), motion=1, segment_size_mb=20)
        _insert_rec(writer, "u3", "back", time.time(), segment_size_mb=5)

    mqtt_cfg = fc.MqttConfig(
        host="example",
        port=1883,
        username="",
        password="",
        discovery_prefix="homeassistant",
        base_topic="frigate_compressor",
        client_id="test-client",
        publish_interval_seconds=60,
        rate_window_seconds=300,
    )
    stopping = threading.Event()
    publisher = fc.MqttPublisher(ctx, mqtt_cfg, stopping)
    client = _RecordingClient()
    publisher.client = client  # bypass start() so the loop thread isn't spun
    return publisher, ctx, writer, client


def test_publisher_publishes_top_and_per_camera_state(tmp_path, monkeypatch):
    publisher, ctx, writer, client = _build_publisher(tmp_path, monkeypatch)
    try:
        publisher.publish_once()
        topics = {t for t, _, _ in client.publishes}
        # Top-level state
        assert "frigate_compressor/storage/total_bytes/state" in topics
        assert "frigate_compressor/storage/total_files/state" in topics
        assert "frigate_compressor/storage/tier0_bytes/state" in topics
        # Per-camera state (slug = camera name)
        assert "frigate_compressor/front/total_bytes/state" in topics
        assert "frigate_compressor/back/total_bytes/state" in topics
        assert "frigate_compressor/front/motion_bytes/state" in topics
    finally:
        _close_stats_ctx(ctx, writer)


def test_publisher_publishes_discovery_once_per_device(tmp_path, monkeypatch):
    publisher, ctx, writer, client = _build_publisher(tmp_path, monkeypatch)
    try:
        publisher.publish_once()
        first_discovery = [
            (t, p)
            for t, p, _ in client.publishes
            if t.startswith("homeassistant/sensor/")
        ]
        # 12 top-level + 19 per-camera-sensor × 2 cameras = 50 (plain) sensors.
        # Binary sensors (tier1/tier2 backlog) are routed to binary_sensor/.
        assert len(first_discovery) == 12 + 19 * 2
        binary_discovery = [
            (t, p)
            for t, p, _ in client.publishes
            if t.startswith("homeassistant/binary_sensor/")
        ]
        # 2 backlog binary_sensors × 2 cameras
        assert len(binary_discovery) == 2 * 2
        # Payload shape for a binary_sensor: payload_on/off and no state_class.
        backlog_cfg = json.loads(binary_discovery[0][1])
        assert backlog_cfg["device_class"] == "problem"
        assert backlog_cfg["payload_on"] == "ON"
        assert backlog_cfg["payload_off"] == "OFF"
        assert "state_class" not in backlog_cfg

        # Verify one discovery payload schema
        top_total = next(
            json.loads(p)
            for t, p in first_discovery
            if t.endswith("frigate_compressor_storage/total_bytes/config")
        )
        assert top_total["unit_of_measurement"] == "B"
        assert top_total["device_class"] == "data_size"
        assert top_total["state_class"] == "measurement"
        assert top_total["unique_id"] == "frigate_compressor_storage_total_bytes"
        assert top_total["device"]["identifiers"] == ["frigate_compressor_storage"]

        # Second pass: discovery already published → no new discovery topics
        before = len(client.publishes)
        publisher.publish_once()
        after_discovery = [
            (t, p)
            for t, p, _ in client.publishes[before:]
            if t.startswith("homeassistant/sensor/")
        ]
        assert after_discovery == []
    finally:
        _close_stats_ctx(ctx, writer)


def test_publisher_rate_sensors_appear_after_second_pass(tmp_path, monkeypatch):
    publisher, ctx, writer, client = _build_publisher(tmp_path, monkeypatch)
    try:
        publisher.publish_once()
        # First pass: tracker has 1 sample → no tracker-derived rate state
        # published.  ``recording_bytes_rate`` is exempt — it's a windowed
        # SQL-side measurement that publishes on every pass.
        rate_topics_first = [
            t
            for t, _, _ in client.publishes
            if "_rate/state" in t and not t.endswith("recording_bytes_rate/state")
        ]
        assert rate_topics_first == []

        # Mutate the underlying data so the second pass sees a delta.
        _insert_rec(writer, "u4", "front", time.time(), segment_size_mb=50)
        # Force a sleep so the dt > 0 in the rate tracker.
        time.sleep(0.01)

        before = len(client.publishes)
        publisher.publish_once()
        rate_topics_second = [
            t for t, _, _ in client.publishes[before:] if "_rate/state" in t
        ]
        # At minimum total_bytes_rate (top + front camera) should appear.
        assert any(
            t.endswith("storage/total_bytes_rate/state") for t in rate_topics_second
        )
        assert any(
            t.endswith("front/total_bytes_rate/state") for t in rate_topics_second
        )
    finally:
        _close_stats_ctx(ctx, writer)


def test_publisher_publishes_backlog_binary_state(tmp_path, monkeypatch):
    """Backlog booleans are serialized to the MQTT state topic as ON/OFF."""
    publisher, ctx, writer, client = _build_publisher(
        tmp_path, monkeypatch, insert_rows=False
    )
    try:
        # 'cam' is in the resolved config with tier1.min_days=8 and tier2
        # enabled by default in the test defaults.  Insert a file well past
        # tier1 eligibility and the backlog timeout, probed but not yet
        # compressed.
        st = time.time() - _T1_MIN_DAYS * 86400 - 7200
        _insert_rec(writer, "r1", "cam", st)
        _record_probed(ctx, "r1", "cam", start_time=st)
        publisher.publish_once()
        by_topic = {t: p for t, p, _ in client.publishes}
        assert by_topic["frigate_compressor/cam/tier1_backlog_error/state"] == "ON"
        # r1 is still at tier 0 → tier2 pending predicate doesn't fire.
        assert by_topic["frigate_compressor/cam/tier2_backlog_error/state"] == "OFF"
    finally:
        _close_stats_ctx(ctx, writer)


def test_publisher_publishes_recording_rate_on_first_pass(tmp_path, monkeypatch):
    """Per-camera recording_bytes_rate is a fresh windowed measurement and
    therefore reaches MQTT on the very first publish pass (unlike the
    RateTracker-derived _rate sensors)."""
    publisher, ctx, writer, client = _build_publisher(
        tmp_path, monkeypatch, insert_rows=False
    )
    window = float(ctx.cfg.mqtt.rate_window_seconds)
    try:
        now = time.time()
        _insert_rec(writer, "fresh", "cam", now - 5, segment_size_mb=30)
        _insert_rec(writer, "old", "cam", now - window - 60, segment_size_mb=100)
        publisher.publish_once()

        by_topic = {t: p for t, p, _ in client.publishes}
        expected = f"{(30 * _MB) / window:.6g}"
        assert by_topic["frigate_compressor/cam/recording_bytes_rate/state"] == expected
    finally:
        _close_stats_ctx(ctx, writer)


def test_publisher_camera_slug_handles_special_chars(tmp_path, monkeypatch):
    publisher, ctx, writer, client = _build_publisher(
        tmp_path, monkeypatch, insert_rows=False
    )
    try:
        # Camera names with spaces, dots, and dashes — must be slugified.
        _insert_rec(writer, "x1", "Front Door", time.time())
        _insert_rec(writer, "x2", "back.lot-2", time.time())
        publisher.publish_once()
        topics = {t for t, _, _ in client.publishes}
        assert "frigate_compressor/front_door/total_bytes/state" in topics
        assert "frigate_compressor/back_lot_2/total_bytes/state" in topics
    finally:
        _close_stats_ctx(ctx, writer)


def test_slugify_camera_helper():
    assert fc._slugify_camera("Front Door") == "front_door"
    assert fc._slugify_camera("back.lot-2") == "back_lot_2"
    assert fc._slugify_camera("___A___") == "a"
    assert fc._slugify_camera("") == "unknown"


# ═══════════════════════════════════════════════════════════════════════════════
# MqttConfig + Config integration
# ═══════════════════════════════════════════════════════════════════════════════


def test_mqtt_disabled_when_host_empty(tmp_path):
    cfg = _make_config(tmp_path)
    assert cfg.mqtt.enabled is False


def test_mqtt_enabled_when_host_set(tmp_path):
    cfg = _make_config(tmp_path, mqtt_host="broker.local")
    assert cfg.mqtt.enabled is True
    assert cfg.mqtt.host == "broker.local"
    assert cfg.mqtt.port == 1883


def test_mqtt_options_loaded(tmp_path):
    cfg = _make_config(
        tmp_path,
        mqtt_host="broker.local",
        mqtt_port=2883,
        mqtt_username="user",
        mqtt_password="pw",
        mqtt_base_topic="custom_topic",
        mqtt_publish_interval_seconds=120,
        rate_window_seconds=600,
        mqtt_disconnect_timeout_seconds=450,
    )
    assert cfg.mqtt.host == "broker.local"
    assert cfg.mqtt.port == 2883
    assert cfg.mqtt.username == "user"
    assert cfg.mqtt.password == "pw"
    assert cfg.mqtt.base_topic == "custom_topic"
    assert cfg.mqtt.publish_interval_seconds == 120
    assert cfg.mqtt.rate_window_seconds == 600
    assert cfg.mqtt.disconnect_timeout_seconds == 450


def test_mqtt_disconnect_timeout_defaults_to_300(tmp_path):
    cfg = _make_config(tmp_path, mqtt_host="broker.local")
    assert cfg.mqtt.disconnect_timeout_seconds == 300


# ═══════════════════════════════════════════════════════════════════════════════
# MqttHealth + watchdogs
# ═══════════════════════════════════════════════════════════════════════════════


def test_health_marked_connected_on_successful_publish(tmp_path, monkeypatch):
    """A successful state publish should stamp last_state_publish_ok."""
    publisher, ctx, writer, _client = _build_publisher(tmp_path, monkeypatch)
    try:
        assert publisher.health.last_state_publish_ok == 0.0
        publisher.publish_once()
        assert publisher.health.last_state_publish_ok > 0.0
    finally:
        _close_stats_ctx(ctx, writer)


def test_watchdog_exits_11_after_long_disconnect(tmp_path, monkeypatch):
    publisher, ctx, writer, _client = _build_publisher(tmp_path, monkeypatch)
    try:
        publisher.mqtt_cfg.disconnect_timeout_seconds = 60
        publisher.health.connected = False
        publisher.health.last_disconnect = 1000.0
        # 1100 - 1000 = 100s > 60s timeout
        assert publisher._check_watchdogs(now=1100.0) is True
        assert publisher.exit_code == 11
        assert publisher.stopping.is_set()
    finally:
        _close_stats_ctx(ctx, writer)


def test_watchdog_does_not_exit_while_connected(tmp_path, monkeypatch):
    publisher, ctx, writer, _client = _build_publisher(tmp_path, monkeypatch)
    try:
        publisher.mqtt_cfg.disconnect_timeout_seconds = 60
        publisher.health.connected = True
        publisher.health.last_disconnect = 1000.0  # irrelevant while connected
        publisher.health.last_state_publish_ok = 1090.0
        assert publisher._check_watchdogs(now=1100.0) is False
        assert publisher.exit_code is None
        assert not publisher.stopping.is_set()
    finally:
        _close_stats_ctx(ctx, writer)


def test_watchdog_exits_12_on_state_publish_stall(tmp_path, monkeypatch):
    publisher, ctx, writer, _client = _build_publisher(tmp_path, monkeypatch)
    try:
        # stall_timeout = max(60, publish_interval_seconds * 4) = 240 with default 60s
        publisher.health.connected = True
        publisher.health.last_state_publish_ok = 1000.0
        # 1300 - 1000 = 300s > 240s stall timeout
        assert publisher._check_watchdogs(now=1300.0) is True
        assert publisher.exit_code == 12
        assert publisher.stopping.is_set()
    finally:
        _close_stats_ctx(ctx, writer)


def test_watchdog_ignores_stall_before_first_publish(tmp_path, monkeypatch):
    """If we've never published yet, the stall watchdog must not fire."""
    publisher, ctx, writer, _client = _build_publisher(tmp_path, monkeypatch)
    try:
        publisher.health.connected = True
        publisher.health.last_state_publish_ok = 0.0
        assert publisher._check_watchdogs(now=100000.0) is False
        assert publisher.exit_code is None
    finally:
        _close_stats_ctx(ctx, writer)
