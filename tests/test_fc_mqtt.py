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


def _make_stats_ctx(tmp_path: Path) -> tuple[fc.CompressorContext, sqlite3.Connection]:
    """Build a CompressorContext suitable for collect_frigate_stats tests.

    Returns (ctx, frigate_writer) — keep the writer connection so the test
    can insert rows after the ctx is built.  Caller is responsible for
    closing both via _close_stats_ctx.
    """
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)  # writer for the test
    compress_conn = _open_compress_db(tmp_path)
    cfg = _make_config(tmp_path, frigate_db=str(frigate_db))

    frigate_ro = sqlite3.connect(str(frigate_db))
    frigate_ro.row_factory = sqlite3.Row
    frigate_rw = sqlite3.connect(str(frigate_db))
    frigate_rw.row_factory = sqlite3.Row

    ctx = fc.CompressorContext(
        cfg=cfg,
        compress_db=compress_conn,
        db_lock=threading.Lock(),
        frigate_ro=frigate_ro,
        frigate_ro_lock=threading.Lock(),
        frigate_rw=frigate_rw,
        frigate_lock=threading.Lock(),
    )
    return ctx, frigate_conn


def _close_stats_ctx(ctx: fc.CompressorContext, writer: sqlite3.Connection) -> None:
    ctx.compress_db.close()
    ctx.frigate_ro.close()
    ctx.frigate_rw.close()
    writer.close()


def _record_compressed(
    ctx: fc.CompressorContext,
    rid: str,
    camera: str,
    tier: int,
    *,
    status: str | None = None,
) -> None:
    """Insert a row into compress DB so collect_frigate_stats sees it as compressed."""
    fc._record(
        ctx.compress_db,
        ctx.db_lock,
        recording_id=rid,
        camera=camera,
        path=f"/media/{camera}/{rid}.mp4",
        tier=tier,
        recording_type="motion",
        encoder="cpu",
        size_before=10 * _MB,
        size_after=5 * _MB,
        duration_sec=1.0,
        status=status or fc.STATUS_OK,
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
        _insert_rec(writer, "r1", "front", time.time() - 86400, segment_size_mb=10)
        _insert_rec(
            writer, "r2", "front", time.time() - 86400, motion=5, segment_size_mb=20
        )
        _insert_rec(
            writer,
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
        _insert_rec(writer, "a", "cam", time.time(), motion=0, objects=0)
        _insert_rec(writer, "b", "cam", time.time(), motion=3, objects=0)
        _insert_rec(writer, "c", "cam", time.time(), motion=3, objects=2)
        # Edge case: object>0 with motion=0 (still classified as object)
        _insert_rec(writer, "d", "cam", time.time(), motion=0, objects=1)
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
        _insert_rec(writer, "u1", "cam", time.time(), segment_size_mb=10)  # tier 0
        _insert_rec(writer, "t1a", "cam", time.time(), segment_size_mb=5)
        _insert_rec(writer, "t1b", "cam", time.time(), segment_size_mb=5)
        _insert_rec(writer, "t2a", "cam", time.time(), segment_size_mb=20)
        # Promote two to tier 1, one to tier 2
        _record_compressed(ctx, "t1a", "cam", tier=1)
        _record_compressed(ctx, "t1b", "cam", tier=1)
        _record_compressed(ctx, "t2a", "cam", tier=2)
        stats = fc.collect_frigate_stats(ctx)
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


def test_collect_stats_segment_update_failed_counts_as_compressed(tmp_path):
    """A row whose status is segment_update_failed has been compressed on disk
    even though Frigate's segment_size update failed — it must still be
    bucketed by its tier, not as tier 0."""
    ctx, writer = _make_stats_ctx(tmp_path)
    try:
        _insert_rec(writer, "x", "cam", time.time(), segment_size_mb=8)
        _record_compressed(
            ctx, "x", "cam", tier=2, status=fc.STATUS_SEGMENT_UPDATE_FAILED
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
        _insert_rec(writer, "x", "cam", time.time(), segment_size_mb=4)
        _record_compressed(ctx, "x", "cam", tier=1, status=fc.STATUS_ERROR)
        stats = fc.collect_frigate_stats(ctx)
        assert stats.tier0_bytes == 4 * _MB
        assert stats.tier1_bytes == 0
    finally:
        _close_stats_ctx(ctx, writer)


def test_collect_stats_null_segment_size_treated_as_zero(tmp_path):
    """Frigate writes recordings rows before segment_size is finalised; we
    must not crash on the NULL and must count those rows as zero bytes."""
    ctx, writer = _make_stats_ctx(tmp_path)
    try:
        writer.execute(
            "INSERT INTO recordings (id, camera, path, start_time, motion, objects)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            ("nullrow", "cam", "/x", time.time(), 0, 0),
        )
        writer.commit()
        stats = fc.collect_frigate_stats(ctx)
        assert stats.total_files == 1
        assert stats.total_bytes == 0
    finally:
        _close_stats_ctx(ctx, writer)


def test_collect_stats_multi_camera(tmp_path):
    ctx, writer = _make_stats_ctx(tmp_path)
    try:
        _insert_rec(writer, "a1", "front", time.time() - 86400, segment_size_mb=10)
        _insert_rec(writer, "b1", "back", time.time() - 2 * 86400, segment_size_mb=20)
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
# RateTracker
# ═══════════════════════════════════════════════════════════════════════════════


def test_rate_tracker_returns_none_on_first_sample():
    rt = fc.RateTracker(window_seconds=300)
    assert rt.update("k", 100, now=1000) is None


def test_rate_tracker_simple_delta():
    rt = fc.RateTracker(window_seconds=300)
    rt.update("k", 0, now=0)
    # 100 units gained over 60s → 100/60 * 3600 = 6000/h
    rate = rt.update("k", 100, now=60)
    assert rate is not None
    assert abs(rate - 6000.0) < 1e-6


def test_rate_tracker_signed_negative():
    rt = fc.RateTracker(window_seconds=300)
    rt.update("k", 1000, now=0)
    rate = rt.update("k", 700, now=60)
    assert rate is not None
    assert rate < 0
    # Lost 300 units in 60s → -300/60 * 3600 = -18000/h
    assert abs(rate - (-18000.0)) < 1e-6


def test_rate_tracker_window_drops_old_samples():
    """Samples older than the window must be dropped before computing the rate."""
    rt = fc.RateTracker(window_seconds=300)
    rt.update("k", 0, now=0)  # will fall outside window
    rt.update("k", 600, now=400)  # 400 > 300 → previous sample dropped
    # After dropping, only one sample remains → return None
    # (we just appended (400, 600); previous (0, 0) is < 400-300=100)
    rate = rt.update("k", 700, now=460)
    # Now samples = [(400, 600), (460, 700)]; rate = 100/60 * 3600 = 6000/h
    assert rate is not None
    assert abs(rate - 6000.0) < 1e-6


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
        # 10 top-level + 16 per-camera × 2 cameras = 42 discovery publishes
        assert len(first_discovery) == 10 + 16 * 2

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
        # First pass: tracker has 1 sample → no rate state published
        rate_topics_first = [t for t, _, _ in client.publishes if "_rate/state" in t]
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
    )
    assert cfg.mqtt.host == "broker.local"
    assert cfg.mqtt.port == 2883
    assert cfg.mqtt.username == "user"
    assert cfg.mqtt.password == "pw"
    assert cfg.mqtt.base_topic == "custom_topic"
    assert cfg.mqtt.publish_interval_seconds == 120
    assert cfg.mqtt.rate_window_seconds == 600
