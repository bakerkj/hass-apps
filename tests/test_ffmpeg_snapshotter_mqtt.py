# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for ffmpeg_snapshotter.mqtt — MqttPublisher discovery + state."""

from __future__ import annotations

import json
import threading

import ffmpeg_snapshotter as fs


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


def _make_mqtt_cfg(**overrides) -> fs.MqttConfig:
    kwargs = dict(
        host="example",
        port=1883,
        username="",
        password="",
        discovery_prefix="homeassistant",
        base_topic="ffmpeg_snapshotter",
        client_id="test-client",
        publish_interval_seconds=60,
        rate_window_seconds=60,
        disconnect_timeout_seconds=300,
        snapshot_error_timeout_seconds=30,
    )
    kwargs.update(overrides)
    return fs.MqttConfig(**kwargs)


def _build_publisher(
    cams: dict[str, fs.SnapshotStats] | None = None,
) -> tuple[fs.MqttPublisher, dict[str, fs.SnapshotStats], _RecordingClient]:
    if cams is None:
        cams = {
            "front": fs.SnapshotStats(
                rate_window_seconds=60.0, error_timeout_seconds=30.0
            ),
            "back": fs.SnapshotStats(
                rate_window_seconds=60.0, error_timeout_seconds=30.0
            ),
        }
    publisher = fs.MqttPublisher(_make_mqtt_cfg(), cams, threading.Event())
    client = _RecordingClient()
    publisher.client = client  # bypass start() so the loop thread isn't spun
    return publisher, cams, client


# ---------------------------------------------------------------------------
# MqttConfig.enabled
# ---------------------------------------------------------------------------


def test_mqtt_config_disabled_when_host_empty():
    assert _make_mqtt_cfg(host="").enabled is False
    assert _make_mqtt_cfg(host="broker").enabled is True


def test_load_mqtt_config_applies_defaults():
    cfg = fs.load_mqtt_config({})
    assert cfg.host == ""
    assert cfg.port == 1883
    assert cfg.discovery_prefix == "homeassistant"
    assert cfg.base_topic == "ffmpeg_snapshotter"
    assert cfg.publish_interval_seconds == 60
    assert cfg.rate_window_seconds == 300
    assert cfg.snapshot_error_timeout_seconds == 300


# ---------------------------------------------------------------------------
# Slug helper
# ---------------------------------------------------------------------------


def test_slugify_camera_handles_special_chars():
    assert fs._slugify_camera("Front Door") == "front_door"
    assert fs._slugify_camera("back.lot-2") == "back_lot_2"
    assert fs._slugify_camera("  trim  ") == "trim"
    assert fs._slugify_camera("!!!") == "unknown"


# ---------------------------------------------------------------------------
# Publisher discovery
# ---------------------------------------------------------------------------


def test_publish_once_emits_discovery_for_each_camera():
    publisher, _, client = _build_publisher()
    publisher.publish_once()

    sensor_topics = [
        t for t, _, _ in client.publishes if t.startswith("homeassistant/sensor/")
    ]
    binary_topics = [
        t
        for t, _, _ in client.publishes
        if t.startswith("homeassistant/binary_sensor/")
    ]
    # 4 sensors × 2 cameras, minus 1 binary sensor each = 3 regular sensors × 2.
    assert len(sensor_topics) == 3 * 2
    # 1 binary sensor × 2 cameras.
    assert len(binary_topics) == 1 * 2
    # Every binary discovery is the snapshot_error entity.
    assert all(t.endswith("/snapshot_error/config") for t in binary_topics)


def test_discovery_payload_binary_sensor_shape():
    publisher, _, client = _build_publisher()
    publisher.publish_once()
    cfg_topic = (
        "homeassistant/binary_sensor/ffmpeg_snapshotter_camera_front"
        "/snapshot_error/config"
    )
    payload = json.loads(next(p for t, p, _ in client.publishes if t == cfg_topic))
    assert payload["device_class"] == "problem"
    assert payload["payload_on"] == "ON"
    assert payload["payload_off"] == "OFF"
    assert "state_class" not in payload
    assert payload["unique_id"] == "ffmpeg_snapshotter_camera_front_snapshot_error"


def test_discovery_payload_regular_sensor_shape():
    publisher, _, client = _build_publisher()
    publisher.publish_once()
    cfg_topic = (
        "homeassistant/sensor/ffmpeg_snapshotter_camera_front/snapshot_rate/config"
    )
    payload = json.loads(next(p for t, p, _ in client.publishes if t == cfg_topic))
    assert payload["unit_of_measurement"] == "/min"
    assert payload["state_class"] == "measurement"
    assert payload["icon"] == "mdi:camera-burst"


def test_discovery_published_only_once_per_device():
    publisher, _, client = _build_publisher()
    publisher.publish_once()
    before = len(client.publishes)
    publisher.publish_once()
    after = client.publishes[before:]
    discovery = [t for t, _, _ in after if t.startswith("homeassistant/")]
    assert discovery == []


def test_discovery_replayed_after_ha_birth_message():
    publisher, _, client = _build_publisher()
    publisher.publish_once()
    before = len(client.publishes)

    class _Msg:
        payload = b"online"

    publisher._on_message(None, None, _Msg())
    publisher.publish_once()
    new_topics = [
        t for t, _, _ in client.publishes[before:] if t.startswith("homeassistant/")
    ]
    assert new_topics  # discovery was re-sent


# ---------------------------------------------------------------------------
# State publishing
# ---------------------------------------------------------------------------


def test_publish_state_reflects_stats_values():
    stats = fs.SnapshotStats(rate_window_seconds=60.0, error_timeout_seconds=30.0)
    # Two successful snapshots in a 60s window, most recent is 2000 bytes.
    stats.record_success(1000, now=1000.0)
    stats.record_success(2000, now=1050.0)

    import time as _t

    _real_time = _t.time
    try:
        # Freeze wall-clock so the publisher's snapshot() call matches.
        _t.time = lambda: 1060.0  # type: ignore[assignment]
        publisher, cams, client = _build_publisher({"cam": stats})
        publisher.publish_once()
    finally:
        _t.time = _real_time  # type: ignore[assignment]

    by_topic = {t: p for t, p, _ in client.publishes}
    assert by_topic["ffmpeg_snapshotter/cam/snapshot_rate/state"] == "2"
    # 3000 bytes / 60s window = 50 B/s
    assert by_topic["ffmpeg_snapshotter/cam/snapshot_bytes_rate/state"] == "50"
    # Latest success bytes
    assert by_topic["ffmpeg_snapshotter/cam/snapshot_bytes/state"] == "2000"
    # No errors and recent success → OFF
    assert by_topic["ffmpeg_snapshotter/cam/snapshot_error/state"] == "OFF"


def test_publish_state_error_flag_on_after_failure():
    stats = fs.SnapshotStats(rate_window_seconds=60.0, error_timeout_seconds=30.0)
    stats.record_error(now=1000.0)

    import time as _t

    _real_time = _t.time
    try:
        _t.time = lambda: 1001.0  # type: ignore[assignment]
        publisher, _, client = _build_publisher({"cam": stats})
        publisher.publish_once()
    finally:
        _t.time = _real_time  # type: ignore[assignment]

    by_topic = {t: p for t, p, _ in client.publishes}
    assert by_topic["ffmpeg_snapshotter/cam/snapshot_error/state"] == "ON"
    # No successes yet → bytes sensors never publish (None filtered out).
    assert "ffmpeg_snapshotter/cam/snapshot_bytes/state" not in by_topic
