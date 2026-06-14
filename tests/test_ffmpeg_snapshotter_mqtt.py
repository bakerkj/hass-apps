# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for ffmpeg_snapshotter.mqtt — MqttPublisher discovery + state."""

from __future__ import annotations

import json
import threading
from typing import Any

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


def _make_mqtt_cfg(**overrides: Any) -> fs.MqttConfig:
    kwargs: dict[str, Any] = dict(
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
    publisher.client = client  # type: ignore[assignment]  # bypass start() so the loop thread isn't spun
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

    _real_monotonic = _t.monotonic
    try:
        # SnapshotStats uses time.monotonic(); freeze it so the publisher's
        # snapshot() call lands on the same scale as the injected samples.
        _t.monotonic = lambda: 1060.0
        publisher, cams, client = _build_publisher({"cam": stats})
        publisher.publish_once()
    finally:
        _t.monotonic = _real_monotonic

    by_topic = {t: p for t, p, _ in client.publishes}
    assert by_topic["ffmpeg_snapshotter/cam/snapshot_rate/state"] == "2"
    # 3000 bytes / 60s window = 50 B/s
    assert by_topic["ffmpeg_snapshotter/cam/snapshot_bytes_rate/state"] == "50"
    # Latest success bytes
    assert by_topic["ffmpeg_snapshotter/cam/snapshot_bytes/state"] == "2000"
    # No errors and recent success → OFF
    assert by_topic["ffmpeg_snapshotter/cam/snapshot_error/state"] == "OFF"


def test_publish_image_posts_bytes_to_retained_topic():
    publisher, _, client = _build_publisher()
    publisher.publish_image("front", b"\xff\xd8FAKEJPEG")
    imgs = [(t, p, r) for t, p, r in client.publishes if t.endswith("/image")]
    assert imgs == [("ffmpeg_snapshotter/front/image", b"\xff\xd8FAKEJPEG", True)]


def test_publish_image_slugifies_camera_name():
    publisher, _, client = _build_publisher(
        {
            "Front Door": fs.SnapshotStats(
                rate_window_seconds=60.0, error_timeout_seconds=30.0
            )
        }
    )
    publisher.publish_image("Front Door", b"\xff\xd8X")
    topics = [t for t, _, _ in client.publishes if t.endswith("/image")]
    assert topics == ["ffmpeg_snapshotter/front_door/image"]


def test_publish_image_skipped_when_publish_images_false():
    cams = {
        "cam": fs.SnapshotStats(rate_window_seconds=60.0, error_timeout_seconds=30.0)
    }
    publisher = fs.MqttPublisher(
        _make_mqtt_cfg(publish_images=False), cams, threading.Event()
    )
    client = _RecordingClient()
    publisher.client = client  # type: ignore[assignment]
    publisher.publish_image("cam", b"\xff\xd8X")
    assert client.publishes == []


def test_publish_image_no_op_when_client_not_connected():
    cams = {
        "cam": fs.SnapshotStats(rate_window_seconds=60.0, error_timeout_seconds=30.0)
    }
    publisher = fs.MqttPublisher(_make_mqtt_cfg(), cams, threading.Event())
    # publisher.client is None — no crash, no publish.
    publisher.publish_image("cam", b"\xff\xd8X")  # must not raise


def test_camera_discovery_payload_shape():
    """The MQTT camera entity uses the ``camera`` component and a ``topic``
    field (not ``state_topic``), with no ``state_class``."""
    publisher, _, client = _build_publisher()
    publisher.publish_once()
    cfg_topic = "homeassistant/camera/ffmpeg_snapshotter_camera_front/image/config"
    matching = [p for t, p, _ in client.publishes if t == cfg_topic]
    assert len(matching) == 1
    payload = json.loads(matching[0])
    assert payload["topic"] == "ffmpeg_snapshotter/front/image"
    assert payload["unique_id"] == "ffmpeg_snapshotter_camera_front_image"
    assert payload["availability_topic"] == "ffmpeg_snapshotter/availability"
    assert "state_class" not in payload
    assert "state_topic" not in payload


def test_camera_discovery_skipped_when_publish_images_false():
    cams = {
        "cam": fs.SnapshotStats(rate_window_seconds=60.0, error_timeout_seconds=30.0)
    }
    publisher = fs.MqttPublisher(
        _make_mqtt_cfg(publish_images=False), cams, threading.Event()
    )
    client = _RecordingClient()
    publisher.client = client  # type: ignore[assignment]
    publisher.publish_once()
    cam_discovery = [
        t for t, _, _ in client.publishes if t.startswith("homeassistant/camera/")
    ]
    assert cam_discovery == []


def test_camera_discovery_published_only_once_per_device():
    publisher, _, client = _build_publisher()
    publisher.publish_once()
    before = len(client.publishes)
    publisher.publish_once()
    after = [
        t for t, _, _ in client.publishes[before:] if t.startswith("homeassistant/")
    ]
    assert after == []


def test_publish_state_error_flag_on_after_failure():
    stats = fs.SnapshotStats(rate_window_seconds=60.0, error_timeout_seconds=30.0)
    stats.record_error(now=1000.0)

    import time as _t

    _real_time = _t.time
    try:
        _t.time = lambda: 1001.0
        publisher, _, client = _build_publisher({"cam": stats})
        publisher.publish_once()
    finally:
        _t.time = _real_time

    by_topic = {t: p for t, p, _ in client.publishes}
    assert by_topic["ffmpeg_snapshotter/cam/snapshot_error/state"] == "ON"
    # No successes yet → bytes sensors never publish (None filtered out).
    assert "ffmpeg_snapshotter/cam/snapshot_bytes/state" not in by_topic
