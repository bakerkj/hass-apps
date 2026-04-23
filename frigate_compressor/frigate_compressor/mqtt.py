# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""MQTT storage publisher: snapshot stats + Home Assistant discovery."""

from __future__ import annotations

import json
import re
import sqlite3
import threading
import time
from dataclasses import dataclass

import paho.mqtt.client as paho_mqtt

from .config import MqttConfig, MqttHealth
from .context import CompressorContext
from .database import (
    STATUS_OK,
    STATUS_SEGMENT_UPDATE_FAILED,
    _attach_frigate_ro,
)
from .util import log


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
    oldest_age_days: float | None  # None when the camera has no recordings
    # Fresh bytes written by this camera in the last ``rate_window_seconds``
    # divided by the window — i.e. how fast the camera is generating video.
    # Unlike ``tier0_bytes_rate`` this is not a pool delta; it's a true
    # write rate derived from ``recordings.start_time`` and is never negative.
    recording_bytes_rate: float


@dataclass
class FrigateStats:
    """Top-level snapshot of Frigate's recording allocation."""

    total_bytes: int
    total_files: int
    oldest_age_days: float | None
    tier0_bytes: int
    tier1_bytes: int
    tier2_bytes: int
    cameras: dict[str, CameraStats]


# Bytes per MB used to convert Frigate's segment_size column (stored as MB,
# float) to whole bytes for the MQTT byte sensors.
_MB_BYTES = 1024 * 1024


def collect_frigate_stats(ctx: CompressorContext) -> FrigateStats:
    """Snapshot Frigate's recording allocation, joined with our compress DB.

    One ATTACH+GROUP BY query: per (camera, tier, recording_type) we get a
    files count, byte total (segment_size→bytes), and earliest start_time.
    Tier comes from a LEFT JOIN against ``files`` — determined by which
    ``t*_status`` column is set.  Uncompressed recordings are tier 0.
    NULL ``segment_size`` is treated as 0 bytes so a half-finalised row
    never crashes the aggregate.

    Opens its own read-only connection so it never contends
    with the probe or compression loops.
    """
    cfg = ctx.cfg
    now = time.time()
    rate_window = float(cfg.mqtt.rate_window_seconds)
    rate_cutoff = now - rate_window

    conn = sqlite3.connect(
        f"file:{cfg.compress_db}?mode=ro", uri=True, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    try:
        _attach_frigate_ro(conn, cfg, "frigate_stats")
        rows = conn.execute(
            f"""
            SELECT
                r.camera                                                AS camera,
                CASE
                  WHEN f.t2_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}') THEN 2
                  WHEN f.t1_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}') THEN 1
                  ELSE 0
                END                                                     AS tier,
                CASE
                  WHEN COALESCE(r.objects, 0) > 0 THEN 'object'
                  WHEN COALESCE(r.motion,  0) > 0 THEN 'motion'
                  ELSE                                  'continuous'
                END                                                     AS rtype,
                COUNT(*)                                                AS files,
                SUM(COALESCE(r.segment_size, 0) * {_MB_BYTES})          AS bytes,
                MIN(r.start_time)                                       AS oldest
            FROM frigate_stats.recordings r
            LEFT JOIN files f
              ON  f.recording_id = r.id
            GROUP BY r.camera, tier, rtype
            """
        ).fetchall()
        # Fresh bytes per camera within the rate window.  Filtered on
        # start_time so in-flight segments (NULL segment_size) contribute
        # 0 until finalised — a small under-count that self-corrects on
        # the next publish.
        recent_rows = conn.execute(
            f"""
            SELECT
                camera                                          AS camera,
                SUM(COALESCE(segment_size, 0) * {_MB_BYTES})    AS bytes
            FROM frigate_stats.recordings
            WHERE start_time >= ?
            GROUP BY camera
            """,
            (rate_cutoff,),
        ).fetchall()
    finally:
        conn.close()

    recording_rate: dict[str, float] = {
        r["camera"]: float(r["bytes"] or 0) / rate_window for r in recent_rows
    }

    cameras: dict[str, dict] = {}
    top_total_bytes = 0
    top_total_files = 0
    top_tier_bytes = {0: 0, 1: 0, 2: 0}
    top_oldest: float | None = None

    for row in rows:
        cam = row["camera"]
        tier = int(row["tier"])
        if tier not in (0, 1, 2):
            tier = 0
        rtype = row["rtype"]
        files = int(row["files"] or 0)
        bytes_ = int(row["bytes"] or 0)
        oldest = row["oldest"]

        c = cameras.setdefault(
            cam,
            {
                "total_bytes": 0,
                "total_files": 0,
                "continuous_bytes": 0,
                "motion_bytes": 0,
                "object_bytes": 0,
                "tier0_bytes": 0,
                "tier1_bytes": 0,
                "tier2_bytes": 0,
                "oldest": None,
            },
        )
        c["total_bytes"] += bytes_
        c["total_files"] += files
        c[f"{rtype}_bytes"] += bytes_
        c[f"tier{tier}_bytes"] += bytes_
        if oldest is not None and (c["oldest"] is None or oldest < c["oldest"]):
            c["oldest"] = oldest

        top_total_bytes += bytes_
        top_total_files += files
        top_tier_bytes[tier] += bytes_
        if oldest is not None and (top_oldest is None or oldest < top_oldest):
            top_oldest = oldest

    def _age(t: float | None) -> float | None:
        return (now - float(t)) / 86400.0 if t is not None else None

    cam_stats = {
        cam: CameraStats(
            total_bytes=c["total_bytes"],
            total_files=c["total_files"],
            continuous_bytes=c["continuous_bytes"],
            motion_bytes=c["motion_bytes"],
            object_bytes=c["object_bytes"],
            tier0_bytes=c["tier0_bytes"],
            tier1_bytes=c["tier1_bytes"],
            tier2_bytes=c["tier2_bytes"],
            oldest_age_days=_age(c["oldest"]),
            recording_bytes_rate=recording_rate.get(cam, 0.0),
        )
        for cam, c in cameras.items()
    }

    return FrigateStats(
        total_bytes=top_total_bytes,
        total_files=top_total_files,
        oldest_age_days=_age(top_oldest),
        tier0_bytes=top_tier_bytes[0],
        tier1_bytes=top_tier_bytes[1],
        tier2_bytes=top_tier_bytes[2],
        cameras=cam_stats,
    )


class RateTracker:
    """Signed per-second rate of change over a fixed time window.

    Stores ``(timestamp, value)`` samples per key.  On each ``update``,
    drops samples older than ``window_seconds`` and returns
    ``(latest - oldest_in_window) / dt``.  Returns ``None`` until at
    least two samples are present.

    Not thread-safe — all updates are expected to come from the publisher
    thread.
    """

    def __init__(self, window_seconds: float):
        self._window = float(window_seconds)
        self._samples: dict[str, list[tuple[float, float]]] = {}

    def update(self, key: str, value: float, now: float | None = None) -> float | None:
        if now is None:
            now = time.time()
        samples = self._samples.setdefault(key, [])
        samples.append((float(now), float(value)))
        cutoff = now - self._window
        while samples and samples[0][0] < cutoff:
            samples.pop(0)
        if len(samples) < 2:
            return None
        t0, v0 = samples[0]
        t1, v1 = samples[-1]
        dt = t1 - t0
        if dt <= 0:
            return None
        return (v1 - v0) / dt

    def reset(self) -> None:
        self._samples.clear()


# Sensor descriptor:
#  key, friendly name, unit, device_class (or None), icon, is_rate
_SensorSpec = tuple[str, str, str | None, str | None, str, bool]

_TOP_SENSORS: list[_SensorSpec] = [
    ("total_bytes", "Total bytes", "B", "data_size", "mdi:database", False),
    ("total_files", "Total files", None, None, "mdi:file-multiple", False),
    (
        "oldest_age_days",
        "Oldest recording age",
        "d",
        "duration",
        "mdi:clock-outline",
        False,
    ),
    (
        "tier0_bytes",
        "Uncompressed bytes",
        "B",
        "data_size",
        "mdi:database-outline",
        False,
    ),
    ("tier1_bytes", "Tier 1 bytes", "B", "data_size", "mdi:database", False),
    ("tier2_bytes", "Tier 2 bytes", "B", "data_size", "mdi:database", False),
    (
        "total_bytes_rate",
        "Total bytes rate",
        "B/s",
        "data_rate",
        "mdi:chart-line",
        True,
    ),
    (
        "tier0_bytes_rate",
        "Uncompressed bytes rate",
        "B/s",
        "data_rate",
        "mdi:chart-line",
        True,
    ),
    (
        "tier1_bytes_rate",
        "Tier 1 bytes rate",
        "B/s",
        "data_rate",
        "mdi:chart-line",
        True,
    ),
    (
        "tier2_bytes_rate",
        "Tier 2 bytes rate",
        "B/s",
        "data_rate",
        "mdi:chart-line",
        True,
    ),
]

_CAMERA_SENSORS: list[_SensorSpec] = [
    ("total_bytes", "Total bytes", "B", "data_size", "mdi:database", False),
    ("total_files", "Total files", None, None, "mdi:file-multiple", False),
    (
        "continuous_bytes",
        "Continuous bytes",
        "B",
        "data_size",
        "mdi:video-outline",
        False,
    ),
    ("motion_bytes", "Motion bytes", "B", "data_size", "mdi:motion-sensor", False),
    ("object_bytes", "Object bytes", "B", "data_size", "mdi:tag", False),
    (
        "tier0_bytes",
        "Uncompressed bytes",
        "B",
        "data_size",
        "mdi:database-outline",
        False,
    ),
    ("tier1_bytes", "Tier 1 bytes", "B", "data_size", "mdi:database", False),
    ("tier2_bytes", "Tier 2 bytes", "B", "data_size", "mdi:database", False),
    (
        "oldest_age_days",
        "Oldest recording age",
        "d",
        "duration",
        "mdi:clock-outline",
        False,
    ),
    (
        "total_bytes_rate",
        "Total bytes rate",
        "B/s",
        "data_rate",
        "mdi:chart-line",
        True,
    ),
    (
        "continuous_bytes_rate",
        "Continuous bytes rate",
        "B/s",
        "data_rate",
        "mdi:chart-line",
        True,
    ),
    (
        "motion_bytes_rate",
        "Motion bytes rate",
        "B/s",
        "data_rate",
        "mdi:chart-line",
        True,
    ),
    (
        "object_bytes_rate",
        "Object bytes rate",
        "B/s",
        "data_rate",
        "mdi:chart-line",
        True,
    ),
    (
        "tier0_bytes_rate",
        "Uncompressed bytes rate",
        "B/s",
        "data_rate",
        "mdi:chart-line",
        True,
    ),
    (
        "tier1_bytes_rate",
        "Tier 1 bytes rate",
        "B/s",
        "data_rate",
        "mdi:chart-line",
        True,
    ),
    (
        "tier2_bytes_rate",
        "Tier 2 bytes rate",
        "B/s",
        "data_rate",
        "mdi:chart-line",
        True,
    ),
    (
        "recording_bytes_rate",
        "Recording rate",
        "B/s",
        "data_rate",
        "mdi:video-plus",
        True,
    ),
]

# Bytes counters that get a corresponding _rate sensor at the top level.
_TOP_RATE_KEYS: tuple[str, ...] = (
    "total_bytes",
    "tier0_bytes",
    "tier1_bytes",
    "tier2_bytes",
)

# Bytes counters that get a corresponding _rate sensor per camera.
_CAMERA_RATE_KEYS: tuple[str, ...] = (
    "total_bytes",
    "continuous_bytes",
    "motion_bytes",
    "object_bytes",
    "tier0_bytes",
    "tier1_bytes",
    "tier2_bytes",
)


_SLUG_RE = re.compile(r"[^a-zA-Z0-9_]+")


def _slugify_camera(name: str) -> str:
    """Return an MQTT-safe slug for a camera name."""
    s = _SLUG_RE.sub("_", str(name).strip().lower()).strip("_")
    return s or "unknown"


class MqttPublisher:
    """Periodically publishes ``FrigateStats`` snapshots to MQTT.

    Owns a paho client + reconnect/will logic, runs its publish loop in a
    daemon thread, and republishes HA discovery on every (re)connect and
    on the HA birth message.
    """

    def __init__(
        self,
        ctx: CompressorContext,
        mqtt_cfg: MqttConfig,
        stopping: threading.Event,
    ):
        self.ctx = ctx
        self.mqtt_cfg = mqtt_cfg
        self.stopping = stopping
        self.tracker = RateTracker(mqtt_cfg.rate_window_seconds)
        self.health = MqttHealth()
        self.client: paho_mqtt.Client | None = None
        self._thread: threading.Thread | None = None
        # Devices for which we've already published HA discovery on the
        # current connection.  Cleared on (re)connect and on HA birth.
        self._discovery_published: set[str] = set()
        self._lock = threading.Lock()
        # Set to 11/12 by the watchdogs when a supervisor restart is needed;
        # main() reads this after the main loop exits.
        self.exit_code: int | None = None

    # ── lifecycle ────────────────────────────────────────────────────────

    def start(self) -> None:
        client = paho_mqtt.Client(client_id=self.mqtt_cfg.client_id, clean_session=True)
        if self.mqtt_cfg.username:
            client.username_pw_set(self.mqtt_cfg.username, self.mqtt_cfg.password)
        availability_topic = f"{self.mqtt_cfg.base_topic}/availability"
        client.will_set(availability_topic, "offline", qos=1, retain=True)
        client.reconnect_delay_set(min_delay=1, max_delay=30)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        self.client = client

        self._connect_with_retry()
        client.loop_start()

        self._thread = threading.Thread(
            target=self._run, name="mqtt-publisher", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        if self._thread is not None:
            self._thread.join(timeout=5)
        client = self.client
        if client is None:
            return
        try:
            client.publish(
                f"{self.mqtt_cfg.base_topic}/availability",
                "offline",
                qos=1,
                retain=True,
            )
        except Exception:
            pass
        try:
            client.loop_stop()
        except Exception:
            pass
        try:
            client.disconnect()
        except Exception:
            pass

    # ── connection ───────────────────────────────────────────────────────

    def _connect_with_retry(self) -> None:
        delay = 5
        while not self.stopping.is_set():
            try:
                assert self.client is not None
                self.client.connect(
                    self.mqtt_cfg.host, self.mqtt_cfg.port, keepalive=60
                )
                return
            except Exception as e:
                log(
                    "WARNING",
                    f"MQTT connect to {self.mqtt_cfg.host}:{self.mqtt_cfg.port}"
                    f" failed: {e} — retry in {delay}s",
                )
                if self.stopping.wait(timeout=delay):
                    return
                delay = min(delay * 2, 60)

    def _on_connect(self, client, _userdata, _flags, rc) -> None:
        if rc == 0:
            self.health.connected = True
            self.health.last_connect_ok = time.time()
            log(
                "INFO",
                f"MQTT connected to {self.mqtt_cfg.host}:{self.mqtt_cfg.port}",
            )
            client.publish(
                f"{self.mqtt_cfg.base_topic}/availability",
                "online",
                qos=1,
                retain=True,
            )
            client.subscribe(f"{self.mqtt_cfg.discovery_prefix}/status", qos=1)
            with self._lock:
                self._discovery_published.clear()
        else:
            self.health.connected = False
            log("ERROR", f"MQTT connect failed rc={rc}")

    def _on_disconnect(self, _client, _userdata, rc) -> None:
        self.health.connected = False
        self.health.last_disconnect = time.time()
        if rc == 0:
            log("WARNING", "MQTT disconnected (clean)")
        else:
            log("WARNING", f"MQTT disconnected rc={rc} (will retry)")

    def _on_message(self, _client, _userdata, msg) -> None:
        try:
            payload = msg.payload.decode("utf-8", errors="replace").strip()
        except Exception:
            return
        if payload == "online":
            log("INFO", "HA birth message received — will republish discovery")
            with self._lock:
                self._discovery_published.clear()

    # ── publish loop ─────────────────────────────────────────────────────

    def _run(self) -> None:
        while not self.stopping.is_set():
            t0 = time.time()
            try:
                self.publish_once()
            except Exception as e:
                log("ERROR", f"MQTT publish failed: {e}")
            if self._check_watchdogs(time.time()):
                return
            elapsed = time.time() - t0
            sleep_for = max(1.0, self.mqtt_cfg.publish_interval_seconds - elapsed)
            if self.stopping.wait(timeout=sleep_for):
                return

    def _check_watchdogs(self, now: float) -> bool:
        """Return True and trigger shutdown if a watchdog fires.

        Mirrors the turbostat/intel_gpu/container_info pattern: exit 11 if
        MQTT has been disconnected for longer than
        ``disconnect_timeout_seconds``, exit 12 if the publisher is
        connected but successful state publishes have stalled for longer
        than ``publish_interval_seconds * 4``.
        """
        disconnect_timeout = self.mqtt_cfg.disconnect_timeout_seconds
        stall_timeout = max(60, self.mqtt_cfg.publish_interval_seconds * 4)

        if (
            not self.health.connected
            and self.health.last_disconnect > 0
            and (now - self.health.last_disconnect) > disconnect_timeout
        ):
            log(
                "ERROR",
                f"MQTT disconnected for {now - self.health.last_disconnect:.1f}s"
                f" (> {disconnect_timeout}s). Exiting for supervisor restart.",
            )
            self.exit_code = 11
            self.stopping.set()
            return True

        if (
            self.health.connected
            and self.health.last_state_publish_ok > 0
            and (now - self.health.last_state_publish_ok) > stall_timeout
        ):
            log(
                "ERROR",
                f"MQTT state publish stalled for"
                f" {now - self.health.last_state_publish_ok:.1f}s"
                f" (> {stall_timeout}s). Exiting for supervisor restart.",
            )
            self.exit_code = 12
            self.stopping.set()
            return True

        return False

    def publish_once(self) -> None:
        """Compute one snapshot and publish state for all sensors.

        Public so tests can drive a single pass without spinning the loop
        thread.
        """
        stats = collect_frigate_stats(self.ctx)
        now = time.time()

        # Top-level "Frigate Storage" device
        top_device_id = "frigate_compressor_storage"
        top_device = {
            "identifiers": [top_device_id],
            "name": "Frigate Storage",
            "manufacturer": "Frigate Compressor",
            "model": "storage",
        }
        self._publish_discovery(top_device_id, top_device, _TOP_SENSORS, "storage")
        self._publish_top_state(top_device_id, stats, now)

        # Per-camera devices
        for cam_name, cam_stats in stats.cameras.items():
            slug = _slugify_camera(cam_name)
            cam_device_id = f"frigate_compressor_camera_{slug}"
            cam_device = {
                "identifiers": [cam_device_id],
                "name": f"Frigate Camera {cam_name}",
                "manufacturer": "Frigate Compressor",
                "model": "camera",
            }
            self._publish_discovery(cam_device_id, cam_device, _CAMERA_SENSORS, slug)
            self._publish_camera_state(cam_device_id, slug, cam_stats, now)

    # ── discovery + state helpers ────────────────────────────────────────

    def _publish_discovery(
        self,
        device_id: str,
        device: dict,
        sensors: list[_SensorSpec],
        topic_subpath: str,
    ) -> None:
        with self._lock:
            if device_id in self._discovery_published:
                return
        base = self.mqtt_cfg.base_topic
        availability_topic = f"{base}/availability"
        client = self.client
        if client is None:
            return
        published = True
        for key, name, unit, device_class, icon, is_rate in sensors:
            state_topic = f"{base}/{topic_subpath}/{key}/state"
            config_topic = (
                f"{self.mqtt_cfg.discovery_prefix}/sensor/{device_id}/{key}/config"
            )
            payload: dict = {
                "name": name,
                "has_entity_name": True,
                "unique_id": f"{device_id}_{key}",
                "state_topic": state_topic,
                "availability_topic": availability_topic,
                "payload_available": "online",
                "payload_not_available": "offline",
                "state_class": "measurement",
                "icon": icon,
                "device": device,
            }
            if unit:
                payload["unit_of_measurement"] = unit
            if device_class:
                payload["device_class"] = device_class
            if is_rate:
                payload["suggested_display_precision"] = 0
            try:
                info = client.publish(
                    config_topic, json.dumps(payload), qos=1, retain=True
                )
                if info.rc != paho_mqtt.MQTT_ERR_SUCCESS:
                    log(
                        "WARNING",
                        f"MQTT discovery publish rc={info.rc} topic={config_topic}",
                    )
                    published = False
            except Exception as e:
                log("WARNING", f"MQTT discovery publish failed for {key}: {e}")
                published = False
        if published:
            with self._lock:
                self._discovery_published.add(device_id)

    def _publish_top_state(
        self, device_id: str, stats: FrigateStats, now: float
    ) -> None:
        base = self.mqtt_cfg.base_topic
        prefix = f"{base}/storage"
        values: dict[str, float | int | None] = {
            "total_bytes": stats.total_bytes,
            "total_files": stats.total_files,
            "oldest_age_days": stats.oldest_age_days,
            "tier0_bytes": stats.tier0_bytes,
            "tier1_bytes": stats.tier1_bytes,
            "tier2_bytes": stats.tier2_bytes,
        }
        for k in _TOP_RATE_KEYS:
            v = values[k]
            values[f"{k}_rate"] = self.tracker.update(
                f"{device_id}/{k}", float(v or 0), now
            )
        self._publish_values(prefix, values)

    def _publish_camera_state(
        self,
        device_id: str,
        slug: str,
        cs: CameraStats,
        now: float,
    ) -> None:
        base = self.mqtt_cfg.base_topic
        prefix = f"{base}/{slug}"
        values: dict[str, float | int | None] = {
            "total_bytes": cs.total_bytes,
            "total_files": cs.total_files,
            "continuous_bytes": cs.continuous_bytes,
            "motion_bytes": cs.motion_bytes,
            "object_bytes": cs.object_bytes,
            "tier0_bytes": cs.tier0_bytes,
            "tier1_bytes": cs.tier1_bytes,
            "tier2_bytes": cs.tier2_bytes,
            "oldest_age_days": cs.oldest_age_days,
        }
        for k in _CAMERA_RATE_KEYS:
            v = values[k]
            values[f"{k}_rate"] = self.tracker.update(
                f"{device_id}/{k}", float(v or 0), now
            )
        # Recording rate is a fresh windowed measurement, not a RateTracker
        # derivative — assign it directly.
        values["recording_bytes_rate"] = cs.recording_bytes_rate
        self._publish_values(prefix, values)

    def _publish_values(
        self, prefix: str, values: dict[str, float | int | None]
    ) -> None:
        client = self.client
        if client is None:
            return
        for key, val in values.items():
            if val is None:
                continue
            if isinstance(val, float):
                payload = f"{val:.6g}"
            else:
                payload = str(val)
            topic = f"{prefix}/{key}/state"
            try:
                info = client.publish(topic, payload, qos=0, retain=False)
                if info.rc == paho_mqtt.MQTT_ERR_SUCCESS:
                    self.health.last_state_publish_ok = time.time()
                else:
                    log("WARNING", f"MQTT state publish rc={info.rc} topic={topic}")
            except Exception as e:
                log("WARNING", f"MQTT state publish failed for {key}: {e}")
