# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""MQTT publisher: per-camera HA device with snapshot-rate/size/error entities."""

import json
import re
import threading
import time

import paho.mqtt.client as paho_mqtt

from .config import MqttConfig, MqttHealth
from .stats import SnapshotStats
from .util import log

# Sensor descriptor:
#  key, friendly name, unit, device_class (or None), icon
# A sensor with ``device_class == 'problem'`` is routed to HA's
# ``binary_sensor`` component with ON/OFF payloads; everything else is a
# plain ``sensor`` with ``state_class: measurement``.
_SensorSpec = tuple[str, str, str | None, str | None, str]

_CAMERA_SENSORS: list[_SensorSpec] = [
    ("snapshot_rate", "Snapshot rate", "/min", None, "mdi:camera-burst"),
    (
        "snapshot_bytes",
        "Snapshot bytes",
        "B",
        "data_size",
        "mdi:image-size-select-large",
    ),
    (
        "snapshot_bytes_rate",
        "Snapshot bytes rate",
        "B/s",
        "data_rate",
        "mdi:chart-line",
    ),
    ("snapshot_error", "Snapshot error", None, "problem", "mdi:alert-circle"),
]


_SLUG_RE = re.compile(r"[^a-zA-Z0-9_]+")


def _slugify_camera(name: str) -> str:
    """Return an MQTT-safe slug for a camera name."""
    s = _SLUG_RE.sub("_", str(name).strip().lower()).strip("_")
    return s or "unknown"


class MqttPublisher:
    """Periodic publisher for per-camera snapshot metrics.

    One HA device per entry in ``streams``.  Discovery is republished on
    every (re)connect and on the HA birth message (``online`` on the
    discovery-prefix status topic).
    """

    def __init__(
        self,
        mqtt_cfg: MqttConfig,
        streams: dict[str, SnapshotStats],
        stopping: threading.Event,
    ):
        self.mqtt_cfg = mqtt_cfg
        self.streams = streams
        self.stopping = stopping
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
        except Exception:  # noqa: BLE001, S110  # best-effort shutdown
            pass
        try:
            client.loop_stop()
        except Exception:  # noqa: BLE001, S110  # best-effort shutdown
            pass
        try:
            client.disconnect()
        except Exception:  # noqa: BLE001, S110  # best-effort shutdown
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
            except Exception as e:  # noqa: BLE001  # retry loop must survive any failure
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
        except Exception:  # noqa: BLE001  # mqtt callback: never propagate
            return
        if payload == "online":
            log("INFO", "HA birth message received — will republish discovery")
            with self._lock:
                self._discovery_published.clear()

    # ── publish loop ─────────────────────────────────────────────────────

    def _run(self) -> None:
        while not self.stopping.is_set():
            # Monotonic: loop pacing must be immune to NTP steps. The watchdog
            # below stays on wall clock to match the health timestamps.
            t0 = time.monotonic()
            try:
                self.publish_once()
            except Exception as e:  # noqa: BLE001  # supervisor loop: log and continue
                log("ERROR", f"MQTT publish failed: {e}")
            if self._check_watchdogs(time.time()):
                return
            elapsed = time.monotonic() - t0
            sleep_for = max(1.0, self.mqtt_cfg.publish_interval_seconds - elapsed)
            if self.stopping.wait(timeout=sleep_for):
                return

    def _check_watchdogs(self, now: float) -> bool:
        """Trigger shutdown if MQTT is wedged.

        Mirrors the frigate_compressor pattern: exit 11 if we've been
        disconnected longer than ``disconnect_timeout_seconds``; exit 12 if
        we're connected but state publishes have stalled.
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
        """Compute one snapshot across all streams and publish state.

        Public so tests can drive a single pass without spinning the loop
        thread.
        """
        for cam_name, stats in self.streams.items():
            slug = _slugify_camera(cam_name)
            device_id = f"ffmpeg_snapshotter_camera_{slug}"
            device = {
                "identifiers": [device_id],
                "name": f"Snapshotter {cam_name}",
                "manufacturer": "FFmpeg Snapshotter",
                "model": "camera",
            }
            self._publish_discovery(device_id, device, _CAMERA_SENSORS, slug)
            if self.mqtt_cfg.publish_images:
                self._publish_camera_discovery(device_id, device, cam_name, slug)
            self._publish_camera_state(slug, stats.snapshot())

    def publish_image(self, camera_name: str, image_bytes: bytes) -> None:
        """Publish a JPEG to the per-camera MQTT topic.

        Called from Worker threads right after a successful snapshot.
        Safe to call before the client is connected — paho's publish is
        thread-safe and messages queued pre-connection will be flushed
        on ``loop_start``.
        """
        if not self.mqtt_cfg.publish_images:
            return
        client = self.client
        if client is None:
            return
        slug = _slugify_camera(camera_name)
        topic = f"{self.mqtt_cfg.base_topic}/{slug}/image"
        try:
            info = client.publish(topic, image_bytes, qos=0, retain=True)
            if info.rc != paho_mqtt.MQTT_ERR_SUCCESS:
                log(
                    "WARNING",
                    f"MQTT image publish rc={info.rc} topic={topic}",
                )
        except Exception as e:  # noqa: BLE001  # best-effort image publish
            log("WARNING", f"MQTT image publish failed for {camera_name}: {e}")

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
        for key, name, unit, device_class, icon in sensors:
            is_binary = device_class == "problem"
            component = "binary_sensor" if is_binary else "sensor"
            state_topic = f"{base}/{topic_subpath}/{key}/state"
            config_topic = (
                f"{self.mqtt_cfg.discovery_prefix}/{component}/{device_id}/{key}/config"
            )
            payload: dict = {
                "name": name,
                "has_entity_name": True,
                "unique_id": f"{device_id}_{key}",
                "state_topic": state_topic,
                "availability_topic": availability_topic,
                "payload_available": "online",
                "payload_not_available": "offline",
                "icon": icon,
                "device": device,
            }
            if is_binary:
                payload["payload_on"] = "ON"
                payload["payload_off"] = "OFF"
            else:
                payload["state_class"] = "measurement"
            if unit:
                payload["unit_of_measurement"] = unit
            if device_class:
                payload["device_class"] = device_class
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
            except Exception as e:  # noqa: BLE001  # best-effort per-sensor discovery
                log("WARNING", f"MQTT discovery publish failed for {key}: {e}")
                published = False
        if published:
            with self._lock:
                self._discovery_published.add(device_id)

    def _publish_camera_discovery(
        self, device_id: str, device: dict, camera_name: str, slug: str
    ) -> None:
        """Publish HA MQTT discovery for the per-camera ``camera`` entity.

        Tracked separately from ``_discovery_published`` under a suffix key
        so a single-failure on the image entity doesn't block sensor
        discovery, and vice versa.
        """
        cam_discovery_key = f"{device_id}:image"
        with self._lock:
            if cam_discovery_key in self._discovery_published:
                return
        client = self.client
        if client is None:
            return
        base = self.mqtt_cfg.base_topic
        availability_topic = f"{base}/availability"
        state_topic = f"{base}/{slug}/image"
        config_topic = (
            f"{self.mqtt_cfg.discovery_prefix}/camera/{device_id}/image/config"
        )
        payload: dict = {
            "name": "Snapshot",
            "has_entity_name": True,
            "unique_id": f"{device_id}_image",
            "topic": state_topic,
            "availability_topic": availability_topic,
            "payload_available": "online",
            "payload_not_available": "offline",
            "icon": "mdi:camera",
            "device": device,
        }
        try:
            info = client.publish(config_topic, json.dumps(payload), qos=1, retain=True)
            if info.rc != paho_mqtt.MQTT_ERR_SUCCESS:
                log(
                    "WARNING",
                    f"MQTT camera discovery publish rc={info.rc} topic={config_topic}",
                )
                return
        except Exception as e:  # noqa: BLE001  # best-effort discovery publish
            log(
                "WARNING",
                f"MQTT camera discovery publish failed for {camera_name}: {e}",
            )
            return
        with self._lock:
            self._discovery_published.add(cam_discovery_key)

    def _publish_camera_state(self, slug: str, view) -> None:
        base = self.mqtt_cfg.base_topic
        prefix = f"{base}/{slug}"
        values: dict[str, float | int | bool | None] = {
            "snapshot_rate": view.snapshots_per_minute,
            "snapshot_bytes": view.last_bytes,
            "snapshot_bytes_rate": view.bytes_per_second,
            "snapshot_error": view.error,
        }
        self._publish_values(prefix, values)

    def _publish_values(
        self, prefix: str, values: dict[str, float | int | bool | None]
    ) -> None:
        client = self.client
        if client is None:
            return
        for key, val in values.items():
            if val is None:
                continue
            # bool is a subclass of int — check it first so True/False render
            # as ON/OFF for HA binary_sensor entities.
            if isinstance(val, bool):
                payload = "ON" if val else "OFF"
            elif isinstance(val, float):
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
            except Exception as e:  # noqa: BLE001  # best-effort per-key state publish
                log("WARNING", f"MQTT state publish failed for {key}: {e}")
