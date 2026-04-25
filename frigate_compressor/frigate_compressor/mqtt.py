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

from .config import Config, MqttConfig, MqttHealth
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
    cameras: dict[str, CameraStats]


# Bytes per MB used to convert Frigate's segment_size column (stored as MB,
# float) to whole bytes for the MQTT byte sensors.
_MB_BYTES = 1024 * 1024


def _open_stats_conn(cfg: Config) -> sqlite3.Connection:
    """Open a read-only compress-db connection with Frigate attached as
    ``frigate_stats`` — the shape expected by ``collect_frigate_stats``.

    Bigger cache helps across the 15+ queries the publisher runs on this
    connection (files_stats read, two Frigate aggregates, 12 backlog
    EXISTS probes): the partial-index top levels and Frigate's
    recordings PK pages get reused repeatedly.
    """
    conn = sqlite3.connect(
        f"file:{cfg.compress_db}?mode=ro", uri=True, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA cache_size=-131072")
    _attach_frigate_ro(conn, cfg, "frigate_stats")
    return conn


def collect_frigate_stats(
    ctx: CompressorContext,
    conn: sqlite3.Connection | None = None,
) -> FrigateStats:
    """Snapshot the compress DB's per-camera rollup for MQTT publishing.

    The bulk of the data comes from the materialised ``files_stats`` table,
    which is maintained transactionally by triggers on ``files``.  Instead
    of aggregating 800K+ rows per publish, we read a handful of rows here.
    Three supplementary queries fill in the pieces ``files_stats`` can't
    carry cheaply:

      * recent recording-bytes rate (needs ``recordings.start_time``)
      * oldest start_time per camera (can't maintain MIN under DELETE)
      * per-tier backlog existence (per-camera EXISTS with partial indexes)

    ``conn`` may be passed in by the publisher (reused across publishes so
    the page cache stays warm).  If ``None`` — tests and ad-hoc callers —
    opens a transient ro connection and closes it on return.
    """
    cfg = ctx.cfg
    now = time.time()
    rate_window = float(cfg.mqtt.rate_window_seconds)
    rate_cutoff = now - rate_window

    if conn is None:
        conn = _open_stats_conn(cfg)
        opened_here = True
    else:
        opened_here = False
    try:
        # Per (camera, rtype) rollup from the materialised stats table.
        # files_stats carries tier0/tier1/tier2 bytes so each row is one
        # rtype bucket for a camera — the publisher fans it out into
        # CameraStats fields.
        stats_rows = conn.execute(
            """
            SELECT camera, rtype, files_count,
                   tier0_bytes, tier1_bytes, tier2_bytes
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
            FROM frigate_stats.recordings INDEXED BY recordings_start_time
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
                "SELECT MIN(start_time) FROM frigate_stats.recordings WHERE camera = ?",
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
    finally:
        if opened_here:
            conn.close()

    recording_rate: dict[str, float] = {
        r["camera"]: float(r["bytes"] or 0) / rate_window for r in recent_rows
    }

    cameras: dict[str, dict] = {}
    top_total_bytes = 0
    top_total_files = 0
    top_tier_bytes = {0: 0, 1: 0, 2: 0}
    top_oldest: float | None = None

    for row in stats_rows:
        cam = row["camera"]
        rtype = row["rtype"]
        files = int(row["files_count"] or 0)
        tier0 = int(row["tier0_bytes"] or 0)
        tier1 = int(row["tier1_bytes"] or 0)
        tier2 = int(row["tier2_bytes"] or 0)
        bucket_bytes = tier0 + tier1 + tier2

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

        top_total_bytes += bucket_bytes
        top_total_files += files
        top_tier_bytes[0] += tier0
        top_tier_bytes[1] += tier1
        top_tier_bytes[2] += tier2

    # Stitch in the oldest start_time per camera (separate query).
    for cam_name, oldest in oldest_by_camera.items():
        c = cameras.setdefault(
            cam_name,
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
    # Binary (problem) sensors: ON when the camera's eligible backlog at
    # that tier has been sitting past the backlog timeout.  Routing to
    # HA's binary_sensor component is keyed off ``device_class == 'problem'``.
    (
        "tier1_backlog_error",
        "Tier 1 backlog",
        None,
        "problem",
        "mdi:alert-circle",
        False,
    ),
    (
        "tier2_backlog_error",
        "Tier 2 backlog",
        None,
        "problem",
        "mdi:alert-circle",
        False,
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
        # Persistent ro compress-db conn for ``collect_frigate_stats`` —
        # lazy-opened on the publisher thread's first publish and reused
        # for the publisher's lifetime.  Owned here (not on ctx) because
        # SQLite connections aren't meant to be handed between threads,
        # and the publisher is the only caller on its thread.
        self._stats_conn: sqlite3.Connection | None = None
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
        if self._stats_conn is not None:
            try:
                self._stats_conn.close()
            except Exception:
                pass
            self._stats_conn = None
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
        if self._stats_conn is None:
            # First publish on this thread: open the persistent conn here
            # (not in ``start()``) so the attach+schema-read happen on the
            # publisher thread, which is the only thread that will use it.
            self._stats_conn = _open_stats_conn(self.ctx.cfg)
        stats = collect_frigate_stats(self.ctx, self._stats_conn)
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
        values: dict[str, float | int | bool | None] = {
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
        values["tier1_backlog_error"] = cs.tier1_backlog_error
        values["tier2_backlog_error"] = cs.tier2_backlog_error
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
            except Exception as e:
                log("WARNING", f"MQTT state publish failed for {key}: {e}")
