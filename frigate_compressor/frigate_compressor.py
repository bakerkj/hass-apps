# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""
frigate_compressor.py
=====================
Long-running daemon that compresses old Frigate NVR recordings.

REVISION HISTORY
────────────────
v1.0  2026-04-05  Initial version.

COMPRESSION PIPELINE
────────────────────
ENCODER: Intel Quick Sync Video (QSV) via h264_qsv  [current: i9-10900 iGPU]
  - Full GPU pipeline: decode (qsv) → scale (scale_qsv) → encode (h264_qsv)
  - Frames never leave GPU memory = low CPU, fast throughput
  - Falls back to h264_nvenc (NVIDIA) or libx264 (CPU) per config

CODEC: H.264
  - H.265 saves ~30% more space but breaks browser playback in Frigate UI
  - H.264 plays natively in every browser without transcoding

QUALITY CONTROL
  - QSV:     -global_quality (0-51, lower = better, ~equivalent to CRF)
  - NVENC:   -cq            (same 0-51 scale)
  - libx264: -crf           (same 0-51 scale)

SCALING: scale_qsv (GPU-native, zero-copy for QSV) / scale= (CPU fallback)
  Per-tier, per-type scale modes:
    none     = keep original resolution
    halve    = iw/2:ih/2 — halves both dimensions
    fixed    = scale to exact WxH in scale_value e.g. "1280:720"
    fraction = multiply source dimensions by float(scale_value)
  Per-camera overrides (camera_overrides) take precedence over tier/type defaults.

TIER LOGIC
──────────
Tier 1 (tier1.min_days → tier2.min_days): recent footage, per-type settings
Tier 2 (tier2.min_days+): archive footage, per-type settings with harder compression

RECORDING TYPES (within each tier)
───────────────────────────────────
  continuous = no motion, no objects detected
  motion     = motion detected, no object detection hit
  object     = at least one object detected (highest value footage)

METADATA PRESERVATION
─────────────────────
  -map_metadata 0      : preserves MP4 timestamps Frigate relies on
  -movflags +faststart : matches how Frigate writes files; enables HTTP seeking

SEGMENT SIZE UPDATE
───────────────────
  After successful compression, updates segment_size (MB) in Frigate's
  recordings table so Frigate's storage UI reflects actual disk usage.

  Safety rationale (audited against Frigate source 2026-04-05):
  - Frigate writes segment_size exactly once at recording creation
    (frigate/record/maintainer.py) and never updates it again.  Our UPDATE
    is the only writer to this column after insert — no conflict possible.
  - No triggers or CHECK constraints exist on segment_size.
  - Frigate's ORM (peewee SqliteQueueDatabase) serialises all its own writes
    through one background thread.  In WAL mode our external UPDATE competes
    safely; busy_timeout=10000 handles any transient contention.
  - Frigate never reads the actual file size from disk after initial insert.
    All storage logic (UI, quota, cleanup) uses the DB value exclusively, so
    our UPDATE is the correct and only mechanism to keep the model accurate.

  Behavioral side-effects of accurate (smaller) segment_size values:
  - Storage UI immediately reflects the post-compression size.  (Intended.)
  - Frigate's MB/hr bandwidth estimate (rolling 100-segment average) drifts
    down over time as compressed segments enter the window.  This causes
    disk-pressure cleanup to trigger less frequently — accurately reflecting
    the smaller on-disk footprint.  Desirable, not a bug.
  - Time-based retention (expire_existing_camera_recordings) does not read
    segment_size at all; it is unaffected.

  If the update fails (e.g. DB locked), the compress DB records
  status='segment_update_failed'.  Housekeeping retries: reads the actual
  file size from disk and re-attempts the Frigate DB write; on success
  promotes the row to status='ok'.

DATABASE
────────
  Frigate DB (/addon_configs/ccab4aaf_frigate-fa/frigate.db): read for inventory, write only segment_size
  Compress DB (/data/compress.db):  full compression tracking, savings views

FUTURE GPU MIGRATION (RTX 3090)
────────────────────────────────
  Set encoder="nvenc" in add-on options. Everything else is automatic.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import sqlite3
import subprocess
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

import paho.mqtt.client as paho_mqtt
import yaml

# Version is supplied at build time by the HA Supervisor (BUILD_VERSION build
# arg, sourced from config.json). config.json is the single source of truth.
__version__ = os.environ.get("ADDON_VERSION", "dev")


# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

# A message is printed when its rank >= the configured log level's rank.
# Higher rank = more severe: DEBUG(0) < INFO(1) < WARNING(2) < ERROR(3).
LOG_LEVEL_RANK = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3}
_log_level = "INFO"


def log(level: str, msg: str) -> None:
    if LOG_LEVEL_RANK.get(level, 1) >= LOG_LEVEL_RANK.get(_log_level, 1):
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"{ts} [{level}] {msg}", flush=True)


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class TypeSettings:
    """Compression settings for one recording type within a tier."""

    enabled: bool  # whether to compress this recording type
    quality: int  # CQ/CRF (0-51, lower = better quality)
    scale_mode: str  # none | halve | fixed | fraction
    scale_value: str  # fixed="1280:720", fraction="0.5"
    fps_mode: str  # none | cap | fraction
    fps_value: float  # cap=max fps, fraction=multiplier (e.g. 0.5 = half)


_TYPE_SETTINGS_FIELDS = (
    "enabled",
    "quality",
    "scale_mode",
    "scale_value",
    "fps_mode",
    "fps_value",
)
_RECORDING_TYPES = ("continuous", "motion", "object")


@dataclass
class TierConfig:
    """Compression settings for one age tier (tier 1 or tier 2)."""

    enabled: bool  # whether this tier is active for this camera
    min_days: int  # age in days before this tier applies
    continuous: TypeSettings  # segments with no motion and no objects
    motion: TypeSettings  # segments with motion but no object detection
    object: TypeSettings  # segments with at least one detected object


@dataclass
class CameraConfig:
    """Fully resolved compression configuration for one camera."""

    name: str
    enabled: bool  # whether to process this camera at all
    dry_run: bool  # per-camera dry run
    tier1: TierConfig
    tier2: TierConfig


@dataclass
class MqttConfig:
    """MQTT publishing configuration. Disabled when ``host`` is empty."""

    host: str
    port: int
    username: str
    password: str
    discovery_prefix: str
    base_topic: str
    client_id: str
    publish_interval_seconds: int
    rate_window_seconds: int
    disconnect_timeout_seconds: int = 300

    @property
    def enabled(self) -> bool:
        return bool(self.host)


class MqttHealth:
    """Liveness tracking for the MQTT publisher, mirrors other addons.

    The watchdog in the publish loop reads ``connected``, ``last_disconnect``
    and ``last_state_publish_ok`` to decide whether to trigger a
    supervisor restart (exit code 11 for stuck-disconnected, 12 for
    connected-but-no-publishes).
    """

    def __init__(self) -> None:
        self.connected: bool = False
        self.last_connect_ok: float = 0.0
        self.last_disconnect: float = 0.0
        self.last_state_publish_ok: float = 0.0


@dataclass
class Config:
    """Top-level add-on configuration.

    HAOS options (options.json) provide infrastructure settings (encoder,
    paths, MQTT).  Camera-centric compression settings come from a separate
    YAML config file and are resolved into per-camera ``CameraConfig`` objects.
    """

    encoder: str  # qsv | vaapi | nvenc | cpu
    max_parallel_jobs: int  # concurrent ffmpeg processes
    housekeeping_interval_days: int  # days between housekeeping runs
    frigate_db: Path  # path to Frigate's SQLite DB
    recordings_dir: Path  # path to Frigate's recordings
    compress_db: Path  # path to our SQLite DB
    log_level: str  # DEBUG | INFO | WARNING | ERROR
    cameras: dict[str, CameraConfig]  # camera_name → fully resolved config
    mqtt: MqttConfig = field(
        default_factory=lambda: MqttConfig(
            host="",
            port=1883,
            username="",
            password="",
            discovery_prefix="homeassistant",
            base_topic="frigate_compressor",
            client_id="frigate-compressor",
            publish_interval_seconds=60,
            rate_window_seconds=300,
            disconnect_timeout_seconds=300,
        )
    )

    @property
    def all_dry_run(self) -> bool:
        """True when every configured camera is in dry-run mode."""
        return (
            all(cam.dry_run for cam in self.cameras.values()) if self.cameras else True
        )


# ── Built-in defaults (match the YAML ``defaults`` block) ────────────────────
# These are used when no config.yaml exists or when fields are omitted.

_BUILTIN_DEFAULTS: dict = {
    "enabled": True,
    "dry_run": False,
    "tier1": {
        "enabled": True,
        "min_days": 7,
        "quality": 28,
        "scale_mode": "none",
        "scale_value": "",
        "fps_mode": "none",
        "fps_value": 1.0,
        "motion": {
            "quality": 26,
            "scale_mode": "halve",
        },
        "object": {
            "quality": 22,
        },
    },
    "tier2": {
        "enabled": True,
        "min_days": 30,
        "quality": 34,
        "scale_mode": "halve",
        "scale_value": "",
        "fps_mode": "cap",
        "fps_value": 4.0,
        "motion": {
            "quality": 30,
            "fps_value": 8.0,
        },
        "object": {
            "quality": 26,
            "fps_value": 8.0,
        },
    },
}


def _merge_type_fields(base: dict, overlay: dict) -> dict:
    """Merge overlay's TypeSettings fields on top of base."""
    result = {k: base[k] for k in _TYPE_SETTINGS_FIELDS if k in base}
    for k in _TYPE_SETTINGS_FIELDS:
        if k in overlay:
            result[k] = overlay[k]
    return result


def _validate_type_settings(d: dict, label: str) -> TypeSettings:
    """Validate and construct a TypeSettings from a fully-merged dict."""
    quality = int(d["quality"])
    if not 0 <= quality <= 51:
        raise ValueError(f"{label}: quality must be 0–51, got {quality}")
    scale_mode = str(d["scale_mode"])
    scale_value = str(d["scale_value"])
    if scale_mode == "fixed" and not scale_value:
        raise ValueError(
            f"{label}: scale_mode='fixed' requires a non-empty scale_value "
            "(e.g. '1280:720')"
        )
    return TypeSettings(
        enabled=bool(d.get("enabled", True)),
        quality=quality,
        scale_mode=scale_mode,
        scale_value=scale_value,
        fps_mode=str(d["fps_mode"]),
        fps_value=float(d["fps_value"]),
    )


def _resolve_tier(
    defaults_tier: dict,
    camera_tier: dict | None,
    camera_name: str,
    tier_num: int,
) -> TierConfig:
    """Resolve a single tier's config using 4-layer merge.

    Resolution order (later layers override earlier):
      1. defaults tier base TypeSettings fields
      2. defaults tier per-type overrides (continuous/motion/object)
      3. camera tier base TypeSettings fields
      4. camera tier per-type overrides
    """
    # Tier-level scalars
    enabled = defaults_tier.get("enabled", True)
    min_days = int(defaults_tier.get("min_days", 7))
    if camera_tier:
        if "enabled" in camera_tier:
            enabled = bool(camera_tier["enabled"])
        if "min_days" in camera_tier:
            min_days = int(camera_tier["min_days"])

    # Base TypeSettings from defaults tier
    base = {k: defaults_tier[k] for k in _TYPE_SETTINGS_FIELDS if k in defaults_tier}

    types: dict[str, TypeSettings] = {}
    for rtype in _RECORDING_TYPES:
        label = f"{camera_name}/tier{tier_num}/{rtype}"
        # Layer 1: defaults tier base
        merged = dict(base)
        # Layer 2: defaults tier per-type override
        defaults_type = defaults_tier.get(rtype)
        if isinstance(defaults_type, dict):
            merged = _merge_type_fields(merged, defaults_type)
        # Layer 3: camera tier base
        if camera_tier:
            merged = _merge_type_fields(merged, camera_tier)
        # Layer 4: camera tier per-type override
        if camera_tier:
            camera_type = camera_tier.get(rtype)
            if isinstance(camera_type, dict):
                merged = _merge_type_fields(merged, camera_type)
        types[rtype] = _validate_type_settings(merged, label)

    return TierConfig(
        enabled=bool(enabled),
        min_days=min_days,
        continuous=types["continuous"],
        motion=types["motion"],
        object=types["object"],
    )


def _resolve_camera(
    name: str,
    defaults: dict,
    camera: dict | None,
) -> CameraConfig:
    """Resolve a single camera's full config by merging defaults + camera block."""
    enabled = defaults.get("enabled", True)
    dry_run = defaults.get("dry_run", False)
    if camera:
        if "enabled" in camera:
            enabled = bool(camera["enabled"])
        if "dry_run" in camera:
            dry_run = bool(camera["dry_run"])

    return CameraConfig(
        name=name,
        enabled=bool(enabled),
        dry_run=bool(dry_run),
        tier1=_resolve_tier(
            defaults.get("tier1") or {},
            (camera or {}).get("tier1"),
            name,
            1,
        ),
        tier2=_resolve_tier(
            defaults.get("tier2") or {},
            (camera or {}).get("tier2"),
            name,
            2,
        ),
    )


def _discover_cameras(frigate_db: Path) -> list[str]:
    """Return distinct camera names from Frigate's recordings table."""
    conn = sqlite3.connect(f"file:{frigate_db}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "SELECT DISTINCT camera FROM recordings ORDER BY camera"
        ).fetchall()
        return [row[0] for row in rows]
    finally:
        conn.close()


def _merge_defaults(builtin: dict, user: dict) -> dict:
    """Deep-merge the user's ``defaults`` block on top of built-in defaults.

    Only dicts are recursed into; scalar values in *user* replace *builtin*.
    """
    result = dict(builtin)
    for k, v in user.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _merge_defaults(result[k], v)
        else:
            result[k] = v
    return result


def load_config(options_path: str, yaml_path: str | None = None) -> Config:
    """Load config from HAOS options.json + camera YAML config file.

    If *yaml_path* is not provided, it is read from the ``config_path`` key
    in options.json (defaulting to
    ``/addon_configs/frigate_compressor/config.yaml``).
    """
    with open(options_path, "r", encoding="utf-8") as f:
        opts = json.load(f)

    if yaml_path is None:
        yaml_path = str(
            opts.get(
                "config_path",
                "/addon_configs/frigate_compressor/config.yaml",
            )
        )

    # Load YAML config (defaults + cameras).  Missing file → empty config.
    yaml_cfg: dict = {}
    yaml_file = Path(yaml_path)
    if yaml_file.is_file():
        with open(yaml_file, "r", encoding="utf-8") as f:
            yaml_cfg = yaml.safe_load(f) or {}

    # Merge user defaults on top of built-in defaults.
    defaults = _merge_defaults(_BUILTIN_DEFAULTS, yaml_cfg.get("defaults") or {})

    # MQTT from options.json
    mqtt_cfg = MqttConfig(
        host=str(opts.get("mqtt_host", "") or ""),
        port=int(opts.get("mqtt_port", 1883)),
        username=str(opts.get("mqtt_username", "") or ""),
        password=str(opts.get("mqtt_password", "") or ""),
        discovery_prefix=str(opts.get("mqtt_discovery_prefix", "homeassistant")),
        base_topic=str(opts.get("mqtt_base_topic", "frigate_compressor")),
        client_id=str(opts.get("mqtt_client_id", "frigate-compressor")),
        publish_interval_seconds=int(opts.get("mqtt_publish_interval_seconds", 60)),
        rate_window_seconds=int(opts.get("rate_window_seconds", 300)),
        disconnect_timeout_seconds=max(
            5, int(opts.get("mqtt_disconnect_timeout_seconds", 300))
        ),
    )

    frigate_db = Path(
        opts.get("frigate_db", "/addon_configs/ccab4aaf_frigate-fa/frigate.db")
    )
    recordings_dir = Path(opts.get("recordings_dir", "/media/frigate/recordings"))

    if not frigate_db.exists():
        raise FileNotFoundError(f"frigate_db not found: {frigate_db}")
    if not recordings_dir.is_dir():
        raise FileNotFoundError(f"recordings_dir not found: {recordings_dir}")

    # Discover cameras from Frigate DB, then resolve configs.
    discovered = _discover_cameras(frigate_db)
    yaml_cameras: dict = yaml_cfg.get("cameras") or {}

    # All camera names: union of YAML-configured + Frigate-discovered.
    all_names = sorted(set(list(yaml_cameras.keys()) + discovered))

    cameras: dict[str, CameraConfig] = {}
    for name in all_names:
        cam_block = yaml_cameras.get(name)  # None for discovered-only cameras
        cameras[name] = _resolve_camera(name, defaults, cam_block)

    # Per-camera tier ordering validation.
    for name, cam_cfg in cameras.items():
        if cam_cfg.tier2.min_days <= cam_cfg.tier1.min_days:
            raise ValueError(
                f"Camera '{name}': tier2.min_days ({cam_cfg.tier2.min_days}) must be "
                f"greater than tier1.min_days ({cam_cfg.tier1.min_days})"
            )

    return Config(
        encoder=opts.get("encoder", "qsv"),
        max_parallel_jobs=int(opts.get("max_parallel_jobs", 2)),
        housekeeping_interval_days=int(opts.get("housekeeping_interval_days", 7)),
        frigate_db=frigate_db,
        recordings_dir=recordings_dir,
        compress_db=Path(opts.get("compress_db", "/data/compress.db")),
        log_level=(opts.get("log_level") or "INFO").upper(),
        cameras=cameras,
        mqtt=mqtt_cfg,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# COMPRESS DATABASE
# ═══════════════════════════════════════════════════════════════════════════════

# Compress DB status values — use these constants everywhere instead of
# bare string literals so a typo becomes a NameError, not a silent data bug.
STATUS_OK = "ok"
STATUS_ERROR = "error"
STATUS_SEGMENT_UPDATE_FAILED = "segment_update_failed"

SCHEMA = f"""
CREATE TABLE IF NOT EXISTS compressed_files (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    recording_id    TEXT    NOT NULL UNIQUE,
    camera          TEXT    NOT NULL,
    path            TEXT    NOT NULL,
    tier            INTEGER NOT NULL,
    recording_type  TEXT    NOT NULL,
    encoder         TEXT    NOT NULL,
    size_before     INTEGER,
    size_after      INTEGER,
    duration_sec    REAL,
    last_attempted_at TEXT    NOT NULL,  -- ISO8601, updated on every attempt
    status          TEXT    NOT NULL,
    error_msg       TEXT
);

CREATE INDEX IF NOT EXISTS idx_recording_id ON compressed_files(recording_id);
CREATE INDEX IF NOT EXISTS idx_camera       ON compressed_files(camera);
CREATE INDEX IF NOT EXISTS idx_status       ON compressed_files(status);

CREATE VIEW IF NOT EXISTS savings_by_camera AS
SELECT
    camera,
    COUNT(*)                                                            AS files_compressed,
    SUM(size_before)                                                    AS bytes_before,
    SUM(size_after)                                                     AS bytes_after,
    SUM(size_before - size_after)                                       AS bytes_saved,
    ROUND(AVG(1.0 - CAST(size_after AS REAL) / size_before) * 100, 1) AS avg_reduction_pct,
    MIN(last_attempted_at)                                              AS first_compressed,
    MAX(last_attempted_at)                                              AS last_compressed
FROM compressed_files
WHERE status = '{STATUS_OK}'
  AND size_before > 0
  AND size_after  > 0
GROUP BY camera;

CREATE VIEW IF NOT EXISTS recent_errors AS
SELECT
    camera,
    path,
    tier,
    last_attempted_at,
    error_msg
FROM compressed_files
WHERE status = '{STATUS_ERROR}'
  AND last_attempted_at >= datetime('now', '-7 days')
ORDER BY last_attempted_at DESC;
"""


def open_compress_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(f"file:{path}", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.executescript(SCHEMA)
    return conn


def open_frigate_db(path: Path) -> sqlite3.Connection:
    """Open Frigate's DB read-only (WAL-safe)."""
    conn = sqlite3.connect(
        f"file:{path}?mode=ro",
        uri=True,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def open_frigate_db_rw(path: Path) -> sqlite3.Connection:
    """Separate RW connection used only for segment_size updates."""
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


# Columns we read from or write to in Frigate's recordings table.
_REQUIRED_FRIGATE_COLUMNS: frozenset[str] = frozenset(
    {"id", "camera", "path", "start_time", "motion", "objects", "segment_size"}
)


def check_frigate_schema(conn: sqlite3.Connection) -> None:
    """Verify that Frigate's recordings table has all expected columns.

    Raises RuntimeError with a descriptive message if the table is absent or
    any required column is missing.  Call this once at startup before entering
    the main loop so that schema drift is caught immediately rather than
    silently producing wrong results hours later.
    """
    rows = conn.execute("PRAGMA table_info(recordings)").fetchall()
    if not rows:
        raise RuntimeError(
            "Frigate DB does not contain a 'recordings' table. "
            "Is this the right database file?"
        )
    present = {row["name"] for row in rows}
    missing = _REQUIRED_FRIGATE_COLUMNS - present
    if missing:
        raise RuntimeError(
            f"Frigate DB schema drift detected — missing column(s): "
            f"{', '.join(sorted(missing))}. "
            f"Check whether Frigate was upgraded and review the column list in "
            f"_REQUIRED_FRIGATE_COLUMNS."
        )


# ═══════════════════════════════════════════════════════════════════════════════
# ENCODER DETECTION
# ═══════════════════════════════════════════════════════════════════════════════


def detect_encoder(preferred: str) -> str:
    if preferred == "cpu":
        return "cpu"
    try:
        result = subprocess.run(
            ["ffmpeg", "-hide_banner", "-encoders"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        output = result.stdout + result.stderr
        if preferred == "qsv" and "h264_qsv" in output:
            return "qsv"
        if preferred == "vaapi" and "h264_vaapi" in output:
            return "vaapi"
        if preferred == "nvenc" and "h264_nvenc" in output:
            return "nvenc"
    except Exception as e:
        log("WARNING", f"ffmpeg encoder probe failed: {e}")
    log(
        "WARNING",
        f"Encoder '{preferred}' not available — falling back to CPU (libx264)",
    )
    return "cpu"


# Encoder-specific test commands.  Each one synthesizes a 1-second test
# pattern with lavfi (no input file needed) and runs it through the chosen
# hardware encoder, discarding the output.  If the encoder can't reach the
# GPU/driver/cgroup, the command exits non-zero with a stderr message that
# names the actual problem ("Operation not permitted", "No VA display
# found", "Cannot load nvcuda.dll", etc.).
_ENCODER_SELF_TEST_CMDS: dict[str, list[str]] = {
    "qsv": [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc2=duration=1:size=320x240:rate=10",
        "-c:v",
        "h264_qsv",
        "-global_quality",
        "28",
        "-f",
        "null",
        "-",
    ],
    "vaapi": [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-vaapi_device",
        "/dev/dri/renderD128",
        "-f",
        "lavfi",
        "-i",
        "testsrc2=duration=1:size=320x240:rate=10",
        "-vf",
        "format=nv12,hwupload",
        "-c:v",
        "h264_vaapi",
        "-qp",
        "28",
        "-f",
        "null",
        "-",
    ],
    "nvenc": [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc2=duration=1:size=320x240:rate=10",
        "-c:v",
        "h264_nvenc",
        "-cq",
        "28",
        "-f",
        "null",
        "-",
    ],
    "cpu": [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc2=duration=1:size=320x240:rate=10",
        "-c:v",
        "libx264",
        "-crf",
        "28",
        "-preset",
        "ultrafast",
        "-f",
        "null",
        "-",
    ],
}


def check_encoder_works(encoder: str) -> tuple[bool, str]:
    """Run a 1-second synthetic encode to confirm the encoder is reachable.

    Returns (ok, message).  Used at startup to fail fast when hardware
    acceleration is misconfigured (missing /dev/dri cgroup access, broken
    driver, libmfx not initializing) instead of producing errors per-file.
    """
    cmd = _ENCODER_SELF_TEST_CMDS.get(encoder)
    if cmd is None:
        return False, f"unknown encoder '{encoder}'"

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except subprocess.TimeoutExpired:
        return False, "self-test ffmpeg timed out after 30s"
    except OSError as e:
        return False, f"failed to invoke ffmpeg: {e}"

    if result.returncode != 0:
        # Concatenate all non-empty stderr lines so the actual driver error
        # (e.g. "Error creating a MFX session: -9", which appears EARLY)
        # isn't lost behind ffmpeg's generic trailing line ("Error opening
        # output files: Invalid argument").
        err_lines = [
            line.strip() for line in (result.stderr or "").splitlines() if line.strip()
        ]
        msg = " | ".join(err_lines) if err_lines else f"rc={result.returncode}"
        return False, msg[:FFMPEG_STDERR_MAX_LEN]

    return True, "ok"


# ═══════════════════════════════════════════════════════════════════════════════
# FFMPEG HELPERS
# ═══════════════════════════════════════════════════════════════════════════════


def _probe_video(filepath: Path) -> tuple[tuple[int, int] | None, float | None]:
    """
    Single ffprobe call that returns (dims, fps) from the MP4 container header.
    dims = (width, height) or None
    fps  = float or None
    Lightweight — reads container metadata only, no frame decoding.
    """
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "quiet",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=width,height,r_frame_rate",
                "-of",
                "default=noprint_wrappers=1",
                str(filepath),
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return None, None

        data: dict[str, str] = {}
        for line in result.stdout.strip().splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                data[k.strip()] = v.strip()

        dims: tuple[int, int] | None = None
        fps: float | None = None

        if "width" in data and "height" in data:
            try:
                dims = (int(data["width"]), int(data["height"]))
            except (ValueError, TypeError):
                pass

        if "r_frame_rate" in data:
            try:
                parts = data["r_frame_rate"].split("/")
                fps = (
                    float(parts[0]) / float(parts[1])
                    if len(parts) == 2
                    else float(parts[0])
                )
            except (ValueError, TypeError, ZeroDivisionError):
                pass

        return dims, fps
    except Exception as e:
        log("WARNING", f"ffprobe failed for {filepath}: {e}")
        return None, None


def _build_scale_filter(
    mode: str,
    value: str,
    encoder: str,
    source_dims: tuple[int, int] | None,
) -> str:
    """
    Returns an ffmpeg scale filter string, or empty string for no scaling.

    mode="none"     : no scaling — keep original resolution
    mode="halve"    : iw/2:ih/2 — halve both dimensions
    mode="fixed"    : exact dimensions in value e.g. "1280:720"
    mode="fraction" : multiply source dimensions by float(value)
                      Falls back to halve if source dims unavailable.
    For QSV encoder: uses scale_qsv= (GPU-native). Otherwise: scale=.
    """
    if mode == "none":
        return ""

    if mode == "halve":
        dims = "iw/2:ih/2"
    elif mode == "fixed":
        dims = value
    elif mode == "fraction":
        try:
            frac = float(value)
            if source_dims is not None:
                w = max(2, int(round(source_dims[0] * frac)) & ~1)
                h = max(2, int(round(source_dims[1] * frac)) & ~1)
                dims = f"{w}:{h}"
            else:
                dims = "iw/2:ih/2"  # fallback if ffprobe failed
        except (ValueError, TypeError):
            dims = "iw/2:ih/2"
    else:
        return ""

    if encoder == "qsv":
        return f"scale_qsv={dims}"
    if encoder == "vaapi":
        return f"scale_vaapi={dims}"
    return f"scale={dims}"


def _build_fps_filter(mode: str, value: float, source_fps: float | None) -> str:
    """
    Returns an ffmpeg fps filter string, or empty string for no fps change.

    mode="none"     : no filter — keep original framerate
    mode="cap"      : hard cap at value fps
    mode="fraction" : multiply source fps by value
                      Falls back to treating value as absolute cap if source fps unavailable.
    """
    if mode == "none":
        return ""
    if mode == "cap":
        fps = max(1, int(round(value)))
        return f"fps={fps}"
    if mode == "fraction":
        if source_fps is not None:
            fps = max(1, int(round(source_fps * value)))
        else:
            fps = max(1, int(round(value)))
        return f"fps={fps}"
    return ""


_ENCODER_PARAMS: dict[str, dict] = {
    "qsv": {
        "hwaccel": ("qsv", "qsv"),
        "hwaccel_extra": [],
        "codec": "h264_qsv",
        "quality_flag": "-global_quality",
        "preset_flag": "-preset",
        "preset": "slower",
    },
    "vaapi": {
        "hwaccel": ("vaapi", "vaapi"),
        # VA-API needs an explicit render node — auto-detect is unreliable
        # when libva probes wayland/X11 first.
        "hwaccel_extra": ["-hwaccel_device", "/dev/dri/renderD128"],
        "codec": "h264_vaapi",
        "quality_flag": "-qp",
        # h264_vaapi uses -compression_level. On Intel iHD VA-API, level 4
        # (the driver default) is the sweet spot: empirical benchmark on 10
        # files × 10 iters showed level 1 is ~29% slower AND produces ~2%
        # LARGER total output than level 4. Levels 4 and 7 are essentially
        # identical. See "Choosing the encoder" in README.md.
        "preset_flag": "-compression_level",
        "preset": "4",
    },
    "nvenc": {
        "hwaccel": ("cuda", "cuda"),
        "hwaccel_extra": [],
        "codec": "h264_nvenc",
        "quality_flag": "-cq",
        "preset_flag": "-preset",
        "preset": "p4",
    },
    "cpu": {
        "hwaccel": None,
        "hwaccel_extra": [],
        "codec": "libx264",
        "quality_flag": "-crf",
        "preset_flag": "-preset",
        "preset": "fast",
    },
}


def build_ffmpeg_cmd(
    input_path: Path,
    output_path: Path,
    encoder: str,
    ts: TypeSettings,
) -> list[str]:
    quality = ts.quality

    # Run ffprobe only when needed (fraction modes require actual source values)
    need_dims = ts.scale_mode == "fraction"
    need_fps = ts.fps_mode == "fraction"
    source_dims: tuple[int, int] | None = None
    source_fps: float | None = None

    if need_dims or need_fps:
        source_dims, source_fps = _probe_video(input_path)

    fps_filter = _build_fps_filter(ts.fps_mode, ts.fps_value, source_fps)
    scale = _build_scale_filter(ts.scale_mode, ts.scale_value, encoder, source_dims)

    vf_parts = [f for f in [fps_filter, scale] if f]
    vf_filter = ",".join(vf_parts)

    common_out = [
        "-c:a",
        "copy",
        "-map_metadata",
        "0",
        "-movflags",
        "+faststart",
        str(output_path),
    ]
    vf_args = ["-vf", vf_filter] if vf_filter else []

    enc = _ENCODER_PARAMS.get(encoder, _ENCODER_PARAMS["cpu"])
    hwaccel_args = (
        [
            "-hwaccel",
            enc["hwaccel"][0],
            "-hwaccel_output_format",
            enc["hwaccel"][1],
            *enc["hwaccel_extra"],
        ]
        if enc["hwaccel"]
        else []
    )
    return [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        *hwaccel_args,
        "-i",
        str(input_path),
        *vf_args,
        "-c:v",
        enc["codec"],
        enc["quality_flag"],
        str(quality),
        enc["preset_flag"],
        enc["preset"],
        *common_out,
    ]


# ═══════════════════════════════════════════════════════════════════════════════
# COMPRESSION WORKER
# ═══════════════════════════════════════════════════════════════════════════════

# Temp files are named .tmp.{recording_id}.mp4 so they are:
#   - unique per recording (no collision between parallel jobs)
#   - distinguishable from real recordings by housekeeping
_TEMP_PREFIX = ".tmp."
_TEMP_GLOB = ".tmp.*.mp4"

# Max wall-clock seconds to allow a single ffmpeg encode job to run.
FFMPEG_TIMEOUT_SEC = 300

# Max bytes of ffmpeg stderr text stored in the compress DB error_msg column.
FFMPEG_STDERR_MAX_LEN = 300

# Bounds on the main loop's sleep between compression passes.
# - MIN avoids hammering Frigate's DB when a recording is just barely
#   under the eligibility threshold.
# - MAX caps the wait so that even pathological states (frigate paused,
#   long fallback paths) re-check at least every 10 minutes instead of
#   sleeping for hours or days at a time.
MIN_SLEEP_SEC = 60.0
MAX_SLEEP_SEC = 600.0


@dataclass
class CompressorContext:
    """Shared, per-daemon state passed to every compression worker."""

    cfg: Config
    compress_db: sqlite3.Connection
    db_lock: threading.Lock
    frigate_ro: sqlite3.Connection
    frigate_ro_lock: threading.Lock
    frigate_rw: sqlite3.Connection
    frigate_lock: threading.Lock


def _recording_type(motion: int | None, objects: int | None) -> str:
    """Classify a recording by its motion/objects counts. Priority: object > motion > continuous."""
    if objects:
        return "object"
    if motion:
        return "motion"
    return "continuous"


def _record(
    conn: sqlite3.Connection,
    lock: threading.Lock,
    *,
    recording_id: str,
    camera: str,
    path: str,
    tier: int,
    recording_type: str,
    encoder: str,
    size_before: int | None,
    size_after: int | None,
    duration_sec: float | None,
    status: str,
    error_msg: str | None = None,
) -> None:
    now = time.strftime("%Y-%m-%dT%H:%M:%S")
    with lock:
        conn.execute(
            """
            INSERT INTO compressed_files
                (recording_id, camera, path, tier, recording_type, encoder,
                 size_before, size_after, duration_sec,
                 last_attempted_at, status, error_msg)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(recording_id) DO UPDATE SET
                tier              = excluded.tier,
                recording_type    = excluded.recording_type,
                encoder           = excluded.encoder,
                size_before       = excluded.size_before,
                size_after        = excluded.size_after,
                duration_sec      = excluded.duration_sec,
                last_attempted_at = excluded.last_attempted_at,
                status            = excluded.status,
                error_msg         = excluded.error_msg
            """,
            (
                recording_id,
                camera,
                path,
                tier,
                recording_type,
                encoder,
                size_before,
                size_after,
                duration_sec,
                now,
                status,
                error_msg,
            ),
        )
        conn.commit()


def compress_one(
    recording_id: str,
    path: str,
    camera: str,
    tier: int,
    recording_type: str,
    encoder: str,
    ctx: CompressorContext,
) -> bool:
    cfg = ctx.cfg
    compress_db = ctx.compress_db
    db_lock = ctx.db_lock
    frigate_ro = ctx.frigate_ro
    frigate_ro_lock = ctx.frigate_ro_lock
    frigate_rw = ctx.frigate_rw
    frigate_lock = ctx.frigate_lock

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
        _record(
            compress_db,
            db_lock,
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
            f"[{camera}] DRY RUN tier={tier} type={recording_type} "
            f"{_display_path(filepath)} ({_fmt(size_before)})",
        )
        return True

    t_start = time.monotonic()
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=FFMPEG_TIMEOUT_SEC
        )
    except subprocess.TimeoutExpired:
        duration = time.monotonic() - t_start
        tmpfile.unlink(missing_ok=True)
        rec(
            size_before=size_before,
            size_after=None,
            duration_sec=duration,
            status=STATUS_ERROR,
            error_msg=f"timeout after {FFMPEG_TIMEOUT_SEC}s",
        )
        log(
            "WARNING",
            f"[{camera}] ffmpeg timeout after {duration:.1f}s "
            f"(limit {FFMPEG_TIMEOUT_SEC}s): {_display_path(filepath)}",
        )
        return False
    except Exception as e:
        duration = time.monotonic() - t_start
        tmpfile.unlink(missing_ok=True)
        rec(
            size_before=size_before,
            size_after=None,
            duration_sec=duration,
            status=STATUS_ERROR,
            error_msg=f"ffmpeg exception: {e}",
        )
        log(
            "ERROR",
            f"[{camera}] ffmpeg raised unexpected exception after {duration:.1f}s: {e}",
        )
        return False

    duration = time.monotonic() - t_start

    if result.returncode != 0:
        tmpfile.unlink(missing_ok=True)
        err = (result.stderr or "")[:FFMPEG_STDERR_MAX_LEN].strip()
        rec(
            size_before=size_before,
            size_after=None,
            duration_sec=duration,
            status=STATUS_ERROR,
            error_msg=err,
        )
        log(
            "WARNING",
            f"[{camera}] ffmpeg failed after {duration:.1f}s "
            f"(rc={result.returncode}): {_display_path(filepath)}",
        )
        if err:
            log("DEBUG", f"[{camera}]   stderr: {err}")
        return False

    if not tmpfile.exists():
        rec(
            size_before=size_before,
            size_after=None,
            duration_sec=duration,
            status=STATUS_ERROR,
            error_msg="output missing",
        )
        log(
            "WARNING",
            f"[{camera}] output missing after encode ({duration:.1f}s): "
            f"{_display_path(filepath)}",
        )
        return False

    size_after = tmpfile.stat().st_size

    # Sanity: output must be at least 10% of original size
    if size_after < size_before // 10:
        tmpfile.unlink(missing_ok=True)
        rec(
            size_before=size_before,
            size_after=size_after,
            duration_sec=duration,
            status=STATUS_ERROR,
            error_msg="output too small",
        )
        log(
            "WARNING",
            f"[{camera}] output suspiciously small after {duration:.1f}s — "
            f"keeping original: {_display_path(filepath)}",
        )
        return False

    # Safety: verify the original still exists and hasn't been modified.
    # Frigate may delete recordings during its own retention cleanup while we
    # were encoding.  If the file changed, the encode is based on stale data.
    if not filepath.exists():
        tmpfile.unlink(missing_ok=True)
        rec(
            size_before=size_before,
            size_after=None,
            duration_sec=duration,
            status=STATUS_ERROR,
            error_msg="original deleted by Frigate during compression",
        )
        log(
            "WARNING",
            f"[{camera}] original deleted during compression ({duration:.1f}s) — "
            f"discarding output: {_display_path(filepath)}",
        )
        return False

    current_size = filepath.stat().st_size
    if current_size != size_before:
        tmpfile.unlink(missing_ok=True)
        rec(
            size_before=size_before,
            size_after=None,
            duration_sec=duration,
            status=STATUS_ERROR,
            error_msg=f"original changed during compression ({size_before}→{current_size} bytes)",
        )
        log(
            "WARNING",
            f"[{camera}] original changed during compression ({duration:.1f}s) — "
            f"discarding output: {_display_path(filepath)}",
        )
        return False

    # Safety: confirm Frigate still has this recording in its DB.
    # Closes the race where Frigate removes the DB row (and possibly the file)
    # between the checks above and the atomic replace below.  Without this,
    # we could create an orphan on disk that Frigate never cleans up.
    with frigate_ro_lock:
        db_row = frigate_ro.execute(
            "SELECT id FROM recordings WHERE id = ?", (recording_id,)
        ).fetchone()
    if db_row is None:
        tmpfile.unlink(missing_ok=True)
        rec(
            size_before=size_before,
            size_after=None,
            duration_sec=duration,
            status=STATUS_ERROR,
            error_msg="recording removed from Frigate DB during compression",
        )
        log(
            "WARNING",
            f"[{camera}] recording removed from Frigate DB during compression "
            f"({duration:.1f}s) — discarding output to prevent orphan: "
            f"{_display_path(filepath)}",
        )
        return False

    # Atomically replace original.  Logged at DEBUG only — the start line
    # already named the file and the success summary follows immediately, so
    # an INFO "Replacing..." line in between is just noise.
    log(
        "DEBUG",
        f"[{camera}] Replacing original with compressed output: {_display_path(filepath)}",
    )
    try:
        tmpfile.replace(filepath)
    except Exception as e:
        tmpfile.unlink(missing_ok=True)
        rec(
            size_before=size_before,
            size_after=size_after,
            duration_sec=duration,
            status=STATUS_ERROR,
            error_msg=f"replace failed: {e}",
        )
        log(
            "ERROR",
            f"[{camera}] failed to replace original after {duration:.1f}s: {e}",
        )
        return False

    saved = size_before - size_after
    pct = (saved / size_before * 100) if size_before else 0.0
    log(
        "INFO",
        f"[{camera}] tier={tier} type={recording_type} "
        f"{_display_path(filepath)} "
        f"{_fmt(size_before)} → {_fmt(size_after)} "
        f"(saved {_fmt(saved)} / {pct:.1f}%, {duration:.1f}s)",
    )

    # Update segment_size in Frigate's DB (MB, float).
    # If this fails we record status='segment_update_failed' so housekeeping
    # can retry; the file itself is already safely replaced.
    new_size_mb = size_after / (1024 * 1024)
    log(
        "DEBUG",
        f"[{camera}] Updating Frigate segment_size to {new_size_mb:.3f}MB for {recording_id}",
    )
    seg_status = STATUS_OK
    seg_error: str | None = None
    try:
        with frigate_lock:
            frigate_rw.execute(
                "UPDATE recordings SET segment_size = ? WHERE id = ?",
                (new_size_mb, recording_id),
            )
            frigate_rw.commit()
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


# ═══════════════════════════════════════════════════════════════════════════════
# ELIGIBLE RECORDINGS QUERY
# ═══════════════════════════════════════════════════════════════════════════════


def _min_tier1_min_days(cfg: Config) -> int:
    """Return the smallest tier1.min_days across all enabled cameras."""
    days = [
        cam.tier1.min_days
        for cam in cfg.cameras.values()
        if cam.enabled and cam.tier1.enabled
    ]
    return min(days) if days else 7


def get_eligible_recordings(ctx: CompressorContext) -> list[dict]:
    """
    Returns recordings eligible for compression that haven't been successfully
    compressed yet.  Each result dict has keys:
        recording_id, camera, path, tier, recording_type

    Filters out disabled cameras and disabled tiers using per-camera config.
    The SQL query uses the most aggressive (minimum) tier1 cutoff across all
    enabled cameras to fetch candidates; Python-side filtering applies each
    camera's actual cutoffs.
    """
    cfg = ctx.cfg
    compress_db = ctx.compress_db
    db_lock = ctx.db_lock

    # Use the most aggressive cutoff for the SQL query to get all candidates.
    min_t1_days = _min_tier1_min_days(cfg)
    earliest_cutoff = time.time() - (min_t1_days * 86400)

    with db_lock:
        _frigate_db_str = str(cfg.frigate_db).replace('"', "")
        compress_db.execute(
            f'ATTACH DATABASE "file:{_frigate_db_str}?mode=ro" AS frigate_eligible'
        )
        try:
            rows = compress_db.execute(
                """
                SELECT r.id, r.camera, r.path, r.start_time, r.motion, r.objects
                FROM   frigate_eligible.recordings r
                WHERE  r.start_time < ?
                  AND  r.id NOT IN (
                           SELECT recording_id
                           FROM   compressed_files
                           WHERE  status IN (?, ?)
                       )
                ORDER BY r.start_time ASC
                """,
                (earliest_cutoff, STATUS_OK, STATUS_SEGMENT_UPDATE_FAILED),
            ).fetchall()
        finally:
            compress_db.execute("DETACH DATABASE frigate_eligible")

    now = time.time()
    results = []
    for row in rows:
        camera = row["camera"]
        cam_cfg = cfg.cameras.get(camera)
        if cam_cfg is None or not cam_cfg.enabled:
            continue

        start_time = row["start_time"]
        age_days_sec = now - start_time

        # Determine which tier applies based on this camera's cutoffs.
        tier: int | None = None
        if cam_cfg.tier2.enabled and age_days_sec >= cam_cfg.tier2.min_days * 86400:
            tier = 2
        elif cam_cfg.tier1.enabled and age_days_sec >= cam_cfg.tier1.min_days * 86400:
            tier = 1
        if tier is None:
            continue

        rtype = _recording_type(row["motion"], row["objects"])
        results.append(
            {
                "recording_id": row["id"],
                "camera": camera,
                "path": row["path"],
                "tier": tier,
                "recording_type": rtype,
            }
        )
    return results


def time_until_next_eligible(ctx: CompressorContext) -> float:
    """
    Returns seconds until the next recording becomes eligible for tier 1
    compression, clamped to ``[MIN_SLEEP_SEC, MAX_SLEEP_SEC]``.

    Uses the minimum tier1.min_days across all enabled cameras.
    Returns ``MAX_SLEEP_SEC`` if nothing is pending.
    """
    cfg = ctx.cfg
    min_days = _min_tier1_min_days(cfg)
    tier1_cutoff = time.time() - (min_days * 86400)

    with ctx.frigate_ro_lock:
        row = ctx.frigate_ro.execute(
            """
            SELECT start_time FROM recordings
            WHERE  start_time > ?
            ORDER  BY start_time ASC
            LIMIT  1
            """,
            (tier1_cutoff,),
        ).fetchone()

    if row is None:
        return MAX_SLEEP_SEC

    eligible_at = row["start_time"] + (min_days * 86400)
    return max(MIN_SLEEP_SEC, min(MAX_SLEEP_SEC, eligible_at - time.time()))


# ═══════════════════════════════════════════════════════════════════════════════
# HOUSEKEEPING
# ═══════════════════════════════════════════════════════════════════════════════


def run_housekeeping(ctx: CompressorContext) -> None:
    cfg = ctx.cfg
    compress_db = ctx.compress_db
    db_lock = ctx.db_lock
    frigate_rw = ctx.frigate_rw
    frigate_lock = ctx.frigate_lock

    log("INFO", "── Housekeeping starting")

    # 1. Remove leftover temp files from crashed runs.
    # Only matches our own temp files (.tmp.{recording_id}.mp4) so active
    # compression jobs are never interrupted — each job uses a unique name.
    temp_files = list(Path(cfg.recordings_dir).rglob(_TEMP_GLOB))
    for tmp in temp_files:
        if cfg.all_dry_run:
            log("INFO", f"DRY RUN: Would remove leftover temp file: {tmp}")
        else:
            log("WARNING", f"Removing leftover temp file: {tmp}")
            tmp.unlink(missing_ok=True)
    if not temp_files:
        log("DEBUG", "No leftover temp files found")

    # 2. Retry pending segment_size updates.
    # Files whose compression succeeded but whose Frigate DB write failed are
    # recorded as status='segment_update_failed'.  Re-read the actual file size
    # from disk and retry; on success promote to status='ok'.
    with db_lock:
        pending_seg = compress_db.execute(
            "SELECT recording_id, camera, path FROM compressed_files WHERE status = ?",
            (STATUS_SEGMENT_UPDATE_FAILED,),
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
            with frigate_lock:
                frigate_rw.execute(
                    "UPDATE recordings SET segment_size = ? WHERE id = ?",
                    (actual_size_mb, row["recording_id"]),
                )
                frigate_rw.commit()
            with db_lock:
                compress_db.execute(
                    "UPDATE compressed_files SET status = ?, error_msg = NULL"
                    " WHERE recording_id = ?",
                    (STATUS_OK, row["recording_id"]),
                )
                compress_db.commit()
            promoted += 1
            log(
                "INFO",
                f"[{row['camera']}] retried segment_size update — ok: {_display_path(fpath)}",
            )
        except Exception as e:
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

    # 3. Prune compress DB rows whose recording no longer exists in Frigate's DB.
    # Attaches the Frigate DB temporarily so a single query can cross-reference
    # it — avoids loading two full ID sets into memory.
    pruned = 0
    with db_lock:
        _frigate_db_str = str(cfg.frigate_db).replace('"', "")
        compress_db.execute(
            f'ATTACH DATABASE "file:{_frigate_db_str}?mode=ro" AS frigate_ro_hk'
        )
        try:
            if cfg.all_dry_run:
                pruned = compress_db.execute(
                    """
                    SELECT COUNT(*) FROM compressed_files
                    WHERE recording_id NOT IN (
                        SELECT id FROM frigate_ro_hk.recordings
                    )
                    """
                ).fetchone()[0]
            else:
                log("INFO", "Pruning orphaned compress DB entries")
                cursor = compress_db.execute(
                    """
                    DELETE FROM compressed_files
                    WHERE recording_id NOT IN (
                        SELECT id FROM frigate_ro_hk.recordings
                    )
                    """
                )
                pruned = cursor.rowcount
                compress_db.commit()
        finally:
            compress_db.execute("DETACH DATABASE frigate_ro_hk")
    if pruned:
        prefix = "DRY RUN: Would prune" if cfg.all_dry_run else "Pruned"
        log("INFO", f"{prefix} {pruned} orphaned DB entries")
    else:
        log("DEBUG", "No orphaned DB entries")

    # 4. Storage savings summary
    with db_lock:
        rows = compress_db.execute(
            "SELECT * FROM savings_by_camera ORDER BY bytes_saved DESC"
        ).fetchall()

    if rows:
        log("INFO", "── Storage savings by camera")
        total_before = total_after = total_files = 0
        log(
            "INFO",
            f"  {'Camera':<20} {'Files':>6} {'Before':>10} {'After':>10} {'Saved':>10} {'Reduction':>10}",
        )
        log(
            "INFO",
            f"  {'-' * 20} {'-' * 6} {'-' * 10} {'-' * 10} {'-' * 10} {'-' * 10}",
        )
        for r in rows:
            log(
                "INFO",
                f"  {r['camera']:<20} {r['files_compressed']:>6} "
                f"{_fmt(r['bytes_before']):>10} {_fmt(r['bytes_after']):>10} "
                f"{_fmt(r['bytes_saved']):>10} {r['avg_reduction_pct']:>9.1f}%",
            )
            total_before += r["bytes_before"] or 0
            total_after += r["bytes_after"] or 0
            total_files += r["files_compressed"] or 0
        log(
            "INFO",
            f"  {'TOTAL':<20} {total_files:>6} "
            f"{_fmt(total_before):>10} {_fmt(total_after):>10} "
            f"{_fmt(total_before - total_after):>10}",
        )
    else:
        log("INFO", "No compression data yet")

    # 5. Recent errors
    with db_lock:
        errors = compress_db.execute("SELECT * FROM recent_errors LIMIT 20").fetchall()
    if errors:
        log("WARNING", "── Recent errors (last 7 days)")
        for err in errors:
            log(
                "WARNING",
                f"  [{err['last_attempted_at']}] {err['camera']} | {err['error_msg']}",
            )

    log("INFO", "── Housekeeping complete")


# ═══════════════════════════════════════════════════════════════════════════════
# UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════


def _display_path(filepath: Path) -> str:
    """Format a Frigate recording path for compact log display.

    Frigate stores recordings as
    ``<recordings_dir>/YYYY-MM-DD/HH/<camera>/MM.SS.mp4``.  The bare filename
    (e.g. ``38.11.mp4``) is meaningless without the date and hour, so we
    return ``YYYY-MM-DD/HH/MM.SS.mp4``.  The camera is already shown as a
    log prefix, so we omit it to avoid redundancy.  Falls back to the bare
    filename if the path is shorter than expected.
    """
    parts = filepath.parts
    if len(parts) >= 4:
        return f"{parts[-4]}/{parts[-3]}/{parts[-1]}"
    return filepath.name


def _fmt(n: int | float | None) -> str:
    """Human-readable byte size string."""
    if n is None:
        return "N/A"
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}PB"


def _fmt_type(ts: TypeSettings) -> str:
    """One-line summary of a TypeSettings for startup logging."""
    if not ts.enabled:
        return "SKIP (compression disabled)"
    sc = f"{ts.scale_mode}({ts.scale_value})" if ts.scale_mode != "none" else "original"
    fp = f"{ts.fps_mode}({ts.fps_value})" if ts.fps_mode != "none" else "original"
    return f"q={ts.quality} scale={sc} fps={fp}"


# ═══════════════════════════════════════════════════════════════════════════════
# MQTT STORAGE PUBLISHER
# ═══════════════════════════════════════════════════════════════════════════════
#
# Periodically publishes a snapshot of how Frigate's recordings are allocated
# (per camera, per recording type, per compression tier) to MQTT for Home
# Assistant discovery.  This is intentionally separate from the per-recording
# compression progress; it answers "where is my disk going?" not "what is the
# compressor doing right now?".
#
# Disabled when ``cfg.mqtt.host`` is empty — the publisher thread is never
# started and no MQTT client is constructed, so existing users see no change.


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


def collect_frigate_stats(ctx: "CompressorContext") -> FrigateStats:
    """Snapshot Frigate's recording allocation, joined with our compress DB.

    One ATTACH+GROUP BY query: per (camera, tier, recording_type) we get a
    files count, byte total (segment_size→bytes), and earliest start_time.
    Tier comes from a LEFT JOIN against ``compressed_files`` — rows with no
    match (or with status that isn't OK / segment_update_failed) are bucketed
    as tier 0 (uncompressed).  NULL ``segment_size`` is treated as 0 bytes
    so a half-finalised row never crashes the aggregate.
    """
    cfg = ctx.cfg
    now = time.time()

    with ctx.db_lock:
        _frigate_db_str = str(cfg.frigate_db).replace('"', "")
        ctx.compress_db.execute(
            f'ATTACH DATABASE "file:{_frigate_db_str}?mode=ro" AS frigate_stats'
        )
        try:
            rows = ctx.compress_db.execute(
                f"""
                SELECT
                    r.camera                                                AS camera,
                    COALESCE(c.tier, 0)                                     AS tier,
                    CASE
                      WHEN COALESCE(r.objects, 0) > 0 THEN 'object'
                      WHEN COALESCE(r.motion,  0) > 0 THEN 'motion'
                      ELSE                                  'continuous'
                    END                                                     AS rtype,
                    COUNT(*)                                                AS files,
                    SUM(COALESCE(r.segment_size, 0) * {_MB_BYTES})          AS bytes,
                    MIN(r.start_time)                                       AS oldest
                FROM frigate_stats.recordings r
                LEFT JOIN compressed_files c
                  ON  c.recording_id = r.id
                  AND c.status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                GROUP BY r.camera, tier, rtype
                """
            ).fetchall()
        finally:
            ctx.compress_db.execute("DETACH DATABASE frigate_stats")

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
        ctx: "CompressorContext",
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


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN DAEMON LOOP
# ═══════════════════════════════════════════════════════════════════════════════


def _warn_qsv_fps_conflicts(cfg: Config, encoder: str) -> None:
    """
    Emit one WARNING per (camera, tier, recording_type) combination where QSV
    encoding is active alongside both an fps filter and a scale filter.
    Mixed CPU/GPU filter chains can cause FFmpeg to fail with a cryptic
    'Error while filtering' message.  Called once at startup to inform the
    user without spamming a warning for every compressed recording.
    """
    if encoder != "qsv":
        return

    for cam_name, cam_cfg in cfg.cameras.items():
        if not cam_cfg.enabled:
            continue
        for tier_num, tier_cfg in ((1, cam_cfg.tier1), (2, cam_cfg.tier2)):
            if not tier_cfg.enabled:
                continue
            for rtype in _RECORDING_TYPES:
                ts: TypeSettings = getattr(tier_cfg, rtype)
                if ts.fps_mode != "none" and ts.scale_mode != "none":
                    log(
                        "WARNING",
                        f"Config [{cam_name} tier{tier_num}/{rtype}]: QSV encoder"
                        f" + fps_mode='{ts.fps_mode}' + scale_mode='{ts.scale_mode}'"
                        " — mixed CPU/GPU filter chain may cause FFmpeg to fail."
                        " Consider fps_mode='none' with QSV, or encoder='cpu'.",
                    )


def main() -> int:
    global _log_level

    ap = argparse.ArgumentParser()
    ap.add_argument("--options", required=True)
    args = ap.parse_args()

    cfg = load_config(args.options)
    _log_level = cfg.log_level

    encoder = detect_encoder(cfg.encoder)
    _warn_qsv_fps_conflicts(cfg, encoder)

    encoder_ok, encoder_msg = check_encoder_works(encoder)
    if encoder_ok:
        log("INFO", f"Encoder self-test: {encoder} OK")
    elif cfg.all_dry_run:
        log(
            "WARNING",
            f"Encoder self-test: {encoder} FAILED — {encoder_msg}. "
            "Continuing because all cameras are dry_run, but real compression "
            "would fail on every file.",
        )
    else:
        log("ERROR", f"Encoder self-test: {encoder} FAILED — {encoder_msg}")
        log("ERROR", "Hardware acceleration is not available. Aborting startup.")
        return 1

    compress_db = open_compress_db(cfg.compress_db)
    frigate_ro = open_frigate_db(cfg.frigate_db)
    frigate_rw = open_frigate_db_rw(cfg.frigate_db)

    try:
        check_frigate_schema(frigate_ro)
    except RuntimeError as e:
        log("ERROR", f"Startup aborted: {e}")
        return 1

    db_lock = threading.Lock()
    frigate_ro_lock = threading.Lock()
    frigate_lock = threading.Lock()

    log("INFO", "════════════════════════════════════════")
    log("INFO", f"Frigate Compressor v{__version__} starting")
    if cfg.all_dry_run:
        log("INFO", "  *** DRY RUN MODE — no files or databases will be modified ***")
    log("INFO", f"  Encoder        : {encoder}")
    log("INFO", f"  Parallel jobs  : {cfg.max_parallel_jobs}")
    log("INFO", f"  Log level      : {cfg.log_level}")
    log("INFO", f"  Housekeeping   : every {cfg.housekeeping_interval_days}d")
    log("INFO", f"  Frigate DB     : {cfg.frigate_db}")
    log("INFO", f"  Recordings     : {cfg.recordings_dir}")
    log("INFO", f"  Compress DB    : {cfg.compress_db}")
    if cfg.mqtt.enabled:
        log(
            "INFO",
            f"  MQTT           : {cfg.mqtt.host}:{cfg.mqtt.port}"
            f" base={cfg.mqtt.base_topic}"
            f" interval={cfg.mqtt.publish_interval_seconds}s"
            f" rate_window={cfg.mqtt.rate_window_seconds}s"
            f" disconnect_timeout={cfg.mqtt.disconnect_timeout_seconds}s",
        )
    else:
        log("INFO", "  MQTT           : disabled (mqtt_host empty)")
    log("INFO", f"  Cameras        : {len(cfg.cameras)}")
    for cam_name, cam_cfg in cfg.cameras.items():
        flags = []
        if not cam_cfg.enabled:
            flags.append("DISABLED")
        if cam_cfg.dry_run:
            flags.append("DRY_RUN")
        flag_str = f" [{', '.join(flags)}]" if flags else ""
        log("INFO", f"  ── {cam_name}{flag_str}")
        for tier_num, tier_cfg in ((1, cam_cfg.tier1), (2, cam_cfg.tier2)):
            tier_flag = "" if tier_cfg.enabled else " [DISABLED]"
            log("INFO", f"      Tier {tier_num} (>{tier_cfg.min_days}d){tier_flag}:")
            for rtype in _RECORDING_TYPES:
                ts = getattr(tier_cfg, rtype)
                log("INFO", f"        {rtype:<12}: {_fmt_type(ts)}")
    log("INFO", "════════════════════════════════════════")

    ctx = CompressorContext(
        cfg=cfg,
        compress_db=compress_db,
        db_lock=db_lock,
        frigate_ro=frigate_ro,
        frigate_ro_lock=frigate_ro_lock,
        frigate_rw=frigate_rw,
        frigate_lock=frigate_lock,
    )

    # Use threading.Event so signal handlers can wake the sleep loop immediately.
    stopping = threading.Event()
    housekeeping_interval_sec = cfg.housekeeping_interval_days * 86400

    def handle_sig(_sig, _frame):
        stopping.set()

    signal.signal(signal.SIGTERM, handle_sig)
    signal.signal(signal.SIGINT, handle_sig)

    publisher: MqttPublisher | None = None
    if cfg.mqtt.enabled:
        try:
            publisher = MqttPublisher(ctx, cfg.mqtt, stopping)
            publisher.start()
        except Exception as e:
            log("ERROR", f"Failed to start MQTT publisher: {e}")
            publisher = None

    try:
        run_main_loop(ctx, encoder, stopping, housekeeping_interval_sec)
    finally:
        if publisher is not None:
            try:
                publisher.stop()
            except Exception as e:
                log("WARNING", f"MQTT publisher stop failed: {e}")
        log("INFO", "Frigate Compressor stopped")
        compress_db.close()
        frigate_ro.close()
        frigate_rw.close()

    if publisher is not None and publisher.exit_code is not None:
        return publisher.exit_code
    return 0


def run_main_loop(
    ctx: CompressorContext,
    encoder: str,
    stopping: threading.Event,
    housekeeping_interval_sec: float,
) -> None:
    """Process eligible recordings forever, sleeping only when caught up.

    Extracted from ``main()`` so the loop's scheduling behavior (run-then-
    re-check vs sleep-until-next) is testable in isolation.
    """
    cfg = ctx.cfg
    last_housekeeping = time.time()

    while not stopping.is_set():
        # ── Housekeeping ──────────────────────────────────────────────────
        if (time.time() - last_housekeeping) >= housekeeping_interval_sec:
            try:
                run_housekeeping(ctx)
            except Exception as e:
                log("ERROR", f"Housekeeping failed: {e}")
            last_housekeeping = time.time()

        # ── Find eligible recordings ──────────────────────────────────────
        try:
            eligible = get_eligible_recordings(ctx)
        except Exception as e:
            log("ERROR", f"Failed to query eligible recordings: {e}")
            stopping.wait(timeout=60)
            continue

        if eligible:
            suffix = " (DRY RUN — skipping ffmpeg)" if cfg.all_dry_run else ""
            log("INFO", f"Found {len(eligible)} recording(s) to compress{suffix}")
            camera_counts = Counter(r["camera"] for r in eligible)
            breakdown = ", ".join(
                f"{cam}={n}" for cam, n in sorted(camera_counts.items())
            )
            log("INFO", f"  per-camera: {breakdown}")

            with ThreadPoolExecutor(max_workers=cfg.max_parallel_jobs) as pool:
                futures = {
                    pool.submit(
                        compress_one,
                        r["recording_id"],
                        r["path"],
                        r["camera"],
                        r["tier"],
                        r["recording_type"],
                        encoder,
                        ctx,
                    ): r
                    for r in eligible
                    if not stopping.is_set()
                }
                for future in as_completed(futures):
                    if stopping.is_set():
                        break
                    r = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        log("ERROR", f"[{r['camera']}] unhandled error: {e}")
            # ThreadPoolExecutor.__exit__ calls shutdown(wait=True), so all
            # running jobs complete before we reach this point.

            # We just did real work — additional recordings may have aged
            # into eligibility while we were busy.  Loop straight back to
            # the top so housekeeping still runs and we re-query eligible
            # without sleeping.  Sleeping only when truly caught up means
            # we can never fall arbitrarily far behind a steady recording
            # rate just because the previous pass was long.
            continue

        # ── No work — sleep until the next recording becomes eligible ────
        if not stopping.is_set():
            try:
                sleep_sec = time_until_next_eligible(ctx)
            except Exception as e:
                log("WARNING", f"time_until_next_eligible failed: {e}")
                sleep_sec = MAX_SLEEP_SEC

            log("INFO", f"Next check in {sleep_sec / 60:.1f} min")

            # stopping.wait() returns immediately when the event is set, so a
            # signal wakes us without waiting for the full sleep duration.
            stopping.wait(timeout=sleep_sec)


if __name__ == "__main__":
    raise SystemExit(main())
