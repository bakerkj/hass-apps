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
  Per-camera overrides in config.yaml take precedence over defaults.

TIER LOGIC
──────────
Tier 1 (tier1.min_days → tier2.min_days): recent footage, per-type settings
Tier 2 (tier2.min_days+): archive footage, per-type settings with harder compression

CONFIGURATION
─────────────
  HAOS options (options.json): infrastructure settings — encoder, paths, MQTT.
  YAML config (/config/config.yaml): camera-centric compression settings.
    defaults block: base settings for all cameras.
    cameras block: per-camera overrides (enabled, dry_run, tier settings).
  Resolution order: builtin defaults → YAML defaults → camera-level → tier-level → type-level.

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
  Compress DB (/config/compress.db):  full compression tracking, savings views

FUTURE GPU MIGRATION (RTX 3090)
────────────────────────────────
  Set encoder="nvenc" in add-on options. Everything else is automatic.
"""

from __future__ import annotations

import argparse
import json
import math
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
    def cam_name_width(self) -> int:
        """Max camera name length, for aligned log output."""
        return max((len(n) for n in self.cameras), default=0)

    @property
    def all_dry_run(self) -> bool:
        """True when every configured camera is in dry-run mode."""
        return (
            all(cam.dry_run for cam in self.cameras.values()) if self.cameras else True
        )


# ── Built-in defaults (match the YAML ``defaults`` block) ────────────────────
# These are used when no config.yaml exists or when fields are omitted.

_BUILTIN_DEFAULTS: dict = {
    "enabled": False,
    "dry_run": True,
    "tier1": {
        "enabled": False,
        "min_days": 7,
        "quality": 0,
        "scale_mode": "none",
        "scale_value": "",
        "fps_mode": "none",
        "fps_value": 1.0,
        "motion": {},
        "object": {},
    },
    "tier2": {
        "enabled": False,
        "min_days": 30,
        "quality": 0,
        "scale_mode": "none",
        "scale_value": "",
        "fps_mode": "none",
        "fps_value": 1.0,
        "motion": {},
        "object": {},
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
    ``/config/config.yaml``).
    """
    with open(options_path, "r", encoding="utf-8") as f:
        opts = json.load(f)

    if yaml_path is None:
        yaml_path = str(
            opts.get(
                "config_path",
                "/config/config.yaml",
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

    # Per-camera validation.
    for name, cam_cfg in cameras.items():
        if cam_cfg.tier2.min_days <= cam_cfg.tier1.min_days:
            raise ValueError(
                f"Camera '{name}': tier2.min_days ({cam_cfg.tier2.min_days}) must be "
                f"greater than tier1.min_days ({cam_cfg.tier1.min_days})"
            )
        # Require quality to be explicitly configured for enabled tiers.
        for tier_num, tier_cfg in [(1, cam_cfg.tier1), (2, cam_cfg.tier2)]:
            if not tier_cfg.enabled:
                continue
            for rtype in _RECORDING_TYPES:
                ts: TypeSettings = getattr(tier_cfg, rtype)
                if ts.enabled and ts.quality <= 0:
                    raise ValueError(
                        f"Camera '{name}' tier{tier_num}/{rtype}: quality must be "
                        f"set explicitly in config.yaml (got {ts.quality})"
                    )

    return Config(
        encoder=opts.get("encoder", "qsv"),
        max_parallel_jobs=int(opts.get("max_parallel_jobs", 2)),
        housekeeping_interval_days=int(opts.get("housekeeping_interval_days", 7)),
        frigate_db=frigate_db,
        recordings_dir=recordings_dir,
        compress_db=Path(opts.get("compress_db", "/config/compress.db")),
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

SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    recording_id    TEXT    PRIMARY KEY,
    camera          TEXT    NOT NULL,
    path            TEXT    NOT NULL,
    recording_type  TEXT,

    -- Original probe (from camera, filled by probe loop)
    codec           TEXT,
    width           INTEGER,
    height          INTEGER,
    fps             REAL,
    bitrate         INTEGER,
    duration_sec    REAL,
    file_size       INTEGER,
    scanned_at      TEXT,

    -- Tier 1 compression
    t1_encoder      TEXT,
    t1_width        INTEGER,
    t1_height       INTEGER,
    t1_fps          REAL,
    t1_bitrate      INTEGER,
    t1_file_size    INTEGER,
    t1_encode_sec   REAL,
    t1_status       TEXT,
    t1_error_msg    TEXT,
    t1_compressed_at TEXT,

    -- Tier 2 compression
    t2_encoder      TEXT,
    t2_width        INTEGER,
    t2_height       INTEGER,
    t2_fps          REAL,
    t2_bitrate      INTEGER,
    t2_file_size    INTEGER,
    t2_encode_sec   REAL,
    t2_status       TEXT,
    t2_error_msg    TEXT,
    t2_compressed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_files_camera ON files(camera);
"""

VIEWS = f"""
CREATE VIEW IF NOT EXISTS savings_by_camera AS
SELECT
    camera,
    COUNT(CASE WHEN t1_status = '{STATUS_OK}' THEN 1 END)                    AS t1_files,
    COUNT(CASE WHEN t2_status = '{STATUS_OK}' THEN 1 END)                    AS t2_files,
    SUM(CASE WHEN t1_status = '{STATUS_OK}' THEN file_size END)              AS t1_bytes_before,
    SUM(CASE WHEN t1_status = '{STATUS_OK}' THEN t1_file_size END)           AS t1_bytes_after,
    SUM(CASE WHEN t2_status = '{STATUS_OK}'
             THEN COALESCE(t1_file_size, file_size) END)                     AS t2_bytes_before,
    SUM(CASE WHEN t2_status = '{STATUS_OK}' THEN t2_file_size END)           AS t2_bytes_after
FROM files
WHERE file_size > 0
GROUP BY camera;

CREATE VIEW IF NOT EXISTS recent_errors AS
SELECT camera, path, recording_type,
       t1_compressed_at, t1_error_msg,
       t2_compressed_at, t2_error_msg
FROM files
WHERE (t1_status = '{STATUS_ERROR}' AND t1_compressed_at >= datetime('now', '-7 days'))
   OR (t2_status = '{STATUS_ERROR}' AND t2_compressed_at >= datetime('now', '-7 days'))
ORDER BY COALESCE(t2_compressed_at, t1_compressed_at) DESC;
"""


def _migrate_to_files_table(conn: sqlite3.Connection) -> None:
    """Migrate data from the old compressed_files/probed_files tables to files."""
    tables = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if "compressed_files" not in tables:
        return  # already migrated or fresh DB

    conn.executescript(
        """
        INSERT OR IGNORE INTO files
            (recording_id, camera, path, recording_type, file_size,
             t1_encoder, t1_file_size, t1_encode_sec, t1_status,
             t1_error_msg, t1_compressed_at)
        SELECT recording_id, camera, path, recording_type, size_before,
               encoder, size_after, duration_sec, status,
               error_msg, last_attempted_at
        FROM compressed_files
        WHERE tier = 1;

        INSERT OR IGNORE INTO files
            (recording_id, camera, path, recording_type, file_size,
             t2_encoder, t2_file_size, t2_encode_sec, t2_status,
             t2_error_msg, t2_compressed_at)
        SELECT recording_id, camera, path, recording_type, size_before,
               encoder, size_after, duration_sec, status,
               error_msg, last_attempted_at
        FROM compressed_files
        WHERE tier = 2;

        DROP TABLE IF EXISTS compressed_files;
        DROP TABLE IF EXISTS probed_files;
        DROP VIEW IF EXISTS savings_by_camera;
        DROP VIEW IF EXISTS recent_errors;
        """
    )
    conn.commit()
    log("INFO", "Migrated compressed_files → files table")


def open_compress_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(f"file:{path}", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.executescript(SCHEMA)
    _migrate_to_files_table(conn)
    # Views are created after migration so they reference the final table.
    conn.executescript(VIEWS)
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


def _probe_full(filepath: Path) -> dict | None:
    """Run ffprobe to capture codec, resolution, fps, bitrate, duration, and file size.

    Returns a dict with keys: codec, width, height, fps, bitrate, duration_sec,
    file_size.  Returns None if the file cannot be probed.
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
                "stream=codec_name,width,height,r_frame_rate,bit_rate",
                "-show_entries",
                "format=duration,size",
                "-of",
                "default=noprint_wrappers=1",
                str(filepath),
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return None

        data: dict[str, str] = {}
        for line in result.stdout.strip().splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                data[k.strip()] = v.strip()

        info: dict = {}
        info["codec"] = data.get("codec_name")

        try:
            info["width"] = int(data["width"])
            info["height"] = int(data["height"])
        except (KeyError, ValueError, TypeError):
            info["width"] = None
            info["height"] = None

        info["fps"] = None
        if "r_frame_rate" in data:
            try:
                parts = data["r_frame_rate"].split("/")
                info["fps"] = (
                    float(parts[0]) / float(parts[1])
                    if len(parts) == 2
                    else float(parts[0])
                )
            except (ValueError, TypeError, ZeroDivisionError):
                pass

        try:
            info["bitrate"] = int(data["bit_rate"])
        except (KeyError, ValueError, TypeError):
            info["bitrate"] = None

        try:
            info["duration_sec"] = float(data["duration"])
        except (KeyError, ValueError, TypeError):
            info["duration_sec"] = None

        try:
            info["file_size"] = int(data["size"])
        except (KeyError, ValueError, TypeError):
            # Fall back to stat if ffprobe didn't report size
            try:
                info["file_size"] = filepath.stat().st_size
            except OSError:
                info["file_size"] = None

        return info
    except Exception as e:
        log("WARNING", f"ffprobe failed for {filepath}: {e}")
        return None


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
FFMPEG_TIMEOUT_SEC = 30

# Max bytes of ffmpeg stderr text stored in the compress DB error_msg column.
FFMPEG_STDERR_MAX_LEN = 300

# Bounds on the main loop's sleep between compression passes.
# - MIN avoids hammering Frigate's DB when a recording is just barely
#   under the eligibility threshold.
# - MAX caps the wait so that even pathological states (frigate paused,
#   long fallback paths) re-check at least every 10 minutes instead of
#   sleeping for hours or days at a time.
MIN_SLEEP_SEC = 10.0
MAX_SLEEP_SEC = 600.0

# Throttle target is OVERHEAD × workload (files/min), where workload counts
# every recording needing compression in the next minute (backlog already
# past the line + incoming about to cross).  When backlog is large the
# target far exceeds GPU capacity so the rate limiter never sleeps (full-
# speed catchup, e.g. 100k backlog → target ~105k/min vs ~170/min capacity).
# Once the queue drains, target settles to OVERHEAD × the steady-state
# inflow rate.  Empirical testing showed file size barely predicts encode
# time (r=0.33, R²=0.11 over 6k samples), so a count-based throttle gets
# us essentially the same accuracy with less complexity.
_THROTTLE_WORKLOAD_WINDOW_SEC = 60.0
_THROTTLE_OVERHEAD = 1.05


class RateLimiter:
    """Thread-safe rate limiter shared across worker threads.

    Workers call ``acquire(target_per_min, stopping)`` after each completed
    compression.  The limiter spaces concurrent completions evenly so that
    aggregate throughput stays at or below ``target_per_min`` files/min.
    No-op when ``target_per_min`` ≤ 0.  State persists across batches.
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.next_allowed = 0.0

    def acquire(self, target_per_min: float, stopping: threading.Event) -> None:
        if target_per_min <= 0:
            return
        interval_sec = 60.0 / target_per_min
        with self.lock:
            now = time.time()
            wait = self.next_allowed - now
            self.next_allowed = max(now, self.next_allowed) + interval_sec
        if wait > 0:
            stopping.wait(timeout=wait)


@dataclass
class ThroughputEMA:
    """Time-weighted exponential moving average of throughput (files/min).

    Each call to ``update`` consumes a fresh sample.  The blend weight is
    derived from the elapsed time since the previous sample with a 60-second
    time constant — a sample one minute after the previous contributes
    ~63% (1 − 1/e) to the new value, two minutes ~86%, etc.  Samples taken
    rapidly contribute proportionally less, so the EWMA reflects roughly the
    last-minute behavior even under variable batch cadence.

    Not thread-safe: ``update`` must only be called from the main loop
    thread.  Workers may read ``value`` (a single float read is atomic in
    CPython) but must not call ``update``.
    """

    tau_sec: float = 60.0
    value: float = 0.0
    last_sample_time: float = 0.0

    def update(self, sample_per_min: float) -> float:
        now = time.time()
        if self.last_sample_time == 0.0:
            self.value = sample_per_min
        else:
            dt = max(now - self.last_sample_time, 0.0)
            alpha = 1.0 - math.exp(-dt / self.tau_sec)
            self.value = alpha * sample_per_min + (1.0 - alpha) * self.value
        self.last_sample_time = now
        return self.value


@dataclass
class CompressorContext:
    """Shared, per-daemon state passed to every compression worker.

    Each thread opens its own SQLite connection to the compress DB (WAL mode
    handles concurrency).  ``compress_db`` is only set in tests for convenience.
    """

    cfg: Config
    frigate_ro: sqlite3.Connection
    frigate_rw: sqlite3.Connection
    compress_db: sqlite3.Connection | None = None
    rate_limiter: RateLimiter = field(default_factory=RateLimiter)
    throughput_ema: ThroughputEMA = field(default_factory=ThroughputEMA)


def _recording_type(motion: int | None, objects: int | None) -> str:
    """Classify a recording by its motion/objects counts. Priority: object > motion > continuous."""
    if objects:
        return "object"
    if motion:
        return "motion"
    return "continuous"


def _record(
    conn: sqlite3.Connection,
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
    t = f"t{tier}"
    conn.execute(
        f"""
        INSERT INTO files
            (recording_id, camera, path, recording_type, file_size,
             {t}_encoder, {t}_file_size, {t}_encode_sec,
             {t}_compressed_at, {t}_status, {t}_error_msg)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(recording_id) DO UPDATE SET
            recording_type     = excluded.recording_type,
            file_size          = COALESCE(files.file_size, excluded.file_size),
            {t}_encoder        = excluded.{t}_encoder,
            {t}_file_size      = excluded.{t}_file_size,
            {t}_encode_sec     = excluded.{t}_encode_sec,
            {t}_compressed_at  = excluded.{t}_compressed_at,
            {t}_status         = excluded.{t}_status,
            {t}_error_msg      = excluded.{t}_error_msg
        """,
        (
            recording_id,
            camera,
            path,
            recording_type,
            size_before,
            encoder,
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
    compress_db = sqlite3.connect(
        f"file:{cfg.compress_db}", uri=True, check_same_thread=False
    )
    compress_db.row_factory = sqlite3.Row
    compress_db.execute("PRAGMA journal_mode=WAL")
    compress_db.execute("PRAGMA busy_timeout=10000")
    frigate_ro = sqlite3.connect(
        f"file:{cfg.frigate_db}?mode=ro", uri=True, check_same_thread=False
    )
    frigate_ro.row_factory = sqlite3.Row
    frigate_rw = sqlite3.connect(str(cfg.frigate_db), check_same_thread=False)
    frigate_rw.row_factory = sqlite3.Row
    frigate_rw.execute("PRAGMA busy_timeout=10000")
    try:
        return _compress_one_inner(
            recording_id,
            path,
            camera,
            tier,
            recording_type,
            encoder,
            cfg,
            compress_db,
            frigate_ro,
            frigate_rw,
        )
    finally:
        compress_db.close()
        frigate_ro.close()
        frigate_rw.close()


def _compress_one_inner(
    recording_id: str,
    path: str,
    camera: str,
    tier: int,
    recording_type: str,
    encoder: str,
    cfg: Config,
    compress_db: sqlite3.Connection,
    frigate_ro: sqlite3.Connection,
    frigate_rw: sqlite3.Connection,
) -> bool:

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

    # Require probe data before compressing.
    probe_row = compress_db.execute(
        "SELECT width, height, fps FROM files WHERE recording_id = ?",
        (recording_id,),
    ).fetchone()
    if not probe_row or not probe_row["width"] or not probe_row["height"]:
        log("DEBUG", f"[{camera}] Not yet probed, skipping: {_display_path(filepath)}")
        return False

    src_info = f"{probe_row['width']}x{probe_row['height']}"
    src_info = f"{src_info:<10}"

    # Format target settings for log messages.
    tgt_res = ""
    if ts.scale_mode != "none" and ts.scale_value:
        tgt_res = ts.scale_value.replace(":", "x") + " "
    elif ts.scale_mode == "halve":
        tgt_res = "halve "
    tgt_info = f"→{tgt_res}q{ts.quality}"
    tgt_info = f"{tgt_info:<14}"

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
            f"[{camera:<{cfg.cam_name_width}}] DRY RUN t{tier}:{recording_type[:3]}  "
            f"{_display_path(filepath)}  {src_info}{tgt_info}"
            f"{_fmt(size_before, 10)}",
        )
        return True

    t_start = time.monotonic()
    try:
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=FFMPEG_TIMEOUT_SEC
            )
        except subprocess.TimeoutExpired:
            duration = time.monotonic() - t_start
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

        # Sanity: for very small output (<3% of original), run ffprobe
        # to verify the output is a valid video with matching duration.
        if size_after * 100 < size_before * 3:
            out_info = _probe_full(tmpfile)
            src_info_full = _probe_full(filepath)
            if out_info is None:
                rec(
                    size_before=size_before,
                    size_after=size_after,
                    duration_sec=duration,
                    status=STATUS_ERROR,
                    error_msg="output too small and ffprobe failed",
                )
                log(
                    "WARNING",
                    f"[{camera}] output small and invalid after {duration:.1f}s — "
                    f"keeping original: {_display_path(filepath)}",
                )
                return False
            # Verify duration matches within 1 second.
            if (
                src_info_full is not None
                and src_info_full.get("duration_sec")
                and out_info.get("duration_sec")
                and abs(src_info_full["duration_sec"] - out_info["duration_sec"]) > 1.0
            ):
                rec(
                    size_before=size_before,
                    size_after=size_after,
                    duration_sec=duration,
                    status=STATUS_ERROR,
                    error_msg=f"output too small and duration mismatch "
                    f"({src_info_full['duration_sec']:.1f}s vs {out_info['duration_sec']:.1f}s)",
                )
                log(
                    "WARNING",
                    f"[{camera}] output small and duration mismatch "
                    f"({src_info_full['duration_sec']:.1f}s vs {out_info['duration_sec']:.1f}s) — "
                    f"keeping original: {_display_path(filepath)}",
                )
                return False
            log(
                "DEBUG",
                f"[{camera}] output small ({size_after * 100 // size_before}% of "
                f"original) but valid ({out_info.get('duration_sec', '?')}s): "
                f"{_display_path(filepath)}",
            )

        # Safety: verify the original still exists and hasn't been modified.
        # Frigate may delete recordings during its own retention cleanup while
        # we were encoding.  If the file changed, the encode is based on stale
        # data.
        if not filepath.exists():
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
        # Closes the race where Frigate removes the DB row (and possibly the
        # file) between the checks above and the atomic replace below.
        # Without this, we could create an orphan on disk that Frigate never
        # cleans up.
        db_row = frigate_ro.execute(
            "SELECT id FROM recordings WHERE id = ?", (recording_id,)
        ).fetchone()
        if db_row is None:
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

        # Atomically replace original.
        log(
            "DEBUG",
            f"[{camera}] Replacing original with compressed output: "
            f"{_display_path(filepath)}",
        )
        try:
            tmpfile.replace(filepath)
        except Exception as e:
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
    finally:
        # Ensure temp file is always cleaned up, even on thread cancellation.
        tmpfile.unlink(missing_ok=True)

    pct = ((size_before - size_after) / size_before * 100) if size_before else 0.0
    log(
        "INFO",
        f"[{camera:<{cfg.cam_name_width}}] t{tier}:{recording_type[:3]}  "
        f"{_display_path(filepath)}  {src_info}{tgt_info}"
        f"{_fmt(size_before, 10)}→{_fmt(size_after, 10)}  {pct:>3.0f}%  {duration:>5.1f}s",
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


_ELIGIBLE_BATCH_SIZE = 500


def _build_eligible_where(cfg: Config, effective_now: float) -> tuple[str, list]:
    """Build the SQL WHERE clause + params shared by eligible-recordings queries.

    ``effective_now`` is "now" for normal eligibility queries.  Callers can
    pass a future-shifted value to count recordings that *will* be eligible
    by that time (current backlog + incoming).

    Returns ("", []) if no enabled cameras have any enabled tier — caller must
    short-circuit (no rows can match).
    """
    _ok_statuses = (STATUS_OK, STATUS_SEGMENT_UPDATE_FAILED)
    ok_placeholders = ",".join("?" for _ in _ok_statuses)

    # A recording needs work if:
    #   - tier1 enabled AND old enough AND t1 not done, OR
    #   - tier2 enabled AND old enough AND t1 done AND t2 not done
    cam_clauses: list[str] = []
    params: list = []
    for name, cam in cfg.cameras.items():
        if not cam.enabled:
            continue
        subclauses = []
        sub_params: list = []
        if cam.tier1.enabled:
            t1_cutoff = effective_now - (cam.tier1.min_days * 86400)
            subclauses.append(
                f"(r.start_time < ? AND (f.t1_status IS NULL OR f.t1_status NOT IN ({ok_placeholders})))"
            )
            sub_params.extend([t1_cutoff, *_ok_statuses])
        if cam.tier2.enabled:
            t2_cutoff = effective_now - (cam.tier2.min_days * 86400)
            subclauses.append(
                f"(r.start_time < ? AND f.t1_status IN ({ok_placeholders}) AND (f.t2_status IS NULL OR f.t2_status NOT IN ({ok_placeholders})))"
            )
            sub_params.extend([t2_cutoff, *_ok_statuses, *_ok_statuses])
        if not subclauses:
            continue
        combined = " OR ".join(subclauses)
        cam_clauses.append(f"(r.camera = ? AND ({combined}))")
        params.append(name)
        params.extend(sub_params)

    if not cam_clauses:
        return "", []
    return " OR ".join(cam_clauses), params


def _open_eligible_conn(cfg: Config) -> sqlite3.Connection:
    """Open a read-only compress-db connection with frigate.recordings attached."""
    conn = sqlite3.connect(
        f"file:{cfg.compress_db}?mode=ro", uri=True, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    _frigate_db_str = str(cfg.frigate_db).replace('"', "")
    conn.execute(
        f'ATTACH DATABASE "file:{_frigate_db_str}?mode=ro" AS frigate_eligible'
    )
    return conn


def get_eligible_recordings(ctx: CompressorContext) -> list[dict]:
    """
    Returns up to ``_ELIGIBLE_BATCH_SIZE`` recordings eligible for compression
    that haven't been successfully compressed yet.  Each result dict has keys:
        recording_id, camera, path, tier, recording_type

    All filtering is done in SQL — camera, age, and tier completion status.
    """
    cfg = ctx.cfg
    where, params = _build_eligible_where(cfg, time.time())
    if not where:
        return []
    params.append(_ELIGIBLE_BATCH_SIZE)

    conn = _open_eligible_conn(cfg)
    try:
        rows = conn.execute(
            f"""
            SELECT r.id, r.camera, r.path, r.start_time,
                   r.motion, r.objects,
                   f.t1_status, f.t2_status
            FROM   frigate_eligible.recordings r
            LEFT JOIN files f ON f.recording_id = r.id
            WHERE  ({where})
            ORDER BY r.start_time ASC
            LIMIT ?
            """,
            params,
        ).fetchall()
    finally:
        conn.close()

    _ok_statuses = (STATUS_OK, STATUS_SEGMENT_UPDATE_FAILED)
    results = []
    for row in rows:
        camera = row["camera"]
        t1_done = row["t1_status"] in _ok_statuses

        # Determine tier — SQL guarantees eligibility, just pick which.
        if t1_done:
            tier = 2
        else:
            tier = 1

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
    Returns seconds until the next recording becomes eligible for either
    tier 1 or tier 2 compression, clamped to ``[MIN_SLEEP_SEC, MAX_SLEEP_SEC]``.

    Returns ``MAX_SLEEP_SEC`` if nothing is pending.
    """
    cfg = ctx.cfg
    now = time.time()
    soonest = MAX_SLEEP_SEC

    # Check tier 1: next recording aging past min_days.
    min_t1_days = _min_tier1_min_days(cfg)
    t1_cutoff = now - (min_t1_days * 86400)
    row = ctx.frigate_ro.execute(
        """
        SELECT start_time FROM recordings
        WHERE  start_time > ?
        ORDER  BY start_time ASC
        LIMIT  1
        """,
        (t1_cutoff,),
    ).fetchone()
    if row is not None:
        eligible_at = row["start_time"] + (min_t1_days * 86400)
        soonest = min(soonest, eligible_at - now)

    # Check tier 2: next t1-compressed recording aging past tier2.min_days.
    min_t2_days = min(
        (
            cam.tier2.min_days
            for cam in cfg.cameras.values()
            if cam.enabled and cam.tier2.enabled
        ),
        default=None,
    )
    if min_t2_days is not None:
        t2_cutoff = now - (min_t2_days * 86400)
        row = ctx.frigate_ro.execute(
            """
            SELECT start_time FROM recordings
            WHERE  start_time > ?
            ORDER  BY start_time ASC
            LIMIT  1
            """,
            (t2_cutoff,),
        ).fetchone()
        if row is not None:
            eligible_at = row["start_time"] + (min_t2_days * 86400)
            soonest = min(soonest, eligible_at - now)

    return max(MIN_SLEEP_SEC, min(MAX_SLEEP_SEC, soonest))


# ═══════════════════════════════════════════════════════════════════════════════
# HOUSEKEEPING
# ═══════════════════════════════════════════════════════════════════════════════


def run_housekeeping(ctx: CompressorContext) -> None:
    cfg = ctx.cfg
    frigate_rw = ctx.frigate_rw

    compress_db = sqlite3.connect(
        f"file:{cfg.compress_db}", uri=True, check_same_thread=False
    )
    compress_db.row_factory = sqlite3.Row
    compress_db.execute("PRAGMA journal_mode=WAL")
    compress_db.execute("PRAGMA busy_timeout=10000")

    try:
        _run_housekeeping_inner(cfg, compress_db, frigate_rw)
    finally:
        compress_db.close()


def _run_housekeeping_inner(
    cfg: Config,
    compress_db: sqlite3.Connection,
    frigate_rw: sqlite3.Connection,
) -> None:
    log("INFO", "── Housekeeping starting")

    # 1. Remove leftover temp files from crashed runs.
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
            frigate_rw.execute(
                "UPDATE recordings SET segment_size = ? WHERE id = ?",
                (actual_size_mb, row["recording_id"]),
            )
            frigate_rw.commit()
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
    pruned = 0
    _frigate_db_str = str(cfg.frigate_db).replace('"', "")
    compress_db.execute(
        f'ATTACH DATABASE "file:{_frigate_db_str}?mode=ro" AS frigate_ro_hk'
    )
    try:
        if cfg.all_dry_run:
            pruned = compress_db.execute(
                """
                SELECT COUNT(*) FROM files
                WHERE recording_id NOT IN (
                    SELECT id FROM frigate_ro_hk.recordings
                )
                """
            ).fetchone()[0]
        else:
            log("INFO", "Pruning orphaned compress DB entries")
            cursor = compress_db.execute(
                """
                DELETE FROM files
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
    rows = compress_db.execute(
        "SELECT * FROM savings_by_camera ORDER BY camera"
    ).fetchall()

    if rows:
        log("INFO", "── Storage savings by camera")
        log(
            "INFO",
            f"  {'Camera':<20} {'T1':>5} {'T1 Before':>10} {'T1 After':>10}"
            f" {'T2':>5} {'T2 Before':>10} {'T2 After':>10}",
        )
        log(
            "INFO",
            f"  {'-' * 20} {'-' * 5} {'-' * 10} {'-' * 10}"
            f" {'-' * 5} {'-' * 10} {'-' * 10}",
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
    else:
        log("INFO", "No compression data yet")

    # 5. Recent errors
    errors = compress_db.execute("SELECT * FROM recent_errors LIMIT 20").fetchall()
    if errors:
        log("WARNING", "── Recent errors (last 7 days)")
        for err in errors:
            ts = err["t2_compressed_at"] or err["t1_compressed_at"]
            msg = err["t2_error_msg"] or err["t1_error_msg"]
            log("WARNING", f"  [{ts}] {err['camera']} | {msg}")

    log("INFO", "── Housekeeping complete")


# ═══════════════════════════════════════════════════════════════════════════════
# PROBE LOOP
# ═══════════════════════════════════════════════════════════════════════════════

PROBE_SLEEP_SEC = 10.0


_PROBE_BATCH_SIZE = 5000


def _get_unprobed_recordings(cfg: Config, conn: sqlite3.Connection) -> list[dict]:
    """Return a batch of Frigate recordings not yet probed.

    Limited to ``_PROBE_BATCH_SIZE`` rows per call to keep memory bounded.
    """
    _frigate_db_str = str(cfg.frigate_db).replace('"', "")
    conn.execute(f'ATTACH DATABASE "file:{_frigate_db_str}?mode=ro" AS frigate_probe')
    try:
        rows = conn.execute(
            """
            SELECT r.id, r.camera, r.path
            FROM   frigate_probe.recordings r
            LEFT JOIN files f ON f.recording_id = r.id
            WHERE  f.recording_id IS NULL
               OR  f.scanned_at IS NULL
            ORDER BY r.start_time ASC
            LIMIT ?
            """,
            (_PROBE_BATCH_SIZE,),
        ).fetchall()
    finally:
        conn.execute("DETACH DATABASE frigate_probe")

    return [
        {"recording_id": row["id"], "camera": row["camera"], "path": row["path"]}
        for row in rows
    ]


def _store_probe(
    conn: sqlite3.Connection,
    recording_id: str,
    camera: str,
    path: str,
    info: dict,
) -> None:
    """Insert or update probe results in the files table."""
    now = time.strftime("%Y-%m-%dT%H:%M:%S")
    conn.execute(
        """
        INSERT INTO files
            (recording_id, camera, path, codec, width, height,
             fps, bitrate, duration_sec, file_size, scanned_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(recording_id) DO UPDATE SET
            codec       = excluded.codec,
            width       = excluded.width,
            height      = excluded.height,
            fps         = excluded.fps,
            bitrate     = excluded.bitrate,
            duration_sec = excluded.duration_sec,
            file_size   = excluded.file_size,
            scanned_at   = excluded.scanned_at
        """,
        (
            recording_id,
            camera,
            path,
            info.get("codec"),
            info.get("width"),
            info.get("height"),
            info.get("fps"),
            info.get("bitrate"),
            info.get("duration_sec"),
            info.get("file_size"),
            now,
        ),
    )
    conn.commit()


def run_probe_loop(ctx: CompressorContext, stopping: threading.Event) -> None:
    """Continuously probe unprobed Frigate recordings.

    Each cycle fetches a batch of recordings not yet probed in ``files``,
    runs ``_probe_full`` on each, and stores the results.  Sleeps
    ``PROBE_SLEEP_SEC`` when fully caught up.

    Opens its own read-write connection to the compress DB so it never
    contends with the compression loop.  WAL mode allows
    concurrent readers and a single writer with busy_timeout handling
    contention.
    """
    probe_conn = sqlite3.connect(
        f"file:{ctx.cfg.compress_db}", uri=True, check_same_thread=False
    )
    probe_conn.row_factory = sqlite3.Row
    probe_conn.execute("PRAGMA journal_mode=WAL")
    probe_conn.execute("PRAGMA busy_timeout=10000")

    try:
        while not stopping.is_set():
            try:
                unprobed = _get_unprobed_recordings(ctx.cfg, probe_conn)
            except Exception as e:
                log("ERROR", f"Probe loop: failed to query unprobed recordings: {e}")
                stopping.wait(timeout=PROBE_SLEEP_SEC)
                continue

            if not unprobed:
                stopping.wait(timeout=PROBE_SLEEP_SEC)
                continue

            log("DEBUG", f"Probing {len(unprobed)} recording(s)")
            probed = 0
            for rec in unprobed:
                if stopping.is_set():
                    break
                info = _probe_full(Path(rec["path"]))
                if info is not None:
                    _store_probe(
                        probe_conn,
                        rec["recording_id"],
                        rec["camera"],
                        rec["path"],
                        info,
                    )
                    probed += 1
                else:
                    log(
                        "DEBUG",
                        f"[{rec['camera']}] Probe failed, skipping: {rec['path']}",
                    )
            if probed:
                log("DEBUG", f"Probed {probed} recording(s)")
    finally:
        probe_conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════


def _display_path(filepath: Path) -> str:
    """Format a Frigate recording path as a compact timestamp.

    Frigate stores recordings as
    ``<recordings_dir>/YYYY-MM-DD/HH/<camera>/MM.SS.mp4``.
    Returns ``YYYYMMDDHHmmss`` (e.g. ``20260418130103``).  The camera is
    already shown as a log prefix.  Falls back to the bare filename if
    the path is shorter than expected.
    """
    parts = filepath.parts
    if len(parts) >= 4:
        date = parts[-4].replace("-", "")  # YYYY-MM-DD → YYYYMMDD
        hour = parts[-3]  # HH
        mm = filepath.stem.split(".")[0] if "." in filepath.stem else filepath.stem
        ss = filepath.stem.split(".")[1] if "." in filepath.stem else "00"
        return f"{date} {hour}:{mm}:{ss}"
    return filepath.name


def _fmt(n: int | float | None, width: int = 0) -> str:
    """Human-readable byte size string, optionally right-justified to *width*."""
    if n is None:
        s = "N/A"
    else:
        n = float(n)
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if n < 1024:
                s = f"{n:.1f}{unit}"
                break
            n /= 1024
        else:
            s = f"{n:.1f}PB"
    return s.rjust(width) if width else s
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
    Tier comes from a LEFT JOIN against ``files`` — determined by which
    ``t*_status`` column is set.  Uncompressed recordings are tier 0.
    NULL ``segment_size`` is treated as 0 bytes so a half-finalised row
    never crashes the aggregate.

    Opens its own read-only connection so it never contends
    with the probe or compression loops.
    """
    cfg = ctx.cfg
    now = time.time()

    conn = sqlite3.connect(
        f"file:{cfg.compress_db}?mode=ro", uri=True, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    try:
        _frigate_db_str = str(cfg.frigate_db).replace('"', "")
        conn.execute(
            f'ATTACH DATABASE "file:{_frigate_db_str}?mode=ro" AS frigate_stats'
        )
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
    finally:
        conn.close()

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

    log("INFO", "════════════════════════════════════════")
    log("INFO", f"Frigate Compressor v{__version__} starting")
    if cfg.all_dry_run:
        log("INFO", "  *** DRY RUN MODE — no files or databases will be modified ***")
    log("INFO", f"  Encoder        : {encoder}")
    log("INFO", f"  Parallel jobs  : {cfg.max_parallel_jobs}")
    log("INFO", f"  Log level      : {cfg.log_level}")
    log("INFO", f"  Housekeeping   : every {cfg.housekeeping_interval_days}d")
    log(
        "INFO",
        f"  Throttle       : auto (target ≈ {_THROTTLE_OVERHEAD}× pending "
        f"work/min; smoothed via recent throughput)",
    )
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
        frigate_ro=frigate_ro,
        frigate_rw=frigate_rw,
    )

    # Use threading.Event so signal handlers can wake the sleep loop immediately.
    stopping = threading.Event()
    housekeeping_interval_sec = cfg.housekeeping_interval_days * 86400

    def handle_sig(_sig, _frame):
        stopping.set()

    signal.signal(signal.SIGTERM, handle_sig)
    signal.signal(signal.SIGINT, handle_sig)

    probe_thread = threading.Thread(
        target=run_probe_loop, args=(ctx, stopping), daemon=True
    )
    probe_thread.start()

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


def measure_workload_per_min(ctx: CompressorContext) -> float:
    """Recordings needing compression in the next minute, per minute.

    Counts every recording that will be eligible by ``now + window`` and
    isn't yet processed for the relevant tier — backlog (already past the
    line ⇒ we've fallen behind) plus incoming (about to cross).  Divided
    by window-minutes gives the rate the throttle targets.
    """
    cfg = ctx.cfg
    window_sec = _THROTTLE_WORKLOAD_WINDOW_SEC
    where, params = _build_eligible_where(cfg, time.time() + window_sec)
    if not where:
        return 0.0

    conn = _open_eligible_conn(cfg)
    try:
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS n
            FROM   frigate_eligible.recordings r
            LEFT JOIN files f ON f.recording_id = r.id
            WHERE  ({where})
            """,
            params,
        ).fetchone()
    finally:
        conn.close()
    n = int(row["n"]) if row else 0
    return n / (window_sec / 60.0)


def sample_throughput_per_min(
    ctx: CompressorContext, window_sec: float = 60.0
) -> float:
    """Observed throughput: tier1 + tier2 completions per minute over window.

    Note: cutoff is formatted in local time to match ``_record``'s
    ``time.strftime`` storage format.  This works correctly except across
    DST transitions, where one hour of completions can be double-counted
    (fall-back) or missed (spring-forward).
    """
    cfg = ctx.cfg
    cutoff_str = time.strftime(
        "%Y-%m-%dT%H:%M:%S", time.localtime(time.time() - window_sec)
    )
    conn = sqlite3.connect(
        f"file:{cfg.compress_db}?mode=ro", uri=True, check_same_thread=False
    )
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM files"
            " WHERE t1_compressed_at >= ? OR t2_compressed_at >= ?",
            (cutoff_str, cutoff_str),
        ).fetchone()
    finally:
        conn.close()
    n = int(row[0]) if row else 0
    return n / (window_sec / 60.0)


def _effective_throttle_target(ctx: CompressorContext) -> float:
    """Throttle target in files/min for the next batch.  Pure read of state.

    target = max(workload × OVERHEAD, speed_ema)

    Workload encodes "what we need to do" (backlog + next-minute incoming).
    The EWMA value, read from ``ctx.throughput_ema.value``, encodes "what
    we're actually doing right now".  Caller is responsible for sampling +
    updating the EWMA before calling this function — keeping the side
    effect explicit at the call site rather than buried here.

    The max() gives smooth catchup-tail transitions: workload >> speed
    during catchup → huge target, limiter is a no-op; as backlog drains,
    workload falls, EWMA holds the floor while it decays over the 60s
    time constant.

    Returns 0 (no throttle) when there's no eligible work and no recent
    activity.
    """
    try:
        workload = measure_workload_per_min(ctx)
    except Exception as e:
        log("WARNING", f"throttle: workload measurement failed: {e}")
        return 0.0
    speed_ema = ctx.throughput_ema.value
    if workload <= 0 and speed_ema <= 0:
        return 0.0
    target = max(workload * _THROTTLE_OVERHEAD, speed_ema)
    log(
        "INFO",
        f"Throttle: target {target:.0f}/min "
        f"(workload={workload:.0f}/min, speed_ema={speed_ema:.0f}/min)",
    )
    return target


def _compress_then_pace(
    target_per_min: float,
    stopping: threading.Event,
    rid: str,
    path: str,
    camera: str,
    tier: int,
    rtype: str,
    encoder: str,
    ctx: CompressorContext,
) -> bool:
    """Worker wrapper: run compress_one then pace via the shared rate limiter."""
    try:
        return compress_one(rid, path, camera, tier, rtype, encoder, ctx)
    finally:
        ctx.rate_limiter.acquire(target_per_min, stopping)


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

            # Refresh throughput EWMA before computing target — keeps the
            # side effect at the call site instead of inside _effective_*.
            try:
                ctx.throughput_ema.update(sample_throughput_per_min(ctx))
            except Exception as e:
                log("WARNING", f"throttle: throughput sample failed: {e}")
            target_per_min = _effective_throttle_target(ctx)
            pool = ThreadPoolExecutor(max_workers=cfg.max_parallel_jobs)
            futures = {
                pool.submit(
                    _compress_then_pace,
                    target_per_min,
                    stopping,
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
            if stopping.is_set():
                pool.shutdown(wait=False, cancel_futures=True)
            else:
                pool.shutdown(wait=True)

            # In dry-run mode, recordings are never marked as done, so
            # re-querying would return the same set.  Fall through to sleep.
            if cfg.all_dry_run:
                pass  # fall through to sleep
            else:
                # Per-file pacing in the workers already enforced the
                # target rate.  Loop back immediately so we keep draining
                # backlog (or aged-in incoming) without an extra delay
                # here.  Returning a partial batch doesn't mean the queue
                # is empty — we re-check every pass.
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
