# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Config dataclasses + ``load_config`` + per-camera resolution helpers."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

import yaml


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


_TIER_SOURCES = ("chained", "direct")


@dataclass
class TierConfig:
    """Compression settings for one age tier (tier 1 or tier 2)."""

    enabled: bool  # whether this tier is active for this camera
    min_days: int  # age in days before this tier applies
    continuous: TypeSettings  # segments with no motion and no objects
    motion: TypeSettings  # segments with motion but no object detection
    object: TypeSettings  # segments with at least one detected object
    # Tier-2 only. "chained" (default) re-encodes tier-1 → tier-2 at min_days.
    # "direct" encodes tier-2 from the native source at tier1.min_days
    # (alongside tier-1), parks it at a sibling path, then swaps at min_days.
    # Saves one generation of encode loss; costs disk during the overlap.
    # Tier-1 ignores this field.
    source: str = "chained"


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
    # How long a recording can sit past its tier eligibility before the
    # per-camera backlog binary sensor flips to "problem". Default 1 hour.
    backlog_timeout_seconds: int = 3600

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
    # When True, spawn ffmpeg via the double-fork ``run_detached`` helper so
    # ffmpeg's IO is reaped by PID 1 rather than wait4()'d by the daemon.
    # See ``detached_subprocess.py``.  Default off; opt in to verify on real
    # workloads before flipping the default.
    detached_ffmpeg: bool = False
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
        "source": "chained",
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
    source = str(defaults_tier.get("source", "chained"))
    if camera_tier:
        if "enabled" in camera_tier:
            enabled = bool(camera_tier["enabled"])
        if "min_days" in camera_tier:
            min_days = int(camera_tier["min_days"])
        if "source" in camera_tier:
            source = str(camera_tier["source"])
    if source not in _TIER_SOURCES:
        raise ValueError(
            f"{camera_name}/tier{tier_num}: source must be one of "
            f"{_TIER_SOURCES} (got {source!r})"
        )

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
        source=source,
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
    in options.json (defaulting to ``/config/config.yaml``).
    """
    with open(options_path, "r", encoding="utf-8") as f:
        opts = json.load(f)

    if yaml_path is None:
        yaml_path = str(opts.get("config_path", "/config/config.yaml"))

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
        backlog_timeout_seconds=max(
            60, int(opts.get("mqtt_backlog_timeout_seconds", 3600))
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
        detached_ffmpeg=bool(opts.get("detached_ffmpeg", False)),
    )


def _fmt_type(ts: TypeSettings) -> str:
    """One-line summary of a TypeSettings for startup logging."""
    if not ts.enabled:
        return "SKIP (compression disabled)"
    sc = f"{ts.scale_mode}({ts.scale_value})" if ts.scale_mode != "none" else "original"
    fp = f"{ts.fps_mode}({ts.fps_value})" if ts.fps_mode != "none" else "original"
    return f"q={ts.quality} scale={sc} fps={fp}"
