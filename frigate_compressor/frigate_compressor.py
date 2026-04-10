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
import signal
import sqlite3
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

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

    quality: int  # CQ/CRF (0-51, lower = better quality)
    scale_mode: str  # none | halve | fixed | fraction
    scale_value: str  # fixed="1280:720", fraction="0.5"
    fps_mode: str  # none | cap | fraction
    fps_value: float  # cap=max fps, fraction=multiplier (e.g. 0.5 = half)


@dataclass
class TierConfig:
    """Compression settings for one age tier (tier 1 or tier 2)."""

    min_days: int  # age in days before this tier applies
    continuous: TypeSettings  # segments with no motion and no objects
    motion: TypeSettings  # segments with motion but no object detection
    object: TypeSettings  # segments with at least one detected object


@dataclass
class Config:
    """Top-level add-on configuration loaded from options.json."""

    encoder: str  # qsv | nvenc | cpu
    max_parallel_jobs: int  # concurrent ffmpeg processes
    tier1: TierConfig
    tier2: TierConfig
    housekeeping_interval_days: int  # days between housekeeping runs
    frigate_db: Path  # path to Frigate's SQLite DB
    recordings_dir: Path  # path to Frigate's recordings
    compress_db: Path  # path to our SQLite DB
    log_level: str  # DEBUG | INFO | WARNING | ERROR
    # (camera, tier, recording_type) → partial TypeSettings field overrides.
    # Any field absent from the dict falls back to the global tier setting.
    camera_overrides: dict[tuple[str, int, str], dict[str, int | float | str]]
    dry_run: bool = (
        False  # when True: log all actions but do not modify any files or DBs
    )


_TIER1_DEFAULTS: dict = {
    "min_days": 7,
    "continuous": {
        "quality": 28,
        "scale_mode": "none",
        "scale_value": "",
        "fps_mode": "none",
        "fps_value": 1.0,
    },
    "motion": {
        "quality": 26,
        "scale_mode": "halve",
        "scale_value": "",
        "fps_mode": "none",
        "fps_value": 1.0,
    },
    "object": {
        "quality": 22,
        "scale_mode": "none",
        "scale_value": "",
        "fps_mode": "none",
        "fps_value": 1.0,
    },
}
_TIER2_DEFAULTS: dict = {
    "min_days": 30,
    "continuous": {
        "quality": 34,
        "scale_mode": "halve",
        "scale_value": "",
        "fps_mode": "cap",
        "fps_value": 4.0,
    },
    "motion": {
        "quality": 30,
        "scale_mode": "halve",
        "scale_value": "",
        "fps_mode": "cap",
        "fps_value": 8.0,
    },
    "object": {
        "quality": 26,
        "scale_mode": "halve",
        "scale_value": "",
        "fps_mode": "cap",
        "fps_value": 8.0,
    },
}


def _load_type_settings(d: dict, defaults: dict) -> TypeSettings:
    quality = int(d.get("quality", defaults["quality"]))
    if not 0 <= quality <= 51:
        raise ValueError(f"quality must be 0–51, got {quality}")
    scale_mode = str(d.get("scale_mode", defaults["scale_mode"]))
    scale_value = str(d.get("scale_value", defaults["scale_value"]))
    if scale_mode == "fixed" and not scale_value:
        raise ValueError(
            "scale_mode='fixed' requires a non-empty scale_value (e.g. '1280:720')"
        )
    return TypeSettings(
        quality=quality,
        scale_mode=scale_mode,
        scale_value=scale_value,
        fps_mode=str(d.get("fps_mode", defaults["fps_mode"])),
        fps_value=float(d.get("fps_value", defaults["fps_value"])),
    )


def _load_tier(t: dict, defaults: dict) -> TierConfig:
    return TierConfig(
        min_days=int(t.get("min_days", defaults["min_days"])),
        continuous=_load_type_settings(
            t.get("continuous") or {}, defaults["continuous"]
        ),
        motion=_load_type_settings(t.get("motion") or {}, defaults["motion"]),
        object=_load_type_settings(t.get("object") or {}, defaults["object"]),
    )


def _load_partial_type_settings(d: dict) -> dict:
    """
    Validate and return a dict containing only the TypeSettings fields present
    in *d*.  Used for per-camera overrides — absent fields fall back to the
    global tier setting at resolution time.
    """
    result: dict = {}
    if "quality" in d:
        q = int(d["quality"])
        if not 0 <= q <= 51:
            raise ValueError(f"quality must be 0–51, got {q}")
        result["quality"] = q
    if "scale_mode" in d:
        result["scale_mode"] = str(d["scale_mode"])
    if "scale_value" in d:
        result["scale_value"] = str(d["scale_value"])
    if result.get("scale_mode") == "fixed" and not result.get("scale_value"):
        raise ValueError(
            "scale_mode='fixed' requires a non-empty scale_value (e.g. '1280:720')"
        )
    if "fps_mode" in d:
        result["fps_mode"] = str(d["fps_mode"])
    if "fps_value" in d:
        result["fps_value"] = float(d["fps_value"])
    return result


def _resolve_type_settings(
    cfg: Config, camera: str, tier: int, recording_type: str
) -> TypeSettings:
    """
    Return TypeSettings for this (camera, tier, recording_type), merging any
    per-camera override on top of the global tier/type defaults.
    """
    tier_cfg = cfg.tier1 if tier == 1 else cfg.tier2
    base: TypeSettings | None = getattr(tier_cfg, recording_type, None)
    if base is None:
        log(
            "WARNING",
            f"Unknown recording_type '{recording_type}' for camera '{camera}' tier {tier}"
            " — falling back to 'continuous' settings",
        )
        base = tier_cfg.continuous
    override = cfg.camera_overrides.get((camera, tier, recording_type))
    if not override:
        return base
    return TypeSettings(
        quality=int(override.get("quality", base.quality)),
        scale_mode=str(override.get("scale_mode", base.scale_mode)),
        scale_value=str(override.get("scale_value", base.scale_value)),
        fps_mode=str(override.get("fps_mode", base.fps_mode)),
        fps_value=float(override.get("fps_value", base.fps_value)),
    )


def load_config(options_path: str) -> Config:
    with open(options_path, "r", encoding="utf-8") as f:
        opts = json.load(f)

    cam_overrides: dict[tuple[str, int, str], dict] = {}
    for entry in opts.get("camera_overrides") or []:
        key = (
            str(entry["name"]),
            int(entry["tier"]),
            str(entry["recording_type"]),
        )
        cam_overrides[key] = _load_partial_type_settings(entry)

    cfg = Config(
        encoder=opts.get("encoder", "qsv"),
        max_parallel_jobs=int(opts.get("max_parallel_jobs", 2)),
        tier1=_load_tier(opts.get("tier1") or {}, _TIER1_DEFAULTS),
        tier2=_load_tier(opts.get("tier2") or {}, _TIER2_DEFAULTS),
        housekeeping_interval_days=int(opts.get("housekeeping_interval_days", 7)),
        frigate_db=Path(
            opts.get("frigate_db", "/addon_configs/ccab4aaf_frigate-fa/frigate.db")
        ),
        recordings_dir=Path(opts.get("recordings_dir", "/media/frigate/recordings")),
        compress_db=Path(opts.get("compress_db", "/data/compress.db")),
        log_level=(opts.get("log_level") or "INFO").upper(),
        camera_overrides=cam_overrides,
        dry_run=bool(opts.get("dry_run", True)),
    )

    if cfg.tier2.min_days <= cfg.tier1.min_days:
        raise ValueError(
            f"tier2.min_days ({cfg.tier2.min_days}) must be greater than "
            f"tier1.min_days ({cfg.tier1.min_days})"
        )

    if not cfg.frigate_db.exists():
        raise FileNotFoundError(f"frigate_db not found: {cfg.frigate_db}")
    if not cfg.recordings_dir.is_dir():
        raise FileNotFoundError(f"recordings_dir not found: {cfg.recordings_dir}")

    return cfg


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
        if preferred == "nvenc" and "h264_nvenc" in output:
            return "nvenc"
    except Exception as e:
        log("WARNING", f"ffmpeg encoder probe failed: {e}")
    log(
        "WARNING",
        f"Encoder '{preferred}' not available — falling back to CPU (libx264)",
    )
    return "cpu"


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

    return f"scale_qsv={dims}" if encoder == "qsv" else f"scale={dims}"


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
        "codec": "h264_qsv",
        "quality_flag": "-global_quality",
        "preset": "slower",
    },
    "nvenc": {
        "hwaccel": ("cuda", "cuda"),
        "codec": "h264_nvenc",
        "quality_flag": "-cq",
        "preset": "p4",
    },
    "cpu": {
        "hwaccel": None,
        "codec": "libx264",
        "quality_flag": "-crf",
        "preset": "fast",
    },
}


def build_ffmpeg_cmd(
    input_path: Path,
    output_path: Path,
    encoder: str,
    tier: int,
    camera: str,
    recording_type: str,
    cfg: Config,
) -> list[str]:
    ts = _resolve_type_settings(cfg, camera, tier, recording_type)
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
        ["-hwaccel", enc["hwaccel"][0], "-hwaccel_output_format", enc["hwaccel"][1]]
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
        "-preset",
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
    cmd = build_ffmpeg_cmd(
        filepath, tmpfile, encoder, tier, camera, recording_type, cfg
    )

    log(
        "INFO",
        f"[{camera}] tier={tier} type={recording_type} {filepath.name} ({_fmt(size_before)})",
    )
    log("DEBUG", f"[{camera}]   cmd: {' '.join(cmd)}")

    if cfg.dry_run:
        log("INFO", f"[{camera}] DRY RUN: skipping ffmpeg — no files modified")
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
            f"[{camera}] ffmpeg timeout ({FFMPEG_TIMEOUT_SEC}s): {filepath.name}",
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
        log("ERROR", f"[{camera}] ffmpeg raised unexpected exception: {e}")
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
            f"[{camera}] ffmpeg failed (rc={result.returncode}): {filepath.name}",
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
        log("WARNING", f"[{camera}] output missing after encode: {filepath.name}")
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
            f"[{camera}] output suspiciously small — keeping original: {filepath.name}",
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
            f"[{camera}] original deleted during compression — discarding output: {filepath.name}",
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
            f"[{camera}] original changed during compression — discarding output: {filepath.name}",
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
            f"[{camera}] recording removed from Frigate DB during compression — discarding output to prevent orphan: {filepath.name}",
        )
        return False

    # Atomically replace original.
    log(
        "INFO", f"[{camera}] Replacing original with compressed output: {filepath.name}"
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
        log("ERROR", f"[{camera}] failed to replace original: {e}")
        return False

    saved = size_before - size_after
    log(
        "INFO",
        f"[{camera}] {_fmt(size_before)} → {_fmt(size_after)} "
        f"(saved {_fmt(saved)}, {duration:.1f}s)",
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


def get_eligible_recordings(ctx: CompressorContext) -> list[dict]:
    """
    Returns recordings eligible for compression that haven't been successfully
    compressed yet.  Each result dict has keys:
        recording_id, camera, path, tier, recording_type

    Attaches Frigate's DB to the compress connection so a single query can
    cross-reference both tables, avoiding fetching already-done rows.
    """
    cfg = ctx.cfg
    compress_db = ctx.compress_db
    db_lock = ctx.db_lock

    tier1_cutoff = time.time() - (cfg.tier1.min_days * 86400)
    tier2_cutoff = time.time() - (cfg.tier2.min_days * 86400)

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
                (tier1_cutoff, STATUS_OK, STATUS_SEGMENT_UPDATE_FAILED),
            ).fetchall()
        finally:
            compress_db.execute("DETACH DATABASE frigate_eligible")

    results = []
    for row in rows:
        tier = 2 if row["start_time"] < tier2_cutoff else 1
        rtype = _recording_type(row["motion"], row["objects"])
        results.append(
            {
                "recording_id": row["id"],
                "camera": row["camera"],
                "path": row["path"],
                "tier": tier,
                "recording_type": rtype,
            }
        )
    return results


def time_until_next_eligible(ctx: CompressorContext) -> float:
    """
    Returns seconds until the next recording becomes eligible for tier 1 compression.
    Returns 3600 if nothing is pending (no future candidates in the DB).
    """
    cfg = ctx.cfg
    tier1_cutoff = time.time() - (cfg.tier1.min_days * 86400)

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
        return 3600.0

    eligible_at = row["start_time"] + (cfg.tier1.min_days * 86400)
    return max(60.0, eligible_at - time.time())


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
        if cfg.dry_run:
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
                f"[{row['camera']}] segment_update_failed file no longer on disk, skipping: {fpath.name}",
            )
            continue
        actual_size_mb = fpath.stat().st_size / (1024 * 1024)
        if cfg.dry_run:
            log(
                "INFO",
                f"[{row['camera']}] DRY RUN: would retry segment_size update"
                f" ({actual_size_mb:.3f}MB): {fpath.name}",
            )
            continue
        try:
            log(
                "DEBUG",
                f"[{row['camera']}] Retrying segment_size update ({actual_size_mb:.3f}MB): {fpath.name}",
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
                f"[{row['camera']}] retried segment_size update — ok: {fpath.name}",
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
            if cfg.dry_run:
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
        prefix = "DRY RUN: Would prune" if cfg.dry_run else "Pruned"
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
    sc = f"{ts.scale_mode}({ts.scale_value})" if ts.scale_mode != "none" else "original"
    fp = f"{ts.fps_mode}({ts.fps_value})" if ts.fps_mode != "none" else "original"
    return f"q={ts.quality} scale={sc} fps={fp}"


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN DAEMON LOOP
# ═══════════════════════════════════════════════════════════════════════════════


def _warn_qsv_fps_conflicts(cfg: Config, encoder: str) -> None:
    """
    Emit one WARNING per (label, tier, recording_type) combination where QSV
    encoding is active alongside both an fps filter and a scale filter.
    Mixed CPU/GPU filter chains can cause FFmpeg to fail with a cryptic
    'Error while filtering' message.  Called once at startup to inform the
    user without spamming a warning for every compressed recording.
    """
    if encoder != "qsv":
        return

    # Check global tier settings first.
    for tier_num, tier_cfg in ((1, cfg.tier1), (2, cfg.tier2)):
        for rtype in ("continuous", "motion", "object"):
            ts: TypeSettings = getattr(tier_cfg, rtype)
            if ts.fps_mode != "none" and ts.scale_mode != "none":
                log(
                    "WARNING",
                    f"Config [tier{tier_num}/{rtype}]: QSV encoder + fps_mode='{ts.fps_mode}'"
                    f" + scale_mode='{ts.scale_mode}' — mixed CPU/GPU filter chain may cause"
                    " FFmpeg to fail. Consider fps_mode='none' with QSV, or encoder='cpu'.",
                )

    # Check camera overrides: resolve the full merged settings and warn if both
    # filters are active after merging with the global defaults.
    for cam, tier_num, rtype in cfg.camera_overrides:
        ts = _resolve_type_settings(cfg, cam, tier_num, rtype)
        if ts.fps_mode != "none" and ts.scale_mode != "none":
            log(
                "WARNING",
                f"Config [{cam} tier{tier_num}/{rtype} override]: QSV encoder"
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
    if cfg.dry_run:
        log("INFO", "  *** DRY RUN MODE — no files or databases will be modified ***")
    log("INFO", f"  Encoder        : {encoder}")
    log("INFO", f"  Parallel jobs  : {cfg.max_parallel_jobs}")
    log("INFO", f"  Log level      : {cfg.log_level}")
    log("INFO", f"  Tier 1  (>{cfg.tier1.min_days}d):")
    log("INFO", f"    continuous : {_fmt_type(cfg.tier1.continuous)}")
    log("INFO", f"    motion     : {_fmt_type(cfg.tier1.motion)}")
    log("INFO", f"    object     : {_fmt_type(cfg.tier1.object)}")
    log("INFO", f"  Tier 2  (>{cfg.tier2.min_days}d):")
    log("INFO", f"    continuous : {_fmt_type(cfg.tier2.continuous)}")
    log("INFO", f"    motion     : {_fmt_type(cfg.tier2.motion)}")
    log("INFO", f"    object     : {_fmt_type(cfg.tier2.object)}")
    log("INFO", f"  Housekeeping   : every {cfg.housekeeping_interval_days}d")
    log("INFO", f"  Frigate DB     : {cfg.frigate_db}")
    log("INFO", f"  Recordings     : {cfg.recordings_dir}")
    log("INFO", f"  Compress DB    : {cfg.compress_db}")
    if cfg.camera_overrides:
        log("INFO", "  Camera overrides:")
        grouped: dict[str, list] = {}
        for (cam, t, rtype), fields in cfg.camera_overrides.items():
            grouped.setdefault(cam, []).append((t, rtype, fields))
        for cam in sorted(grouped):
            for t, rtype, fields in sorted(grouped[cam]):
                field_str = ", ".join(f"{k}={v}" for k, v in fields.items())
                log("INFO", f"    {cam} tier{t}/{rtype}: {field_str}")
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
    last_housekeeping = time.time()
    housekeeping_interval_sec = cfg.housekeeping_interval_days * 86400

    def handle_sig(_sig, _frame):
        stopping.set()

    signal.signal(signal.SIGTERM, handle_sig)
    signal.signal(signal.SIGINT, handle_sig)

    try:
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
                suffix = " (DRY RUN — skipping ffmpeg)" if cfg.dry_run else ""
                log("INFO", f"Found {len(eligible)} recording(s) to compress{suffix}")

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
                # running jobs complete before we reach the sleep or DB close.

            # ── Sleep until next recording becomes eligible ───────────────────
            if not stopping.is_set():
                try:
                    sleep_sec = time_until_next_eligible(ctx)
                except Exception as e:
                    log("WARNING", f"time_until_next_eligible failed: {e}")
                    sleep_sec = 3600.0

                log("INFO", f"Next check in {sleep_sec / 60:.1f} min")

                # stopping.wait() returns immediately when the event is set, so a
                # signal wakes us without waiting for the full sleep duration.
                stopping.wait(timeout=sleep_sec)
    finally:
        log("INFO", "Frigate Compressor stopped")
        compress_db.close()
        frigate_ro.close()
        frigate_rw.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
