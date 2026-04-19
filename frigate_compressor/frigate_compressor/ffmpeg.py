# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""ffmpeg/ffprobe wrappers: encoder selection, probing, filter + cmd builders."""

from __future__ import annotations

import subprocess
from pathlib import Path

from .config import TypeSettings
from .util import log

# Max wall-clock seconds to allow a single ffmpeg encode job to run.
FFMPEG_TIMEOUT_SEC = 30

# Max bytes of ffmpeg stderr text stored in the compress DB error_msg column.
FFMPEG_STDERR_MAX_LEN = 300

# Temp files are named .tmp.{recording_id}.mp4 so they are:
#   - unique per recording (no collision between parallel jobs)
#   - distinguishable from real recordings by housekeeping
_TEMP_PREFIX = ".tmp."
_TEMP_GLOB = ".tmp.*.mp4"


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


def _parse_fps(fps_str: str | None) -> float | None:
    """Parse ffprobe's r_frame_rate (e.g. '30/1' or '29.97') to a float."""
    if not fps_str:
        return None
    try:
        parts = fps_str.split("/")
        if len(parts) == 2:
            return float(parts[0]) / float(parts[1])
        return float(parts[0])
    except (ValueError, ZeroDivisionError):
        return None


def _probe(filepath: Path) -> dict | None:
    """Run ffprobe to capture all container-level metadata in one call.

    Returns a dict with keys: codec, width, height, fps, bitrate,
    duration_sec, file_size.  Any individual key may be None if ffprobe
    didn't report that field.  Returns None if the file can't be probed.

    Reads the container header only — no frame decoding, so the cost is
    one fork + a sub-millisecond ffprobe read regardless of file size.
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
    except Exception as e:
        log("WARNING", f"ffprobe failed for {filepath}: {e}")
        return None
    if result.returncode != 0 or not result.stdout.strip():
        return None

    data: dict[str, str] = {}
    for line in result.stdout.strip().splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            data[k.strip()] = v.strip()

    def _try(key: str, parser):
        try:
            return parser(data[key])
        except (KeyError, ValueError, TypeError):
            return None

    info: dict = {
        "codec": data.get("codec_name"),
        "width": _try("width", int),
        "height": _try("height", int),
        "fps": _parse_fps(data.get("r_frame_rate")),
        "bitrate": _try("bit_rate", int),
        "duration_sec": _try("duration", float),
        "file_size": _try("size", int),
    }
    if info["file_size"] is None:
        # Fall back to stat if ffprobe didn't report size.
        try:
            info["file_size"] = filepath.stat().st_size
        except OSError:
            pass
    return info


def _probe_dims(info: dict | None) -> tuple[int, int] | None:
    """Pull (width, height) from a ``_probe`` result, or None if missing."""
    if not info or info.get("width") is None or info.get("height") is None:
        return None
    return (info["width"], info["height"])


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
        info = _probe(input_path)
        source_dims = _probe_dims(info)
        source_fps = (info or {}).get("fps")

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
