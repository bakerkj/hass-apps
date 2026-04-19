# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for FFmpeg helpers: scale/fps filters, command building, and ffprobe."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import frigate_compressor as fc

from fc_helpers import _make_config


# ═══════════════════════════════════════════════════════════════════════════════
# _build_scale_filter
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    ("mode", "value", "encoder", "dims", "expected"),
    [
        ("none", "", "cpu", None, ""),
        ("halve", "", "cpu", None, "scale=iw/2:ih/2"),
        ("halve", "", "qsv", None, "scale_qsv=iw/2:ih/2"),
        ("halve", "", "vaapi", None, "scale_vaapi=iw/2:ih/2"),
        ("fixed", "1280:720", "cpu", None, "scale=1280:720"),
        ("fixed", "1280:720", "qsv", None, "scale_qsv=1280:720"),
        ("fixed", "1280:720", "vaapi", None, "scale_vaapi=1280:720"),
        ("fraction", "0.5", "cpu", (1920, 1080), "scale=960:540"),
        ("fraction", "0.5", "qsv", (1920, 1080), "scale_qsv=960:540"),
        ("fraction", "0.5", "vaapi", (1920, 1080), "scale_vaapi=960:540"),
        ("fraction", "0.5", "cpu", None, "scale=iw/2:ih/2"),
        ("fraction", "not_a_float", "cpu", (1920, 1080), "scale=iw/2:ih/2"),
        ("bogus", "", "cpu", None, ""),
    ],
)
def test_build_scale_filter(mode, value, encoder, dims, expected):
    assert fc._build_scale_filter(mode, value, encoder, dims) == expected


def test_scale_fraction_even_pixels():
    # 1920 * 0.333 = 639.36 → 640 (rounded, forced even); 1080 * 0.333 = 359.64 → 360
    result = fc._build_scale_filter("fraction", "0.333", "cpu", (1920, 1080))
    w, h = result.replace("scale=", "").split(":")
    assert int(w) % 2 == 0
    assert int(h) % 2 == 0


# ═══════════════════════════════════════════════════════════════════════════════
# _build_fps_filter
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    ("mode", "value", "source_fps", "expected"),
    [
        ("none", 8.0, 20.0, ""),
        ("cap", 8.0, None, "fps=8"),
        ("cap", 7.6, None, "fps=8"),
        ("cap", 0.1, None, "fps=1"),
        ("fraction", 0.5, 20.0, "fps=10"),
        ("fraction", 8.0, None, "fps=8"),
        ("bogus", 8.0, 20.0, ""),
    ],
)
def test_build_fps_filter(mode, value, source_fps, expected):
    assert fc._build_fps_filter(mode, value, source_fps) == expected


# ═══════════════════════════════════════════════════════════════════════════════
# build_ffmpeg_cmd
# ═══════════════════════════════════════════════════════════════════════════════


def _cmd(tmp_path, encoder="cpu", tier=1, recording_type="continuous"):
    cfg = _make_config(tmp_path)
    cam = cfg.cameras["cam"]
    tier_cfg = cam.tier1 if tier == 1 else cam.tier2
    ts = getattr(tier_cfg, recording_type)
    return fc.build_ffmpeg_cmd(
        Path("/input.mp4"),
        Path("/output.mp4"),
        encoder,
        ts,
    )


def test_build_ffmpeg_cmd_cpu_basic(tmp_path):
    cmd = _cmd(tmp_path, encoder="cpu")
    assert "ffmpeg" in cmd
    assert "libx264" in cmd
    assert "-crf" in cmd


def test_build_ffmpeg_cmd_qsv(tmp_path):
    cmd = _cmd(tmp_path, encoder="qsv")
    assert "h264_qsv" in cmd
    assert "-hwaccel" in cmd
    assert "qsv" in cmd
    # qsv uses -global_quality and -preset
    assert "-global_quality" in cmd
    assert "-preset" in cmd


def test_build_ffmpeg_cmd_vaapi(tmp_path):
    cmd = _cmd(tmp_path, encoder="vaapi")
    assert "h264_vaapi" in cmd
    assert "-hwaccel" in cmd
    assert "vaapi" in cmd
    # vaapi needs an explicit render node — auto-detect is unreliable
    assert "-hwaccel_device" in cmd
    assert "/dev/dri/renderD128" in cmd
    # vaapi uses -qp and -compression_level (not -preset)
    assert "-qp" in cmd
    assert "-compression_level" in cmd
    assert "-preset" not in cmd


def test_build_ffmpeg_cmd_nvenc(tmp_path):
    cmd = _cmd(tmp_path, encoder="nvenc")
    assert "h264_nvenc" in cmd
    assert "-hwaccel" in cmd
    assert "cuda" in cmd


def test_build_ffmpeg_cmd_no_vf_when_no_filters(tmp_path):
    # tier1 continuous: scale_mode=none, fps_mode=none → no -vf
    cmd = _cmd(tmp_path, encoder="cpu", tier=1, recording_type="continuous")
    assert "-vf" not in cmd


def test_build_ffmpeg_cmd_has_vf_when_scale(tmp_path):
    # tier2 continuous: scale_mode=fixed → -vf present with scale
    cmd = _cmd(tmp_path, encoder="cpu", tier=2, recording_type="continuous")
    assert "-vf" in cmd
    vf = cmd[cmd.index("-vf") + 1]
    assert "scale" in vf


def test_build_ffmpeg_cmd_has_vf_when_fps(tmp_path):
    # tier2 continuous: fps_mode=cap → -vf present
    cmd = _cmd(tmp_path, encoder="cpu", tier=2, recording_type="continuous")
    assert "-vf" in cmd
    assert "fps" in cmd[cmd.index("-vf") + 1]


def test_build_ffmpeg_cmd_quality_tier1_object(tmp_path):
    cfg = _make_config(tmp_path)
    cam = cfg.cameras["cam"]
    ts = cam.tier1.object
    cmd = fc.build_ffmpeg_cmd(Path("/in.mp4"), Path("/out.mp4"), "cpu", ts)
    assert "-crf" in cmd
    assert str(ts.quality) in cmd


def test_build_ffmpeg_cmd_quality_tier2_continuous(tmp_path):
    cfg = _make_config(tmp_path)
    cam = cfg.cameras["cam"]
    ts = cam.tier2.continuous
    cmd = fc.build_ffmpeg_cmd(Path("/in.mp4"), Path("/out.mp4"), "cpu", ts)
    assert str(ts.quality) in cmd


def test_build_ffmpeg_cmd_metadata_flags(tmp_path):
    cmd = _cmd(tmp_path)
    assert "-map_metadata" in cmd
    assert "-movflags" in cmd
    assert "+faststart" in cmd


def test_build_ffmpeg_cmd_copy_audio(tmp_path):
    cmd = _cmd(tmp_path)
    assert "-c:a" in cmd
    assert "copy" in cmd


# ═══════════════════════════════════════════════════════════════════════════════
# _probe (dims/fps view via _probe_dims + info["fps"])
# ═══════════════════════════════════════════════════════════════════════════════


def _fake_probe_result(stdout: str, returncode: int = 0) -> MagicMock:
    m = MagicMock()
    m.returncode = returncode
    m.stdout = stdout
    m.stderr = ""
    return m


def test_probe_empty_output(tmp_path):
    f = tmp_path / "clip.mp4"
    f.write_bytes(b"")
    with patch("subprocess.run", return_value=_fake_probe_result("")):
        info = fc._probe(f)
    assert info is None
    assert fc._probe_dims(info) is None


def test_probe_nonzero_returncode(tmp_path):
    f = tmp_path / "clip.mp4"
    f.write_bytes(b"")
    with patch(
        "subprocess.run", return_value=_fake_probe_result("width=1920\n", returncode=1)
    ):
        info = fc._probe(f)
    assert info is None
    assert fc._probe_dims(info) is None


def test_probe_zero_division_fps(tmp_path):
    f = tmp_path / "clip.mp4"
    f.write_bytes(b"")
    stdout = "width=1920\nheight=1080\nr_frame_rate=0/0\n"
    with patch("subprocess.run", return_value=_fake_probe_result(stdout)):
        info = fc._probe(f)
    assert fc._probe_dims(info) == (1920, 1080)
    assert info["fps"] is None  # ZeroDivisionError swallowed gracefully


def test_probe_bad_dimensions(tmp_path):
    f = tmp_path / "clip.mp4"
    f.write_bytes(b"")
    stdout = "width=N/A\nheight=N/A\nr_frame_rate=25/1\n"
    with patch("subprocess.run", return_value=_fake_probe_result(stdout)):
        info = fc._probe(f)
    assert fc._probe_dims(info) is None
    assert info["fps"] == pytest.approx(25.0)


def test_probe_valid_dims_and_fps(tmp_path):
    f = tmp_path / "clip.mp4"
    f.write_bytes(b"")
    stdout = "width=1920\nheight=1080\nr_frame_rate=30000/1001\n"
    with patch("subprocess.run", return_value=_fake_probe_result(stdout)):
        info = fc._probe(f)
    assert fc._probe_dims(info) == (1920, 1080)
    assert info["fps"] == pytest.approx(30000 / 1001)
