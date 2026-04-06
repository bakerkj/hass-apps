# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for recording type classification, config loading, and type-settings resolution."""

from __future__ import annotations

import json

import pytest

import frigate_compressor as fc

from fc_helpers import _make_config, _make_options


# ═══════════════════════════════════════════════════════════════════════════════
# _recording_type
# ═══════════════════════════════════════════════════════════════════════════════


def test_recording_type_object():
    assert fc._recording_type(5, 3) == "object"


def test_recording_type_object_zero_motion():
    assert fc._recording_type(0, 2) == "object"


def test_recording_type_motion_no_objects():
    assert fc._recording_type(10, 0) == "motion"


def test_recording_type_motion_null_objects():
    assert fc._recording_type(10, None) == "motion"


def test_recording_type_continuous_both_zero():
    assert fc._recording_type(0, 0) == "continuous"


def test_recording_type_continuous_both_null():
    assert fc._recording_type(None, None) == "continuous"


def test_recording_type_continuous_motion_zero():
    assert fc._recording_type(0, None) == "continuous"


# ═══════════════════════════════════════════════════════════════════════════════
# load_config
# ═══════════════════════════════════════════════════════════════════════════════


def test_load_config_defaults(tmp_path):
    cfg = _make_config(tmp_path)
    assert cfg.encoder == "cpu"
    assert cfg.max_parallel_jobs == 1
    assert cfg.tier1.min_days == 7
    assert cfg.tier2.min_days == 30
    assert cfg.log_level == "DEBUG"


def test_load_config_tier1_type_settings(tmp_path):
    cfg = _make_config(tmp_path)
    assert cfg.tier1.continuous.quality == 28
    assert cfg.tier1.continuous.scale_mode == "none"
    assert cfg.tier1.motion.scale_mode == "halve"
    assert cfg.tier1.object.quality == 22


def test_load_config_tier2_type_settings(tmp_path):
    cfg = _make_config(tmp_path)
    assert cfg.tier2.continuous.fps_mode == "cap"
    assert cfg.tier2.continuous.fps_value == 4.0
    assert cfg.tier2.motion.fps_value == 8.0


def test_load_config_camera_overrides(tmp_path):
    cfg = _make_config(
        tmp_path,
        camera_overrides=[
            {
                "name": "cam1",
                "tier": 1,
                "recording_type": "object",
                "quality": 18,
                "scale_mode": "fixed",
                "scale_value": "1280:720",
            }
        ],
    )
    assert cfg.camera_overrides == {
        ("cam1", 1, "object"): {
            "quality": 18,
            "scale_mode": "fixed",
            "scale_value": "1280:720",
        }
    }


def test_load_config_empty_overrides(tmp_path):
    cfg = _make_config(tmp_path)
    assert cfg.camera_overrides == {}


def test_load_config_paths(tmp_path):
    cfg = _make_config(tmp_path)
    assert isinstance(cfg.frigate_db, type(cfg.frigate_db))  # Path
    assert isinstance(cfg.compress_db, type(cfg.compress_db))
    assert isinstance(cfg.recordings_dir, type(cfg.recordings_dir))


def test_load_config_invalid_tier_ordering(tmp_path):
    p = _make_options(tmp_path)
    data = json.loads(p.read_text())
    data["tier1"]["min_days"] = 30
    data["tier2"]["min_days"] = 7
    p.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="tier2.min_days"):
        fc.load_config(str(p))


def test_load_config_tier_equal_min_days_raises(tmp_path):
    p = _make_options(tmp_path)
    data = json.loads(p.read_text())
    data["tier1"]["min_days"] = 14
    data["tier2"]["min_days"] = 14
    p.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="tier2.min_days"):
        fc.load_config(str(p))


def test_load_config_quality_out_of_range(tmp_path):
    p = _make_options(tmp_path)
    data = json.loads(p.read_text())
    data["tier1"]["continuous"]["quality"] = 99
    p.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="quality"):
        fc.load_config(str(p))


def test_load_config_quality_negative_raises(tmp_path):
    p = _make_options(tmp_path)
    data = json.loads(p.read_text())
    data["tier1"]["motion"]["quality"] = -1
    p.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="quality"):
        fc.load_config(str(p))


def test_load_config_fixed_scale_mode_requires_scale_value(tmp_path):
    p = _make_options(tmp_path)
    data = json.loads(p.read_text())
    data["tier1"]["continuous"]["scale_mode"] = "fixed"
    data["tier1"]["continuous"]["scale_value"] = ""
    p.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="scale_mode='fixed'"):
        fc.load_config(str(p))


def test_load_config_fixed_scale_mode_with_value_ok(tmp_path):
    p = _make_options(tmp_path)
    data = json.loads(p.read_text())
    data["tier1"]["continuous"]["scale_mode"] = "fixed"
    data["tier1"]["continuous"]["scale_value"] = "1280:720"
    p.write_text(json.dumps(data))
    cfg = fc.load_config(str(p))
    assert cfg.tier1.continuous.scale_value == "1280:720"


# ═══════════════════════════════════════════════════════════════════════════════
# _resolve_type_settings
# ═══════════════════════════════════════════════════════════════════════════════


def test_resolve_type_settings_no_override(tmp_path):
    cfg = _make_config(tmp_path)
    ts = fc._resolve_type_settings(cfg, "cam1", 1, "motion")
    assert ts.quality == 26
    assert ts.scale_mode == "halve"


def test_resolve_type_settings_with_override(tmp_path):
    cfg = _make_config(
        tmp_path,
        camera_overrides=[
            {
                "name": "front_door",
                "tier": 1,
                "recording_type": "object",
                "quality": 18,
                "scale_mode": "fixed",
                "scale_value": "1920:1080",
            }
        ],
    )
    ts = fc._resolve_type_settings(cfg, "front_door", 1, "object")
    assert ts.quality == 18
    assert ts.scale_mode == "fixed"
    assert ts.scale_value == "1920:1080"
    assert ts.fps_mode == "none"  # unspecified — falls back to global tier default


def test_resolve_type_settings_override_scoped_to_tier(tmp_path):
    # Override for tier 1 must not affect tier 2 for the same camera/type.
    cfg = _make_config(
        tmp_path,
        camera_overrides=[
            {"name": "front_door", "tier": 1, "recording_type": "object", "quality": 18}
        ],
    )
    ts2 = fc._resolve_type_settings(cfg, "front_door", 2, "object")
    assert ts2.quality == 26  # global tier2 object default


def test_resolve_type_settings_override_scoped_to_type(tmp_path):
    # Override for object must not affect motion for the same camera/tier.
    cfg = _make_config(
        tmp_path,
        camera_overrides=[
            {"name": "front_door", "tier": 1, "recording_type": "object", "quality": 18}
        ],
    )
    ts = fc._resolve_type_settings(cfg, "front_door", 1, "motion")
    assert ts.quality == 26  # global tier1 motion default
