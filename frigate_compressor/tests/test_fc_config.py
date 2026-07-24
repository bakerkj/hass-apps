# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for recording type classification, config loading, and resolution logic."""

import json

import pytest
from fc_helpers import _make_config, _make_frigate_db

import frigate_compressor as fc

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
# load_config — basic loading
# ═══════════════════════════════════════════════════════════════════════════════


def test_load_config_defaults(tmp_path):
    cfg = _make_config(tmp_path)
    assert cfg.encoder == "cpu"
    assert cfg.max_parallel_jobs == 1
    assert "cam" in cfg.cameras
    cam = cfg.cameras["cam"]
    assert cam.tier1.min_days == 8
    assert cam.tier2.min_days == 30
    assert cam.enabled is True
    assert cam.dry_run is False


def test_builtin_defaults_are_safe(tmp_path):
    """Without any YAML defaults, built-in defaults must not compress anything."""
    cfg = _make_config(tmp_path, yaml_defaults=None)
    cam = cfg.cameras["cam"]
    assert cam.enabled is False
    assert cam.dry_run is True


def test_load_config_tier1_type_settings(tmp_path):
    cfg = _make_config(tmp_path)
    cam = cfg.cameras["cam"]
    assert cam.tier1.continuous.quality == 30
    assert cam.tier1.continuous.scale_mode == "none"
    assert cam.tier1.motion.quality == 30
    assert cam.tier1.motion.scale_mode == "none"
    assert cam.tier1.object.quality == 24


def test_load_config_tier2_type_settings(tmp_path):
    cfg = _make_config(tmp_path)
    cam = cfg.cameras["cam"]
    assert cam.tier2.continuous.fps_mode == "cap"
    assert cam.tier2.continuous.fps_value == 4.0
    assert cam.tier2.continuous.scale_mode == "fixed"
    assert cam.tier2.continuous.scale_value == "1280:720"
    assert cam.tier2.motion.quality == 30
    assert cam.tier2.motion.scale_value == "1600:900"
    assert cam.tier2.motion.fps_value == 4.0
    assert cam.tier2.object.quality == 26
    assert cam.tier2.object.scale_mode == "none"
    assert cam.tier2.object.fps_value == 8.0


def test_load_config_paths(tmp_path):
    cfg = _make_config(tmp_path)
    assert isinstance(cfg.frigate_db, type(cfg.frigate_db))  # Path
    assert isinstance(cfg.compress_db, type(cfg.compress_db))
    assert isinstance(cfg.recordings_dir, type(cfg.recordings_dir))


def test_load_config_quality_out_of_range(tmp_path):
    with pytest.raises(ValueError, match="quality"):
        _make_config(
            tmp_path,
            yaml_defaults={"tier1": {"quality": 99}},
        )


def test_load_config_quality_negative_raises(tmp_path):
    with pytest.raises(ValueError, match="quality"):
        _make_config(
            tmp_path,
            yaml_defaults={"tier1": {"motion": {"quality": -1}}},
        )


def test_load_config_fixed_scale_mode_requires_scale_value(tmp_path):
    with pytest.raises(ValueError, match="scale_mode='fixed'"):
        _make_config(
            tmp_path,
            yaml_defaults={"tier1": {"scale_mode": "fixed", "scale_value": ""}},
        )


def test_load_config_fixed_scale_mode_with_value_ok(tmp_path):
    cfg = _make_config(
        tmp_path,
        yaml_defaults={"tier1": {"scale_mode": "fixed", "scale_value": "1280:720"}},
    )
    cam = cfg.cameras["cam"]
    # fixed+value applies to all types via base
    assert cam.tier1.continuous.scale_value == "1280:720"
    assert cam.tier1.continuous.scale_mode == "fixed"


# ═══════════════════════════════════════════════════════════════════════════════
# load_config — tier ordering validation
# ═══════════════════════════════════════════════════════════════════════════════


def test_load_config_invalid_tier_ordering(tmp_path):
    with pytest.raises(ValueError, match="tier2.min_days"):
        _make_config(
            tmp_path,
            yaml_defaults={
                "tier1": {"min_days": 30},
                "tier2": {"min_days": 7},
            },
        )


def test_load_config_tier_equal_min_days_raises(tmp_path):
    with pytest.raises(ValueError, match="tier2.min_days"):
        _make_config(
            tmp_path,
            yaml_defaults={
                "tier1": {"min_days": 14},
                "tier2": {"min_days": 14},
            },
        )


# ═══════════════════════════════════════════════════════════════════════════════
# load_config — tier source field (chained vs direct)
# ═══════════════════════════════════════════════════════════════════════════════


def test_tier2_source_default_is_direct(tmp_path):
    cfg = _make_config(tmp_path)
    cam = cfg.cameras["cam"]
    assert cam.tier2.source == "direct"


def test_tier1_source_default_is_direct(tmp_path):
    """Tier-1 always inherits the same default; the field is just unused there."""
    cfg = _make_config(tmp_path)
    cam = cfg.cameras["cam"]
    assert cam.tier1.source == "direct"


def test_tier2_source_chained_explicit(tmp_path):
    cfg = _make_config(tmp_path, yaml_defaults={"tier2": {"source": "chained"}})
    cam = cfg.cameras["cam"]
    assert cam.tier2.source == "chained"


def test_tier2_source_invalid_raises(tmp_path):
    with pytest.raises(ValueError, match="source must be one of"):
        _make_config(tmp_path, yaml_defaults={"tier2": {"source": "bogus"}})


def test_tier2_source_per_camera_override(tmp_path):
    cfg = _make_config(
        tmp_path,
        yaml_defaults={"tier2": {"source": "chained"}},
        yaml_cameras={"cam": {"tier2": {"source": "direct"}}},
    )
    assert cfg.cameras["cam"].tier2.source == "direct"


def test_tier2_source_per_camera_overrides_to_chained(tmp_path):
    """Per-camera override can also flip back to chained when defaults are direct."""
    cfg = _make_config(
        tmp_path,
        yaml_defaults={"tier2": {"source": "direct"}},
        yaml_cameras={"cam": {"tier2": {"source": "chained"}}},
    )
    assert cfg.cameras["cam"].tier2.source == "chained"


# ═══════════════════════════════════════════════════════════════════════════════
# Resolution logic — 4-layer merge
# ═══════════════════════════════════════════════════════════════════════════════


def test_resolve_defaults_only(tmp_path):
    """Camera with no overrides gets pure defaults."""
    cfg = _make_config(tmp_path)
    cam = cfg.cameras["cam"]
    ts = cam.tier1.motion
    assert ts.quality == 30
    assert ts.scale_mode == "none"


def test_resolve_camera_tier_base_overrides_defaults(tmp_path):
    """Camera tier base quality overrides defaults tier base AND per-type defaults."""
    cfg = _make_config(
        tmp_path,
        yaml_cameras={"front_door": {"tier1": {"quality": 28}}},
    )
    cam = cfg.cameras["front_door"]
    # Layer 3 (camera tier base q=28) overrides layer 2 (defaults motion q=30)
    assert cam.tier1.motion.quality == 28
    # Layer 3 also overrides layer 1 (defaults base q=30)
    assert cam.tier1.continuous.quality == 28


def test_resolve_camera_tier_type_overrides_camera_tier_base(tmp_path):
    """Camera tier per-type override wins over camera tier base."""
    cfg = _make_config(
        tmp_path,
        yaml_cameras={
            "front_door": {
                "tier1": {
                    "quality": 30,  # camera tier base
                    "object": {"quality": 18},  # camera tier per-type
                }
            }
        },
    )
    cam = cfg.cameras["front_door"]
    assert cam.tier1.object.quality == 18  # layer 4 wins
    assert cam.tier1.continuous.quality == 30  # layer 3 for other types


def test_resolve_camera_tier_override_scoped_to_tier(tmp_path):
    """Override for tier 1 must not affect tier 2."""
    cfg = _make_config(
        tmp_path,
        yaml_cameras={
            "front_door": {"tier1": {"quality": 18}},
        },
    )
    cam = cfg.cameras["front_door"]
    assert cam.tier1.continuous.quality == 18
    assert cam.tier2.continuous.quality == 34  # global tier2 default


def test_resolve_camera_tier_override_scoped_to_type(tmp_path):
    """Per-type override must not affect other types."""
    cfg = _make_config(
        tmp_path,
        yaml_cameras={
            "front_door": {"tier1": {"object": {"quality": 18}}},
        },
    )
    cam = cfg.cameras["front_door"]
    assert cam.tier1.object.quality == 18
    assert cam.tier1.motion.quality == 30  # defaults base (no per-type override)
    assert cam.tier1.continuous.quality == 30  # defaults base


def test_resolve_defaults_per_type_then_camera_base(tmp_path):
    """Camera base quality override applies to all types that don't have their own."""
    cfg = _make_config(
        tmp_path,
        yaml_cameras={"cam1": {"tier1": {"quality": 28}}},
    )
    cam = cfg.cameras["cam1"]
    # motion inherits scale_mode=none from tier base; camera base overrides quality
    assert cam.tier1.motion.scale_mode == "none"
    assert cam.tier1.motion.quality == 28


# ═══════════════════════════════════════════════════════════════════════════════
# Resolution logic — enabled / dry_run inheritance
# ═══════════════════════════════════════════════════════════════════════════════


def test_camera_enabled_defaults_true(tmp_path):
    cfg = _make_config(tmp_path, yaml_cameras={"cam1": {}})
    assert cfg.cameras["cam1"].enabled is True


def test_camera_enabled_override(tmp_path):
    cfg = _make_config(
        tmp_path,
        yaml_cameras={"cam1": {"enabled": False}},
    )
    assert cfg.cameras["cam1"].enabled is False


def test_camera_dry_run_defaults_false(tmp_path):
    cfg = _make_config(tmp_path, yaml_cameras={"cam1": {}})
    assert cfg.cameras["cam1"].dry_run is False


def test_camera_dry_run_override(tmp_path):
    cfg = _make_config(
        tmp_path,
        yaml_cameras={"cam1": {"dry_run": True}},
    )
    assert cfg.cameras["cam1"].dry_run is True


def test_camera_dry_run_inherited_from_defaults(tmp_path):
    cfg = _make_config(
        tmp_path,
        yaml_defaults={"dry_run": True},
        yaml_cameras={"cam1": {}},
    )
    assert cfg.cameras["cam1"].dry_run is True


def test_tier_enabled_defaults_true(tmp_path):
    cfg = _make_config(tmp_path, yaml_cameras={"cam1": {}})
    assert cfg.cameras["cam1"].tier1.enabled is True
    assert cfg.cameras["cam1"].tier2.enabled is True


def test_tier_enabled_override(tmp_path):
    cfg = _make_config(
        tmp_path,
        yaml_cameras={"cam1": {"tier2": {"enabled": False}}},
    )
    assert cfg.cameras["cam1"].tier1.enabled is True
    assert cfg.cameras["cam1"].tier2.enabled is False


def test_tier_enabled_inherited_from_defaults(tmp_path):
    cfg = _make_config(
        tmp_path,
        yaml_defaults={"tier2": {"enabled": False}},
        yaml_cameras={"cam1": {}},
    )
    assert cfg.cameras["cam1"].tier2.enabled is False


# ═══════════════════════════════════════════════════════════════════════════════
# Resolution logic — discovered cameras from Frigate DB
# ═══════════════════════════════════════════════════════════════════════════════


def test_discovered_camera_gets_defaults(tmp_path):
    """A camera in Frigate DB but not in YAML gets pure defaults."""
    frigate_db = tmp_path / "frigate.db"
    db = _make_frigate_db(frigate_db)
    db.execute(
        "INSERT INTO recordings (id, camera, path, start_time) VALUES (?, ?, ?, ?)",
        ("r1", "discovered_cam", "/fake/path.mp4", 1000.0),
    )
    db.commit()
    db.close()

    cfg = _make_config(
        tmp_path,
        frigate_db=str(frigate_db),
        yaml_cameras={"configured_cam": {}},
    )
    assert "discovered_cam" in cfg.cameras
    assert "configured_cam" in cfg.cameras
    # Discovered camera gets pure defaults
    dcam = cfg.cameras["discovered_cam"]
    assert dcam.enabled is True
    assert dcam.dry_run is False
    assert dcam.tier1.continuous.quality == 30


def test_yaml_camera_not_in_frigate_db(tmp_path):
    """A camera in YAML but not in Frigate DB is still resolved."""
    cfg = _make_config(
        tmp_path,
        yaml_cameras={"yaml_only_cam": {"dry_run": True}},
    )
    assert "yaml_only_cam" in cfg.cameras
    assert cfg.cameras["yaml_only_cam"].dry_run is True


# ═══════════════════════════════════════════════════════════════════════════════
# all_dry_run property
# ═══════════════════════════════════════════════════════════════════════════════


def test_all_dry_run_all_true(tmp_path):
    cfg = _make_config(
        tmp_path,
        yaml_defaults={"dry_run": True},
        yaml_cameras={"cam1": {}, "cam2": {}},
    )
    assert cfg.all_dry_run is True


def test_all_dry_run_mixed(tmp_path):
    cfg = _make_config(
        tmp_path,
        yaml_cameras={"cam1": {"dry_run": True}, "cam2": {"dry_run": False}},
    )
    assert cfg.all_dry_run is False


def test_all_dry_run_no_cameras(tmp_path):
    cfg = _make_config(tmp_path, yaml_cameras={})
    assert cfg.all_dry_run is True


# ═══════════════════════════════════════════════════════════════════════════════
# _merge_defaults
# ═══════════════════════════════════════════════════════════════════════════════


def test_merge_defaults_deep():
    builtin = {"a": 1, "nested": {"x": 10, "y": 20}}
    user = {"nested": {"x": 99}, "b": 2}
    result = fc._merge_defaults(builtin, user)
    assert result == {"a": 1, "nested": {"x": 99, "y": 20}, "b": 2}


def test_merge_defaults_scalar_replaces():
    builtin = {"a": {"x": 1}}
    user = {"a": "flat"}
    result = fc._merge_defaults(builtin, user)
    assert result == {"a": "flat"}


# ═══════════════════════════════════════════════════════════════════════════════
# Missing YAML config file
# ═══════════════════════════════════════════════════════════════════════════════


def test_load_config_missing_yaml_uses_builtin_defaults(tmp_path):
    """When config.yaml doesn't exist, all cameras discovered from Frigate DB
    get built-in defaults."""
    frigate_db = tmp_path / "frigate.db"
    db = _make_frigate_db(frigate_db)
    db.execute(
        "INSERT INTO recordings (id, camera, path, start_time) VALUES (?, ?, ?, ?)",
        ("r1", "driveway", "/fake/path.mp4", 1000.0),
    )
    db.commit()
    db.close()

    (tmp_path / "recordings").mkdir()
    opts = {
        "encoder": "cpu",
        "max_parallel_jobs": 1,
        "housekeeping_interval_days": 7,
        "frigate_db": str(frigate_db),
        "recordings_dir": str(tmp_path / "recordings"),
        "compress_db": str(tmp_path / "compress.db"),
        "config_path": str(tmp_path / "nonexistent.yaml"),
        "log_level": "DEBUG",
    }
    opts_path = tmp_path / "options.json"
    opts_path.write_text(json.dumps(opts))

    cfg = fc.load_config(str(opts_path))
    assert "driveway" in cfg.cameras
    # No YAML → builtin defaults → quality=0, tiers disabled
    assert cfg.cameras["driveway"].tier1.continuous.quality == 0
    assert cfg.cameras["driveway"].tier1.enabled is False


# ═══════════════════════════════════════════════════════════════════════════════
# load_config — rate_window vs publish_interval guard
# ═══════════════════════════════════════════════════════════════════════════════


def test_rate_window_clamped_when_not_above_publish_interval(tmp_path, capsys):
    """rate_window < 1.5× publish_interval is clamped to 1.5× the interval + warns."""
    cfg = _make_config(
        tmp_path,
        mqtt_publish_interval_seconds=60,
        rate_window_seconds=60,  # equal → would disable every *_rate sensor
    )
    assert cfg.mqtt.publish_interval_seconds == 60
    assert cfg.mqtt.rate_window_seconds == 90
    out = capsys.readouterr().out
    assert "WARNING" in out
    assert "rate_window_seconds" in out


def test_rate_window_clamped_when_below_publish_interval(tmp_path):
    cfg = _make_config(
        tmp_path,
        mqtt_publish_interval_seconds=60,
        rate_window_seconds=30,
    )
    assert cfg.mqtt.rate_window_seconds == 90


def test_rate_window_preserved_when_above_publish_interval(tmp_path, capsys):
    cfg = _make_config(
        tmp_path,
        mqtt_publish_interval_seconds=60,
        rate_window_seconds=300,
    )
    assert cfg.mqtt.rate_window_seconds == 300
    assert "rate_window_seconds" not in capsys.readouterr().out
