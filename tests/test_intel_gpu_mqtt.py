# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for pure/utility functions in intel_gpu_mqtt.intel_gpu_mqtt."""

from __future__ import annotations

import json
import logging

import pytest

import intel_gpu_mqtt as igm

_LOG = logging.getLogger("test")


# ---------------------------------------------------------------------------
# safe_float
# ---------------------------------------------------------------------------


def test_safe_float_int():
    assert igm.safe_float(42) == 42.0


def test_safe_float_float():
    assert igm.safe_float(3.14) == pytest.approx(3.14)


def test_safe_float_string_rejected():
    # intel_gpu_mqtt.safe_float only accepts int/float, not str
    assert igm.safe_float("3.14") is None


def test_safe_float_none():
    assert igm.safe_float(None) is None


def test_safe_float_bool_treated_as_int():
    # bool is a subclass of int in Python
    assert igm.safe_float(True) == 1.0
    assert igm.safe_float(False) == 0.0


def test_safe_float_zero():
    assert igm.safe_float(0) == 0.0


# ---------------------------------------------------------------------------
# dig
# ---------------------------------------------------------------------------


def test_dig_simple_path():
    d = {"a": {"b": {"c": 99}}}
    assert igm.dig(d, ["a", "b", "c"]) == 99


def test_dig_missing_key():
    d = {"a": {"b": 1}}
    assert igm.dig(d, ["a", "x"]) is None


def test_dig_empty_path():
    d = {"a": 1}
    assert igm.dig(d, []) == d


def test_dig_non_dict_intermediate():
    d = {"a": 42}
    assert igm.dig(d, ["a", "b"]) is None


def test_dig_none_value_at_leaf():
    d = {"a": {"b": None}}
    assert igm.dig(d, ["a", "b"]) is None


# ---------------------------------------------------------------------------
# find_engine_field
# ---------------------------------------------------------------------------


def test_find_engine_field_found():
    raw = {
        "engines": {
            "Render/3D": {"busy": 55.5, "sema": 0.0},
        }
    }
    assert igm.find_engine_field(raw, "Render/3D", "busy") == pytest.approx(55.5)


def test_find_engine_field_case_insensitive():
    raw = {
        "engines": {
            "VIDEO": {"busy": 10.0},
        }
    }
    assert igm.find_engine_field(raw, "video", "busy") == pytest.approx(10.0)


def test_find_engine_field_missing_engine():
    raw = {"engines": {"Blitter": {"busy": 1.0}}}
    assert igm.find_engine_field(raw, "Video", "busy") is None


def test_find_engine_field_missing_field():
    raw = {"engines": {"Video": {"busy": 5.0}}}
    assert igm.find_engine_field(raw, "Video", "wait") is None


def test_find_engine_field_no_engines_key():
    assert igm.find_engine_field({}, "Render/3D", "busy") is None


def test_find_engine_field_engines_not_dict():
    raw = {"engines": [{"busy": 1.0}]}
    assert igm.find_engine_field(raw, "Render/3D", "busy") is None


def test_find_engine_field_field_value_not_numeric():
    raw = {"engines": {"Video": {"busy": "fast"}}}
    # safe_float("fast") is None inside find_engine_field — but wait:
    # intel_gpu_mqtt.safe_float only accepts int/float.  A string returns None.
    assert igm.find_engine_field(raw, "Video", "busy") is None


# ---------------------------------------------------------------------------
# extract_latest_json_object
# ---------------------------------------------------------------------------


def test_extract_single_object():
    buf = '{"rc6": {"value": 80}, "frequency": {"actual": 1200}}'
    obj, remaining = igm.extract_latest_json_object(buf)
    assert obj is not None
    assert obj["rc6"]["value"] == 80
    # remaining should be a (possibly empty) string, not the full buffer
    assert isinstance(remaining, str)


def test_extract_returns_last_of_multiple_objects():
    first = json.dumps({"seq": 1})
    second = json.dumps({"seq": 2})
    third = json.dumps({"seq": 3})
    buf = f"[{first},{second},{third}"  # unclosed array as real tool emits
    obj, _ = igm.extract_latest_json_object(buf)
    assert obj is not None
    assert obj["seq"] == 3


def test_extract_empty_buffer():
    obj, remaining = igm.extract_latest_json_object("")
    assert obj is None
    assert isinstance(remaining, str)


def test_extract_incomplete_json():
    buf = '{"rc6": {"value":'
    obj, _ = igm.extract_latest_json_object(buf)
    assert obj is None


def test_extract_array_wrapper_stripped():
    # intel_gpu_top -J wraps output in "[", then streams objects
    inner = json.dumps({"frequency": {"actual": 900}})
    buf = f"[{inner}"
    obj, _ = igm.extract_latest_json_object(buf)
    assert obj is not None
    assert obj["frequency"]["actual"] == 900


def test_extract_remaining_is_bounded():
    # Remaining buffer should be capped at 200 000 chars
    big = json.dumps({"x": "y"}) + ("z" * 300_000)
    _, remaining = igm.extract_latest_json_object(big)
    assert len(remaining) <= 200_000


# ---------------------------------------------------------------------------
# build_metrics
# ---------------------------------------------------------------------------


def _sample_raw() -> dict:
    return {
        "rc6": {"value": 75.0},
        "frequency": {"actual": 1000.0, "requested": 1200.0},
        "power": {"GPU": 5.5, "Package": 12.0},
        "interrupts": {"count": 3000.0},
        "engines": {
            "Render/3D": {"busy": 50.0, "sema": 1.0, "wait": 0.5},
            "Video": {"busy": 20.0, "sema": 0.0, "wait": 0.0},
            "VideoEnhance": {"busy": 0.0, "sema": 0.0, "wait": 0.0},
            "Blitter": {"busy": 5.0, "sema": 0.0, "wait": 0.0},
        },
    }


def test_build_metrics_returns_expected_keys():
    metrics = igm.build_metrics(_sample_raw())
    expected_keys = {
        "rc6_percent",
        "freq_mhz",
        "freq_requested_mhz",
        "interrupts_per_s",
        "power_gpu_w",
        "power_pkg_w",
        "engine_render_3d_busy_percent",
        "engine_render_3d_semaphore_percent",
        "engine_render_3d_wait_percent",
        "engine_video_busy_percent",
        "engine_video_semaphore_percent",
        "engine_video_wait_percent",
        "engine_videoenhance_busy_percent",
        "engine_videoenhance_semaphore_percent",
        "engine_videoenhance_wait_percent",
        "engine_blitter_busy_percent",
        "engine_blitter_semaphore_percent",
        "engine_blitter_wait_percent",
    }
    assert set(metrics.keys()) == expected_keys


def test_build_metrics_rc6_value():
    metrics = igm.build_metrics(_sample_raw())
    assert metrics["rc6_percent"]["value"] == pytest.approx(75.0)


def test_build_metrics_power_fallback_lowercase():
    raw = {
        "power": {"gpu": 3.3, "pkg": 8.0},
        "frequency": {},
        "interrupts": {},
        "engines": {},
    }
    metrics = igm.build_metrics(raw)
    assert metrics["power_gpu_w"]["value"] == pytest.approx(3.3)
    assert metrics["power_pkg_w"]["value"] == pytest.approx(8.0)


def test_build_metrics_power_fallback_package():
    raw = {
        "power": {"GPU": None, "package": 9.9},
        "frequency": {},
        "interrupts": {},
        "engines": {},
    }
    metrics = igm.build_metrics(raw)
    assert metrics["power_pkg_w"]["value"] == pytest.approx(9.9)


def test_build_metrics_missing_engine_returns_none():
    raw = {"engines": {}, "power": {}, "frequency": {}, "interrupts": {}}
    metrics = igm.build_metrics(raw)
    assert metrics["engine_render_3d_busy_percent"]["value"] is None


def test_build_metrics_each_entry_has_required_fields():
    metrics = igm.build_metrics(_sample_raw())
    for key, m in metrics.items():
        assert "key" in m, f"{key} missing 'key'"
        assert "name" in m, f"{key} missing 'name'"
        assert "value" in m, f"{key} missing 'value'"
        assert "unit" in m, f"{key} missing 'unit'"
        assert "attrs" in m, f"{key} missing 'attrs'"


def test_build_metrics_common_attrs_propagated():
    raw = _sample_raw()
    raw["device"] = "i915"
    raw["driver"] = "xe"
    metrics = igm.build_metrics(raw)
    attrs = metrics["rc6_percent"]["attrs"]
    assert attrs.get("device") == "i915"
    assert attrs.get("driver") == "xe"


# ---------------------------------------------------------------------------
# auto_select_device_arg
# ---------------------------------------------------------------------------


def test_auto_select_no_candidates():
    device_arg, path = igm.auto_select_device_arg("", "", _LOG)
    assert device_arg is None
    assert path is None


def test_auto_select_first_candidate():
    listing = "card=renderD128 /dev/dri/renderD128 i915\n"
    device_arg, path = igm.auto_select_device_arg(listing, "", _LOG)
    assert path == "/dev/dri/renderD128"
    assert device_arg == "drm:/dev/dri/renderD128"


def test_auto_select_regex_preferred():
    listing = (
        "card=renderD128 /dev/dri/renderD128 i915\n"
        "card=renderD129 /dev/dri/renderD129 xe\n"
    )
    device_arg, path = igm.auto_select_device_arg(listing, "xe", _LOG)
    assert path == "/dev/dri/renderD129"


def test_auto_select_regex_no_match_falls_back_to_first():
    listing = "card=renderD128 /dev/dri/renderD128 i915\n"
    device_arg, path = igm.auto_select_device_arg(listing, "amdgpu", _LOG)
    assert path == "/dev/dri/renderD128"


def test_auto_select_invalid_regex_falls_back():
    listing = "card=renderD128 /dev/dri/renderD128 i915\n"
    # Invalid regex should not raise; falls back to first candidate.
    device_arg, path = igm.auto_select_device_arg(listing, "(unclosed", _LOG)
    assert path == "/dev/dri/renderD128"


def test_auto_select_listing_no_render_nodes():
    listing = "no render nodes here at all\n"
    device_arg, path = igm.auto_select_device_arg(listing, "", _LOG)
    assert device_arg is None
    assert path is None
