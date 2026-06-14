# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for pure/utility functions in intel_gpu_mqtt.intel_gpu_mqtt."""

from __future__ import annotations

import json
import logging
import subprocess
from unittest.mock import MagicMock, patch

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
    raw: dict[str, dict[str, object]] = {
        "engines": {},
        "power": {},
        "frequency": {},
        "interrupts": {},
    }
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


# ---------------------------------------------------------------------------
# MqttHealth
# ---------------------------------------------------------------------------


def test_mqtt_health_initial_state():
    h = igm.MqttHealth()
    assert h.connected is False
    assert h.last_connect_ok == 0.0
    assert h.last_disconnect == 0.0


# ---------------------------------------------------------------------------
# list_intel_gpu_top_devices
# ---------------------------------------------------------------------------


def test_list_devices_success():
    listing = "card=renderD128 /dev/dri/renderD128 i915\n"
    with patch(
        "intel_gpu_mqtt.subprocess.check_output", return_value=listing
    ) as mock_co:
        result = igm.list_intel_gpu_top_devices(_LOG)
    assert result == listing
    mock_co.assert_called_once_with(
        ["intel_gpu_top", "-L"], text=True, stderr=subprocess.STDOUT, timeout=5
    )


def test_list_devices_failure_returns_empty():
    with patch(
        "intel_gpu_mqtt.subprocess.check_output",
        side_effect=subprocess.CalledProcessError(1, "intel_gpu_top"),
    ):
        result = igm.list_intel_gpu_top_devices(_LOG)
    assert result == ""


def test_list_devices_timeout_returns_empty():
    with patch(
        "intel_gpu_mqtt.subprocess.check_output",
        side_effect=subprocess.TimeoutExpired("intel_gpu_top", 5),
    ):
        result = igm.list_intel_gpu_top_devices(_LOG)
    assert result == ""


def test_list_devices_file_not_found_returns_empty():
    with patch(
        "intel_gpu_mqtt.subprocess.check_output",
        side_effect=FileNotFoundError("intel_gpu_top"),
    ):
        result = igm.list_intel_gpu_top_devices(_LOG)
    assert result == ""


# ---------------------------------------------------------------------------
# publish_discovery
# ---------------------------------------------------------------------------


def test_publish_discovery_publishes_correct_topics():
    client = MagicMock()
    client.publish.return_value = MagicMock(mid=1, rc=0)

    metrics = {
        "rc6_percent": {
            "key": "rc6_percent",
            "name": "Intel GPU RC6",
            "value": 75.0,
            "unit": "%",
            "attrs": {},
        },
        "power_gpu_w": {
            "key": "power_gpu_w",
            "name": "Intel GPU Power",
            "value": 5.5,
            "unit": "W",
            "attrs": {},
        },
    }

    igm.publish_discovery(
        client=client,
        discovery_prefix="homeassistant",
        base_topic="intel_gpu_top",
        device_id="intel_gpu_top",
        device_name="Intel GPU Top",
        metrics=metrics,
        expire_after_s=60,
        log=_LOG,
    )

    assert client.publish.call_count == 2

    # Collect published topics and payloads
    calls = {
        call.args[0]: json.loads(call.args[1]) for call in client.publish.call_args_list
    }

    rc6_topic = "homeassistant/sensor/intel_gpu_top/rc6_percent/config"
    assert rc6_topic in calls
    rc6_payload = calls[rc6_topic]
    assert rc6_payload["name"] == "Intel GPU RC6"
    assert rc6_payload["unique_id"] == "intel_gpu_top_rc6_percent"
    assert rc6_payload["state_topic"] == "intel_gpu_top/rc6_percent/state"
    assert rc6_payload["unit_of_measurement"] == "%"
    assert rc6_payload["expire_after"] == 60
    assert rc6_payload["availability_topic"] == "intel_gpu_top/availability"
    assert rc6_payload["suggested_display_precision"] == 1
    assert rc6_payload["icon"] == "mdi:sleep"
    assert "device" in rc6_payload
    assert rc6_payload["device"]["identifiers"] == ["intel_gpu_top"]

    power_topic = "homeassistant/sensor/intel_gpu_top/power_gpu_w/config"
    assert power_topic in calls
    power_payload = calls[power_topic]
    assert power_payload["device_class"] == "power"
    assert power_payload["icon"] == "mdi:flash-outline"
    assert power_payload["suggested_display_precision"] == 1


def test_publish_discovery_engine_icon():
    client = MagicMock()
    client.publish.return_value = MagicMock(mid=1, rc=0)

    metrics = {
        "engine_video_busy_percent": {
            "key": "engine_video_busy_percent",
            "name": "Intel GPU Engine Video Busy",
            "value": 20.0,
            "unit": "%",
            "attrs": {"engine": "Video", "field": "busy"},
        },
    }

    igm.publish_discovery(
        client=client,
        discovery_prefix="homeassistant",
        base_topic="intel_gpu_top",
        device_id="intel_gpu_top",
        device_name="Intel GPU Top",
        metrics=metrics,
        expire_after_s=60,
        log=_LOG,
    )

    payload = json.loads(client.publish.call_args_list[0].args[1])
    assert payload["icon"] == "mdi:video-outline"


def test_publish_discovery_freq_icon():
    client = MagicMock()
    client.publish.return_value = MagicMock(mid=1, rc=0)

    metrics = {
        "freq_mhz": {
            "key": "freq_mhz",
            "name": "Intel GPU Frequency Actual",
            "value": 1000.0,
            "unit": "MHz",
            "attrs": {},
        },
    }

    igm.publish_discovery(
        client=client,
        discovery_prefix="homeassistant",
        base_topic="intel_gpu_top",
        device_id="intel_gpu_top",
        device_name="Intel GPU Top",
        metrics=metrics,
        expire_after_s=60,
        log=_LOG,
    )

    payload = json.loads(client.publish.call_args_list[0].args[1])
    assert payload["icon"] == "mdi:speedometer"
    assert payload["suggested_display_precision"] == 0


# ---------------------------------------------------------------------------
# start_intel_gpu_top
# ---------------------------------------------------------------------------


def test_start_intel_gpu_top_without_dev_arg():
    mock_proc = MagicMock()
    mock_proc.pid = 12345
    with patch("intel_gpu_mqtt.subprocess.Popen", return_value=mock_proc) as mock_popen:
        result = igm.start_intel_gpu_top(1000, None, _LOG)

    assert result is mock_proc
    mock_popen.assert_called_once_with(
        ["intel_gpu_top", "-J", "-s", "1000", "-o", "-"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )


def test_start_intel_gpu_top_with_dev_arg():
    mock_proc = MagicMock()
    mock_proc.pid = 12345
    with patch("intel_gpu_mqtt.subprocess.Popen", return_value=mock_proc) as mock_popen:
        result = igm.start_intel_gpu_top(500, "drm:/dev/dri/renderD128", _LOG)

    assert result is mock_proc
    mock_popen.assert_called_once_with(
        [
            "intel_gpu_top",
            "-J",
            "-s",
            "500",
            "-o",
            "-",
            "-d",
            "drm:/dev/dri/renderD128",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )


def test_start_intel_gpu_top_file_not_found():
    with patch("intel_gpu_mqtt.subprocess.Popen", side_effect=FileNotFoundError):
        with pytest.raises(FileNotFoundError):
            igm.start_intel_gpu_top(1000, None, _LOG)


# ---------------------------------------------------------------------------
# build_metrics — additional edge cases
# ---------------------------------------------------------------------------


def test_build_metrics_rc6_direct_float():
    """rc6 as a bare float (not nested {"value": ...}) exercises the or-fallback."""
    raw = {
        "rc6": 88.5,
        "frequency": {},
        "interrupts": {},
        "power": {},
        "engines": {},
    }
    metrics = igm.build_metrics(raw)
    assert metrics["rc6_percent"]["value"] == pytest.approx(88.5)


def test_build_metrics_no_power_keys():
    """When power dict is present but has no recognized keys, values are None."""
    raw = {
        "rc6": {"value": 10.0},
        "frequency": {"actual": 500.0},
        "interrupts": {"count": 100.0},
        "power": {"unknown_key": 7.7},
        "engines": {},
    }
    metrics = igm.build_metrics(raw)
    assert metrics["power_gpu_w"]["value"] is None
    assert metrics["power_pkg_w"]["value"] is None


def test_build_metrics_missing_top_level_keys():
    """Completely empty raw dict — nothing should raise."""
    metrics = igm.build_metrics({})
    assert metrics["rc6_percent"]["value"] is None
    assert metrics["freq_mhz"]["value"] is None
    assert metrics["power_gpu_w"]["value"] is None
    assert metrics["power_pkg_w"]["value"] is None
    assert metrics["interrupts_per_s"]["value"] is None


def test_build_metrics_common_attrs_all_keys():
    """pci_id, card, gt are propagated into common attrs."""
    raw = {
        "pci_id": "0x1234",
        "device": "i915",
        "driver": "xe",
        "card": "card0",
        "gt": 0,
        "rc6": {"value": 1.0},
        "frequency": {},
        "interrupts": {},
        "power": {},
        "engines": {},
    }
    metrics = igm.build_metrics(raw)
    attrs = metrics["rc6_percent"]["attrs"]
    assert attrs["pci_id"] == "0x1234"
    assert attrs["card"] == "card0"
    assert attrs["gt"] == 0


def test_build_metrics_power_gpu_fallback_only():
    """GPU key is None, gpu (lowercase) is present."""
    raw = {
        "power": {"GPU": None, "gpu": 2.2},
        "frequency": {},
        "interrupts": {},
        "engines": {},
    }
    metrics = igm.build_metrics(raw)
    assert metrics["power_gpu_w"]["value"] == pytest.approx(2.2)


def test_build_metrics_power_pkg_fallback_chain():
    """Package is None, pkg is None, package (lowercase) is present."""
    raw = {
        "power": {"Package": None, "pkg": None, "package": 6.6},
        "frequency": {},
        "interrupts": {},
        "engines": {},
    }
    metrics = igm.build_metrics(raw)
    assert metrics["power_pkg_w"]["value"] == pytest.approx(6.6)


def test_build_metrics_power_pkg_from_pkg():
    """Package is None, pkg has value — stops at pkg without falling to package."""
    raw = {
        "power": {"Package": None, "pkg": 4.4, "package": 9.9},
        "frequency": {},
        "interrupts": {},
        "engines": {},
    }
    metrics = igm.build_metrics(raw)
    assert metrics["power_pkg_w"]["value"] == pytest.approx(4.4)


# ---------------------------------------------------------------------------
# publish_discovery — additional edge cases
# ---------------------------------------------------------------------------


def test_publish_discovery_interrupts_no_special_icon():
    """Interrupts metric should have no device_class and no special icon."""
    client = MagicMock()
    client.publish.return_value = MagicMock(mid=1, rc=0)

    metrics = {
        "interrupts_per_s": {
            "key": "interrupts_per_s",
            "name": "Intel GPU Interrupts",
            "value": 3000.0,
            "unit": "irq/s",
            "attrs": {},
        },
    }

    igm.publish_discovery(
        client=client,
        discovery_prefix="homeassistant",
        base_topic="intel_gpu_top",
        device_id="intel_gpu_top",
        device_name="Intel GPU Top",
        metrics=metrics,
        expire_after_s=60,
        log=_LOG,
    )

    payload = json.loads(client.publish.call_args_list[0].args[1])
    # No device_class for irq/s
    assert "device_class" not in payload
    # No icon set for interrupts (no matching branch)
    assert "icon" not in payload
    # No suggested_display_precision for irq/s
    assert "suggested_display_precision" not in payload


def test_publish_discovery_videoenhance_icon():
    """VideoEnhance engine gets mdi:video-plus-outline icon."""
    client = MagicMock()
    client.publish.return_value = MagicMock(mid=1, rc=0)

    metrics = {
        "engine_videoenhance_busy_percent": {
            "key": "engine_videoenhance_busy_percent",
            "name": "Intel GPU Engine VideoEnhance Busy",
            "value": 10.0,
            "unit": "%",
            "attrs": {"engine": "VideoEnhance", "field": "busy"},
        },
    }

    igm.publish_discovery(
        client=client,
        discovery_prefix="homeassistant",
        base_topic="intel_gpu_top",
        device_id="intel_gpu_top",
        device_name="Intel GPU Top",
        metrics=metrics,
        expire_after_s=60,
        log=_LOG,
    )

    payload = json.loads(client.publish.call_args_list[0].args[1])
    assert payload["icon"] == "mdi:video-plus-outline"


def test_publish_discovery_blitter_icon():
    """Blitter engine gets mdi:image-move icon."""
    client = MagicMock()
    client.publish.return_value = MagicMock(mid=1, rc=0)

    metrics = {
        "engine_blitter_busy_percent": {
            "key": "engine_blitter_busy_percent",
            "name": "Intel GPU Engine Blitter Busy",
            "value": 5.0,
            "unit": "%",
            "attrs": {"engine": "Blitter", "field": "busy"},
        },
    }

    igm.publish_discovery(
        client=client,
        discovery_prefix="homeassistant",
        base_topic="intel_gpu_top",
        device_id="intel_gpu_top",
        device_name="Intel GPU Top",
        metrics=metrics,
        expire_after_s=60,
        log=_LOG,
    )

    payload = json.loads(client.publish.call_args_list[0].args[1])
    assert payload["icon"] == "mdi:image-move"


def test_publish_discovery_render3d_icon():
    """Render/3D engine gets mdi:cube-outline icon."""
    client = MagicMock()
    client.publish.return_value = MagicMock(mid=1, rc=0)

    metrics = {
        "engine_render_3d_busy_percent": {
            "key": "engine_render_3d_busy_percent",
            "name": "Intel GPU Engine Render/3D Busy",
            "value": 50.0,
            "unit": "%",
            "attrs": {"engine": "Render/3D", "field": "busy"},
        },
    }

    igm.publish_discovery(
        client=client,
        discovery_prefix="homeassistant",
        base_topic="intel_gpu_top",
        device_id="intel_gpu_top",
        device_name="Intel GPU Top",
        metrics=metrics,
        expire_after_s=60,
        log=_LOG,
    )

    payload = json.loads(client.publish.call_args_list[0].args[1])
    assert payload["icon"] == "mdi:cube-outline"


def test_publish_discovery_freq_requested():
    """freq_requested_mhz gets speedometer icon and precision 0."""
    client = MagicMock()
    client.publish.return_value = MagicMock(mid=1, rc=0)

    metrics = {
        "freq_requested_mhz": {
            "key": "freq_requested_mhz",
            "name": "Intel GPU Frequency Requested",
            "value": 1200.0,
            "unit": "MHz",
            "attrs": {},
        },
    }

    igm.publish_discovery(
        client=client,
        discovery_prefix="homeassistant",
        base_topic="intel_gpu_top",
        device_id="intel_gpu_top",
        device_name="Intel GPU Top",
        metrics=metrics,
        expire_after_s=60,
        log=_LOG,
    )

    payload = json.loads(client.publish.call_args_list[0].args[1])
    assert payload["icon"] == "mdi:speedometer"
    assert payload["suggested_display_precision"] == 0


# ---------------------------------------------------------------------------
# auto_select_device_arg — additional edge cases
# ---------------------------------------------------------------------------


def test_auto_select_multiple_candidates_no_regex():
    """With multiple candidates and empty regex, first is selected."""
    listing = (
        "card=renderD128 /dev/dri/renderD128 i915\n"
        "card=renderD129 /dev/dri/renderD129 xe\n"
    )
    device_arg, path = igm.auto_select_device_arg(listing, "", _LOG)
    assert path == "/dev/dri/renderD128"
    assert device_arg == "drm:/dev/dri/renderD128"


def test_auto_select_regex_matches_path():
    """Regex can match against the path itself, not just the full line."""
    listing = "card=renderD128 /dev/dri/renderD128 i915\n"
    device_arg, path = igm.auto_select_device_arg(listing, "renderD128", _LOG)
    assert path == "/dev/dri/renderD128"
