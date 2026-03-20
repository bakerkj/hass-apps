# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for pure/utility functions in system_resource_tuner.system_resource_tuner."""

from __future__ import annotations

import logging
from typing import Any

import pytest

import system_resource_tuner as srt

_LOG = logging.getLogger("test")


# ---------------------------------------------------------------------------
# parse_bool
# ---------------------------------------------------------------------------


def test_parse_bool_none_returns_default():
    assert srt.parse_bool(None, default=True) is True
    assert srt.parse_bool(None, default=False) is False


def test_parse_bool_true_bool():
    assert srt.parse_bool(True) is True


def test_parse_bool_false_bool():
    assert srt.parse_bool(False) is False


def test_parse_bool_int_nonzero():
    assert srt.parse_bool(1) is True


def test_parse_bool_int_zero():
    assert srt.parse_bool(0) is False


def test_parse_bool_string_true_variants():
    for s in ("1", "true", "yes", "on", "True", "YES", "ON"):
        assert srt.parse_bool(s) is True, f"failed for {s!r}"


def test_parse_bool_string_false_variants():
    for s in ("0", "false", "no", "off", "False", "NO", "OFF"):
        assert srt.parse_bool(s) is False, f"failed for {s!r}"


def test_parse_bool_unknown_string_returns_default():
    assert srt.parse_bool("maybe", default=True) is True
    assert srt.parse_bool("maybe", default=False) is False


def test_parse_bool_float():
    assert srt.parse_bool(1.5) is True
    assert srt.parse_bool(0.0) is False


# ---------------------------------------------------------------------------
# parse_cpuset_expression
# ---------------------------------------------------------------------------


def test_parse_cpuset_empty_string():
    assert srt.parse_cpuset_expression("") == set()


def test_parse_cpuset_single_cpu():
    assert srt.parse_cpuset_expression("0") == {0}


def test_parse_cpuset_list():
    assert srt.parse_cpuset_expression("0,1,2") == {0, 1, 2}


def test_parse_cpuset_range():
    assert srt.parse_cpuset_expression("0-3") == {0, 1, 2, 3}


def test_parse_cpuset_mixed():
    assert srt.parse_cpuset_expression("0,2-4,7") == {0, 2, 3, 4, 7}


def test_parse_cpuset_whitespace_stripped():
    assert srt.parse_cpuset_expression("  0-2  ") == {0, 1, 2}


def test_parse_cpuset_invalid_non_digit_token():
    assert srt.parse_cpuset_expression("abc") is None


def test_parse_cpuset_invalid_reversed_range():
    assert srt.parse_cpuset_expression("3-1") is None


def test_parse_cpuset_invalid_empty_token_in_list():
    # "0,,1" → empty token after split → None
    assert srt.parse_cpuset_expression("0,,1") is None


def test_parse_cpuset_single_cpu_range():
    # "2-2" is a valid range of one element
    assert srt.parse_cpuset_expression("2-2") == {2}


# ---------------------------------------------------------------------------
# cpuset_matches
# ---------------------------------------------------------------------------


def test_cpuset_matches_identical_strings():
    assert srt.cpuset_matches("0-3", "0-3") is True


def test_cpuset_matches_equivalent_expressions():
    # "0,1,2,3" and "0-3" expand to the same set
    assert srt.cpuset_matches("0,1,2,3", "0-3") is True


def test_cpuset_matches_different_sets():
    assert srt.cpuset_matches("0,1", "0-3") is False


def test_cpuset_matches_with_whitespace():
    assert srt.cpuset_matches("  0-2  ", "0,1,2") is True


def test_cpuset_matches_invalid_expressions_fall_back_to_string():
    # If parsing fails for either, falls back to string comparison
    assert srt.cpuset_matches("all", "all") is True
    assert srt.cpuset_matches("all", "0-3") is False


# ---------------------------------------------------------------------------
# parse_targets
# ---------------------------------------------------------------------------


def test_parse_targets_none_returns_empty():
    assert srt.parse_targets(None, _LOG) == []


def test_parse_targets_empty_list():
    assert srt.parse_targets([], _LOG) == []


def test_parse_targets_valid_entry():
    raw = [{"container": "myapp", "cpuset_cpus": "0-3"}]
    targets = srt.parse_targets(raw, _LOG)
    assert len(targets) == 1
    assert targets[0].container == "myapp"
    assert targets[0].cpuset_cpus == "0-3"


def test_parse_targets_cpu_shares():
    raw = [{"container": "myapp", "cpu_shares": 512}]
    targets = srt.parse_targets(raw, _LOG)
    assert targets[0].cpu_shares == 512


def test_parse_targets_blkio_weight():
    raw = [{"container": "myapp", "blkio_weight": 100}]
    targets = srt.parse_targets(raw, _LOG)
    assert targets[0].blkio_weight == 100


def test_parse_targets_missing_container_raises():
    with pytest.raises(ValueError, match="container is required"):
        srt.parse_targets([{"cpuset_cpus": "0"}], _LOG)


def test_parse_targets_no_tuning_values_skipped():
    # Entry with no cpuset/cpu_shares/blkio_weight should be silently skipped
    raw = [{"container": "myapp"}]
    targets = srt.parse_targets(raw, _LOG)
    assert targets == []


def test_parse_targets_invalid_cpuset_raises():
    with pytest.raises(ValueError, match="cpuset_cpus is invalid"):
        srt.parse_targets([{"container": "myapp", "cpuset_cpus": "abc-xyz"}], _LOG)


def test_parse_targets_not_a_list_raises():
    with pytest.raises(ValueError):
        srt.parse_targets({"container": "myapp"}, _LOG)


def test_parse_targets_item_not_dict_raises():
    with pytest.raises(ValueError):
        srt.parse_targets(["not-a-dict"], _LOG)


# ---------------------------------------------------------------------------
# parse_process_tuning
# ---------------------------------------------------------------------------


def test_parse_process_tuning_none_returns_default():
    pt = srt.parse_process_tuning(None, "test_block")
    assert pt.nice is None
    assert pt.cpuset_cpus is None
    assert pt.process_match_regex == ""


def test_parse_process_tuning_valid():
    raw = {
        "container": "myapp",
        "process_match_regex": "ffmpeg",
        "nice": 10,
    }
    pt = srt.parse_process_tuning(raw, "test_block")
    assert pt.container == "myapp"
    assert pt.nice == 10
    assert pt.process_match_regex == "ffmpeg"


def test_parse_process_tuning_nice_out_of_range_high():
    raw = {
        "container": "myapp",
        "process_match_regex": "ffmpeg",
        "nice": 20,
    }
    with pytest.raises(ValueError, match="nice must be between"):
        srt.parse_process_tuning(raw, "test_block")


def test_parse_process_tuning_nice_out_of_range_low():
    raw = {
        "container": "myapp",
        "process_match_regex": "ffmpeg",
        "nice": -21,
    }
    with pytest.raises(ValueError, match="nice must be between"):
        srt.parse_process_tuning(raw, "test_block")


def test_parse_process_tuning_nice_boundary_values_ok():
    for nice in (-20, 19):
        raw = {
            "container": "c",
            "process_match_regex": "x",
            "nice": nice,
        }
        pt = srt.parse_process_tuning(raw, "blk")
        assert pt.nice == nice


def test_parse_process_tuning_invalid_regex_raises():
    raw = {
        "container": "myapp",
        "process_match_regex": "(unclosed",
        "nice": 5,
    }
    with pytest.raises(ValueError, match="process_match_regex is invalid"):
        srt.parse_process_tuning(raw, "blk")


def test_parse_process_tuning_configured_without_pattern_raises():
    raw = {"container": "myapp", "nice": 5}
    with pytest.raises(ValueError, match="process_match_regex is required"):
        srt.parse_process_tuning(raw, "blk")


def test_parse_process_tuning_configured_without_container_raises():
    raw = {"process_match_regex": "ffmpeg", "nice": 5}
    with pytest.raises(ValueError, match="container is required"):
        srt.parse_process_tuning(raw, "blk", require_container=True)


def test_parse_process_tuning_not_dict_raises():
    with pytest.raises(ValueError, match="must be an object"):
        srt.parse_process_tuning("string", "blk")


def test_parse_process_tuning_invalid_cpuset_raises():
    raw = {
        "container": "myapp",
        "process_match_regex": "ffmpeg",
        "cpuset_cpus": "bad-cpuset!",
    }
    with pytest.raises(ValueError, match="cpuset_cpus must be a valid"):
        srt.parse_process_tuning(raw, "blk")


# ---------------------------------------------------------------------------
# ProcessTuning dataclass properties
# ---------------------------------------------------------------------------


def test_process_tuning_is_host_true_when_container_none():
    pt = srt.ProcessTuning(container=None, process_match_regex="x", nice=5)
    assert pt.is_host is True


def test_process_tuning_is_host_false_when_container_set():
    pt = srt.ProcessTuning(container="myapp", process_match_regex="x", nice=5)
    assert pt.is_host is False


def test_process_tuning_container_label_host():
    pt = srt.ProcessTuning(container=None, process_match_regex="x")
    assert pt.container_label == "host"


def test_process_tuning_container_label_named():
    pt = srt.ProcessTuning(container="myapp", process_match_regex="x")
    assert pt.container_label == "myapp"


def test_process_tuning_is_configured_true():
    pt = srt.ProcessTuning(container="c", process_match_regex="x", nice=5)
    assert pt.is_configured is True


def test_process_tuning_is_configured_false():
    pt = srt.ProcessTuning(container="c", process_match_regex="x")
    assert pt.is_configured is False


def test_process_tuning_is_configured_via_cpuset():
    pt = srt.ProcessTuning(container="c", process_match_regex="x", cpuset_cpus="0-1")
    assert pt.is_configured is True


# ---------------------------------------------------------------------------
# desired_update_args
# ---------------------------------------------------------------------------


def _target(**kwargs) -> srt.Target:
    defaults: dict[str, Any] = {
        "container": "myapp",
        "cpuset_cpus": None,
        "cpu_shares": None,
        "blkio_weight": None,
    }
    defaults.update(kwargs)
    return srt.Target(**defaults)


def test_desired_update_args_no_changes_needed():
    target = _target(cpuset_cpus="0-3", cpu_shares=512, blkio_weight=100)
    current = {"cpuset_cpus": "0-3", "cpu_shares": 512, "blkio_weight": 100}
    assert srt.desired_update_args(target, current) == []


def test_desired_update_args_cpuset_differs():
    target = _target(cpuset_cpus="0-1")
    current = {"cpuset_cpus": "0-3", "cpu_shares": 0, "blkio_weight": 0}
    args = srt.desired_update_args(target, current)
    assert "--cpuset-cpus" in args
    assert "0-1" in args


def test_desired_update_args_cpu_shares_differs():
    target = _target(cpu_shares=1024)
    current = {"cpuset_cpus": "", "cpu_shares": 512, "blkio_weight": 0}
    args = srt.desired_update_args(target, current)
    assert "--cpu-shares" in args
    assert "1024" in args


def test_desired_update_args_blkio_differs():
    target = _target(blkio_weight=200)
    current = {"cpuset_cpus": "", "cpu_shares": 0, "blkio_weight": 100}
    args = srt.desired_update_args(target, current)
    assert "--blkio-weight" in args
    assert "200" in args


def test_desired_update_args_cpuset_equivalent_no_update():
    # "0,1,2,3" and "0-3" are equivalent sets — no update needed
    target = _target(cpuset_cpus="0-3")
    current = {"cpuset_cpus": "0,1,2,3", "cpu_shares": 0, "blkio_weight": 0}
    args = srt.desired_update_args(target, current)
    assert "--cpuset-cpus" not in args


def test_desired_update_args_only_non_none_fields_checked():
    # If target field is None, that field is never added to args
    target = _target(cpuset_cpus=None, cpu_shares=None, blkio_weight=None)
    current = {"cpuset_cpus": "0", "cpu_shares": 1024, "blkio_weight": 50}
    assert srt.desired_update_args(target, current) == []


# ---------------------------------------------------------------------------
# parse_process_targets / parse_host_process_targets
# ---------------------------------------------------------------------------


def test_parse_process_targets_none():
    assert srt.parse_process_targets(None, _LOG) == []


def test_parse_process_targets_valid():
    raw = [
        {
            "container": "encoder",
            "process_match_regex": "ffmpeg",
            "nice": 5,
        }
    ]
    result = srt.parse_process_targets(raw, _LOG)
    assert len(result) == 1
    assert result[0].nice == 5


def test_parse_process_targets_not_list_raises():
    with pytest.raises(ValueError):
        srt.parse_process_targets({"key": "val"}, _LOG)


def test_parse_host_process_targets_none():
    assert srt.parse_host_process_targets(None, _LOG) == []


def test_parse_host_process_targets_no_container_required():
    raw = [
        {
            "process_match_regex": "kworker",
            "nice": 19,
        }
    ]
    result = srt.parse_host_process_targets(raw, _LOG)
    assert len(result) == 1
    assert result[0].is_host is True


def test_parse_host_process_targets_unconfigured_entry_skipped():
    raw = [{"process_match_regex": "foo"}]  # no nice or cpuset_cpus
    result = srt.parse_host_process_targets(raw, _LOG)
    assert result == []
