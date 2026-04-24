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


# ---------------------------------------------------------------------------
# apply_process_nice
#
# These tests pin down the bounded re-scan loop and per-thread setpriority
# behavior.  Linux nice is per-task, so a regression that reverts to a
# single setpriority(PRIO_PROCESS, host_pid, ...) call would only update
# the main thread and leave worker threads at the old nice — exactly what
# happened before this code was rewritten.
# ---------------------------------------------------------------------------


def _process_tuning(nice: int | None = 5) -> srt.ProcessTuning:
    """Build a minimal ProcessTuning whose only configured field is nice."""
    return srt.ProcessTuning(
        container=None,
        process_match_regex="",
        nice=nice,
        cpuset_cpus=None,
    )


class _FakeProcStub:
    """Test stub: replaces list_process_threads + read_process_nice + os.setpriority.

    Tracks which TIDs setpriority was called with so tests can assert the
    correct iteration / re-scan behavior.
    """

    def __init__(
        self,
        scan_results: list[list[int]],
        initial_nice: int = 0,
        setpriority_raises: dict[int, Exception] | None = None,
    ):
        # scan_results[i] is what list_process_threads returns on the i-th
        # call.  After exhausting the list, the last entry is repeated.
        self._scans = list(scan_results)
        self._scan_idx = 0
        self._initial_nice = initial_nice
        self._setpriority_raises = setpriority_raises or {}
        # Per-tid current nice; reads return this, setpriority updates it
        # (so the fast-path "all match" check sees the right values).
        self._current: dict[int, int] = {}
        # Recording of every (tid, nice) call for assertions.
        self.setpriority_calls: list[tuple[int, int]] = []
        self.scan_count = 0
        self.read_calls: list[int] = []

    def list_process_threads(self, host_pid, label, log):
        self.scan_count += 1
        idx = min(self._scan_idx, len(self._scans) - 1)
        self._scan_idx += 1
        return list(self._scans[idx])

    def read_process_nice(self, tid, label, log):
        self.read_calls.append(tid)
        return self._current.get(tid, self._initial_nice)

    def setpriority(self, which, tid, nice):
        if tid in self._setpriority_raises:
            raise self._setpriority_raises[tid]
        self.setpriority_calls.append((tid, nice))
        self._current[tid] = nice


def _install_stub(monkeypatch, stub: _FakeProcStub) -> None:
    # Patch at the submodule where these names are actually resolved by
    # ``apply_process_nice``; patching ``srt.x`` alone wouldn't rebind the
    # ``x`` lookup inside ``system_resource_tuner.process``.
    monkeypatch.setattr(srt.process, "list_process_threads", stub.list_process_threads)
    monkeypatch.setattr(srt.process, "read_process_nice", stub.read_process_nice)
    monkeypatch.setattr(srt.process.os, "setpriority", stub.setpriority)


def test_apply_process_nice_raises_when_nice_is_none():
    with pytest.raises(ValueError, match="tuning.nice=None"):
        srt.apply_process_nice(_process_tuning(nice=None), 1234, False, _LOG)


def test_apply_process_nice_returns_when_no_threads(monkeypatch):
    """If list_process_threads returns None (process gone) we exit silently."""
    monkeypatch.setattr(srt.process, "list_process_threads", lambda *a, **kw: None)
    # Should not raise even though setpriority is unmocked — it must never
    # be called.
    monkeypatch.setattr(
        srt.process.os,
        "setpriority",
        lambda *a, **kw: pytest.fail("setpriority called"),
    )
    srt.apply_process_nice(_process_tuning(nice=5), 1234, False, _LOG)


def test_apply_process_nice_skip_when_all_threads_already_match(monkeypatch):
    """Fast path: every thread already has the desired nice, no syscalls."""
    stub = _FakeProcStub(scan_results=[[100, 101, 102]], initial_nice=5)
    _install_stub(monkeypatch, stub)

    srt.apply_process_nice(_process_tuning(nice=5), 1234, False, _LOG)

    assert stub.setpriority_calls == []  # no writes
    assert stub.scan_count == 1  # only the fast-path scan, no re-scans
    assert stub.read_calls == [100, 101, 102]


def test_apply_process_nice_updates_all_threads_in_one_scan(monkeypatch):
    """Steady-state process: one scan finds all threads, all get updated."""
    stub = _FakeProcStub(scan_results=[[100, 101, 102]], initial_nice=0)
    _install_stub(monkeypatch, stub)

    srt.apply_process_nice(_process_tuning(nice=5), 1234, False, _LOG)

    # Every TID got setpriority called exactly once with the new value.
    assert sorted(stub.setpriority_calls) == [(100, 5), (101, 5), (102, 5)]
    # Two scans total: 1 for the fast-path check + 1 inside the apply loop.
    # The apply loop's second iteration sees no new TIDs and breaks.
    assert stub.scan_count == 3  # fast-path + 2 loop iterations (one finds nothing new)


def test_apply_process_nice_dry_run_does_not_call_setpriority(monkeypatch):
    stub = _FakeProcStub(scan_results=[[100, 101]], initial_nice=0)
    _install_stub(monkeypatch, stub)

    srt.apply_process_nice(_process_tuning(nice=5), 1234, dry_run=True, log=_LOG)

    assert stub.setpriority_calls == []


def test_apply_process_nice_rescan_catches_thread_spawned_during_iteration(
    monkeypatch,
):
    """The race window we close: a new thread appears between scans.

    First scan returns [100, 101].  Second scan returns [100, 101, 102]
    — TID 102 was created after the fast-path scan saw the process.  The
    re-scan loop must pick up 102 and call setpriority on it.
    """
    stub = _FakeProcStub(
        scan_results=[
            [100, 101],  # fast-path scan: nice mismatch detected on 100
            [100, 101],  # first apply pass: update 100 and 101
            [100, 101, 102],  # second apply pass: 102 has appeared
            [100, 101, 102],  # third apply pass: nothing new, exit loop
        ],
        initial_nice=0,
    )
    _install_stub(monkeypatch, stub)

    srt.apply_process_nice(_process_tuning(nice=5), 1234, False, _LOG)

    # Every TID — including the late-arriving 102 — got nice=5.
    assert sorted(stub.setpriority_calls) == [(100, 5), (101, 5), (102, 5)]


def test_apply_process_nice_rescan_is_bounded(monkeypatch):
    """A pathological process spawning fresh threads forever cannot loop us
    forever — the loop is capped at _NICE_RESCAN_PASSES iterations."""
    # Each scan returns two NEW TIDs that have never been seen before.
    # Without the bound, this would loop forever.
    stub = _FakeProcStub(
        scan_results=[
            [100, 101],  # fast-path scan
            [100, 101],  # apply pass 1: process 100, 101
            [102, 103],  # apply pass 2: process 102, 103
            [104, 105],  # apply pass 3: process 104, 105
            [106, 107],  # would be apply pass 4 — must NOT be reached
        ],
        initial_nice=0,
    )
    _install_stub(monkeypatch, stub)

    srt.apply_process_nice(_process_tuning(nice=5), 1234, False, _LOG)

    # 3 apply passes × 2 new tids each = 6 setpriority calls.  No more.
    # If 106/107 were in the call list, the loop wasn't bounded.
    assert sorted(stub.setpriority_calls) == [
        (100, 5),
        (101, 5),
        (102, 5),
        (103, 5),
        (104, 5),
        (105, 5),
    ]


def test_apply_process_nice_skips_dead_threads(monkeypatch):
    """A thread dying mid-iteration (ProcessLookupError) is logged at DEBUG
    and skipped — it does not abort the rest of the iteration."""
    stub = _FakeProcStub(
        scan_results=[[100, 101, 102]],
        initial_nice=0,
        setpriority_raises={101: ProcessLookupError("died")},
    )
    _install_stub(monkeypatch, stub)

    srt.apply_process_nice(_process_tuning(nice=5), 1234, False, _LOG)

    # 100 and 102 should still have been updated; 101 was skipped.
    tids_updated = sorted(tid for tid, _ in stub.setpriority_calls)
    assert tids_updated == [100, 102]


def test_apply_process_nice_permission_error_aborts_with_log(monkeypatch, caplog):
    """A PermissionError is collected and reported as an error — the
    function returns without claiming success."""
    stub = _FakeProcStub(
        scan_results=[[100, 101]],
        initial_nice=0,
        setpriority_raises={
            100: PermissionError("EPERM"),
            101: PermissionError("EPERM"),
        },
    )
    _install_stub(monkeypatch, stub)

    with caplog.at_level(logging.ERROR):
        srt.apply_process_nice(_process_tuning(nice=5), 1234, False, _LOG)

    assert any("Failed applying nice=5" in r.message for r in caplog.records)


def test_apply_process_nice_does_not_redo_already_seen_tids(monkeypatch):
    """The seen-set prevents re-calling setpriority on the same TID across
    rescans, even when the process keeps the same threads."""
    stub = _FakeProcStub(
        scan_results=[
            [100, 101],
            [100, 101],
            [100, 101],
            [100, 101],
        ],
        initial_nice=0,
    )
    _install_stub(monkeypatch, stub)

    srt.apply_process_nice(_process_tuning(nice=5), 1234, False, _LOG)

    # Each TID should have setpriority called exactly once, not on every
    # re-scan iteration.
    assert sorted(stub.setpriority_calls) == [(100, 5), (101, 5)]
