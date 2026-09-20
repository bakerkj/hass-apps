# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Unit tests for the pure health-check helpers."""

import asyncio
import datetime
import shlex
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiodocker.exceptions import DockerError
from container_hooks.health import (
    _parse_docker_ts,
    check_applied,
    check_sentinel,
    container_started_at,
    last_put_archive_ts,
    render_binary_state,
)

from container_hooks import health

# --- _parse_docker_ts ------------------------------------------------------


class TestParseDockerTs:
    def test_iso_with_nanoseconds_and_z(self):
        # Docker emits nanosecond precision + trailing Z; Python only parses
        # microseconds, so anything past 6 digits has to be truncated first.
        ts = _parse_docker_ts("2026-09-19T22:47:38.140882144Z")
        assert ts is not None
        expected = datetime.datetime(
            2026, 9, 19, 22, 47, 38, 140882, tzinfo=datetime.UTC
        ).timestamp()
        assert ts == pytest.approx(expected)

    def test_zero_time_returns_none(self):
        # Docker uses the zero-time for containers that have never started;
        # treating it as a real epoch would misclassify "never ran" as "ran
        # in 0001 AD" and every check would false-positive.
        assert _parse_docker_ts("0001-01-01T00:00:00Z") is None

    def test_empty_returns_none(self):
        assert _parse_docker_ts("") is None
        assert _parse_docker_ts("   ") is None

    def test_unparseable_returns_none(self):
        assert _parse_docker_ts("not-a-date") is None


# --- last_put_archive_ts ---------------------------------------------------


class TestLastPutArchiveTs:
    def test_missing_file_returns_none(self, tmp_path: Path):
        assert last_put_archive_ts(tmp_path / "does-not-exist") is None

    def test_empty_file_returns_none(self, tmp_path: Path):
        p = tmp_path / "pre-start.log"
        p.write_text("")
        assert last_put_archive_ts(p) is None

    def test_finds_latest_entry(self, tmp_path: Path):
        p = tmp_path / "pre-start.log"
        lines = [
            "[2026-09-01T10:00:00.000-04:00] put_archive ok: 1 files, 100 bytes",
            "[2026-09-15T11:00:00.000-04:00] some other line",
            "[2026-09-19T18:48:23.038-04:00] put_archive ok: 2 files, 3901 bytes",
        ]
        p.write_text("\n".join(lines) + "\n")
        ts = last_put_archive_ts(p)
        assert ts is not None
        expected = datetime.datetime(
            2026,
            9,
            19,
            18,
            48,
            23,
            38000,
            tzinfo=datetime.timezone(datetime.timedelta(hours=-4)),
        ).timestamp()
        assert ts == pytest.approx(expected)

    def test_large_log_seeks_and_drops_partial_leading_line(self, tmp_path: Path):
        # Write a >1 MiB log with a bunch of chatty pre-start-script noise
        # followed by exactly one put_archive line at the tail. The seek
        # window lands inside the noise, so the first line in the buffer
        # is a partial ("straddling") one that must be discarded before
        # regex matching.
        p = tmp_path / "pre-start.log"
        # 2 MiB of noise, then the put_archive line.
        noise_line = "[2026-09-01T00:00:00.000-04:00] some random noise line\n"
        with p.open("w") as f:
            while p.stat().st_size < 2 * 1024 * 1024:
                f.write(noise_line)
            f.write(
                "[2026-09-19T18:48:23.038-04:00] put_archive ok: 2 files, 3901 bytes\n"
            )
        ts = last_put_archive_ts(p)
        assert ts is not None
        # Confirm we got the newest entry, not a spurious hit.
        expected = datetime.datetime(
            2026,
            9,
            19,
            18,
            48,
            23,
            38000,
            tzinfo=datetime.timezone(datetime.timedelta(hours=-4)),
        ).timestamp()
        assert ts == pytest.approx(expected)

    def test_ignores_non_matching_lines(self, tmp_path: Path):
        p = tmp_path / "pre-start.log"
        p.write_text("[2026-09-19T18:48:23.038-04:00] running pre-start hook for foo\n")
        # Line exists but doesn't match "put_archive ok:" — treat as none.
        assert last_put_archive_ts(p) is None


# --- check_applied ---------------------------------------------------------


def _mock_docker_show(payload: dict) -> MagicMock:
    """MagicMock docker client whose containers.get().show() returns ``payload``."""
    container = MagicMock()
    container.show = AsyncMock(return_value=payload)
    docker = MagicMock()
    docker.containers = MagicMock()
    docker.containers.get = AsyncMock(return_value=container)
    return docker


def _mock_docker_started_at(iso: str | None) -> MagicMock:
    return _mock_docker_show({"State": {"StartedAt": iso}})


def _mock_docker_created_at(iso: str | None) -> MagicMock:
    return _mock_docker_show({"Created": iso, "State": {}})


class TestCheckApplied:
    @pytest.mark.asyncio
    async def test_missing_container_returns_none(self, tmp_path: Path):
        docker = MagicMock()
        docker.containers = MagicMock()
        docker.containers.get = AsyncMock(
            side_effect=DockerError(404, {"message": "no such container"})
        )
        result = await check_applied(docker, "gone", tmp_path / "pre-start.log")
        assert result is None

    @pytest.mark.asyncio
    async def test_never_ran_returns_off(self, tmp_path: Path):
        # Created is real, log doesn't exist -> off with reason.
        docker = _mock_docker_created_at("2026-09-19T22:47:38.14Z")
        p = tmp_path / "pre-start.log"
        result = await check_applied(docker, "x", p)
        assert result is not None
        assert result.value is False
        assert "no successful put_archive" in result.reason

    @pytest.mark.asyncio
    async def test_stale_log_returns_off(self, tmp_path: Path):
        # Log entry from a week before the container was Created — a stale
        # entry from a prior lifecycle. Since ``check_applied`` now anchors
        # on Created (not StartedAt), a container that got recreated has
        # a fresh Created and any prior-lifecycle log entry is >> slack in
        # the past.
        docker = _mock_docker_created_at("2026-09-19T22:47:38.14Z")
        p = tmp_path / "pre-start.log"
        p.write_text(
            "[2026-09-12T10:00:00.000-04:00] put_archive ok: 2 files, 100 bytes\n"
        )
        result = await check_applied(docker, "x", p)
        assert result is not None
        assert result.value is False
        assert "before container create" in result.reason

    @pytest.mark.asyncio
    async def test_fresh_log_returns_on(self, tmp_path: Path):
        # put_archive fires on the create event; the log entry timestamp
        # is essentially co-located with Created (usually within tens of ms).
        docker = _mock_docker_created_at("2026-09-19T22:47:38.14Z")
        p = tmp_path / "pre-start.log"
        p.write_text(
            "[2026-09-19T22:47:39.500-00:00] put_archive ok: 2 files, 3901 bytes\n"
        )
        result = await check_applied(docker, "x", p)
        assert result is not None
        assert result.value is True

    @pytest.mark.asyncio
    async def test_delayed_start_still_on(self, tmp_path: Path):
        # Third-party flow: docker create; sleep N; docker start. Log entry
        # is at Created; StartedAt is minutes later. Because we anchor on
        # Created, no widened slack is needed to keep applied=True here.
        docker = _mock_docker_created_at("2026-09-19T22:47:38.14Z")
        p = tmp_path / "pre-start.log"
        p.write_text(
            "[2026-09-19T22:47:38.20-00:00] put_archive ok: 2 files, 3901 bytes\n"
        )
        result = await check_applied(docker, "x", p)
        assert result is not None
        assert result.value is True

    @pytest.mark.asyncio
    async def test_missed_recreate_within_5min_returns_off(self, tmp_path: Path):
        # Instance-1 succeeded at T. Instance-2 was recreated at T+60s
        # (well within the OLD 300s widened slack) but the create event
        # was missed. Since Created for instance-2 is fresh, the T-stamped
        # log entry (from instance-1) is > slack in the past relative to
        # instance-2's Created → correctly OFF.
        docker = _mock_docker_created_at("2026-09-19T22:48:38.14Z")  # T+60s
        p = tmp_path / "pre-start.log"
        p.write_text(
            "[2026-09-19T22:47:38.20-00:00] put_archive ok: 2 files, 3901 bytes\n"
        )
        result = await check_applied(docker, "x", p)
        assert result is not None
        assert result.value is False


# --- check_sentinel --------------------------------------------------------


def _mock_docker_exec(
    docker: MagicMock,
    *,
    fstype: str,
    sentinel_rc: int,
    mtime: str,
    sentinel_body: bytes = b"",
    pid1_stat: bytes = b"",
) -> None:
    """Attach an .exec that returns fstype for findmnt, mtime for stat,
    and (optionally) content bytes for ``cat <sentinel>`` and
    ``cat /proc/1/stat`` — needed for the content-identity check."""
    call_log: list[list[str]] = []

    async def exec_impl(cmd: list[str], *, stdout: bool, stderr: bool):
        call_log.append(cmd)

        class _Stream:
            _emitted = False

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return None

            async def read_out(self):
                if self._emitted:
                    return None
                self._emitted = True
                if cmd[:2] == ["findmnt", "-T"]:
                    payload = fstype.encode()
                elif cmd[0] == "test":
                    payload = b""
                elif cmd[0] == "sh" and "/proc/1/stat" in cmd[2]:
                    payload = pid1_stat
                elif cmd[0] == "sh" and "head -c" in cmd[2]:
                    payload = sentinel_body
                elif cmd[0] == "sh":
                    payload = mtime.encode()
                elif cmd[:2] == ["cat", "/proc/1/stat"]:
                    payload = pid1_stat
                elif cmd[0] == "cat":
                    payload = sentinel_body
                else:
                    payload = b""
                return MagicMock(data=payload)

        class _Exec:
            def start(self, *, detach=False):
                return _Stream()

            async def inspect(self):
                if cmd[:2] == ["findmnt", "-T"]:
                    return {"ExitCode": 0 if fstype else 1}
                if cmd[0] == "test":
                    return {"ExitCode": sentinel_rc}
                return {"ExitCode": 0}

        return _Exec()

    container = MagicMock()
    container.show = AsyncMock(
        return_value={"State": {"StartedAt": "2026-09-19T22:47:38.14Z"}}
    )
    container.exec = AsyncMock(side_effect=exec_impl)
    docker.containers = MagicMock()
    docker.containers.get = AsyncMock(return_value=container)


def _pid1_stat_line(ticks: int) -> bytes:
    """A synthetic ``/proc/1/stat`` line where field 22 is ``ticks``.
    Real format: ``<pid> (<comm>) <state> <ppid> ...``; field 22 is
    ``starttime``. Comm can hold spaces/parens, so we test both."""
    fields = ["1", "(complex init)"] + ["X"] * 19 + [str(ticks)] + ["0"] * 30
    return (" ".join(fields) + "\n").encode()


class TestCheckSentinel:
    @pytest.mark.asyncio
    async def test_tmpfs_present_returns_on(self):
        docker = MagicMock()
        _mock_docker_exec(docker, fstype="tmpfs", sentinel_rc=0, mtime="")
        result = await check_sentinel(docker, "x", "/dev/shm/marker")
        assert result is not None
        assert result.value is True
        assert "tmpfs" in result.reason

    @pytest.mark.asyncio
    async def test_tmpfs_missing_returns_off(self):
        docker = MagicMock()
        _mock_docker_exec(docker, fstype="tmpfs", sentinel_rc=1, mtime="")
        result = await check_sentinel(docker, "x", "/dev/shm/marker")
        assert result is not None
        assert result.value is False

    @pytest.mark.asyncio
    async def test_overlay_fresh_mtime_returns_on(self):
        docker = MagicMock()
        # StartedAt = 2026-09-19T22:47:38.14Z ≈ 1789821400 epoch, but exact
        # value depends on the parser. Pass an mtime 60s AFTER StartedAt
        # to guarantee freshness regardless of local time.
        started = _parse_docker_ts("2026-09-19T22:47:38.14Z")
        assert started is not None
        _mock_docker_exec(
            docker, fstype="overlay", sentinel_rc=0, mtime=str(int(started + 60))
        )
        result = await check_sentinel(docker, "x", "/tmp/marker")
        assert result is not None
        assert result.value is True

    @pytest.mark.asyncio
    async def test_overlay_stale_mtime_returns_off(self):
        docker = MagicMock()
        started = _parse_docker_ts("2026-09-19T22:47:38.14Z")
        assert started is not None
        _mock_docker_exec(
            docker, fstype="overlay", sentinel_rc=0, mtime=str(int(started - 3600))
        )
        result = await check_sentinel(docker, "x", "/tmp/marker")
        assert result is not None
        assert result.value is False

    @pytest.mark.asyncio
    async def test_presence_mode_ignores_body(self, monkeypatch: pytest.MonkeyPatch):
        # Presence mode never opens the file — a garbage body on a
        # present tmpfs sentinel is still ON.
        docker = MagicMock()
        _mock_docker_exec(
            docker,
            fstype="tmpfs",
            sentinel_rc=0,
            mtime="",
            sentinel_body=b"total garbage",
        )
        result = await check_sentinel(docker, "x", "/dev/shm/marker", mode="presence")
        assert result is not None
        assert result.value is True


class TestCheckSentinelContentMode:
    @pytest.mark.asyncio
    async def test_content_identity_match_returns_on(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setattr(health, "_host_boot_id", lambda: "boot-abc")
        docker = MagicMock()
        _mock_docker_exec(
            docker,
            fstype="tmpfs",
            sentinel_rc=0,
            mtime="",
            sentinel_body=(
                b'{"boot_id": "boot-abc", "pid1_start_ticks_since_boot": 42}'
            ),
            pid1_stat=_pid1_stat_line(42),
        )
        result = await check_sentinel(docker, "x", "/dev/shm/marker", mode="content")
        assert result is not None, "expected a HealthResult, got None"
        assert result.value is True
        assert "identity verified" in result.reason

    @pytest.mark.asyncio
    async def test_content_boot_id_mismatch_returns_off(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setattr(health, "_host_boot_id", lambda: "boot-live")
        docker = MagicMock()
        _mock_docker_exec(
            docker,
            fstype="tmpfs",
            sentinel_rc=0,
            mtime="",
            sentinel_body=(
                b'{"boot_id": "boot-stale", "pid1_start_ticks_since_boot": 42}'
            ),
            pid1_stat=_pid1_stat_line(42),
        )
        result = await check_sentinel(docker, "x", "/dev/shm/marker", mode="content")
        assert result is not None
        assert result.value is False
        assert "prior host boot" in result.reason

    @pytest.mark.asyncio
    async def test_content_pid1_mismatch_returns_off(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setattr(health, "_host_boot_id", lambda: "boot-abc")
        docker = MagicMock()
        _mock_docker_exec(
            docker,
            fstype="tmpfs",
            sentinel_rc=0,
            mtime="",
            sentinel_body=(
                b'{"boot_id": "boot-abc", "pid1_start_ticks_since_boot": 42}'
            ),
            pid1_stat=_pid1_stat_line(999),
        )
        result = await check_sentinel(docker, "x", "/dev/shm/marker", mode="content")
        assert result is not None
        assert result.value is False
        assert "prior container lifecycle" in result.reason

    @pytest.mark.asyncio
    async def test_content_non_json_body_returns_off(self):
        # In content mode a non-JSON body is a payload contract violation,
        # so OFF — not the presence-mode fallthrough.
        docker = MagicMock()
        _mock_docker_exec(
            docker,
            fstype="tmpfs",
            sentinel_rc=0,
            mtime="",
            sentinel_body=b"garbage not json",
        )
        result = await check_sentinel(docker, "x", "/dev/shm/marker", mode="content")
        assert result is not None
        assert result.value is False
        assert "not JSON" in result.reason

    @pytest.mark.asyncio
    async def test_content_missing_pid1_field_returns_off(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setattr(health, "_host_boot_id", lambda: "boot-abc")
        docker = MagicMock()
        _mock_docker_exec(
            docker,
            fstype="tmpfs",
            sentinel_rc=0,
            mtime="",
            sentinel_body=b'{"boot_id": "boot-abc"}',
            pid1_stat=_pid1_stat_line(42),
        )
        result = await check_sentinel(docker, "x", "/dev/shm/marker", mode="content")
        assert result is not None
        assert result.value is False
        assert "pid1_start_ticks_since_boot" in result.reason

    @pytest.mark.asyncio
    async def test_content_missing_file_returns_off(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        # In content mode ``head -c`` on a missing file fails; the single
        # read is what tells us the file is gone (no separate presence
        # pre-check exists — that removed the two-call race window).
        monkeypatch.setattr(health, "_host_boot_id", lambda: "boot-abc")
        docker = MagicMock()
        _mock_docker_exec(
            docker,
            fstype="tmpfs",
            sentinel_rc=1,
            mtime="",
            sentinel_body=b"",  # empty because head -c on missing file returns no output
        )
        result = await check_sentinel(docker, "x", "/dev/shm/marker", mode="content")
        assert result is not None
        assert result.value is False
        assert "empty" in result.reason or "missing" in result.reason

    @pytest.mark.asyncio
    async def test_content_empty_body_returns_off_with_specific_reason(self):
        # File exists but is empty — distinguish from unreadable so the
        # sensor's reason attribute points at the actual failure mode.
        docker = MagicMock()
        _mock_docker_exec(
            docker, fstype="tmpfs", sentinel_rc=0, mtime="", sentinel_body=b""
        )
        result = await check_sentinel(docker, "x", "/dev/shm/marker", mode="content")
        assert result is not None
        assert result.value is False
        assert "empty" in result.reason

    @pytest.mark.asyncio
    async def test_content_shell_metachars_in_path_are_quoted(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        # Regression guard: the ``head -c`` shell string uses
        # ``shlex.quote`` on the path. A path containing ``$``/spaces/``;``
        # must land as a single argument to ``head``.
        monkeypatch.setattr(health, "_host_boot_id", lambda: "boot-abc")
        seen_cmds: list[list[str]] = []

        docker = MagicMock()
        _mock_docker_exec(
            docker,
            fstype="tmpfs",
            sentinel_rc=0,
            mtime="",
            sentinel_body=(
                b'{"boot_id": "boot-abc", "pid1_start_ticks_since_boot": 42}'
            ),
            pid1_stat=_pid1_stat_line(42),
        )
        orig_exec = docker.containers.get.return_value.exec

        async def spy(cmd, **kwargs):
            seen_cmds.append(list(cmd))
            return await orig_exec(cmd, **kwargs)

        docker.containers.get.return_value.exec = AsyncMock(side_effect=spy)

        nasty = "/dev/shm/ok; rm -rf /$(id)"
        result = await check_sentinel(docker, "x", nasty, mode="content")
        assert result is not None
        assert result.value is True
        # The head -c command targeting the sentinel (not /proc/1/stat)
        # must contain the nasty path shlex-quoted so the shell sees
        # exactly one argument, not injected commands.
        sentinel_reads = [
            c
            for c in seen_cmds
            if c[0] == "sh" and "head -c" in c[2] and "/proc/1/stat" not in c[2]
        ]
        assert sentinel_reads, "expected a head -c on the sentinel path"
        for c in sentinel_reads:
            assert shlex.quote(nasty) in c[2], (
                f"path not quoted in shell string: {c[2]!r}"
            )

    @pytest.mark.asyncio
    async def test_unknown_mode_falls_back_to_presence(self):
        # Fail-closed: a mode value that isn't ``content`` (typo, future
        # value, stale caller) never triggers the identity read.
        docker = MagicMock()
        _mock_docker_exec(
            docker,
            fstype="tmpfs",
            sentinel_rc=0,
            mtime="",
            sentinel_body=b'{"boot_id": "wrong", "pid1_start_ticks_since_boot": 1}',
        )
        result = await check_sentinel(
            docker, "x", "/dev/shm/marker", mode="not-a-real-mode"
        )
        assert result is not None
        # Presence-only: file exists → ON, content mismatch is not consulted.
        assert result.value is True


# --- rendering -------------------------------------------------------------


class TestRendering:
    def test_binary_state(self):
        from container_hooks.health import HealthResult

        assert render_binary_state(None) == "unknown"
        assert render_binary_state(HealthResult(True, "ok")) == "ON"
        assert render_binary_state(HealthResult(False, "nope")) == "OFF"


# --- container_started_at + sentinel_is_tmpfs edge cases -------------------


class TestStartedAt:
    @pytest.mark.asyncio
    async def test_missing_container_returns_none(self):
        docker = MagicMock()
        docker.containers = MagicMock()
        docker.containers.get = AsyncMock(
            side_effect=DockerError(404, {"message": "no such container"})
        )
        assert await container_started_at(docker, "gone") is None

    @pytest.mark.asyncio
    async def test_never_started_returns_none(self):
        docker = _mock_docker_started_at("0001-01-01T00:00:00Z")
        assert await container_started_at(docker, "x") is None

    @pytest.mark.asyncio
    async def test_hung_containers_get_times_out(self, monkeypatch: pytest.MonkeyPatch):
        # containers.get() itself round-trips to the daemon; wait_for must
        # cover it, not just c.show().
        monkeypatch.setattr(health, "_DOCKER_CHECK_TIMEOUT", 0.05)

        async def _hang(_name: str):
            # Keep this short so a regression fails in ~0.5s, not 10s.
            await asyncio.sleep(0.5)
            raise AssertionError("should have been cancelled")

        docker = MagicMock()
        docker.containers = MagicMock()
        docker.containers.get = _hang
        assert await container_started_at(docker, "x") is None
