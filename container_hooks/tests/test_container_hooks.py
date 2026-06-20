# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for container_hooks.

Covers the pure-Python helpers (config parsing, hook-env construction,
script resolution, dispatch logic) plus the async dispatchers. The
aiodocker client is monkeypatched throughout so no real docker daemon
is involved.
"""

import asyncio
import io
import json
import logging
import tarfile
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

import container_hooks as rocs
from container_hooks.app import (
    _LAST_RUN_PRUNE_AGE_MULTIPLIER,
    _dispatch,
    _dispatch_pre_start,
    _hook_env,
    _max_debounce,
    _prune_last_run,
    _resolve_debounce,
    _resolve_pre_start_files_subdir,
    _resolve_pre_start_patches,
    _resolve_pre_start_scripts,
    _resolve_scripts,
    _with_self_skip,
)
from container_hooks.config import (
    ContainerOverride,
    Options,
    load_options,
    post_start_log,
    pre_start_log,
)
from container_hooks.docker import (
    _build_dir_tree_tar,
    _build_single_file_tar,
    _categorize_patch_items,
    _strip_diff_prefix,
    apply_patch,
    docker_ps_running,
    docker_url,
    run_hook,
    run_pre_start_hook,
    self_container_name,
)
from aiodocker.exceptions import DockerError
import patch_ng

_LOG = logging.getLogger("test")


# ---------------------------------------------------------------------------
# config.load_options
# ---------------------------------------------------------------------------


def _write_options(tmp_path: Path, **overrides: Any) -> Path:
    """Write an options.json with the given overrides and return its path."""
    path = tmp_path / "options.json"
    path.write_text(json.dumps(overrides))
    return path


def test_load_options_defaults_when_empty(tmp_path: Path) -> None:
    path = _write_options(tmp_path)
    o = load_options(str(path))
    assert o.log_level == "INFO"
    assert o.base_dir == Path("/homeassistant/container_hooks")
    assert o.initial_sweep is True
    assert o.debounce_seconds == 2
    assert o.skip_containers == ()
    assert o.watch_create_events is False


def test_load_options_custom_base_dir(tmp_path: Path) -> None:
    path = _write_options(tmp_path, base_dir="/elsewhere")
    assert load_options(str(path)).base_dir == Path("/elsewhere")


def test_load_options_parses_container_overrides(tmp_path: Path) -> None:
    path = _write_options(
        tmp_path,
        container_overrides=[
            {"container": "addon_a", "debounce_seconds": 10},
            {"container": " addon_b ", "debounce_seconds": 0},
            {"container": "addon_c"},  # no override fields → debounce stays None
            {"container": "", "debounce_seconds": 5},  # skipped, empty name
        ],
    )
    o = load_options(str(path))
    assert o.container_overrides == (
        ContainerOverride(container="addon_a", debounce_seconds=10),
        ContainerOverride(container="addon_b", debounce_seconds=0),
        ContainerOverride(container="addon_c", debounce_seconds=None),
    )


def test_load_options_watch_create_events(tmp_path: Path) -> None:
    path = _write_options(tmp_path, watch_create_events=True)
    assert load_options(str(path)).watch_create_events is True


def test_load_options_uppercases_log_level(tmp_path: Path) -> None:
    path = _write_options(tmp_path, log_level="debug")
    assert load_options(str(path)).log_level == "DEBUG"


def test_load_options_strips_and_dedupes_skip_containers(tmp_path: Path) -> None:
    path = _write_options(tmp_path, skip_containers=[" foo ", "", "bar"])
    o = load_options(str(path))
    assert o.skip_containers == ("foo", "bar")


def test_load_options_clamps_negative_debounce(tmp_path: Path) -> None:
    path = _write_options(tmp_path, debounce_seconds=-5)
    assert load_options(str(path)).debounce_seconds == 0


# ---------------------------------------------------------------------------
# config path helpers
# ---------------------------------------------------------------------------


def _opts(tmp_path: Path, **kw: Any) -> Options:
    return Options(base_dir=tmp_path / "container_hooks", **kw)


def test_post_start_log_path(tmp_path: Path) -> None:
    o = _opts(tmp_path)
    expected = tmp_path / "container_hooks" / "addon_x" / "logs" / "post-start.log"
    assert post_start_log(o, "addon_x") == expected


def test_pre_start_log_path(tmp_path: Path) -> None:
    o = _opts(tmp_path)
    expected = tmp_path / "container_hooks" / "addon_x" / "logs" / "pre-start.log"
    assert pre_start_log(o, "addon_x") == expected


# ---------------------------------------------------------------------------
# script / patch resolution
# ---------------------------------------------------------------------------


def _make_dir(opts: Options, container: str, subdir: str) -> Path:
    d = opts.base_dir / container / subdir
    d.mkdir(parents=True, exist_ok=True)
    return d


# (resolver, subdir, suffix, ignored_other) — parameter set covering all three
# resolvers that wrap ``_lex_sorted_files`` (scripts/, pre-start/, pre-start-patches/).
_RESOLVER_CASES = [
    pytest.param(_resolve_scripts, "scripts", ".sh", "readme.md", id="scripts"),
    pytest.param(
        _resolve_pre_start_scripts, "pre-start", ".sh", "readme.md", id="pre-start"
    ),
    pytest.param(
        _resolve_pre_start_patches,
        "pre-start-patches",
        ".patch",
        "notes.md",
        id="pre-start-patches",
    ),
]


@pytest.mark.parametrize("resolver, subdir, suffix, ignored", _RESOLVER_CASES)
def test_resolver_empty_when_dir_missing(
    tmp_path: Path, resolver, subdir, suffix, ignored
) -> None:
    assert resolver(_opts(tmp_path), "addon_x") == []


@pytest.mark.parametrize("resolver, subdir, suffix, ignored", _RESOLVER_CASES)
def test_resolver_returns_lex_sorted_matching_suffix_only(
    tmp_path: Path, resolver, subdir, suffix, ignored
) -> None:
    """Lex order, suffix-filter, non-files ignored — for every directory."""
    opts = _opts(tmp_path)
    d = _make_dir(opts, "addon_x", subdir)
    for n in (f"99-c{suffix}", f"00-a{suffix}", f"10-b{suffix}"):
        (d / n).write_text("payload")
    (d / ignored).write_text("noise")
    (d / "subdir").mkdir()
    got = resolver(opts, "addon_x")
    assert [p.name for p in got] == [f"00-a{suffix}", f"10-b{suffix}", f"99-c{suffix}"]


def test_resolve_scripts_scoped_per_container(tmp_path: Path) -> None:
    """A different container's directory is never picked up — one smoke test is enough."""
    opts = _opts(tmp_path)
    d = _make_dir(opts, "addon_x", "scripts")
    (d / "00-x.sh").write_text("#!/bin/sh\n")
    assert _resolve_scripts(opts, "addon_other") == []


def test_resolve_pre_start_files_subdir_none_when_missing(tmp_path: Path) -> None:
    assert _resolve_pre_start_files_subdir(_opts(tmp_path), "addon_x") is None


def test_resolve_pre_start_files_subdir_none_when_empty(tmp_path: Path) -> None:
    opts = _opts(tmp_path)
    _make_dir(opts, "addon_x", "pre-start-files")
    assert _resolve_pre_start_files_subdir(opts, "addon_x") is None


def test_resolve_pre_start_files_subdir_returns_when_populated(tmp_path: Path) -> None:
    opts = _opts(tmp_path)
    d = _make_dir(opts, "addon_x", "pre-start-files")
    (d / "marker").write_text("data")
    assert _resolve_pre_start_files_subdir(opts, "addon_x") == d


# ---------------------------------------------------------------------------
# _hook_env
# ---------------------------------------------------------------------------


def test_hook_env_baseline_keys_always_present() -> None:
    env = _hook_env("addon_x", "initial_sweep")
    assert env == {"ROCS_REASON": "initial_sweep", "ROCS_CONTAINER": "addon_x"}


def test_hook_env_adds_event_fields_when_event_provided() -> None:
    event = {
        "Actor": {
            "ID": "abc123",
            "Attributes": {"name": "addon_x", "image": "example/img:1.2"},
        },
        "time": 1700000000,
    }
    env = _hook_env("addon_x", "event_start", event=event)
    assert env["ROCS_REASON"] == "event_start"
    assert env["ROCS_CONTAINER"] == "addon_x"
    assert env["ROCS_CONTAINER_ID"] == "abc123"
    assert env["ROCS_IMAGE"] == "example/img:1.2"
    assert env["ROCS_TIMESTAMP"] == "1700000000"


def test_hook_env_tolerates_missing_event_attributes() -> None:
    env = _hook_env("addon_x", "event_start", event={})
    assert env["ROCS_REASON"] == "event_start"
    assert env["ROCS_CONTAINER"] == "addon_x"
    assert env["ROCS_CONTAINER_ID"] == ""
    assert env["ROCS_IMAGE"] == ""
    assert "ROCS_TIMESTAMP" not in env


# ---------------------------------------------------------------------------
# _resolve_debounce
# ---------------------------------------------------------------------------


def test_resolve_debounce_falls_back_to_global(tmp_path: Path) -> None:
    opts = _opts(tmp_path, debounce_seconds=7)
    assert _resolve_debounce(opts, "addon_x") == 7


def test_resolve_debounce_uses_per_container_override(tmp_path: Path) -> None:
    opts = _opts(
        tmp_path,
        debounce_seconds=7,
        container_overrides=(
            ContainerOverride(container="addon_x", debounce_seconds=0),
        ),
    )
    assert _resolve_debounce(opts, "addon_x") == 0
    assert _resolve_debounce(opts, "addon_other") == 7


def test_resolve_debounce_override_with_no_field_falls_through(
    tmp_path: Path,
) -> None:
    opts = _opts(
        tmp_path,
        debounce_seconds=4,
        container_overrides=(ContainerOverride(container="addon_x"),),
    )
    assert _resolve_debounce(opts, "addon_x") == 4


# ---------------------------------------------------------------------------
# _dispatch (post-start, async)
# ---------------------------------------------------------------------------


async def test_dispatch_no_script_is_no_op(monkeypatch, tmp_path: Path) -> None:
    fake = AsyncMock()
    monkeypatch.setattr("container_hooks.app.run_hook", fake)
    await _dispatch(MagicMock(), "addon_x", _opts(tmp_path), _LOG, reason="event_start")
    fake.assert_not_awaited()


async def test_dispatch_runs_all_scripts_in_lex_order(
    monkeypatch, tmp_path: Path
) -> None:
    opts = _opts(tmp_path)
    d = _make_dir(opts, "addon_x", "scripts")
    for n in ("00-first.sh", "10-second.sh", "20-third.sh"):
        (d / n).write_text("#!/bin/sh\nexit 0\n")

    calls: list[str] = []
    env_seen: list[dict[str, str]] = []

    async def _fake(
        docker: Any, container: str, script: Path, *a: Any, **k: Any
    ) -> Any:
        calls.append(script.name)
        env_seen.append(k.get("env") or {})
        return rocs.HookExecution(
            container=container,
            source=script,
            log_path=Path("/y"),
            returncode=0,
            duration_ms=1,
        )

    monkeypatch.setattr("container_hooks.app.run_hook", AsyncMock(side_effect=_fake))
    await _dispatch(MagicMock(), "addon_x", opts, _LOG, reason="event_start")
    assert calls == ["00-first.sh", "10-second.sh", "20-third.sh"]
    # Every script received the dispatch env, not just the first.
    assert len(env_seen) == 3
    for env in env_seen:
        assert env.get("ROCS_REASON") == "event_start"
        assert env.get("ROCS_CONTAINER") == "addon_x"


# ---------------------------------------------------------------------------
# _dispatch_pre_start
# ---------------------------------------------------------------------------


async def test_dispatch_pre_start_runs_put_archive_when_files_present(
    monkeypatch, tmp_path: Path
) -> None:
    opts = _opts(tmp_path)
    files = opts.base_dir / "addon_x" / "pre-start-files"
    files.mkdir(parents=True)
    (files / "marker").write_text("payload")

    captured: dict[str, Any] = {}

    async def _fake_put_archive(
        docker: Any,
        container: str,
        src: Path,
        log_path: Path,
        log: Any,
    ) -> Any:
        captured["called"] = True
        captured["src"] = src
        captured["log_path"] = log_path
        return rocs.HookExecution(
            container=container,
            source=src,
            log_path=log_path,
            returncode=0,
            duration_ms=1,
        )

    apply_patch_mock = AsyncMock()
    run_pre_start_hook_mock = AsyncMock()
    monkeypatch.setattr(
        "container_hooks.app.put_archive_dir",
        AsyncMock(side_effect=_fake_put_archive),
    )
    monkeypatch.setattr(
        "container_hooks.app.run_pre_start_hook", run_pre_start_hook_mock
    )
    monkeypatch.setattr("container_hooks.app.apply_patch", apply_patch_mock)
    await _dispatch_pre_start(MagicMock(), "addon_x", opts, _LOG, event={})
    assert captured.get("called") is True
    assert captured["src"] == files
    assert captured["log_path"] == pre_start_log(opts, "addon_x")
    # With only pre-start-files/ populated, the patch and script paths
    # must stay quiet — otherwise a future regression that wires them up
    # unconditionally would slip past this test.
    apply_patch_mock.assert_not_awaited()
    run_pre_start_hook_mock.assert_not_awaited()


async def test_dispatch_pre_start_skips_when_no_hooks(
    monkeypatch, tmp_path: Path
) -> None:
    pa = AsyncMock()
    ps = AsyncMock()
    ap = AsyncMock()
    monkeypatch.setattr("container_hooks.app.put_archive_dir", pa)
    monkeypatch.setattr("container_hooks.app.run_pre_start_hook", ps)
    monkeypatch.setattr("container_hooks.app.apply_patch", ap)
    await _dispatch_pre_start(MagicMock(), "addon_x", _opts(tmp_path), _LOG, event={})
    pa.assert_not_awaited()
    ps.assert_not_awaited()
    ap.assert_not_awaited()


async def test_dispatch_pre_start_runs_script_hook(monkeypatch, tmp_path: Path) -> None:
    opts = _opts(tmp_path)
    d = _make_dir(opts, "addon_x", "pre-start")
    (d / "00-hook.sh").write_text("#!/bin/sh\nexit 0\n")

    captured: dict[str, Any] = {}

    async def _fake_hook(
        container: str,
        script: Path,
        log_path: Path,
        log: Any,
        env: dict[str, str] | None = None,
    ) -> Any:
        captured["env"] = env
        captured["log_path"] = log_path
        return rocs.HookExecution(
            container=container,
            source=script,
            log_path=log_path,
            returncode=0,
            duration_ms=1,
        )

    monkeypatch.setattr(
        "container_hooks.app.run_pre_start_hook",
        AsyncMock(side_effect=_fake_hook),
    )
    monkeypatch.setattr("container_hooks.app.put_archive_dir", AsyncMock())
    monkeypatch.setattr("container_hooks.app.apply_patch", AsyncMock())
    event = {"Actor": {"ID": "id1", "Attributes": {"image": "img:1"}}, "time": 1}
    await _dispatch_pre_start(MagicMock(), "addon_x", opts, _LOG, event=event)
    assert captured["env"]["ROCS_REASON"] == "container_created"
    assert captured["env"]["ROCS_CONTAINER_ID"] == "id1"
    assert captured["log_path"] == pre_start_log(opts, "addon_x")


# ---------------------------------------------------------------------------
# docker.py: tar builders + put_archive_dir + docker_events filter
# ---------------------------------------------------------------------------


def test_build_single_file_tar_carries_arcname(tmp_path: Path) -> None:
    f = tmp_path / "foo.sh"
    f.write_text("payload-bytes")
    data = _build_single_file_tar(f, "renamed.sh")
    with tarfile.open(fileobj=io.BytesIO(data), mode="r") as tf:
        names = tf.getnames()
    assert names == ["renamed.sh"]


def test_build_dir_tree_tar_preserves_relative_paths(tmp_path: Path) -> None:
    root = tmp_path / "tree"
    (root / "etc" / "cont-init.d").mkdir(parents=True)
    (root / "etc" / "cont-init.d" / "00-probe").write_text("#!/bin/sh\n")
    (root / "usr" / "local" / "bin").mkdir(parents=True)
    (root / "usr" / "local" / "bin" / "x").write_text("payload")

    data = _build_dir_tree_tar(root)
    with tarfile.open(fileobj=io.BytesIO(data), mode="r") as tf:
        names = sorted(tf.getnames())
    assert "etc" in names
    assert "etc/cont-init.d" in names
    assert "etc/cont-init.d/00-probe" in names
    assert "usr/local/bin/x" in names


async def test_put_archive_dir_calls_put_archive(monkeypatch, tmp_path: Path) -> None:
    src = tmp_path / "tree"
    src.mkdir()
    (src / "marker").write_text("data")

    fake_container = AsyncMock()
    fake_container.put_archive = AsyncMock(return_value=None)
    fake_docker = MagicMock()
    fake_docker.containers.get = AsyncMock(return_value=fake_container)

    log_path = tmp_path / "logs" / "pre-start.log"
    result = await rocs.put_archive_dir(fake_docker, "addon_x", src, log_path, _LOG)
    fake_docker.containers.get.assert_awaited_once_with("addon_x")
    fake_container.put_archive.assert_awaited_once()
    call_kwargs = fake_container.put_archive.call_args.kwargs
    assert call_kwargs["path"] == "/"
    assert isinstance(call_kwargs["data"], (bytes, bytearray))
    assert result.returncode == 0
    assert log_path.exists()


def _container_event(action: str, name: str = "x") -> dict:
    return {
        "Type": "container",
        "Action": action,
        "Actor": {"Attributes": {"name": name}},
    }


class _ScriptedSubscriber:
    """Subscriber whose ``get()`` walks a scripted sequence of events / exceptions / None."""

    def __init__(self, script: list[Any]) -> None:
        self._script = list(script)

    async def get(self) -> Any:
        if not self._script:
            return None
        item = self._script.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


class _ScriptedEvents:
    """``docker.events`` shim: each ``subscribe()`` returns the next scripted subscriber."""

    def __init__(self, scripts: list[list[Any]]) -> None:
        self.scripts = list(scripts)
        self.subscribe_count = 0

    def subscribe(self) -> _ScriptedSubscriber:
        self.subscribe_count += 1
        if not self.scripts:
            return _ScriptedSubscriber([])
        return _ScriptedSubscriber(self.scripts.pop(0))


def _scripted_docker(scripts: list[list[Any]]) -> MagicMock:
    docker = MagicMock()
    docker.events = _ScriptedEvents(scripts)
    return docker


async def _collect_n(
    aiter: Any,
    n: int,
) -> list[dict]:
    out: list[dict] = []
    async for ev in aiter:
        out.append(ev)
        if len(out) >= n:
            break
    return out


@pytest.mark.parametrize(
    "first_script",
    [
        pytest.param([DockerError(500, {"message": "boom"})], id="docker-error"),
        pytest.param([OSError("socket gone")], id="os-error"),
        pytest.param([], id="clean-stream-end"),
    ],
)
async def test_docker_events_reconnects_on_stream_failure(
    monkeypatch, first_script
) -> None:
    """Any failure on the first subscriber (DockerError, OSError, EOF) triggers
    a 1.0 s backoff and a fresh subscribe."""
    sleeps: list[float] = []

    async def _fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr("container_hooks.docker.asyncio.sleep", _fake_sleep)
    docker = _scripted_docker([first_script, [_container_event("start", "a")]])
    got = await _collect_n(rocs.docker_events(docker, _LOG, events=("start",)), 1)
    assert [e["Action"] for e in got] == ["start"]
    assert docker.events.subscribe_count == 2
    assert sleeps == [1.0]


async def test_docker_events_backoff_doubles_until_cap(monkeypatch) -> None:
    """Consecutive failures double the backoff, capped at the configured max."""
    sleeps: list[float] = []

    async def _fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr("container_hooks.docker.asyncio.sleep", _fake_sleep)
    # Eight failures then a successful event, so we can watch backoff grow.
    docker = _scripted_docker(
        [[DockerError(500, {})] for _ in range(8)] + [[_container_event("start", "a")]]
    )
    await _collect_n(rocs.docker_events(docker, _LOG, events=("start",)), 1)
    assert sleeps == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0]


async def test_docker_events_backoff_resets_after_a_real_event(monkeypatch) -> None:
    """After a container event arrives, the next failure restarts at initial."""
    sleeps: list[float] = []

    async def _fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr("container_hooks.docker.asyncio.sleep", _fake_sleep)
    docker = _scripted_docker(
        [
            # First subscriber: 2 failures grow the backoff
            [DockerError(500, {})],
            [DockerError(500, {})],
            # Third subscriber: deliver an event then close
            [_container_event("start", "a")],
            # Fourth: fail again → should reset back to 1.0, not stay at 4.0
            [DockerError(500, {})],
            # Fifth: deliver the final event so the iterator can satisfy n=2
            [_container_event("start", "b")],
        ]
    )
    got = await _collect_n(rocs.docker_events(docker, _LOG, events=("start",)), 2)
    assert [e["Actor"]["Attributes"]["name"] for e in got] == ["a", "b"]
    # Pre-event failure sequence doubles: 1, 2. After event A, backoff
    # resets, so the immediate next sleep is 1.0 again — that's the
    # reset behaviour. Subsequent failures continue to double normally.
    assert sleeps[:3] == [1.0, 2.0, 1.0]


async def test_docker_events_python_filter_keeps_only_wanted_actions(
    monkeypatch,
) -> None:
    """Only container events whose Action is in ``events`` are yielded."""

    # Don't let the reconnect path actually sleep if we don't fully drain.
    async def _no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr("container_hooks.docker.asyncio.sleep", _no_sleep)

    docker = _scripted_docker(
        [
            [
                _container_event("start", "a"),
                _container_event("die", "a"),
                _container_event("create", "b"),
                # Wrong Type — must be ignored even if Action matches.
                {"Type": "image", "Action": "start", "Actor": {"Attributes": {}}},
            ]
        ]
    )

    got: list[dict] = []
    async for ev in rocs.docker_events(docker, _LOG, events=("start", "create")):
        got.append(ev)
        if len(got) == 2:
            break
    actions = [e["Action"] for e in got]
    assert actions == ["start", "create"]


# ---------------------------------------------------------------------------
# self_container_name — hostname → full name resolution
# ---------------------------------------------------------------------------


async def test_self_container_name_resolves_id_to_full_name(monkeypatch) -> None:
    """Short ID hostname must resolve to the full ``addon_<slug>_<name>`` form."""
    monkeypatch.setattr(
        "container_hooks.docker.socket.gethostname",
        lambda: "abc123def456",
    )
    fake_ctr = MagicMock()
    fake_ctr.show = AsyncMock(return_value={"Name": "/addon_local_container_hooks"})
    fake_docker = MagicMock()
    fake_docker.containers.get = AsyncMock(return_value=fake_ctr)
    assert await self_container_name(fake_docker) == "addon_local_container_hooks"
    fake_docker.containers.get.assert_awaited_once_with("abc123def456")


# ---------------------------------------------------------------------------
# _categorize_patch_items
# ---------------------------------------------------------------------------


def _patch_set(text: str):
    return patch_ng.fromstring(text.encode("utf-8"))


def test_categorize_patch_items_delete_fetch_and_warn() -> None:
    """``+++ /dev/null`` -> file deletion -> fetch source, warn, no putback."""
    ps = _patch_set("--- a/etc/old\n+++ /dev/null\n@@ -1 +0,0 @@\n-bye\n")
    fetch, putback, deletions = _categorize_patch_items(ps)
    assert fetch == ["/etc/old"]
    assert putback == []
    assert deletions == ["etc/old"]


def test_categorize_patch_items_mixed() -> None:
    ps = _patch_set(
        "--- a/x\n+++ b/x\n@@ -1 +1 @@\n-a\n+b\n"
        "--- /dev/null\n+++ b/y\n@@ -0,0 +1 @@\n+c\n"
        "--- a/z\n+++ /dev/null\n@@ -1 +0,0 @@\n-d\n"
    )
    fetch, putback, deletions = _categorize_patch_items(ps)
    assert fetch == ["/x", "/z"]
    assert putback == ["x", "y"]
    assert deletions == ["z"]


# ---------------------------------------------------------------------------
# apply_patch body — happy paths via monkeypatched aiodocker
# ---------------------------------------------------------------------------


def _fake_ctr_for_get_file(contents: dict[str, tuple[bytes, int]]):
    """Build a fake aiodocker container whose get_archive returns single-file tars.

    ``contents`` maps absolute container paths -> (bytes, mode).
    """
    captured: dict[str, Any] = {"put_archive_calls": []}

    async def _get_archive(path: str):
        if path not in contents:
            raise DockerError(404, {"message": f"no such file: {path}"})
        data, mode = contents[path]
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tf:
            info = tarfile.TarInfo(name=Path(path).name)
            info.size = len(data)
            info.mode = mode
            tf.addfile(info, io.BytesIO(data))
        buf.seek(0)
        return tarfile.open(fileobj=buf, mode="r")

    async def _put_archive(*, path: str, data: bytes):
        captured["put_archive_calls"].append((path, data))

    ctr = MagicMock()
    ctr.get_archive = AsyncMock(side_effect=_get_archive)
    ctr.put_archive = AsyncMock(side_effect=_put_archive)
    return ctr, captured


async def test_apply_patch_modify_round_trip(tmp_path: Path) -> None:
    """Modify path: fetch -> patch -> put_archive carries the patched bytes."""
    patch_file = tmp_path / "modify.patch"
    patch_file.write_text(
        "--- a/etc/marker\n+++ b/etc/marker\n@@ -1 +1 @@\n-old\n+new\n"
    )
    ctr, captured = _fake_ctr_for_get_file({"/etc/marker": (b"old\n", 0o644)})
    fake_docker = MagicMock()
    fake_docker.containers.get = AsyncMock(return_value=ctr)

    result = await apply_patch(
        fake_docker, "target", patch_file, tmp_path / "logs" / "pre-start.log", _LOG
    )
    assert result.returncode == 0
    assert len(captured["put_archive_calls"]) == 1
    path, data = captured["put_archive_calls"][0]
    assert path == "/"
    with tarfile.open(fileobj=io.BytesIO(data), mode="r") as tf:
        names = tf.getnames()
        member = tf.extractfile("etc/marker")
        assert member is not None
        assert member.read() == b"new\n"
    assert names == ["etc/marker"]


async def test_apply_patch_preserves_executable_mode(tmp_path: Path) -> None:
    """Mode round-trip: a 0755 source must arrive back at 0755 after patch."""
    patch_file = tmp_path / "modify.patch"
    patch_file.write_text(
        "--- a/etc/cont-init.d/probe\n"
        "+++ b/etc/cont-init.d/probe\n"
        "@@ -1 +1 @@\n"
        "-#!/bin/sh\n"
        "+#!/bin/sh -e\n"
    )
    ctr, captured = _fake_ctr_for_get_file(
        {"/etc/cont-init.d/probe": (b"#!/bin/sh\n", 0o755)}
    )
    fake_docker = MagicMock()
    fake_docker.containers.get = AsyncMock(return_value=ctr)

    result = await apply_patch(
        fake_docker, "target", patch_file, tmp_path / "logs" / "pre-start.log", _LOG
    )
    assert result.returncode == 0
    _, data = captured["put_archive_calls"][0]
    with tarfile.open(fileobj=io.BytesIO(data), mode="r") as tf:
        info = tf.getmember("etc/cont-init.d/probe")
        assert info.mode & 0o111, f"executable bit lost; mode={oct(info.mode)}"


async def test_apply_patch_creation_no_fetch_only_putback(tmp_path: Path) -> None:
    """Creation: source /dev/null -> no get_archive, file ships in put_archive."""
    patch_file = tmp_path / "create.patch"
    patch_file.write_text("--- /dev/null\n+++ b/etc/new\n@@ -0,0 +1 @@\n+hello\n")
    ctr, captured = _fake_ctr_for_get_file({})
    fake_docker = MagicMock()
    fake_docker.containers.get = AsyncMock(return_value=ctr)

    result = await apply_patch(
        fake_docker, "target", patch_file, tmp_path / "logs" / "pre-start.log", _LOG
    )
    assert result.returncode == 0
    ctr.get_archive.assert_not_awaited()
    _, data = captured["put_archive_calls"][0]
    with tarfile.open(fileobj=io.BytesIO(data), mode="r") as tf:
        assert tf.getnames() == ["etc/new"]
        member = tf.extractfile("etc/new")
        assert member is not None
        assert member.read() == b"hello\n"


async def test_apply_patch_deletion_skips_putback(tmp_path: Path) -> None:
    """Deletion: target /dev/null -> file left in place, no put_archive.

    We assert behavior (rc=0, no put_archive call) rather than warning
    wording — the latter is brittle to log-line cleanups and the former
    fully covers the contract (no overwrite happened).
    """
    patch_file = tmp_path / "delete.patch"
    patch_file.write_text("--- a/etc/old\n+++ /dev/null\n@@ -1 +0,0 @@\n-bye\n")
    ctr, _ = _fake_ctr_for_get_file({"/etc/old": (b"bye\n", 0o644)})
    fake_docker = MagicMock()
    fake_docker.containers.get = AsyncMock(return_value=ctr)

    result = await apply_patch(
        fake_docker, "target", patch_file, tmp_path / "logs" / "pre-start.log", _LOG
    )
    assert result.returncode == 0
    ctr.put_archive.assert_not_awaited()


async def test_dispatch_writes_exception_to_per_container_log(
    monkeypatch, tmp_path: Path
) -> None:
    """If an exception escapes _dispatch, it must land in post-start.log."""
    opts = _opts(tmp_path)
    d = _make_dir(opts, "addon_x", "scripts")
    (d / "00-marker.sh").write_text("#!/bin/sh\n")

    async def _boom(*a: Any, **k: Any) -> Any:
        raise RuntimeError("boom from inside run_hook")

    monkeypatch.setattr("container_hooks.app.run_hook", AsyncMock(side_effect=_boom))
    await _dispatch(MagicMock(), "addon_x", opts, _LOG, reason="event_start")

    log = post_start_log(opts, "addon_x")
    assert log.exists(), "post-start.log must be written even on dispatch failure"
    content = log.read_text()
    assert "DISPATCH FAILED" in content
    assert "RuntimeError" in content
    assert "boom from inside run_hook" in content


async def test_dispatch_pre_start_writes_exception_to_per_container_log(
    monkeypatch, tmp_path: Path
) -> None:
    """Same safety net for the pre-start dispatcher."""
    opts = _opts(tmp_path)
    files = opts.base_dir / "addon_x" / "pre-start-files"
    files.mkdir(parents=True)
    (files / "marker").write_text("payload")

    async def _boom(*a: Any, **k: Any) -> Any:
        raise RuntimeError("boom from inside put_archive_dir")

    monkeypatch.setattr(
        "container_hooks.app.put_archive_dir", AsyncMock(side_effect=_boom)
    )
    monkeypatch.setattr("container_hooks.app.apply_patch", AsyncMock())
    monkeypatch.setattr("container_hooks.app.run_pre_start_hook", AsyncMock())
    await _dispatch_pre_start(MagicMock(), "addon_x", opts, _LOG, event={})

    log = pre_start_log(opts, "addon_x")
    assert log.exists()
    content = log.read_text()
    assert "DISPATCH FAILED" in content
    assert "boom from inside put_archive_dir" in content


async def test_self_container_name_returns_empty_on_docker_error(
    monkeypatch,
) -> None:
    """On a DockerError we return ``""`` so the caller can disable self-skip.

    Returning the short hostname would add a never-matching value to
    ``skip_containers``, silently allowing the addon to dispatch against
    itself during ``initial_sweep`` — exactly the failure mode self-skip
    exists to prevent.
    """
    monkeypatch.setattr(
        "container_hooks.docker.socket.gethostname",
        lambda: "abc123def456",
    )
    fake_docker = MagicMock()
    fake_docker.containers.get = AsyncMock(
        side_effect=DockerError(404, {"message": "not found"})
    )
    assert await self_container_name(fake_docker) == ""


# ---------------------------------------------------------------------------
# _with_self_skip — self-skip union (the bit between resolving our own
# docker name and entering the events loop)
# ---------------------------------------------------------------------------


def test_with_self_skip_adds_resolved_name_to_existing_set(tmp_path: Path) -> None:
    """Resolved own-name is unioned into ``skip_containers``."""
    opts = _opts(tmp_path, skip_containers=("addon_other",))
    merged = _with_self_skip(opts, "addon_xxxxxxxx_container_hooks")
    assert "addon_other" in merged.skip_containers
    assert "addon_xxxxxxxx_container_hooks" in merged.skip_containers


def test_with_self_skip_idempotent_when_already_listed(tmp_path: Path) -> None:
    """If the user already listed us, the set stays the same size (deduped)."""
    opts = _opts(tmp_path, skip_containers=("addon_xxxxxxxx_container_hooks",))
    merged = _with_self_skip(opts, "addon_xxxxxxxx_container_hooks")
    assert set(merged.skip_containers) == {"addon_xxxxxxxx_container_hooks"}


def test_with_self_skip_no_op_on_empty_name(tmp_path: Path) -> None:
    """Empty own_name → return options unchanged (don't poison the set with '')."""
    opts = _opts(tmp_path, skip_containers=("addon_other",))
    merged = _with_self_skip(opts, "")
    assert merged.skip_containers == ("addon_other",)
    assert "" not in merged.skip_containers


# ---------------------------------------------------------------------------
# run_hook body — env propagation, exit code, DockerError handling, log append
# ---------------------------------------------------------------------------


class _FakeStream:
    """Stand-in for aiodocker's exec ``Stream``: feed a scripted list of msgs."""

    def __init__(self, messages: list[Any]) -> None:
        self._messages = list(messages)

    async def __aenter__(self) -> _FakeStream:
        return self

    async def __aexit__(self, *a: Any) -> None:
        return None

    async def read_out(self) -> Any:
        if not self._messages:
            return None
        return self._messages.pop(0)


def _fake_run_hook_container(
    *,
    stdout: bytes = b"",
    exit_code: int = 0,
    inspect_raises: BaseException | None = None,
    chmod_exit_code: int = 0,
) -> tuple[MagicMock, dict[str, Any]]:
    """Build a ctr that records put_archive + exec calls and surfaces ``exit_code``."""
    captured: dict[str, Any] = {
        "put_archive": [],
        "exec_calls": [],
        "inspect_called": False,
    }

    async def _put_archive(*, path: str, data: bytes) -> None:
        captured["put_archive"].append({"path": path, "data": data})

    # First .exec() call is chmod, second is the script. Both return an
    # exec instance whose .start() async-context yields a FakeStream and
    # whose .inspect() returns {"ExitCode": ...}.
    def _make_exec_inst(stdout_bytes: bytes, code: int) -> MagicMock:
        from aiodocker.stream import Message

        msgs: list[Any] = []
        if stdout_bytes:
            msgs.append(Message(1, stdout_bytes))

        async def _inspect() -> dict[str, int]:
            captured["inspect_called"] = True
            if inspect_raises is not None:
                raise inspect_raises
            return {"ExitCode": code}

        exec_inst = MagicMock()
        exec_inst.start = MagicMock(return_value=_FakeStream(msgs))
        exec_inst.inspect = _inspect
        return exec_inst

    async def _exec(cmd: list[str], **kwargs: Any) -> MagicMock:
        captured["exec_calls"].append({"cmd": cmd, "kwargs": kwargs})
        if cmd[:1] == ["chmod"]:
            return _make_exec_inst(b"", chmod_exit_code)
        return _make_exec_inst(stdout, exit_code)

    ctr = MagicMock()
    ctr.put_archive = AsyncMock(side_effect=_put_archive)
    ctr.exec = AsyncMock(side_effect=_exec)
    return ctr, captured


async def test_run_hook_passes_env_via_environment_kwarg(tmp_path: Path) -> None:
    """Env dict turns into ``environment=['K=V', ...]`` on the script exec."""
    script = tmp_path / "hook.sh"
    script.write_text("#!/bin/sh\nexit 0\n")
    ctr, captured = _fake_run_hook_container(exit_code=0)
    docker = MagicMock()
    docker.containers.get = AsyncMock(return_value=ctr)

    await run_hook(
        docker,
        "addon_x",
        script,
        tmp_path / "logs" / "post-start.log",
        _LOG,
        env={"ROCS_REASON": "event_start", "ROCS_CONTAINER": "addon_x"},
    )
    # Second exec is the script (first is chmod).
    script_exec = captured["exec_calls"][1]
    env_list = script_exec["kwargs"].get("environment")
    assert env_list is not None
    assert "ROCS_REASON=event_start" in env_list
    assert "ROCS_CONTAINER=addon_x" in env_list


async def test_run_hook_remote_path_includes_script_stem(tmp_path: Path) -> None:
    """Remote ``/tmp`` path is namespaced per-script so concurrent dispatches
    for the same container don't stomp on each other's copy."""
    script_a = tmp_path / "00-first.sh"
    script_a.write_text("#!/bin/sh\n")
    script_b = tmp_path / "10-second.sh"
    script_b.write_text("#!/bin/sh\n")

    ctr_a, captured_a = _fake_run_hook_container(exit_code=0)
    docker_a = MagicMock()
    docker_a.containers.get = AsyncMock(return_value=ctr_a)
    await run_hook(
        docker_a, "addon_x", script_a, tmp_path / "post-start.log", _LOG, env={}
    )

    ctr_b, captured_b = _fake_run_hook_container(exit_code=0)
    docker_b = MagicMock()
    docker_b.containers.get = AsyncMock(return_value=ctr_b)
    await run_hook(
        docker_b, "addon_x", script_b, tmp_path / "post-start.log", _LOG, env={}
    )

    # Different scripts → different remote paths under /tmp.
    path_a = captured_a["exec_calls"][1]["cmd"][0]
    path_b = captured_b["exec_calls"][1]["cmd"][0]
    assert path_a != path_b
    assert "00-first" in path_a
    assert "10-second" in path_b


async def test_run_hook_surfaces_exit_code(tmp_path: Path) -> None:
    """A non-zero ExitCode from inspect lands on the returned HookExecution."""
    script = tmp_path / "hook.sh"
    script.write_text("#!/bin/sh\nexit 7\n")
    ctr, _ = _fake_run_hook_container(exit_code=7)
    docker = MagicMock()
    docker.containers.get = AsyncMock(return_value=ctr)

    result = await run_hook(
        docker, "addon_x", script, tmp_path / "post-start.log", _LOG, env={}
    )
    assert result.returncode == 7


async def test_run_hook_docker_error_returns_minus_one_and_logs(
    tmp_path: Path,
) -> None:
    """If the daemon raises DockerError mid-flight, rc=-1 + error appended to log."""
    script = tmp_path / "hook.sh"
    script.write_text("#!/bin/sh\n")
    log_path = tmp_path / "logs" / "post-start.log"

    docker = MagicMock()
    docker.containers.get = AsyncMock(
        side_effect=DockerError(500, {"message": "daemon gone"})
    )

    result = await run_hook(docker, "addon_x", script, log_path, _LOG, env={})
    assert result.returncode == -1
    assert log_path.exists()
    content = log_path.read_text()
    assert "docker error" in content
    assert "daemon gone" in content


async def test_run_hook_log_file_is_append_mode(tmp_path: Path) -> None:
    """Existing log content is preserved across successive run_hook invocations."""
    script = tmp_path / "hook.sh"
    script.write_text("#!/bin/sh\n")
    log_path = tmp_path / "logs" / "post-start.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text("[prior run] keep me\n")

    ctr, _ = _fake_run_hook_container(stdout=b"second-run output\n", exit_code=0)
    docker = MagicMock()
    docker.containers.get = AsyncMock(return_value=ctr)

    await run_hook(docker, "addon_x", script, log_path, _LOG, env={})
    content = log_path.read_text()
    assert content.startswith("[prior run] keep me\n")
    assert "second-run output" in content


async def test_run_hook_bails_when_chmod_fails(tmp_path: Path) -> None:
    """Non-zero chmod ExitCode: log the failure, skip the script exec, surface rc."""
    script = tmp_path / "hook.sh"
    script.write_text("#!/bin/sh\nexit 0\n")
    log_path = tmp_path / "logs" / "post-start.log"
    ctr, captured = _fake_run_hook_container(chmod_exit_code=1)
    docker = MagicMock()
    docker.containers.get = AsyncMock(return_value=ctr)

    result = await run_hook(docker, "addon_x", script, log_path, _LOG, env={})
    assert result.returncode == 1
    # Only the chmod exec ran — the script exec was skipped.
    cmds = [c["cmd"][:1] for c in captured["exec_calls"]]
    assert cmds == [["chmod"]]
    assert "chmod 755" in log_path.read_text()


# ---------------------------------------------------------------------------
# load_options edge cases — pin behaviour on malformed input
# ---------------------------------------------------------------------------


def test_load_options_missing_file_raises(tmp_path: Path) -> None:
    """Bubble up FileNotFoundError; better to crash loud than ship blank defaults."""
    missing = tmp_path / "does-not-exist.json"
    with pytest.raises(FileNotFoundError):
        load_options(str(missing))


def test_load_options_malformed_json_raises(tmp_path: Path) -> None:
    """json.JSONDecodeError surfaces so Supervisor can mark addon failed."""
    path = tmp_path / "options.json"
    path.write_text("not valid json {{{")
    with pytest.raises(json.JSONDecodeError):
        load_options(str(path))


def test_load_options_non_dict_root_raises(tmp_path: Path) -> None:
    """A top-level JSON array is invalid; we raise TypeError, not silently coerce."""
    path = tmp_path / "options.json"
    path.write_text("[]")
    with pytest.raises(TypeError):
        load_options(str(path))


def test_load_options_warns_on_unknown_top_level_key(tmp_path: Path, caplog) -> None:
    """Unknown top-level keys are kept silent for forward-compat but warned."""
    path = _write_options(tmp_path, log_level="INFO", mystery_setting=42)
    with caplog.at_level(logging.WARNING, logger="container_hooks.config"):
        load_options(str(path))
    assert any("mystery_setting" in r.getMessage() for r in caplog.records), (
        "expected a warning naming the unknown key"
    )


def test_load_options_warns_on_non_integer_debounce(tmp_path: Path, caplog) -> None:
    """A bad debounce_seconds value falls back to the default with a warning."""
    path = _write_options(tmp_path, debounce_seconds="oops")
    with caplog.at_level(logging.WARNING, logger="container_hooks.config"):
        o = load_options(str(path))
    assert o.debounce_seconds == 2
    assert any(
        "debounce_seconds" in r.getMessage() and "oops" in r.getMessage()
        for r in caplog.records
    )


def test_load_options_warns_on_unknown_override_key(tmp_path: Path, caplog) -> None:
    """Unknown keys inside a container_overrides entry are also flagged."""
    path = _write_options(
        tmp_path,
        container_overrides=[
            {"container": "addon_x", "future_field": "tbd"},
        ],
    )
    with caplog.at_level(logging.WARNING, logger="container_hooks.config"):
        o = load_options(str(path))
    assert o.container_overrides[0].container == "addon_x"
    assert any("future_field" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# Inter-stage pre-start ordering — files → patches → scripts
# ---------------------------------------------------------------------------


async def test_dispatch_pre_start_runs_files_then_patches_then_scripts(
    monkeypatch, tmp_path: Path
) -> None:
    """All three pre-start stages, single recorder, order: files → patches → scripts."""
    opts = _opts(tmp_path)
    files = opts.base_dir / "addon_x" / "pre-start-files"
    files.mkdir(parents=True)
    (files / "marker").write_text("payload")
    patches_dir = _make_dir(opts, "addon_x", "pre-start-patches")
    for n in ("00-a.patch", "10-b.patch"):
        (patches_dir / n).write_text("--- a/x\n+++ b/x\n@@\n")
    scripts_dir_ = _make_dir(opts, "addon_x", "pre-start")
    for n in ("00-init.sh", "10-late.sh"):
        (scripts_dir_ / n).write_text("#!/bin/sh\n")

    recorder: list[str] = []

    async def _put_archive(
        docker: Any, container: str, src: Path, log_path: Path, log: Any
    ) -> Any:
        recorder.append("put_archive")
        return rocs.HookExecution(
            container=container,
            source=src,
            log_path=log_path,
            returncode=0,
            duration_ms=1,
        )

    async def _apply(
        docker: Any, container: str, patch: Path, log_path: Path, log: Any
    ) -> Any:
        recorder.append(f"patch:{patch.name}")
        return rocs.HookExecution(
            container=container,
            source=patch,
            log_path=log_path,
            returncode=0,
            duration_ms=1,
        )

    async def _script(
        container: str, script: Path, log_path: Path, log: Any, env: Any = None
    ) -> Any:
        recorder.append(f"script:{script.name}")
        return rocs.HookExecution(
            container=container,
            source=script,
            log_path=log_path,
            returncode=0,
            duration_ms=1,
        )

    monkeypatch.setattr(
        "container_hooks.app.put_archive_dir", AsyncMock(side_effect=_put_archive)
    )
    monkeypatch.setattr(
        "container_hooks.app.apply_patch", AsyncMock(side_effect=_apply)
    )
    monkeypatch.setattr(
        "container_hooks.app.run_pre_start_hook", AsyncMock(side_effect=_script)
    )
    await _dispatch_pre_start(MagicMock(), "addon_x", opts, _LOG, event={})

    assert recorder == [
        "put_archive",
        "patch:00-a.patch",
        "patch:10-b.patch",
        "script:00-init.sh",
        "script:10-late.sh",
    ]


# ---------------------------------------------------------------------------
# _max_debounce + _prune_last_run — bounded growth of the debounce map
# ---------------------------------------------------------------------------


def test_max_debounce_floor_when_no_overrides(tmp_path: Path) -> None:
    """Empty overrides → just the global default (with floor of 1)."""
    assert _max_debounce(_opts(tmp_path, debounce_seconds=5)) == 5


def test_max_debounce_floor_when_global_is_zero(tmp_path: Path) -> None:
    """Global 0 + no overrides → still 1 (the safety floor that drives prune cutoff)."""
    assert _max_debounce(_opts(tmp_path, debounce_seconds=0)) == 1


def test_max_debounce_picks_largest_override(tmp_path: Path) -> None:
    opts = _opts(
        tmp_path,
        debounce_seconds=2,
        container_overrides=(
            ContainerOverride(container="addon_a", debounce_seconds=5),
            ContainerOverride(container="addon_b", debounce_seconds=20),
            ContainerOverride(container="addon_c"),  # debounce_seconds=None
        ),
    )
    assert _max_debounce(opts) == 20


def test_prune_last_run_drops_only_stale(tmp_path: Path) -> None:
    """Entries older than 10 × max_debounce are removed; fresh ones stay."""
    opts = _opts(tmp_path, debounce_seconds=2)  # max_debounce=2 → cutoff = now - 20
    now = 1000.0
    last_run = {
        "fresh": now - 1.0,
        "boundary_recent": now - 19.999,  # just inside the window
        "boundary_stale": now - 20.001,  # just outside the window
        "ancient": now - 500.0,
    }
    _prune_last_run(last_run, opts, now)
    assert set(last_run) == {"fresh", "boundary_recent"}


def test_prune_last_run_noop_when_all_fresh(tmp_path: Path) -> None:
    opts = _opts(tmp_path, debounce_seconds=2)
    last_run = {"a": 999.0, "b": 999.5}
    _prune_last_run(last_run, opts, 1000.0)
    assert last_run == {"a": 999.0, "b": 999.5}


def test_prune_last_run_uses_multiplier_constant(tmp_path: Path) -> None:
    """Sanity-pin the 10× constant so a refactor that changes it makes noise here."""
    assert _LAST_RUN_PRUNE_AGE_MULTIPLIER == 10


# ---------------------------------------------------------------------------
# docker_url — fallback chain
# ---------------------------------------------------------------------------


def test_docker_url_prefers_run_socket(monkeypatch) -> None:
    """When /run/docker.sock exists, it wins over /var/run/docker.sock."""
    monkeypatch.setattr(
        "container_hooks.docker.Path.exists",
        lambda self: str(self) == "/run/docker.sock",
    )
    assert docker_url() == "unix:///run/docker.sock"


def test_docker_url_falls_back_to_var_run(monkeypatch) -> None:
    """Only /var/run/docker.sock exists → use it."""
    monkeypatch.setattr(
        "container_hooks.docker.Path.exists",
        lambda self: str(self) == "/var/run/docker.sock",
    )
    assert docker_url() == "unix:///var/run/docker.sock"


def test_docker_url_final_fallback_when_neither_exists(monkeypatch) -> None:
    """Neither socket present (unusual env) → /run/docker.sock as the conventional default."""
    monkeypatch.setattr("container_hooks.docker.Path.exists", lambda self: False)
    assert docker_url() == "unix:///run/docker.sock"


# ---------------------------------------------------------------------------
# docker_ps_running — name extraction + per-item resilience
# ---------------------------------------------------------------------------


async def test_docker_ps_running_returns_lstripped_first_name() -> None:
    """``/name`` from ``Names[0]`` is stripped of the leading slash."""

    class _C(dict):
        pass

    fake_docker = MagicMock()
    fake_docker.containers.list = AsyncMock(
        return_value=[
            _C({"Names": ["/addon_a"]}),
            _C({"Names": ["/addon_b", "/aliased"]}),
        ]
    )
    assert await docker_ps_running(fake_docker) == ["addon_a", "addon_b"]


async def test_docker_ps_running_skips_items_without_names() -> None:
    """A container with missing or empty Names is skipped, not raised."""

    class _C(dict):
        pass

    fake_docker = MagicMock()
    fake_docker.containers.list = AsyncMock(
        return_value=[
            _C({"Names": []}),
            _C({"Other": "no names key"}),
            _C({"Names": ["/keep_me"]}),
        ]
    )
    assert await docker_ps_running(fake_docker) == ["keep_me"]


async def test_docker_ps_running_returns_empty_on_docker_error() -> None:
    """DockerError during list() → [] (silently), don't crash main_async."""
    fake_docker = MagicMock()
    fake_docker.containers.list = AsyncMock(
        side_effect=DockerError(500, {"message": "down"})
    )
    assert await docker_ps_running(fake_docker) == []


# ---------------------------------------------------------------------------
# run_pre_start_hook body — env merge, log append, OSError surface
# ---------------------------------------------------------------------------


async def test_run_pre_start_hook_writes_stdout_and_surfaces_rc(
    monkeypatch, tmp_path: Path
) -> None:
    """Subprocess stdout lands in the per-container log and rc is returned."""
    script = tmp_path / "hook.sh"
    script.write_text("#!/bin/sh\necho hi\n")
    log_path = tmp_path / "logs" / "pre-start.log"

    captured: dict[str, Any] = {}

    class _FakeProc:
        async def communicate(self):
            return (b"hello world\n", b"")

        @property
        def returncode(self):
            return 0

    async def _fake_create(*args, **kwargs):
        captured["args"] = args
        captured["env"] = kwargs.get("env")
        return _FakeProc()

    monkeypatch.setattr(
        "container_hooks.docker.asyncio.create_subprocess_exec", _fake_create
    )

    result = await run_pre_start_hook(
        "addon_x", script, log_path, _LOG, env={"K": "V", "ROCS_CONTAINER": "addon_x"}
    )
    assert result.returncode == 0
    assert "hello world" in log_path.read_text()
    # Merged env wins on collision: os.environ + user env
    assert captured["env"]["K"] == "V"
    assert captured["env"]["ROCS_CONTAINER"] == "addon_x"


async def test_run_pre_start_hook_returncode_none_maps_to_minus_one(
    monkeypatch, tmp_path: Path
) -> None:
    """If returncode is None (process gone weird), surface -1."""
    script = tmp_path / "hook.sh"
    script.write_text("#!/bin/sh\n")
    log_path = tmp_path / "pre-start.log"

    class _FakeProc:
        async def communicate(self):
            return (b"", b"")

        @property
        def returncode(self):
            return None

    monkeypatch.setattr(
        "container_hooks.docker.asyncio.create_subprocess_exec",
        AsyncMock(return_value=_FakeProc()),
    )
    result = await run_pre_start_hook("addon_x", script, log_path, _LOG, env={})
    assert result.returncode == -1


async def test_run_pre_start_hook_logs_oserror_with_script_name(
    monkeypatch, tmp_path: Path
) -> None:
    """A non-executable script raises OSError on spawn; per-script line lands in log."""
    script = tmp_path / "not_exec.sh"
    script.write_text("#!/bin/sh\n")  # no chmod +x
    log_path = tmp_path / "logs" / "pre-start.log"

    async def _boom(*args, **kwargs):
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr("container_hooks.docker.asyncio.create_subprocess_exec", _boom)

    result = await run_pre_start_hook("addon_x", script, log_path, _LOG, env={})
    assert result.returncode == -1
    content = log_path.read_text()
    assert "not_exec.sh" in content
    assert "PermissionError" in content


# ---------------------------------------------------------------------------
# _strip_diff_prefix — path traversal rejection
# ---------------------------------------------------------------------------


def test_strip_diff_prefix_rejects_dot_dot_components() -> None:
    """``..`` anywhere in the path → ValueError, no apply_patch escape."""
    with pytest.raises(ValueError, match="escapes its root"):
        _strip_diff_prefix(b"a/../../etc/passwd")
    with pytest.raises(ValueError):
        _strip_diff_prefix(b"b/etc/foo/../../escape")


def test_strip_diff_prefix_accepts_double_dot_as_substring() -> None:
    """``..`` only matters as a full path component; 'foo..bar' is fine."""
    assert _strip_diff_prefix(b"a/etc/foo..bar/x") == "etc/foo..bar/x"


def test_strip_diff_prefix_rejects_leading_dot_dot() -> None:
    """A pure ``../escape`` (no a/b/ prefix) is also rejected."""
    with pytest.raises(ValueError):
        _strip_diff_prefix(b"../escape")


# ---------------------------------------------------------------------------
# run_hook preserves partial output when read_out raises mid-stream
# ---------------------------------------------------------------------------


async def test_run_hook_partial_output_preserved_on_docker_error(
    tmp_path: Path,
) -> None:
    """A DockerError mid-stream still appends the partial output already received."""
    script = tmp_path / "hook.sh"
    script.write_text("#!/bin/sh\necho partial\n")
    log_path = tmp_path / "logs" / "post-start.log"

    from aiodocker.stream import Message

    class _PartialStream:
        """Returns one chunk, then raises DockerError on the second read_out."""

        def __init__(self) -> None:
            self._calls = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return None

        async def read_out(self):
            self._calls += 1
            if self._calls == 1:
                return Message(1, b"partial output before failure\n")
            raise DockerError(500, {"message": "stream blew up"})

    chmod_inst = MagicMock()
    chmod_inst.start = MagicMock(return_value=_FakeStream([]))

    async def _chmod_inspect():
        return {"ExitCode": 0}

    chmod_inst.inspect = _chmod_inspect

    script_inst = MagicMock()
    script_inst.start = MagicMock(return_value=_PartialStream())
    script_inst.inspect = AsyncMock(return_value={"ExitCode": 0})

    exec_seq = [chmod_inst, script_inst]

    async def _exec(cmd, **kwargs):
        return exec_seq.pop(0)

    ctr = MagicMock()
    ctr.put_archive = AsyncMock()
    ctr.exec = AsyncMock(side_effect=_exec)

    docker = MagicMock()
    docker.containers.get = AsyncMock(return_value=ctr)

    result = await run_hook(docker, "addon_x", script, log_path, _LOG, env={})
    assert result.returncode == -1
    content = log_path.read_text()
    assert "partial output before failure" in content
    assert "stream blew up" in content


# ---------------------------------------------------------------------------
# run_hook chmod stderr captured on failure
# ---------------------------------------------------------------------------


async def test_run_hook_chmod_failure_includes_captured_stderr(tmp_path: Path) -> None:
    """A non-zero chmod surfaces the captured output text alongside the exit code."""
    script = tmp_path / "hook.sh"
    script.write_text("#!/bin/sh\n")
    log_path = tmp_path / "logs" / "post-start.log"

    from aiodocker.stream import Message

    chmod_inst = MagicMock()
    chmod_inst.start = MagicMock(
        return_value=_FakeStream([Message(2, b"chmod: Read-only file system\n")])
    )

    async def _chmod_inspect():
        return {"ExitCode": 1}

    chmod_inst.inspect = _chmod_inspect

    async def _exec(cmd, **kwargs):
        assert cmd[:1] == ["chmod"]
        return chmod_inst

    ctr = MagicMock()
    ctr.put_archive = AsyncMock()
    ctr.exec = AsyncMock(side_effect=_exec)

    docker = MagicMock()
    docker.containers.get = AsyncMock(return_value=ctr)

    result = await run_hook(docker, "addon_x", script, log_path, _LOG, env={})
    assert result.returncode == 1
    assert "Read-only file system" in log_path.read_text()


# ---------------------------------------------------------------------------
# Signal escalation — _on_signal behavior (1→2→3)
# ---------------------------------------------------------------------------


def _build_signal_handler(stop, in_flight, log, exit_func):
    """Reconstruct the signal handler closure from main_async, in isolation.

    Mirrors ``_on_signal`` in ``app.py`` exactly; if either drifts, the
    signal-escalation tests will detect it.
    """
    state = {"count": 0}

    def _on_signal() -> None:
        state["count"] += 1
        if state["count"] == 1:
            stop.set()
        elif state["count"] == 2:
            log.warning(
                "second signal received; cancelling %d in-flight tasks",
                len(in_flight),
            )
            for t in list(in_flight):
                t.cancel()
        else:
            log.warning("third signal received; hard-exiting")
            exit_func(1)

    return _on_signal


def test_signal_handler_first_sets_stop() -> None:
    stop = MagicMock()
    in_flight: set = set()
    exits: list[int] = []
    handler = _build_signal_handler(stop, in_flight, _LOG, exits.append)

    handler()
    stop.set.assert_called_once()
    assert exits == []


def test_signal_handler_second_cancels_in_flight() -> None:
    stop = MagicMock()
    cancelled: list[Any] = []
    t = MagicMock()
    t.cancel = lambda: cancelled.append(t)
    in_flight = {t}
    exits: list[int] = []
    handler = _build_signal_handler(stop, in_flight, _LOG, exits.append)

    handler()
    handler()
    assert cancelled == [t]
    assert exits == []


def test_signal_handler_third_hard_exits() -> None:
    stop = MagicMock()
    in_flight: set = set()
    exits: list[int] = []
    handler = _build_signal_handler(stop, in_flight, _LOG, exits.append)

    handler()
    handler()
    handler()
    assert exits == [1]


# ---------------------------------------------------------------------------
# Dispatch concurrency cap — Semaphore-gated spawn does not exceed the cap
# ---------------------------------------------------------------------------


async def test_gated_spawn_caps_concurrent_dispatches() -> None:
    """A burst of N coroutines through the gate never exceeds the configured cap."""
    cap = 3
    sem = asyncio.Semaphore(cap)
    in_flight: set[asyncio.Task] = set()
    live = 0
    peak = 0

    async def _work() -> None:
        nonlocal live, peak
        live += 1
        peak = max(peak, live)
        # Yield so other coroutines get a chance to run.
        await asyncio.sleep(0.01)
        live -= 1

    async def _gated(coro):
        async with sem:
            await coro

    def spawn(coro):
        task = asyncio.create_task(_gated(coro))
        in_flight.add(task)
        task.add_done_callback(in_flight.discard)
        return task

    for _ in range(10):
        spawn(_work())
    await asyncio.gather(*in_flight, return_exceptions=True)
    assert peak <= cap, f"peak concurrency {peak} exceeded cap {cap}"


# ---------------------------------------------------------------------------
# apply_patch FAILED log line — unparsable patch / no destinations / docker error
# ---------------------------------------------------------------------------


async def test_apply_patch_writes_failed_line_on_unparsable_patch(
    tmp_path: Path,
) -> None:
    """patch_ng returning a falsy patch_set surfaces FAILED + rc=-1."""
    patch_file = tmp_path / "bogus.patch"
    patch_file.write_text("this is not a diff at all\n")
    fake_docker = MagicMock()
    fake_docker.containers.get = AsyncMock()
    log_path = tmp_path / "logs" / "pre-start.log"

    result = await apply_patch(fake_docker, "target", patch_file, log_path, _LOG)
    assert result.returncode == -1
    content = log_path.read_text()
    assert "apply_patch FAILED" in content
    assert patch_file.name in content


async def test_apply_patch_failed_line_when_no_destinations(tmp_path: Path) -> None:
    """A patch with only ``--- /dev/null`` + ``+++ /dev/null`` items has no
    putback or fetch paths; apply_patch refuses with FAILED + rc=-1."""
    patch_file = tmp_path / "nowhere.patch"
    patch_file.write_text("--- /dev/null\n+++ /dev/null\n@@ -0,0 +0,0 @@\n")
    fake_docker = MagicMock()
    fake_docker.containers.get = AsyncMock()
    log_path = tmp_path / "logs" / "pre-start.log"

    result = await apply_patch(fake_docker, "target", patch_file, log_path, _LOG)
    assert result.returncode == -1
    assert "apply_patch FAILED" in log_path.read_text()


# ---------------------------------------------------------------------------
# put_archive_dir failure branches — DockerError and OSError
# ---------------------------------------------------------------------------


async def test_put_archive_dir_logs_failed_on_docker_error(tmp_path: Path) -> None:
    """``containers.get`` raising DockerError → rc=-1 + FAILED line."""
    src = tmp_path / "tree"
    src.mkdir()
    (src / "marker").write_text("data")
    log_path = tmp_path / "logs" / "pre-start.log"

    fake_docker = MagicMock()
    fake_docker.containers.get = AsyncMock(
        side_effect=DockerError(500, {"message": "daemon gone"})
    )
    result = await rocs.put_archive_dir(fake_docker, "addon_x", src, log_path, _LOG)
    assert result.returncode == -1
    content = log_path.read_text()
    assert "put_archive FAILED" in content
    assert "docker error" in content
    assert "daemon gone" in content


async def test_put_archive_dir_logs_failed_on_os_error(
    monkeypatch, tmp_path: Path
) -> None:
    """A read-time OSError (e.g. permission denied while tarring) → rc=-1."""
    src = tmp_path / "tree"
    src.mkdir()
    (src / "marker").write_text("data")
    log_path = tmp_path / "logs" / "pre-start.log"

    def _boom(_src_dir):
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr("container_hooks.docker._build_dir_tree_tar", _boom)
    fake_docker = MagicMock()
    fake_docker.containers.get = AsyncMock()  # never called: tar build fails first

    result = await rocs.put_archive_dir(fake_docker, "addon_x", src, log_path, _LOG)
    assert result.returncode == -1
    content = log_path.read_text()
    assert "put_archive FAILED" in content
    assert "OS error" in content
    assert "Permission denied" in content
