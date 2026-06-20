# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""End-to-end harness: build the real addon image, run it against the
host docker daemon, and exercise hooks against a throwaway target
container.

Bring-up costs:
  * addon image build (~30-60 s once per session; cacheable via
    ``CONTAINER_HOOKS_IMAGE`` env var when CI prebuilds it via
    docker/build-push-action with a gha cache, mirroring the
    ``DEP_PROXY_IMAGE`` pattern in
    ``dashboard_entity_proxy/tests/integration/conftest.py``)
  * target ``alpine`` image pull (~5 s once per session)
  * addon + target container start (~500 ms each, per test)

Run with::

    pytest container_hooks/tests/integration/ -m e2e

The default ``pytest`` invocation excludes the ``e2e`` marker (see
``tests`` job in ``.github/workflows/tests.yml``).

Requirements: docker, host ``/var/run/docker.sock`` accessible. No HA
needed — these tests only exercise the addon's docker-events plumbing.
"""

import json
import os
import shutil
import subprocess
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

# Renovate tracks the addon BUILD_FROM and target image pins via these
# comments so a base-image bump opens a PR on conftest + Dockerfile +
# tests.yml together.
# renovate: datasource=docker depName=ghcr.io/home-assistant/amd64-base
ADDON_BASE_IMAGE = "ghcr.io/home-assistant/amd64-base:3.24"
# renovate: datasource=docker depName=alpine
TARGET_IMAGE = "alpine:3.20"

ADDON_SOURCE_DIR = Path(__file__).resolve().parents[2]
ADDON_BOOT_TIMEOUT = 30
DEFAULT_DEBOUNCE_SECONDS = 1


# ---------------------------------------------------------------------------
# session fixtures
# ---------------------------------------------------------------------------


def _docker(*args: str, check: bool = True, capture: bool = True) -> str:
    """Thin wrapper around ``docker`` so error output is visible on failure.

    Merges stderr into stdout so ``docker logs`` returns the addon's log
    output — Python ``logging.basicConfig`` writes to stderr by default.
    """
    if capture:
        result = subprocess.run(
            ["docker", *args],
            check=check,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    else:
        result = subprocess.run(["docker", *args], check=check, text=True)
    return result.stdout or ""


def _have_docker() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        _docker("version", "--format", "{{.Server.Version}}")
    except subprocess.CalledProcessError, FileNotFoundError:
        return False
    return True


@pytest.fixture(scope="session", autouse=True)
def _require_docker() -> None:
    if not _have_docker():
        pytest.skip("docker daemon not reachable; skipping e2e suite")


@pytest.fixture(scope="session")
def addon_image() -> str:
    """Build (or reuse) the container_hooks addon image.

    If ``CONTAINER_HOOKS_IMAGE`` is set, trust the caller (CI prebuild)
    and use that tag directly. Otherwise build locally with the addon's
    Dockerfile.
    """
    prebuilt = os.environ.get("CONTAINER_HOOKS_IMAGE")
    if prebuilt:
        return prebuilt

    tag = f"container_hooks_e2e:{uuid.uuid4().hex[:8]}"
    _docker(
        "build",
        "--build-arg",
        f"BUILD_FROM={ADDON_BASE_IMAGE}",
        "--build-arg",
        "BUILD_VERSION=e2e",
        "-t",
        tag,
        str(ADDON_SOURCE_DIR),
    )
    return tag


@pytest.fixture(scope="session", autouse=True)
def _pull_target_image() -> None:
    """Ensure the throwaway target image is local before any test runs."""
    _docker("pull", TARGET_IMAGE)


# ---------------------------------------------------------------------------
# per-test fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def hooks_dir(tmp_path: Path) -> Path:
    """A tmp dir mounted into the addon as ``/homeassistant/container_hooks``."""
    d = tmp_path / "hooks"
    d.mkdir()
    return d


@pytest.fixture
def options_path(tmp_path: Path) -> Path:
    """A tmp ``/data/options.json`` for the addon to read."""
    d = tmp_path / "data"
    d.mkdir()
    return d / "options.json"


def _write_options(path: Path, **overrides: Any) -> None:
    base: dict[str, Any] = {
        "log_level": "DEBUG",
        "base_dir": "/homeassistant/container_hooks",
        "initial_sweep": True,
        "debounce_seconds": DEFAULT_DEBOUNCE_SECONDS,
        "skip_containers": [],
        "container_overrides": [],
        "watch_create_events": False,
    }
    base.update(overrides)
    path.write_text(json.dumps(base))


def _wait_for(
    predicate: Callable[[], bool], timeout: float, interval: float = 0.1
) -> bool:
    """Poll ``predicate`` until it returns truthy or the timeout elapses."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


@pytest.fixture
def unique_name() -> str:
    """A unique container name per test (so leaks from prior runs don't collide)."""
    return f"ch_e2e_{uuid.uuid4().hex[:10]}"


class _AddonHandle:
    """Lifecycle handle for the addon container under test."""

    def __init__(self, name: str, hooks_dir: Path, options_path: Path) -> None:
        self.name = name
        self.hooks_dir = hooks_dir
        self.options_path = options_path

    def start(self, image: str) -> None:
        # ``--rm`` so a leaked container from a crashed test self-cleans;
        # ``--init`` because the addon's run.sh assumes a real PID 1 with
        # signal handling.
        _docker(
            "run",
            "-d",
            "--rm",
            "--init",
            "--name",
            self.name,
            "-v",
            "/var/run/docker.sock:/var/run/docker.sock",
            "-v",
            f"{self.options_path}:/data/options.json:ro",
            "-v",
            f"{self.hooks_dir}:/homeassistant/container_hooks",
            image,
        )
        self._wait_for_log("Configuration:", timeout=ADDON_BOOT_TIMEOUT)

    def _wait_for_log(self, needle: str, timeout: float) -> None:
        def _hit() -> bool:
            try:
                logs = _docker("logs", self.name, capture=True)
            except subprocess.CalledProcessError:
                return False
            return needle in logs

        if not _wait_for(_hit, timeout):
            logs = _docker("logs", self.name, check=False, capture=True)
            raise AssertionError(
                f"addon log never contained {needle!r} within {timeout}s; "
                f"recent log:\n{logs[-2000:]}"
            )

    def logs(self) -> str:
        return _docker("logs", self.name, check=False, capture=True)

    def stop(self) -> None:
        _docker("rm", "-f", self.name, check=False, capture=True)


@pytest.fixture
def addon(
    addon_image: str,
    hooks_dir: Path,
    options_path: Path,
    unique_name: str,
) -> Iterator[_AddonHandle]:
    handle = _AddonHandle(f"{unique_name}_addon", hooks_dir, options_path)
    try:
        yield handle
    finally:
        handle.stop()


class _TargetHandle:
    """Lifecycle handle for a throwaway target container."""

    def __init__(self, name: str) -> None:
        self.name = name

    def create(self, *extra_args: str) -> None:
        """Create the target without starting it (so pre-start hooks fire first).

        The addon-side ``container_created`` dispatch races against
        ``docker start``; tests that want to verify pre-start outcomes
        should create then start.
        """
        _docker(
            "create",
            "--name",
            self.name,
            *extra_args,
            TARGET_IMAGE,
            "sleep",
            "3600",
        )

    def start(self) -> None:
        _docker("start", self.name)

    def run(self, *extra_args: str) -> None:
        """Create + start in one shot (fires ``container_start`` immediately)."""
        _docker(
            "run",
            "-d",
            "--name",
            self.name,
            *extra_args,
            TARGET_IMAGE,
            "sleep",
            "3600",
        )

    def stop(self) -> None:
        _docker("rm", "-f", self.name, check=False, capture=True)

    def exec_check(self, *cmd: str) -> bool:
        """Return True iff the command exits 0 inside the running target."""
        try:
            subprocess.run(
                ["docker", "exec", self.name, *cmd],
                check=True,
                capture_output=True,
            )
            return True
        except subprocess.CalledProcessError:
            return False

    def exec_capture(self, *cmd: str) -> str:
        return subprocess.run(
            ["docker", "exec", self.name, *cmd],
            check=True,
            capture_output=True,
            text=True,
        ).stdout


@pytest.fixture
def target(unique_name: str) -> Iterator[_TargetHandle]:
    handle = _TargetHandle(f"{unique_name}_target")
    try:
        yield handle
    finally:
        handle.stop()


@pytest.fixture
def control_target(unique_name: str) -> Iterator[_TargetHandle]:
    """A second throwaway target, used as a positive-control synchronizer.

    Tests that need to assert "no dispatch happened for X" otherwise
    have nothing concrete to wait on. Starting a control container
    *after* X and waiting for its hook to fire proves the addon has
    processed the relevant events; by then it's safe to assert the
    negative on X.
    """
    handle = _TargetHandle(f"{unique_name}_ctrl")
    try:
        yield handle
    finally:
        handle.stop()


# ---------------------------------------------------------------------------
# helpers re-exported as fixtures so tests stay readable
# ---------------------------------------------------------------------------


@pytest.fixture
def write_options() -> Callable[..., None]:
    return _write_options


@pytest.fixture
def wait_for() -> Callable[..., bool]:
    return _wait_for
