# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for the MQTT health publisher's non-lifecycle helpers.

The aiomqtt session, reconnect backoff, and birth-message listener are
integration-shaped and covered by manual verification; the poll +
publish path (the "what state landed on MQTT for this container?"
question) is the primary correctness surface and gets recording-client
coverage here.
"""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from container_hooks.config import ContainerOverride, Options
from container_hooks.health import HealthResult
from container_hooks.health_publisher import (
    HealthPublisher,
    _friendly_name,
    _list_recipe_containers,
    _sentinel_for,
)


class _RecordingClient:
    """Async MQTT publisher stub."""

    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def publish(
        self,
        topic: str,
        payload: bytes | str = "",
        qos: int = 0,
        retain: bool = False,
    ) -> None:
        self.calls.append(
            {"topic": topic, "payload": payload, "qos": qos, "retain": retain}
        )


# --- pure helpers ----------------------------------------------------------


class TestFriendlyName:
    def test_app_prefix(self):
        assert _friendly_name("app_5c53de3b_esphome") == "Esphome"

    def test_addon_prefix_legacy(self):
        assert _friendly_name("addon_5c53de3b_signalk_bridge") == "Signalk Bridge"

    def test_non_supervisor_container_unchanged(self):
        assert _friendly_name("mqtt-broker") == "mqtt-broker"

    def test_no_underscore_after_slug_falls_through(self):
        # ``app_slug`` (no third component) doesn't produce a name.
        assert _friendly_name("app_slug") == "app_slug"


class TestListRecipeContainers:
    def test_returns_dirs_with_pre_start_files(self, tmp_path: Path):
        (tmp_path / "app_a" / "pre-start-files").mkdir(parents=True)
        (tmp_path / "app_a" / "pre-start-files" / "marker").write_text("x")
        (tmp_path / "app_b" / "scripts").mkdir(parents=True)  # post-start only
        (tmp_path / "app_c" / "pre-start-files").mkdir(parents=True)
        (tmp_path / "app_c" / "pre-start-files" / "marker").write_text("y")
        opts = Options(base_dir=tmp_path)
        assert _list_recipe_containers(opts) == ["app_a", "app_c"]

    def test_skips_skip_containers(self, tmp_path: Path):
        (tmp_path / "app_a" / "pre-start-files").mkdir(parents=True)
        (tmp_path / "app_a" / "pre-start-files" / "m").write_text("")
        (tmp_path / "app_b" / "pre-start-files").mkdir(parents=True)
        (tmp_path / "app_b" / "pre-start-files" / "m").write_text("")
        opts = Options(base_dir=tmp_path, skip_containers=("app_b",))
        assert _list_recipe_containers(opts) == ["app_a"]

    def test_missing_base_dir_empty(self, tmp_path: Path):
        opts = Options(base_dir=tmp_path / "does-not-exist")
        assert _list_recipe_containers(opts) == []


class TestSentinelFor:
    def test_finds_override_by_name(self):
        opts = Options(
            container_overrides=(
                ContainerOverride(container="app_x", success_sentinel="/dev/shm/x"),
            )
        )
        assert _sentinel_for(opts, "app_x") == "/dev/shm/x"

    def test_returns_none_if_no_override(self):
        opts = Options()
        assert _sentinel_for(opts, "app_x") is None

    def test_returns_none_if_override_has_no_sentinel(self):
        opts = Options(container_overrides=(ContainerOverride(container="app_x"),))
        assert _sentinel_for(opts, "app_x") is None


# --- publish path end-to-end -----------------------------------------------


@pytest.fixture
def publisher(tmp_path: Path) -> HealthPublisher:
    """A publisher wired against tmp_path with two recipes."""
    (tmp_path / "app_esphome" / "pre-start-files").mkdir(parents=True)
    (tmp_path / "app_esphome" / "pre-start-files" / "sitecustomize.py").write_text(
        "pass"
    )
    (tmp_path / "app_other" / "pre-start-files").mkdir(parents=True)
    (tmp_path / "app_other" / "pre-start-files" / "x").write_text("")
    opts = Options(
        base_dir=tmp_path,
        mqtt_host="localhost",
        mqtt_base_topic="container_hooks",
        mqtt_discovery_prefix="homeassistant",
        client_id="container-hooks",
        container_overrides=(
            ContainerOverride(
                container="app_esphome",
                success_sentinel="/dev/shm/container_hooks-esphome-ok",
            ),
        ),
    )
    return HealthPublisher(opts, docker=object(), stop=asyncio.Event())  # type: ignore[arg-type]


class TestPollOnce:
    @pytest.mark.asyncio
    async def test_publishes_applied_only_when_no_sentinel(
        self, publisher: HealthPublisher
    ):
        # Enumerate slugs before poll so _publish_discovery isn't required.
        publisher._rebuild_slug_map()
        client = _RecordingClient()
        with (
            patch(
                "container_hooks.health_publisher.check_applied",
                new=AsyncMock(return_value=HealthResult(True, "fresh")),
            ),
            patch(
                "container_hooks.health_publisher.check_sentinel",
                new=AsyncMock(return_value=HealthResult(True, "sentinel present")),
            ),
        ):
            await publisher._poll_once(client)  # type: ignore[arg-type]

        topics = {c["topic"] for c in client.calls}
        # app_other has no sentinel: applied + summary states + attrs only.
        assert "container_hooks/app_other/applied/state" in topics
        assert "container_hooks/app_other/summary/state" in topics
        assert "container_hooks/app_other/sentinel/state" not in topics
        # app_esphome has a sentinel: all three.
        assert "container_hooks/app_esphome/applied/state" in topics
        assert "container_hooks/app_esphome/sentinel/state" in topics
        assert "container_hooks/app_esphome/summary/state" in topics

    @pytest.mark.asyncio
    async def test_summary_state_when_both_pass(self, publisher: HealthPublisher):
        publisher._rebuild_slug_map()
        client = _RecordingClient()
        with (
            patch(
                "container_hooks.health_publisher.check_applied",
                new=AsyncMock(return_value=HealthResult(True, "fresh")),
            ),
            patch(
                "container_hooks.health_publisher.check_sentinel",
                new=AsyncMock(return_value=HealthResult(True, "sentinel present")),
            ),
        ):
            await publisher._poll_once(client)  # type: ignore[arg-type]

        summary = next(
            c
            for c in client.calls
            if c["topic"] == "container_hooks/app_esphome/summary/state"
        )
        assert summary["payload"] == "ok"

    @pytest.mark.asyncio
    async def test_summary_state_flags_boot_race(self, publisher: HealthPublisher):
        publisher._rebuild_slug_map()
        client = _RecordingClient()
        with (
            patch(
                "container_hooks.health_publisher.check_applied",
                new=AsyncMock(return_value=HealthResult(False, "stale")),
            ),
            patch(
                "container_hooks.health_publisher.check_sentinel",
                new=AsyncMock(return_value=HealthResult(False, "missing")),
            ),
        ):
            await publisher._poll_once(client)  # type: ignore[arg-type]

        summary = next(
            c
            for c in client.calls
            if c["topic"] == "container_hooks/app_esphome/summary/state"
        )
        assert summary["payload"] == "boot_race"

    @pytest.mark.asyncio
    async def test_summary_state_flags_payload_no_effect(
        self, publisher: HealthPublisher
    ):
        publisher._rebuild_slug_map()
        client = _RecordingClient()
        with (
            patch(
                "container_hooks.health_publisher.check_applied",
                new=AsyncMock(return_value=HealthResult(True, "fresh")),
            ),
            patch(
                "container_hooks.health_publisher.check_sentinel",
                new=AsyncMock(return_value=HealthResult(False, "missing")),
            ),
        ):
            await publisher._poll_once(client)  # type: ignore[arg-type]

        summary = next(
            c
            for c in client.calls
            if c["topic"] == "container_hooks/app_esphome/summary/state"
        )
        assert summary["payload"] == "payload_no_effect"

    @pytest.mark.asyncio
    async def test_stop_bails_between_containers(self, publisher: HealthPublisher):
        publisher._rebuild_slug_map()
        # Set stop BEFORE any publish; _poll_once should return immediately
        # because the first container check happens after the stop check.
        publisher.stop.set()
        client = _RecordingClient()
        with (
            patch(
                "container_hooks.health_publisher.check_applied",
                new=AsyncMock(return_value=HealthResult(True, "fresh")),
            ),
        ):
            await publisher._poll_once(client)  # type: ignore[arg-type]
        assert client.calls == []


class TestPublishDiscovery:
    @pytest.mark.asyncio
    async def test_clears_dropped_slugs_on_republish(
        self, publisher: HealthPublisher, tmp_path: Path
    ):
        # First discovery pass sees both recipes.
        await publisher._publish_discovery(_RecordingClient())  # type: ignore[arg-type]
        assert "app_esphome" in publisher._slugs
        assert "app_other" in publisher._slugs
        # Simulate the user deleting the app_other recipe on disk.
        import shutil

        shutil.rmtree(tmp_path / "app_other")
        client = _RecordingClient()
        await publisher._publish_discovery(client)  # type: ignore[arg-type]
        topics = [c["topic"] for c in client.calls]
        # Discovery configs for the removed slug get cleared (empty retained).
        cleared_config_topics = [
            t
            for t in topics
            if t.startswith("homeassistant/")
            and "container-hooks_app_other" in t
            and t.endswith("/config")
        ]
        assert cleared_config_topics, (
            f"expected empty-retained clears for app_other discovery, got {topics!r}"
        )
        # Retained state + attributes topics also cleared.
        assert "container_hooks/app_other/applied/state" in topics
        assert "container_hooks/app_other/summary/state" in topics
        cleared_state = [
            c
            for c in client.calls
            if c["topic"].startswith("container_hooks/app_other/")
        ]
        assert cleared_state
        assert all(c["payload"] == "" and c["retain"] for c in cleared_state)

    @pytest.mark.asyncio
    async def test_publishes_expected_component_set(self, publisher: HealthPublisher):
        client = _RecordingClient()
        await publisher._publish_discovery(client)  # type: ignore[arg-type]
        topics = {c["topic"] for c in client.calls}
        # app_esphome (with sentinel): applied + sentinel binaries + summary sensor.
        assert (
            "homeassistant/binary_sensor/container-hooks_app_esphome/applied/config"
            in topics
        )
        assert (
            "homeassistant/binary_sensor/container-hooks_app_esphome/sentinel/config"
            in topics
        )
        assert (
            "homeassistant/sensor/container-hooks_app_esphome/summary/config" in topics
        )
        # app_other (no sentinel): applied + summary but no sentinel.
        assert (
            "homeassistant/binary_sensor/container-hooks_app_other/applied/config"
            in topics
        )
        assert "homeassistant/sensor/container-hooks_app_other/summary/config" in topics
        # Clear-any-old-sentinel: an empty retained publish for a slug without
        # a sentinel is fine (idempotent), so it may be present.
        assert publisher._discovered is True
