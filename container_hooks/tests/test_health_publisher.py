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


class _FakeMessage:
    def __init__(self, topic: str, payload: bytes) -> None:
        self.topic = topic
        self.payload = payload


class _FakeMqttClient:
    """Minimal aiomqtt.Client stub for _scan_retained_slugs.

    ``messages`` is a single stable async iterator (as with real
    aiomqtt — backed by an internal queue, not a fresh generator per
    access). It yields the pre-loaded retained messages, then hangs
    forever so ``asyncio.wait_for`` in the scan hits its timeout the
    same way it would against a live broker after the retained flush.
    """

    def __init__(self, retained: list[tuple[str, bytes]]) -> None:
        self._retained = list(retained)
        self._msg_iter = self._iter_impl()
        self.subscribed: list[str] = []
        self.unsubscribed: list[str] = []

    async def subscribe(self, topic: str, qos: int = 0) -> None:
        self.subscribed.append(topic)

    async def unsubscribe(self, topic: str) -> None:
        self.unsubscribed.append(topic)

    @property
    def messages(self):
        return self._msg_iter

    async def _iter_impl(self):
        for topic, payload in self._retained:
            yield _FakeMessage(topic, payload)
        # After the retained backlog, hang forever so the scan hits its
        # timeout the same way it would against a live broker.
        await asyncio.Event().wait()


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
    async def test_publishes_online_slug_availability_when_check_returns(
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
                new=AsyncMock(return_value=HealthResult(True, "sentinel")),
            ),
        ):
            await publisher._poll_once(client)  # type: ignore[arg-type]
        online_calls = [
            c
            for c in client.calls
            if c["topic"].endswith("/availability") and c["payload"] == "online"
        ]
        assert len(online_calls) == 2  # one per slug

    @pytest.mark.asyncio
    async def test_publishes_offline_slug_availability_when_target_unreachable(
        self, publisher: HealthPublisher
    ):
        publisher._rebuild_slug_map()
        client = _RecordingClient()
        # Both checks return None → target unreachable → per-slug availability flips offline.
        with (
            patch(
                "container_hooks.health_publisher.check_applied",
                new=AsyncMock(return_value=None),
            ),
            patch(
                "container_hooks.health_publisher.check_sentinel",
                new=AsyncMock(return_value=None),
            ),
        ):
            await publisher._poll_once(client)  # type: ignore[arg-type]
        offline_avail = [
            c
            for c in client.calls
            if c["topic"].endswith("/availability") and c["payload"] == "offline"
        ]
        assert offline_avail

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


def _mqtt_topic_filter_valid(topic_filter: str) -> bool:
    """Validate an MQTT topic filter against MQTT-4.7.1.2 / 4.7.1.3.

    ``+`` must occupy an entire level (no ``foo+`` or ``+bar``). ``#``
    must be the last level. Empty levels not allowed except a single
    empty first level (``/foo``). Kept tight so a regression to the
    old ``{client_id}_+`` shape (which mosquitto silently refuses) is
    caught locally.
    """
    if not topic_filter:
        return False
    levels = topic_filter.split("/")
    for i, level in enumerate(levels):
        if level == "":
            # Allow only leading empty (``/foo``), reject internal empty.
            if i != 0:
                return False
            continue
        if "+" in level and level != "+":
            return False
        if "#" in level and (level != "#" or i != len(levels) - 1):
            return False
    return True


class TestScanRetainedSlugs:
    @pytest.mark.asyncio
    async def test_subscribe_filter_is_valid_mqtt_wildcard(
        self, publisher: HealthPublisher, monkeypatch: pytest.MonkeyPatch
    ):
        """Regression: the retained-scan filter used to be
        ``{prefix}/+/{client_id}_+/+/config``, which mixes a literal
        prefix with a `+` in the same topic level. Mosquitto rejects
        that at SUBACK without raising a client-side exception, so the
        scan silently returned an empty set against every real broker.
        This test would have caught it."""
        monkeypatch.setattr(
            "container_hooks.health_publisher._RETAINED_SCAN_TIMEOUT", 0.05
        )
        client = _FakeMqttClient([])
        await publisher._scan_retained_slugs(client)  # type: ignore[arg-type]
        assert client.subscribed, "scan should have subscribed to a filter"
        for topic in client.subscribed:
            assert _mqtt_topic_filter_valid(topic), (
                f"MQTT topic filter {topic!r} violates spec "
                "(a `+` wildcard must occupy an entire level)"
            )

    def test_one_shot_flag_defaults_false(self, publisher: HealthPublisher):
        # Regression: retained-scan is a cross-process-restart concern.
        # A fresh HealthPublisher must NOT have run it yet; the outer
        # reconnect loop gates on ``self._did_retained_scan``, and a
        # mid-session MQTT reconnect within the same process should
        # skip the scan entirely (5s stall + broker-wide discovery
        # subscribe otherwise happens on every network blip).
        assert publisher._did_retained_scan is False

    @pytest.mark.asyncio
    async def test_extracts_slug_from_discovery_topics(
        self, publisher: HealthPublisher, monkeypatch: pytest.MonkeyPatch
    ):
        # Shorten the timeout so the test doesn't hang for 5s.
        monkeypatch.setattr(
            "container_hooks.health_publisher._RETAINED_SCAN_TIMEOUT", 0.1
        )
        client = _FakeMqttClient(
            [
                (
                    "homeassistant/binary_sensor/container-hooks_alpha/applied/config",
                    b'{"unique_id":"...}"',
                ),
                (
                    "homeassistant/binary_sensor/container-hooks_alpha/sentinel/config",
                    b'{"unique_id":"...}"',
                ),
                (
                    "homeassistant/sensor/container-hooks_alpha/summary/config",
                    b'{"unique_id":"...}"',
                ),
                (
                    "homeassistant/binary_sensor/container-hooks_beta/applied/config",
                    b'{"unique_id":"...}"',
                ),
                # Tombstone (empty payload) — should NOT count.
                (
                    "homeassistant/binary_sensor/container-hooks_dead/applied/config",
                    b"",
                ),
                # Foreign device_id (someone else's client) — should NOT count.
                (
                    "homeassistant/binary_sensor/other-client_zeta/applied/config",
                    b'{"unique_id":"...}"',
                ),
                # Malformed topic (wrong number of segments) — ignore.
                (
                    "homeassistant/binary_sensor/container-hooks_x/applied/config/extra",
                    b'{"unique_id":"...}"',
                ),
            ]
        )
        result = await publisher._scan_retained_slugs(client)  # type: ignore[arg-type]
        assert result == {"alpha", "beta"}
        # Confirm we subscribed to the wildcard and unsubscribed after.
        assert any("+/config" in t for t in client.subscribed)
        assert client.unsubscribed == client.subscribed


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
    async def test_clears_slugs_only_present_on_broker_at_startup(
        self, publisher: HealthPublisher
    ):
        # Simulate the retained-scan finding a slug the addon has no
        # in-memory record of (recipe was removed while addon was down).
        publisher._retained_slugs_at_start = {"ghost_slug"}
        client = _RecordingClient()
        await publisher._publish_discovery(client)  # type: ignore[arg-type]
        topics = [c["topic"] for c in client.calls]
        # The ghost slug's discovery + retained state topics get cleared
        # even though it was never in self._slugs during this process.
        assert (
            "homeassistant/binary_sensor/container-hooks_ghost_slug/applied/config"
            in topics
        )
        assert "container_hooks/ghost_slug/applied/state" in topics
        assert "container_hooks/ghost_slug/summary/state" in topics
        # And the set is consumed — a subsequent republish shouldn't
        # re-clear it (would be idempotent anyway, but the state is
        # gone from the publisher).
        assert publisher._retained_slugs_at_start == set()

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
