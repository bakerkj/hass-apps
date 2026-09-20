# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Unit tests for the MQTT payload/topic helpers."""

import json

import pytest
from container_hooks.mqtt import (
    applied_discovery_payload,
    attributes_topic,
    availability_topic,
    clear_container_entities,
    clear_discovery,
    discovery_topic,
    keys_for,
    publish_discovery,
    publish_state,
    sentinel_discovery_payload,
    slugify,
    state_topic,
    summary_attributes,
    summary_discovery_payload,
    summary_state,
)


class _RecordingClient:
    """Async publisher stub. Captures every publish for later assertion."""

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


# --- slugify --------------------------------------------------------------


class TestSlugify:
    def test_addon_name(self):
        assert slugify("app_5c53de3b_esphome") == "app_5c53de3b_esphome"

    def test_hyphen_replaced(self):
        assert slugify("my-project-web-1") == "my_project_web_1"

    def test_dot_replaced(self):
        assert slugify("service.host.local") == "service_host_local"

    def test_collapse_underscores(self):
        assert slugify("a___b") == "a_b"

    def test_strip_leading_trailing_underscores(self):
        assert slugify("__foo__") == "foo"

    def test_empty_fallback(self):
        assert slugify("") == "unknown"
        assert slugify("---") == "unknown"


# --- topic helpers -------------------------------------------------------


class TestTopics:
    def test_availability(self):
        assert availability_topic("container_hooks") == "container_hooks/availability"

    def test_state(self):
        assert (
            state_topic("container_hooks", "esphome", "applied")
            == "container_hooks/esphome/applied/state"
        )

    def test_attrs(self):
        assert (
            attributes_topic("container_hooks", "esphome", "applied")
            == "container_hooks/esphome/applied/attributes"
        )

    def test_discovery(self):
        assert (
            discovery_topic(
                "homeassistant",
                "binary_sensor",
                "container-hooks",
                "esphome",
                "applied",
            )
            == "homeassistant/binary_sensor/container-hooks_esphome/applied/config"
        )


# --- discovery payload shapes --------------------------------------------


class TestDiscoveryPayloads:
    def test_applied_payload_shape(self):
        p = applied_discovery_payload(
            device_id="container-hooks",
            slug="esphome",
            friendly="ESPHome",
            base_topic="container_hooks",
            expire_after_s=120,
        )
        assert p["unique_id"] == "container-hooks_esphome_applied"
        assert p["state_topic"] == "container_hooks/esphome/applied/state"
        assert p["availability_topic"] == "container_hooks/availability"
        assert p["payload_on"] == "ON"
        assert p["payload_off"] == "OFF"
        assert p["expire_after"] == 120
        assert p["device"]["identifiers"] == ["container-hooks_esphome"]

    def test_expire_after_floor(self):
        # 60s floor: too-small values get widened rather than allowed to churn.
        p = applied_discovery_payload(
            device_id="container-hooks",
            slug="x",
            friendly="X",
            base_topic="t",
            expire_after_s=1,
        )
        assert p["expire_after"] == 60

    def test_sentinel_payload_shape(self):
        p = sentinel_discovery_payload(
            device_id="container-hooks",
            slug="esphome",
            friendly="ESPHome",
            base_topic="container_hooks",
            expire_after_s=120,
        )
        assert p["unique_id"] == "container-hooks_esphome_sentinel"

    def test_summary_payload_shape(self):
        p = summary_discovery_payload(
            device_id="container-hooks",
            slug="esphome",
            friendly="ESPHome",
            base_topic="container_hooks",
            expire_after_s=120,
        )
        assert p["unique_id"] == "container-hooks_esphome_summary"
        # summary is a plain sensor, not a binary — no payload_on/off.
        assert "payload_on" not in p
        assert p["state_topic"] == "container_hooks/esphome/summary/state"


# --- publish helpers ----------------------------------------------------


class TestPublish:
    @pytest.mark.asyncio
    async def test_publish_discovery_retained_json(self):
        client = _RecordingClient()
        payload = {"unique_id": "x", "name": "y"}
        await publish_discovery(
            client,
            payload,
            discovery_prefix="homeassistant",
            component="binary_sensor",
            device_id="container-hooks",
            slug="esphome",
            key="applied",
        )
        assert len(client.calls) == 1
        c = client.calls[0]
        assert (
            c["topic"]
            == "homeassistant/binary_sensor/container-hooks_esphome/applied/config"
        )
        assert c["retain"] is True
        assert c["qos"] == 1
        assert json.loads(c["payload"]) == payload

    @pytest.mark.asyncio
    async def test_clear_discovery_publishes_empty_retained(self):
        client = _RecordingClient()
        await clear_discovery(
            client,
            discovery_prefix="homeassistant",
            component="sensor",
            device_id="container-hooks",
            slug="esphome",
            key="summary",
        )
        assert client.calls == [
            {
                "topic": "homeassistant/sensor/container-hooks_esphome/summary/config",
                "payload": "",
                "qos": 1,
                "retain": True,
            }
        ]

    @pytest.mark.asyncio
    async def test_publish_state_retains_both_state_and_attributes(self):
        client = _RecordingClient()
        await publish_state(
            client,
            base_topic="container_hooks",
            slug="esphome",
            key="applied",
            state="ON",
            attributes={"reason": "put_archive newer than StartedAt"},
        )
        assert [c["topic"] for c in client.calls] == [
            "container_hooks/esphome/applied/state",
            "container_hooks/esphome/applied/attributes",
        ]
        assert all(c["retain"] for c in client.calls)

    @pytest.mark.asyncio
    async def test_publish_state_without_attributes_skips_second_publish(self):
        client = _RecordingClient()
        await publish_state(
            client,
            base_topic="container_hooks",
            slug="esphome",
            key="applied",
            state="ON",
            attributes=None,
        )
        assert len(client.calls) == 1

    @pytest.mark.asyncio
    async def test_clear_container_entities_clears_each_key(self):
        client = _RecordingClient()
        await clear_container_entities(
            client,
            discovery_prefix="homeassistant",
            device_id="container-hooks",
            slug="esphome",
            keys=("applied", "sentinel", "summary"),
        )
        topics = [c["topic"] for c in client.calls]
        assert (
            "homeassistant/binary_sensor/container-hooks_esphome/applied/config"
            in topics
        )
        assert (
            "homeassistant/binary_sensor/container-hooks_esphome/sentinel/config"
            in topics
        )
        assert "homeassistant/sensor/container-hooks_esphome/summary/config" in topics
        assert all(c["payload"] == "" for c in client.calls)


# --- summary_state matrix ------------------------------------------------


class TestSummaryState:
    def test_no_sentinel_healthy(self):
        assert summary_state("ON", None) == "ok"

    def test_no_sentinel_missed(self):
        assert summary_state("OFF", None) == "not_applied"

    def test_both_on(self):
        assert summary_state("ON", "ON") == "ok"

    def test_both_off_is_boot_race(self):
        assert summary_state("OFF", "OFF") == "boot_race"

    def test_applied_but_sentinel_missing(self):
        assert summary_state("ON", "OFF") == "payload_no_effect"

    def test_sentinel_stuck_on_without_applied(self):
        assert summary_state("OFF", "ON") == "stale_or_orphan"

    def test_unknown_applied_without_sentinel(self):
        # No sentinel + unknown applied MUST NOT collapse to not_applied —
        # that would false-alarm on any transient Docker-API hiccup.
        assert summary_state("unknown", None) == "unknown"

    def test_unknown_applied_with_sentinel(self):
        assert summary_state("unknown", "ON") == "unknown"
        assert summary_state("unknown", "OFF") == "unknown"

    def test_unknown_sentinel(self):
        assert summary_state("ON", "unknown") == "unknown"
        assert summary_state("OFF", "unknown") == "unknown"


class TestSummaryAttributes:
    def test_includes_both_reasons_and_flag(self):
        attrs = summary_attributes(
            applied_reason="put_archive newer than StartedAt",
            sentinel_reason="tmpfs sentinel /dev/shm/x present",
            sentinel_configured=True,
        )
        assert attrs["sentinel_configured"] is True
        assert attrs["applied_reason"].startswith("put_archive")
        assert attrs["sentinel_reason"].startswith("tmpfs")


# --- keys_for ------------------------------------------------------------


class TestKeysFor:
    def test_no_sentinel(self):
        assert keys_for(False) == ["applied", "summary"]

    def test_with_sentinel(self):
        assert keys_for(True) == ["applied", "summary", "sentinel"]
