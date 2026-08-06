# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""End-to-end publish-path test.

Drives the bridge's real publish path -- a Signal K vessel tree through
``resolve_entities``/``resolve_special`` and out via ``mqtt.publish_discovery``
and the state/attribute topics -- against a recording MQTT client (the same
approach ``container_info_mqtt`` uses). Asserts the exact discovery topics,
payloads, and converted state values Home Assistant would actually receive, so a
regression anywhere from parsing to conversion to MQTT framing is caught.
"""

import json
from typing import Any

import pytest
from signalk_bridge.app import resolve_entities, resolve_special
from signalk_bridge.mqtt import (
    attributes_topic,
    availability_topic,
    publish_discovery,
    state_topic,
)


class RecordingClient:
    """Minimal aiomqtt.Client stub that records publish() calls.

    Matches the ``_Publisher`` protocol in ``signalk_bridge.mqtt``: an async
    ``publish(topic, payload=..., qos=..., retain=...)``.
    """

    def __init__(self) -> None:
        self.published: list[tuple[str, str, int, bool]] = []

    async def publish(
        self,
        topic: str,
        payload: bytes | str = "",
        qos: int = 0,
        retain: bool = False,
    ) -> None:
        if isinstance(payload, bytes):
            payload = payload.decode()
        self.published.append((topic, payload, qos, retain))


async def _publish_all(tree: dict[str, Any]) -> RecordingClient:
    """Mirror the per-cycle publish loop in app.main()."""
    client = RecordingClient()
    entities = {**resolve_entities(tree), **resolve_special(tree)}
    for key, ent in entities.items():
        await publish_discovery(client, "homeassistant", "signalk", key, ent, 40)
        state = ent.get("state")
        if state is not None:
            await client.publish(state_topic("signalk", key), payload=state, qos=0)
        attrs = ent.get("attributes")
        if attrs is not None:
            await client.publish(
                attributes_topic("signalk", key), payload=json.dumps(attrs), qos=0
            )
    await client.publish(
        availability_topic("signalk"), payload="online", qos=1, retain=True
    )
    return client


def _by_topic(client: RecordingClient) -> dict[str, tuple[str, int, bool]]:
    return {t: (p, q, r) for (t, p, q, r) in client.published}


@pytest.mark.asyncio
async def test_numeric_sensor_discovery_and_state(vessel_tree: dict[str, Any]) -> None:
    by = _by_topic(await _publish_all(vessel_tree))
    cfg = "homeassistant/sensor/signalk/navigation_speedoverground/config"
    assert cfg in by
    payload, qos, retain = by[cfg]
    d = json.loads(payload)
    assert d["name"] == "Speed over ground"
    assert d["unit_of_measurement"] == "m/s"
    assert d["device_class"] == "speed"
    assert d["device"]["identifiers"] == ["signalk_navigation"]
    assert qos == 1 and retain is True
    # state carries the (identity-converted) value
    assert by["signalk/navigation_speedoverground/state"][0] == "3.086"


@pytest.mark.asyncio
async def test_voltage_display_precision_is_two(vessel_tree: dict[str, Any]) -> None:
    # Voltage should default to 2 decimals in HA; the state keeps full precision.
    by = _by_topic(await _publish_all(vessel_tree))
    cfg = json.loads(
        by["homeassistant/sensor/signalk/electrical_batteries_house_voltage/config"][0]
    )
    assert cfg["device_class"] == "voltage"
    assert cfg["suggested_display_precision"] == 2


@pytest.mark.asyncio
async def test_conversions_reach_mqtt(vessel_tree: dict[str, Any]) -> None:
    by = _by_topic(await _publish_all(vessel_tree))
    # 1.5708 rad -> ~90 deg (bearing, wrapped 0..360)
    cog = float(by["signalk/navigation_courseovergroundtrue/state"][0])
    assert abs(cog - 90.0) < 0.01
    # 288.15 K -> 15 C
    assert by["signalk/environment_water_temperature/state"][0] == "15"
    # 0.87 ratio -> 87 %
    assert by["signalk/electrical_batteries_house_stateofcharge/state"][0] == "87"


@pytest.mark.asyncio
async def test_binary_sensor_and_device_tracker(vessel_tree: dict[str, Any]) -> None:
    by = _by_topic(await _publish_all(vessel_tree))
    # digital switch -> binary_sensor discovery + ON state
    assert (
        "homeassistant/binary_sensor/signalk/electrical_switches_bank_0_1_state/config"
        in by
    )
    assert by["signalk/electrical_switches_bank_0_1_state/state"][0] == "ON"
    # position -> device_tracker with lat/lon attributes, no state topic
    assert "homeassistant/device_tracker/signalk/navigation_position/config" in by
    attrs = json.loads(by["signalk/navigation_position/attributes"][0])
    assert attrs["latitude"] == 40.0
    assert attrs["longitude"] == -70.0
    assert "signalk/navigation_position/state" not in by


@pytest.mark.asyncio
async def test_availability_online_published(vessel_tree: dict[str, Any]) -> None:
    by = _by_topic(await _publish_all(vessel_tree))
    assert by["signalk/availability"] == ("online", 1, True)


@pytest.mark.asyncio
async def test_diagnostic_catch_all_reaches_discovery_payload() -> None:
    # The catch-all's entity_category (and the bool->binary_sensor path) must
    # survive all the way into the published discovery config, not just the
    # entity dict -- a typo in the mqtt passthrough would silently promote every
    # "(other)" sensor onto the primary device pages.
    from signalk_bridge.app import _unmapped_entities

    client = RecordingClient()
    ents = _unmapped_entities(
        {"electrical.venus.totalPanelPower": 111.0, "electrical.venus.relay": True},
        set(),
    )
    for key, ent in ents.items():
        await publish_discovery(client, "homeassistant", "signalk", key, ent, 40)
    by = _by_topic(client)
    scfg = json.loads(
        by["homeassistant/sensor/signalk/electrical_venus_totalpanelpower/config"][0]
    )
    assert scfg["entity_category"] == "diagnostic"
    bcfg = json.loads(
        by["homeassistant/binary_sensor/signalk/electrical_venus_relay/config"][0]
    )
    assert bcfg["entity_category"] == "diagnostic"
    assert bcfg["payload_on"] == "ON"
    assert bcfg["payload_off"] == "OFF"
