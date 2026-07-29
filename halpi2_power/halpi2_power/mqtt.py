# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""MQTT discovery publishing for the HALPI2 power controller."""

import json
from typing import Any

import paho.mqtt.client as mqtt

from .sensors import DERIVED_DEFS, POWER_STATE_DEF, POWER_STATE_KEY, SENSOR_DEFS


class MqttHealth:
    def __init__(self) -> None:
        self.connected: bool = False
        self.last_connect_ok: float = 0.0
        self.last_disconnect: float = 0.0
        # Set by the MQTT thread when Home Assistant sends its birth message, so
        # the main loop knows to republish discovery. Read/reset in the loop.
        self.rediscover: bool = False


def _device_block(device_id: str, values: dict[str, Any]) -> dict[str, Any]:
    """HA device metadata. All entities share this so they group under one device."""
    block: dict[str, Any] = {
        "identifiers": [device_id],
        "name": "HALPI2 Power Controller",
        "manufacturer": "Hat Labs",
        "model": "HALPI2",
    }
    firmware = values.get("firmware_version")
    hardware = values.get("hardware_version")
    if firmware:
        block["sw_version"] = str(firmware)
    if hardware:
        block["hw_version"] = str(hardware)
    return block


def availability_topic(base_topic: str) -> str:
    return f"{base_topic}/availability"


def state_topic(base_topic: str, key: str) -> str:
    return f"{base_topic}/{key}/state"


def publish_discovery(
    client: mqtt.Client,
    discovery_prefix: str,
    device_id: str,
    base_topic: str,
    key: str,
    definition: dict[str, Any],
    values: dict[str, Any],
    expire_after_s: int,
) -> None:
    """Publish one retained discovery config."""
    config_topic = f"{discovery_prefix}/sensor/{device_id}/{key}/config"

    payload: dict[str, Any] = {
        "name": definition["name"],
        "has_entity_name": True,
        "unique_id": f"{device_id}_{key}",
        # `object_id` is the documented MQTT-discovery hint for the entity_id
        # (yields sensor.halpi2_<key>). `default_entity_id` is not a real
        # discovery key -- HA drops it, so the intended ids were never applied.
        "object_id": f"halpi2_{key}",
        "state_topic": state_topic(base_topic, key),
        "availability_topic": availability_topic(base_topic),
        "payload_available": "online",
        "payload_not_available": "offline",
        # If the publisher dies without a clean disconnect, entities go
        # unavailable rather than showing indefinitely stale power readings.
        "expire_after": max(5, int(expire_after_s)),
        "device": _device_block(device_id, values),
    }

    for src, dst in (
        ("unit", "unit_of_measurement"),
        ("icon", "icon"),
        ("device_class", "device_class"),
        ("state_class", "state_class"),
        ("options", "options"),
        ("suggested_display_precision", "suggested_display_precision"),
    ):
        if src in definition and definition[src] is not None:
            payload[dst] = definition[src]

    client.publish(config_topic, json.dumps(payload), qos=1, retain=True)


def publish_all_discovery(
    client: mqtt.Client,
    discovery_prefix: str,
    device_id: str,
    base_topic: str,
    values: dict[str, Any],
    expire_after_s: int,
) -> None:
    for key, definition in SENSOR_DEFS.items():
        publish_discovery(
            client,
            discovery_prefix,
            device_id,
            base_topic,
            key,
            definition,
            values,
            expire_after_s,
        )
    publish_discovery(
        client,
        discovery_prefix,
        device_id,
        base_topic,
        POWER_STATE_KEY,
        POWER_STATE_DEF,
        values,
        expire_after_s,
    )
    # Derived countdown entities. These are published every cycle like the rest,
    # so the normal expire_after applies and they go unavailable with everything
    # else if the publisher dies.
    for key, definition in DERIVED_DEFS.items():
        publish_discovery(
            client,
            discovery_prefix,
            device_id,
            base_topic,
            key,
            definition,
            values,
            expire_after_s,
        )


def clear_discovery(
    client: mqtt.Client,
    discovery_prefix: str,
    device_id: str,
    key: str,
) -> None:
    """Remove a retained discovery config by publishing an empty payload."""
    client.publish(
        f"{discovery_prefix}/sensor/{device_id}/{key}/config", "", qos=1, retain=True
    )
