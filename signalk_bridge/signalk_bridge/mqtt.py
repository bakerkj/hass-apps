# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""MQTT discovery publishing for Signal K derived entities."""

import json
from typing import Any

import paho.mqtt.client as mqtt


def availability_topic(base_topic: str) -> str:
    return f"{base_topic}/availability"


def state_topic(base_topic: str, key: str) -> str:
    return f"{base_topic}/{key}/state"


def attributes_topic(base_topic: str, key: str) -> str:
    return f"{base_topic}/{key}/attributes"


def _device_block(group_id: str, group_label: str) -> dict[str, Any]:
    """One HA device per boat subsystem.

    Grouping by subsystem (engine, battery bank, tank) rather than lumping
    everything under a single "Signal K" device is what makes the result
    navigable -- each physical thing on the boat gets its own device page.
    """
    return {
        "identifiers": [f"signalk_{group_id}"],
        "name": group_label,
        "manufacturer": "Signal K",
        "model": "NMEA 2000",
    }


def publish_discovery(
    client: mqtt.Client,
    discovery_prefix: str,
    base_topic: str,
    key: str,
    entity: dict[str, Any],
    expire_after_s: int,
) -> None:
    component = entity.get("component", "sensor")
    payload: dict[str, Any] = {
        "name": entity["name"],
        "has_entity_name": True,
        "unique_id": f"signalk_{key}",
        "default_entity_id": f"{component}.signalk_{key}",
        "availability_topic": availability_topic(base_topic),
        "payload_available": "online",
        "payload_not_available": "offline",
        "device": _device_block(entity["group_id"], entity["group_label"]),
    }

    if component == "device_tracker":
        # HA derives home/not_home from the lat/lon it reads out of the
        # attributes, so no state topic is needed -- just feed it coordinates.
        payload["source_type"] = "gps"
        payload["json_attributes_topic"] = attributes_topic(base_topic, key)
    else:
        payload["state_topic"] = state_topic(base_topic, key)
        # Marine data drops out when instruments are powered down; expiring is
        # better than showing a stale reading.
        payload["expire_after"] = max(5, int(expire_after_s))
        if entity.get("attributes") is not None:
            payload["json_attributes_topic"] = attributes_topic(base_topic, key)

    if component == "binary_sensor":
        payload["payload_on"] = "ON"
        payload["payload_off"] = "OFF"

    for src, dst in (
        ("unit", "unit_of_measurement"),
        ("device_class", "device_class"),
        ("state_class", "state_class"),
        ("icon", "icon"),
        ("entity_category", "entity_category"),
    ):
        if entity.get(src):
            payload[dst] = entity[src]

    client.publish(
        f"{discovery_prefix}/{component}/signalk/{key}/config",
        json.dumps(payload),
        qos=1,
        retain=True,
    )
