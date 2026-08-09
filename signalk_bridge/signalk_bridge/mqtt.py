# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""MQTT discovery publishing for Signal K derived entities.

Publishes are async against an ``aiomqtt.Client``: the whole bridge runs on
a single event loop, so any sync send would stall the loop and choke both
the SK poller/subscriber and MQTT keepalives.
"""

import json
from typing import Any, Protocol


class _Publisher(Protocol):
    """Anything with an awaitable ``publish(topic, payload=..., qos=..., retain=...)``.

    Kept as a Protocol so tests can plug in a recording stub without pulling
    aiomqtt into their imports.
    """

    async def publish(
        self,
        topic: str,
        payload: bytes | str = "",
        qos: int = 0,
        retain: bool = False,
    ) -> Any: ...


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


def discovery_signature(entity: dict[str, Any]) -> tuple[Any, ...]:
    """Fields whose change requires re-publishing the discovery config.

    Must cover every entity field that ends up in the payload written by
    :func:`publish_discovery` -- otherwise a late-arriving value (AIS vessel
    name from a Type 5 static-data broadcast; a fleet-health device whose
    product-info PGN follows its address-claim) never reaches HA's card.
    """
    return (
        entity.get("component", "sensor"),
        entity.get("name"),
        entity.get("group_id"),
        entity.get("group_label"),
        entity.get("unit"),
        entity.get("device_class"),
        entity.get("state_class"),
        entity.get("icon"),
        entity.get("entity_category"),
        entity.get("suggested_display_precision"),
        entity.get("attributes") is not None,
    )


async def publish_discovery(
    client: _Publisher,
    discovery_prefix: str,
    base_topic: str,
    key: str,
    entity: dict[str, Any],
    expire_after_s: int,
) -> None:
    component = entity.get("component", "sensor")
    # has_entity_name=true makes HA render friendly_name as
    # "<device.name> <entity.name>", so when the entity is the device's sole
    # slot (AIS tracker, fleet-health binary_sensor) the same string ends up
    # doubled. Suppress the entity-side name in that case; HA falls back to
    # device.name alone, and a late-arriving vessel name flows through the
    # device block cleanly.
    entity_name = entity["name"]
    if entity_name == entity["group_label"]:
        entity_name = None
    payload: dict[str, Any] = {
        "name": entity_name,
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
        ("suggested_display_precision", "suggested_display_precision"),
    ):
        if entity.get(src) is not None:
            payload[dst] = entity[src]

    await client.publish(
        f"{discovery_prefix}/{component}/signalk/{key}/config",
        payload=json.dumps(payload),
        qos=1,
        retain=True,
    )
