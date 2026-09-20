# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""MQTT discovery + availability helpers.

Pure payload/topic builders (session lifecycle lives in health_publisher). One
HA MQTT-Discovery device per target container; up to three sensors per device:
``applied`` + ``sentinel`` (binary_sensor) and ``summary`` (sensor).
Multi-availability with ``all`` mode gates each entity on both an addon-scoped
LWT topic and a per-slug reachability topic."""

import json
import re
from collections.abc import Iterable
from typing import Any, Protocol


class _Publisher(Protocol):
    """Anything with an awaitable ``publish``. Matches ``aiomqtt.Client``."""

    async def publish(
        self,
        topic: str,
        payload: bytes | str = "",
        qos: int = 0,
        retain: bool = False,
    ) -> Any: ...


# --- slugification ----------------------------------------------------------


_SLUG_STRIP = re.compile(r"[^a-z0-9_]+")
_SLUG_COLLAPSE = re.compile(r"_+")


def slugify(name: str) -> str:
    """Docker container name → HA-safe slug (``a-z0-9_``).

    Container names on HA are ``app_<slug>_<addon-name>`` — safe input.
    Third-party names (``docker compose`` projects) may include ``-``
    and ``.``; normalize them the same way sibling addons do so entity
    IDs stay predictable.
    """
    s = name.strip().lower()
    s = _SLUG_STRIP.sub("_", s)
    s = _SLUG_COLLAPSE.sub("_", s).strip("_")
    return s or "unknown"


# --- topics -----------------------------------------------------------------


def availability_topic(base_topic: str) -> str:
    """Addon-scoped availability; LWT flips it offline on addon crash."""
    return f"{base_topic}/availability"


def slug_availability_topic(base_topic: str, slug: str) -> str:
    """Per-target availability; offline when the docker API can't reach it."""
    return f"{base_topic}/{slug}/availability"


def state_topic(base_topic: str, slug: str, key: str) -> str:
    return f"{base_topic}/{slug}/{key}/state"


def attributes_topic(base_topic: str, slug: str, key: str) -> str:
    return f"{base_topic}/{slug}/{key}/attributes"


def discovery_topic(
    discovery_prefix: str, component: str, device_id: str, slug: str, key: str
) -> str:
    return f"{discovery_prefix}/{component}/{device_id}_{slug}/{key}/config"


# --- payload builders -------------------------------------------------------


def _device_block(device_id: str, slug: str, friendly: str) -> dict[str, Any]:
    return {
        "identifiers": [f"{device_id}_{slug}"],
        "name": f"container_hooks: {friendly}",
        "manufacturer": "container_hooks",
        "model": "Docker container",
    }


def _availability_block(base_topic: str, slug: str) -> dict[str, Any]:
    """Multi-availability + ``availability_mode: all``: entity is available iff
    both the addon-scoped and per-slug availability topics report online."""
    return {
        "availability": [
            {
                "topic": availability_topic(base_topic),
                "payload_available": "online",
                "payload_not_available": "offline",
            },
            {
                "topic": slug_availability_topic(base_topic, slug),
                "payload_available": "online",
                "payload_not_available": "offline",
            },
        ],
        "availability_mode": "all",
    }


# Per-entity fields for the three keys we publish. The ``sensor`` component
# (summary) omits payload_on/off and device_class; binary_sensors carry both.
_ENTITY_SPECS: dict[str, dict[str, Any]] = {
    "applied": {
        "component": "binary_sensor",
        "name": "Pre-start applied",
        "entity_id_prefix": "binary_sensor.container_hooks_",
        "entity_id_suffix": "_applied",
        "icon": "mdi:archive-arrow-down",
        "device_class": "running",
    },
    "sentinel": {
        "component": "binary_sensor",
        "name": "Pre-start sentinel",
        "entity_id_prefix": "binary_sensor.container_hooks_",
        "entity_id_suffix": "_sentinel",
        "icon": "mdi:file-check",
        "device_class": "running",
    },
    "summary": {
        "component": "sensor",
        "name": "Health",
        "entity_id_prefix": "sensor.container_hooks_",
        "entity_id_suffix": "_health",
        "icon": "mdi:heart-pulse",
    },
}


def discovery_payload(
    key: str,
    *,
    device_id: str,
    slug: str,
    friendly: str,
    base_topic: str,
    expire_after_s: int,
) -> dict[str, Any]:
    """Build the HA MQTT-Discovery config payload for one entity key.

    key ∈ {"applied", "sentinel", "summary"}.
    """
    spec = _ENTITY_SPECS[key]
    payload: dict[str, Any] = {
        "name": spec["name"],
        "has_entity_name": True,
        "unique_id": f"{device_id}_{slug}_{key}",
        "default_entity_id": f"{spec['entity_id_prefix']}{slug}{spec['entity_id_suffix']}",
        "state_topic": state_topic(base_topic, slug, key),
        "json_attributes_topic": attributes_topic(base_topic, slug, key),
        **_availability_block(base_topic, slug),
        "expire_after": max(60, int(expire_after_s)),
        "icon": spec["icon"],
        "device": _device_block(device_id, slug, friendly),
    }
    if spec["component"] == "binary_sensor":
        payload["payload_on"] = "ON"
        payload["payload_off"] = "OFF"
        payload["device_class"] = spec["device_class"]
    return payload


def component_for(key: str) -> str:
    """HA MQTT-Discovery component for one entity key."""
    return _ENTITY_SPECS[key]["component"]


# --- publish helpers --------------------------------------------------------


async def publish_discovery(
    client: _Publisher,
    payload: dict[str, Any],
    *,
    discovery_prefix: str,
    component: str,
    device_id: str,
    slug: str,
    key: str,
) -> None:
    topic = discovery_topic(discovery_prefix, component, device_id, slug, key)
    await client.publish(
        topic, payload=json.dumps(payload, sort_keys=True), qos=1, retain=True
    )


async def clear_discovery(
    client: _Publisher,
    *,
    discovery_prefix: str,
    component: str,
    device_id: str,
    slug: str,
    key: str,
) -> None:
    """Empty retained payload to a discovery config topic = HA drops the entity."""
    topic = discovery_topic(discovery_prefix, component, device_id, slug, key)
    await client.publish(topic, payload="", qos=1, retain=True)


async def publish_slug_availability(
    client: _Publisher, *, base_topic: str, slug: str, online: bool
) -> None:
    """Retained online/offline on the per-slug availability topic."""
    await client.publish(
        slug_availability_topic(base_topic, slug),
        "online" if online else "offline",
        qos=1,
        retain=True,
    )


async def publish_state(
    client: _Publisher,
    *,
    base_topic: str,
    slug: str,
    key: str,
    state: str,
    attributes: dict[str, Any] | None,
) -> None:
    """Fire-and-forget state (+ attrs). qos=0 retain=False matches siblings:
    ``expire_after`` drives freshness, per-slug availability drives target-gone."""
    await client.publish(state_topic(base_topic, slug, key), state, qos=0, retain=False)
    if attributes is not None:
        await client.publish(
            attributes_topic(base_topic, slug, key),
            json.dumps(attributes, sort_keys=True),
            qos=0,
            retain=False,
        )


def summary_state(applied: str, sentinel: str | None) -> str:
    """Combine the two binary states into a short human-readable verdict.

    Values here are used both as the ``sensor.container_hooks_<x>_health``
    state and as a stable key an automation can match against. Keep the
    set small; expand only when a new failure mode has its own fix.

    ``applied``/``sentinel`` come from ``render_binary_state`` and can be
    ``"unknown"`` when the underlying check returned ``None`` (target
    container transiently unreachable, StartedAt unparsable, etc.). An
    unknown must NOT be collapsed to a concrete failure verdict — a
    transient docker-API hiccup would otherwise fire ``not_applied`` on
    every container without a sentinel and drown the automation in
    false alarms.
    """
    if applied == "unknown" or sentinel == "unknown":
        return "unknown"
    if sentinel is None:
        return "ok" if applied == "ON" else "not_applied"
    if applied == "ON" and sentinel == "ON":
        return "ok"
    if applied == "OFF" and sentinel == "OFF":
        return "boot_race"
    if applied == "ON" and sentinel == "OFF":
        return "payload_no_effect"
    if applied == "OFF" and sentinel == "ON":
        return "stale_or_orphan"
    return "unknown"


def summary_attributes(
    *,
    applied_reason: str | None,
    sentinel_reason: str | None,
    sentinel_configured: bool,
) -> dict[str, Any]:
    """JSON attributes block for the summary sensor."""
    return {
        "applied_reason": applied_reason,
        "sentinel_reason": sentinel_reason,
        "sentinel_configured": sentinel_configured,
    }


# --- pruning ---------------------------------------------------------------


def keys_for(sentinel_configured: bool) -> list[str]:
    """Which per-container entities we publish for a given recipe shape."""
    keys = ["applied", "summary"]
    if sentinel_configured:
        keys.append("sentinel")
    return keys


async def clear_container_entities(
    client: _Publisher,
    *,
    discovery_prefix: str,
    device_id: str,
    slug: str,
    keys: Iterable[str],
    base_topic: str | None = None,
) -> None:
    """Drop every discovery config we might have published for a slug.

    Called from ``_publish_discovery`` when a slug present on the last
    scan is no longer on disk (recipe removed). Clearing the discovery
    config alone makes HA drop the entity, but its retained state and
    attributes topics on the broker survive — a later re-add of the
    same recipe would republish discovery and pick up the stale state
    on subscribe. When ``base_topic`` is supplied we also empty those
    retained state and attributes topics so a re-add starts clean.
    """
    for key in keys:
        await clear_discovery(
            client,
            discovery_prefix=discovery_prefix,
            component=component_for(key),
            device_id=device_id,
            slug=slug,
            key=key,
        )
        if base_topic is not None:
            # Empty-retained on state + attributes clears any leftover
            # retained payload from previous versions (which retained
            # state) or from other clients. New publishes are
            # non-retained, so on a fresh install these are no-ops on
            # the broker — kept for upgrade correctness.
            await client.publish(
                state_topic(base_topic, slug, key), "", qos=1, retain=True
            )
            await client.publish(
                attributes_topic(base_topic, slug, key), "", qos=1, retain=True
            )
    # Also drop the per-slug availability topic so a removed target
    # doesn't leave a ghost ``online``/``offline`` retained value that
    # would confuse a later re-added container of the same name.
    if base_topic is not None:
        await client.publish(
            slug_availability_topic(base_topic, slug), "", qos=1, retain=True
        )
