# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""MQTT discovery + availability helpers for container_hooks.

Kept as pure payload/topic builders so ``app.py`` owns the aiomqtt
session lifecycle (connect, LWT, reconnect backoff, shutdown) and this
module can be unit-tested against a recording stub without touching a
broker.

Model:

* One HA MQTT-discovery *device* per target container. Its identifier
  is stable across restarts (``{client_id}_{slug}``) so re-publishing
  discovery on reconnect does not orphan the entities.
* Up to three sensors per device: ``applied`` (binary_sensor),
  ``sentinel`` (binary_sensor, only when the recipe declares
  ``success_sentinel``), and ``summary`` (sensor whose state is a
  short verdict and whose ``json_attributes_topic`` carries the two
  underlying reasons).

The addon itself has one addon-scoped availability topic
(``{base_topic}/availability``). We do not publish per-target
availability: the sensors track whether the *target* is healthy, and
their state alone answers that.
"""

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
    """Addon-scoped availability. Set to ``offline`` via LWT.

    Reported alongside per-slug availability as a list; HA treats an
    entity as available iff every listed availability topic reports
    ``online``. So this addon-level topic gates every entity: if the
    addon crashes, LWT flips this to ``offline`` and every sensor
    goes ``unavailable`` regardless of retained per-slug state.
    """
    return f"{base_topic}/availability"


def slug_availability_topic(base_topic: str, slug: str) -> str:
    """Per-target availability topic.

    Flipped to ``offline`` when the target container is unreachable
    (both health checks return None). The pair-with-addon-availability
    scheme means a target deleted from docker shows as ``unavailable``
    in HA instead of sitting on a stale last-known state indefinitely.
    """
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
    """HA MQTT-Discovery ``availability`` list + ``availability_mode: all``.

    Every entity is gated on BOTH: the addon-scoped availability (LWT
    flips to offline if the addon crashes) AND the per-slug
    availability (flipped to offline when the target container is
    unreachable). ``all`` means HA marks the entity ``unavailable``
    when either is offline. A target deleted from docker therefore
    surfaces as ``unavailable`` in HA rather than sitting on a stale
    last-known state.
    """
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


def applied_discovery_payload(
    *,
    device_id: str,
    slug: str,
    friendly: str,
    base_topic: str,
    expire_after_s: int,
) -> dict[str, Any]:
    """binary_sensor payload for the ``applied`` check."""
    return {
        "name": "Pre-start applied",
        "has_entity_name": True,
        "unique_id": f"{device_id}_{slug}_applied",
        "default_entity_id": f"binary_sensor.container_hooks_{slug}_applied",
        "state_topic": state_topic(base_topic, slug, "applied"),
        "json_attributes_topic": attributes_topic(base_topic, slug, "applied"),
        **_availability_block(base_topic, slug),
        "payload_on": "ON",
        "payload_off": "OFF",
        "device_class": "running",
        "expire_after": max(60, int(expire_after_s)),
        "icon": "mdi:archive-arrow-down",
        "device": _device_block(device_id, slug, friendly),
    }


def sentinel_discovery_payload(
    *,
    device_id: str,
    slug: str,
    friendly: str,
    base_topic: str,
    expire_after_s: int,
) -> dict[str, Any]:
    """binary_sensor payload for the ``sentinel`` (in-target) check."""
    return {
        "name": "Pre-start sentinel",
        "has_entity_name": True,
        "unique_id": f"{device_id}_{slug}_sentinel",
        "default_entity_id": f"binary_sensor.container_hooks_{slug}_sentinel",
        "state_topic": state_topic(base_topic, slug, "sentinel"),
        "json_attributes_topic": attributes_topic(base_topic, slug, "sentinel"),
        **_availability_block(base_topic, slug),
        "payload_on": "ON",
        "payload_off": "OFF",
        "device_class": "running",
        "expire_after": max(60, int(expire_after_s)),
        "icon": "mdi:file-check",
        "device": _device_block(device_id, slug, friendly),
    }


def summary_discovery_payload(
    *,
    device_id: str,
    slug: str,
    friendly: str,
    base_topic: str,
    expire_after_s: int,
) -> dict[str, Any]:
    """sensor payload for the aggregated ``summary`` state."""
    return {
        "name": "Health",
        "has_entity_name": True,
        "unique_id": f"{device_id}_{slug}_summary",
        "default_entity_id": f"sensor.container_hooks_{slug}_health",
        "state_topic": state_topic(base_topic, slug, "summary"),
        "json_attributes_topic": attributes_topic(base_topic, slug, "summary"),
        **_availability_block(base_topic, slug),
        "expire_after": max(60, int(expire_after_s)),
        "icon": "mdi:heart-pulse",
        "device": _device_block(device_id, slug, friendly),
    }


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
    """Retained ``online``/``offline`` on the per-slug availability topic.

    Retained so a HA subscriber joining mid-cycle sees the last known
    availability. The addon-scoped availability topic (via LWT) is the
    only line of defense against the addon crashing; per-slug topics
    only reflect target-container reachability.
    """
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
    """Publish one entity's state (non-retained) and, optionally, its attributes.

    Non-retained matches the sibling MQTT addons (``turbostat_mqtt``,
    ``intel_gpu_top_mqtt``): the discovery config's ``expire_after``
    drives freshness — HA marks the entity ``unavailable`` if no fresh
    state arrives within the window — and per-slug availability marks
    it unavailable when the target container is gone. A subscriber
    joining mid-cycle waits at most one ``health_interval_seconds`` for
    the next poll rather than seeing a stale ghost value from a prior
    lifecycle.
    """
    await client.publish(state_topic(base_topic, slug, key), state, qos=1, retain=False)
    if attributes is not None:
        await client.publish(
            attributes_topic(base_topic, slug, key),
            json.dumps(attributes, sort_keys=True),
            qos=1,
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
        component = "sensor" if key == "summary" else "binary_sensor"
        await clear_discovery(
            client,
            discovery_prefix=discovery_prefix,
            component=component,
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
