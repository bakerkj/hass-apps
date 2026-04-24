# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""MQTT helpers: health state + HA discovery + retained-config pruning."""

from __future__ import annotations

import json
import logging
from typing import Any

import paho.mqtt.client as mqtt


class MqttHealth:
    def __init__(self) -> None:
        self.connected: bool = False
        self.last_connect_ok: float = 0.0
        self.last_disconnect: float = 0.0


def publish_discovery(
    client: mqtt.Client,
    discovery_prefix: str,
    device_id: str,
    base_topic: str,
    container_slug: str,
    container_display_name_text: str,
    metric_key: str,
    metric_def: dict[str, Any],
    expire_after_s: int,
) -> None:
    node_id = f"{device_id}_{container_slug}"
    config_topic = f"{discovery_prefix}/sensor/{node_id}/{metric_key}/config"
    state_topic = f"{base_topic}/{container_slug}/{metric_key}/state"
    friendly_container_name = f"Container {container_display_name_text}"

    payload: dict[str, Any] = {
        "name": metric_def["name"],
        "has_entity_name": True,
        "unique_id": f"{device_id}_{container_slug}_{metric_key}",
        "default_entity_id": f"sensor.container_{container_slug}_{metric_key}",
        "state_topic": state_topic,
        "availability_topic": f"{base_topic}/{container_slug}/availability",
        "payload_available": "online",
        "payload_not_available": "offline",
        "expire_after": max(5, int(expire_after_s)),
        "device": {
            "identifiers": [f"{device_id}_{container_slug}"],
            "name": friendly_container_name,
            "manufacturer": "Docker",
            "model": "Container",
        },
    }

    if metric_def.get("unit"):
        payload["unit_of_measurement"] = metric_def["unit"]
    if metric_def.get("icon"):
        payload["icon"] = metric_def["icon"]
    if metric_def.get("device_class"):
        payload["device_class"] = metric_def["device_class"]
    if metric_def.get("state_class"):
        payload["state_class"] = metric_def["state_class"]
    if "suggested_display_precision" in metric_def:
        payload["suggested_display_precision"] = metric_def[
            "suggested_display_precision"
        ]

    client.publish(config_topic, json.dumps(payload), qos=1, retain=True)


def publish_summary_discovery(
    client: mqtt.Client,
    discovery_prefix: str,
    device_id: str,
    base_topic: str,
    container_slug: str,
    container_display_name_text: str,
    expire_after_s: int,
) -> None:
    node_id = f"{device_id}_{container_slug}"
    config_topic = f"{discovery_prefix}/sensor/{node_id}/summary/config"
    state_topic = f"{base_topic}/{container_slug}/summary/state"
    attributes_topic = f"{base_topic}/{container_slug}/summary/attributes"
    friendly_container_name = f"Container {container_display_name_text}"

    payload: dict[str, Any] = {
        "name": "Summary",
        "has_entity_name": True,
        "unique_id": f"{device_id}_{container_slug}_summary",
        "default_entity_id": f"sensor.container_{container_slug}_summary",
        "state_topic": state_topic,
        "json_attributes_topic": attributes_topic,
        "availability_topic": f"{base_topic}/{container_slug}/availability",
        "payload_available": "online",
        "payload_not_available": "offline",
        "expire_after": max(5, int(expire_after_s)),
        "icon": "mdi:table",
        "device": {
            "identifiers": [f"{device_id}_{container_slug}"],
            "name": friendly_container_name,
            "manufacturer": "Docker",
            "model": "Container",
        },
    }

    client.publish(config_topic, json.dumps(payload), qos=1, retain=True)


def clear_discovery(
    client: mqtt.Client,
    discovery_prefix: str,
    device_id: str,
    container_slug: str,
    metric_key: str,
) -> None:
    node_id = f"{device_id}_{container_slug}"
    config_topic = f"{discovery_prefix}/sensor/{node_id}/{metric_key}/config"
    client.publish(config_topic, "", qos=1, retain=True)


def prune_stale_discovery(
    client: mqtt.Client,
    discovery_prefix: str,
    base_topic: str,
    device_id: str,
    retained_configs: dict[str, set[str]],
    active_slugs: set[str],
    log: logging.Logger,
) -> int:
    """Clear retained discovery + availability for slugs not currently active.

    `retained_configs` maps node_id -> set of metric_keys observed as retained
    discovery configs on the broker for our device_id. Any node whose slug is
    not in `active_slugs` is pruned: every metric's discovery config is cleared
    and the per-slug availability topic is also cleared.

    Returns the number of stale slugs pruned.
    """
    device_node_prefix = f"{device_id}_"
    active_node_ids = {f"{device_node_prefix}{slug}" for slug in active_slugs}
    stale_node_ids = set(retained_configs.keys()) - active_node_ids
    if not stale_node_ids:
        return 0

    for node_id in stale_node_ids:
        slug = node_id[len(device_node_prefix) :]
        metric_keys = retained_configs[node_id]
        for metric_key in metric_keys:
            clear_discovery(client, discovery_prefix, device_id, slug, metric_key)
        # Also clear the retained availability topic for this slug.
        client.publish(
            f"{base_topic}/{slug}/availability",
            "",
            qos=1,
            retain=True,
        )
        log.info(
            "Pruned stale discovery for slug=%s (metrics: %s)",
            slug,
            ", ".join(sorted(metric_keys)),
        )
    return len(stale_node_ids)
