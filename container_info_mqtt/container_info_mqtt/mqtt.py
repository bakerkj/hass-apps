# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""MQTT helpers: health state + HA discovery + retained-config pruning."""

import json
import logging
from collections.abc import Iterable
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


def clear_container_discovery(
    client: mqtt.Client,
    discovery_prefix: str,
    base_topic: str,
    device_id: str,
    container_slug: str,
    metrics: Iterable[str],
    *,
    clear_summary: bool,
    log: logging.Logger,
) -> None:
    """Clear all retained discovery + availability for one removed container.

    The per-container counterpart to `clear_discovery`: it wipes every
    per-metric discovery config, the summary config, and the retained
    availability topic, so Home Assistant drops the device instead of leaving a
    permanently-unavailable ghost behind when a container is gone for good.
    """
    for metric_key in metrics:
        clear_discovery(client, discovery_prefix, device_id, container_slug, metric_key)
    if clear_summary:
        clear_discovery(client, discovery_prefix, device_id, container_slug, "summary")
    client.publish(
        f"{base_topic}/{container_slug}/availability", "", qos=1, retain=True
    )
    log.info("Cleared discovery for removed container slug=%s", container_slug)


def due_for_removal(
    discovered_slugs: Iterable[str],
    seen_slugs: set[str],
    absent_since: dict[str, float],
    now: float,
    grace_seconds: float,
) -> set[str]:
    """Which published slugs have been absent long enough to be real removals.

    ``docker ps`` lists only *running* containers, so a restart makes a
    container vanish briefly. To avoid churning its HA device on every restart,
    a slug must be continuously absent for ``grace_seconds`` before it counts as
    removed. ``absent_since`` is updated in place: a slug back in ``seen_slugs``
    has its clock cleared, a newly-absent slug starts it, and every slug absent
    at least ``grace_seconds`` is returned.
    """
    for slug in list(absent_since):
        if slug in seen_slugs:
            del absent_since[slug]
    due: set[str] = set()
    for slug in discovered_slugs:
        if slug in seen_slugs:
            continue
        first_absent = absent_since.setdefault(slug, now)
        if now - first_absent >= grace_seconds:
            due.add(slug)
    return due


def prune_stale_discovery(
    client: mqtt.Client,
    discovery_prefix: str,
    base_topic: str,
    device_id: str,
    retained_configs: dict[str, set[str]],
    expected_by_slug: dict[str, set[str]],
    log: logging.Logger,
) -> int:
    """Reconcile retained discovery on the broker against what we publish.

    `retained_configs` maps node_id -> set of metric_keys observed as retained
    discovery configs on the broker for our device_id. `expected_by_slug` maps
    each currently-active slug -> the set of discovery metric_keys this process
    actually publishes for it (everything in `discovered[slug]` plus "summary").

    For every retained node:

    * slug **not** in `expected_by_slug` -> fully stale (removed/excluded
      container): every metric's discovery config is cleared and the per-slug
      availability topic is also cleared. Counts toward the return value.
    * slug **in** `expected_by_slug` -> active: only retained metric configs
      that we no longer publish are cleared (e.g. a metric dropped from
      `include_metrics`, renamed across a version, or now network-gated off).
      The container stays online, so availability is left untouched and the
      slug is *not* counted as pruned.

    Returns the number of fully-stale slugs pruned.
    """
    device_node_prefix = f"{device_id}_"
    pruned_slugs = 0

    # Guard against slug-convention drift (see the app_/addon_ prefix rename
    # incident): refuse to prune if we'd wipe 3+ slugs *and* the current poll
    # shares zero keys with them. Single-container renames still prune.
    retained_slugs = {
        node_id[len(device_node_prefix) :]
        for node_id in retained_configs
        if node_id.startswith(device_node_prefix)
    }
    if len(retained_slugs) >= 3 and not (retained_slugs & expected_by_slug.keys()):
        log.warning(
            "Prune skipped: %d retained slug(s) share no keys with %d expected "
            "slug(s) -- refusing to wipe everything. Retained sample: %s; "
            "expected sample: %s",
            len(retained_slugs),
            len(expected_by_slug),
            ", ".join(sorted(retained_slugs)[:5]),
            ", ".join(sorted(expected_by_slug)[:5]),
        )
        return 0

    for node_id, retained_metrics in retained_configs.items():
        if not node_id.startswith(device_node_prefix):
            continue
        slug = node_id[len(device_node_prefix) :]
        expected = expected_by_slug.get(slug)

        if expected is None:
            for metric_key in retained_metrics:
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
                ", ".join(sorted(retained_metrics)),
            )
            pruned_slugs += 1
            continue

        # Active slug: drop only the discovery configs we no longer publish.
        orphan_metrics = retained_metrics - expected
        if not orphan_metrics:
            continue
        for metric_key in orphan_metrics:
            clear_discovery(client, discovery_prefix, device_id, slug, metric_key)
        log.info(
            "Reconciled discovery for active slug=%s (cleared: %s)",
            slug,
            ", ".join(sorted(orphan_metrics)),
        )

    return pruned_slugs
