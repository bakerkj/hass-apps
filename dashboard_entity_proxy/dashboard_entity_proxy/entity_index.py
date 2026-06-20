# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Builds per-device / per-integration / per-area lookup indexes from HA's
entity and device registries, so the proxy can resolve scope for the
``/config/*`` settings pages (Devices, Integrations, Areas).

When a client navigates to e.g. ``/config/devices/device/<dev_id>``, the
proxy needs to know "which entities belong to that device" to filter the
WebSocket subscription. HA's registry is the only place that mapping
lives; entities don't carry their device id / integration / area in the
state stream itself, only in the registry snapshot.

``build()`` is called from ``RegistryStore.rebuild_index_if_ready``,
first when the initial entity + device registry list responses have
both arrived, and again on every subsequent registry update (either an
incremental sweep that produced a per-entity get, or a full-list
refetch). The resulting ``EntityIndex`` is queried by ``session.py``
whenever the client navigates to a settings page.

Area scope is "entity's own area_id when set, otherwise the area of the
entity's device", matching what the HA frontend does on the Areas
settings page.
"""

import heapq
from typing import Any


class EntityIndex:
    """Maps devices, integrations, and areas to their entity ids."""

    def __init__(self) -> None:
        """Construct an empty index. Populated by the module-level
        ``build()`` factory; direct construction is mostly useful in tests.
        """
        self.by_device: dict[str, list[str]] = {}
        self.by_platform: dict[str, list[str]] = {}
        self.by_area: dict[str, list[str]] = {}

    def device(self, device_id: str) -> list[str]:
        return self.by_device.get(device_id, [])

    def integration(self, domain: str) -> list[str]:
        """Entity ids sourced from the integration with this platform
        ``domain`` (i.e. the entity registry's ``platform`` field). Empty
        list when unknown.
        """
        return self.by_platform.get(domain, [])

    def area(self, area_id: str) -> list[str]:
        """Entity ids assigned to ``area_id`` (directly or via their
        device). Empty list when unknown.
        """
        return self.by_area.get(area_id, [])

    def platforms(self, domains: list[str]) -> list[str]:
        """Sorted, de-duplicated entity ids across the given platforms.

        Merges already-sorted per-platform lists (built once in ``build()``)
        via ``heapq.merge`` and dedupes inline (no per-call ``sorted()``),
        matching the pre-sorted lookup pattern of the sibling methods.
        """
        sources = [self.by_platform[d] for d in domains if d in self.by_platform]
        if not sources:
            return []
        out: list[str] = []
        prev: str | None = None
        for eid in heapq.merge(*sources):
            if eid != prev:
                out.append(eid)
                prev = eid
        return out


def build(entities: list[dict[str, Any]], devices: list[dict[str, Any]]) -> EntityIndex:
    """Build an EntityIndex from entity_registry/list and device_registry/list results.

    An entity's area is its own area_id when set, otherwise its device's.
    """
    device_area: dict[str, str] = {}
    for d in devices:
        area_id = d.get("area_id")
        dev_id = d.get("id")
        if area_id and dev_id:
            device_area[dev_id] = area_id

    idx = EntityIndex()
    for e in entities:
        eid = e.get("entity_id")
        if not eid:
            continue
        device_id = e.get("device_id")
        if device_id:
            idx.by_device.setdefault(device_id, []).append(eid)
        platform = e.get("platform")
        if platform:
            idx.by_platform.setdefault(platform, []).append(eid)
        area = e.get("area_id")
        if not area and device_id:
            area = device_area.get(device_id)
        if area:
            idx.by_area.setdefault(area, []).append(eid)

    for mapping in (idx.by_device, idx.by_platform, idx.by_area):
        for key in mapping:
            mapping[key].sort()
    return idx
