# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""AIS target lifecycle for the Signal K bridge.

Signal K carries AIS decodes as per-vessel keys under ``vessels/*`` (one context
per MMSI). This module owns the mapping from those context-scoped SK trees onto
Home Assistant ``device_tracker`` entities, with the expire + cleanup
machinery the entity family needs to not accumulate ghosts in HA:

- **Expire**: a target whose last delta is older than ``expire_seconds`` is
  dropped from the registry, and the caller publishes empty-retained payloads
  to the discovery + state topics so HA unregisters the entity cleanly.
- **Sticky retain**: MMSIs in ``always_retain`` never expire once they've been
  registered -- their tracker keeps its last-known position with a stale
  ``last_seen`` attribute rather than disappearing.
- **Cap**: ``max_targets`` limits the total tracked count so a busy harbour
  can't blow up the HA entity registry. Sticky targets are seated first, then
  most-recently-seen fills the remainder.

The registry doesn't publish MQTT itself; it exposes a snapshot method that
returns entity dicts in the same shape as the resolvers in :mod:`app`, so the
existing publish loop consumes them without a second discovery/state pipeline.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Any

from .paths import slugify

_MS_TO_KNOTS = 1.94384


def _rad_to_deg_bearing(rad: float) -> float:
    """Signal K radians to a 0-360° compass bearing."""
    return math.degrees(rad) % 360.0


@dataclass
class _Target:
    """One AIS contact's aggregated state.

    Dynamic fields (position, speed_over_ground, course_over_ground,
    heading) come from the frequent Class-A/B position PGNs; static fields
    (name, ship_type, callsign, dimensions) arrive rarely on the static-data
    PGN and are held until superseded. ``last_delta_monotonic`` is the
    freshest change of ANY field -- so a target that keeps broadcasting
    position but not static info still stays alive.
    """

    mmsi: str
    position: dict[str, float] | None = None  # {latitude, longitude}
    speed_over_ground_ms: float | None = None
    course_over_ground_rad: float | None = None
    heading_rad: float | None = None
    name: str | None = None
    callsign: str | None = None
    ship_type: str | int | None = None
    length: float | None = None
    beam: float | None = None
    draft: float | None = None
    last_seen_iso: str | None = None
    last_delta_monotonic: float = field(default_factory=time.monotonic)


@dataclass
class AISRegistry:
    """AIS target store.

    Populated by :meth:`ingest` (called with an ``ais_snapshot()`` from the
    WS subscriber). :meth:`expire` prunes targets past the window and returns
    the MMSIs that were dropped, so the caller can publish the cleanup
    retained-empty payloads. :meth:`entities` renders every live target as
    an entity dict compatible with the existing publish loop.
    """

    expire_seconds: float = 900.0
    max_targets: int = 200
    always_retain: frozenset[str] = frozenset()
    _targets: dict[str, _Target] = field(default_factory=dict)

    # ---- ingest ----------------------------------------------------------

    def ingest(
        self,
        ais_snapshot: dict[str, dict[str, Any]],
        now_monotonic: float,
        *,
        dirty: set[str] | None = None,
    ) -> None:
        """Fold a snapshot of ``{mmsi: sk_tree}`` into the registry.

        ``sk_tree`` is the per-context nested dict the WS subscriber maintains
        -- same shape as ``vessels/self`` but scoped to this MMSI. Only leaves
        AIS actually publishes are read; anything unknown is ignored so a
        SK schema change or a plugin adding a novel path is silent, not an
        exception.

        ``dirty`` is the set of MMSIs that got a delta since the last tick
        (from :meth:`WSSubscriber.take_dirty_ais`). Only MMSIs in this set
        have their ``last_delta_monotonic`` clock refreshed -- otherwise
        every silent target's clock would be re-zeroed every tick just by
        being present in the snapshot, and :meth:`expire` would never fire.
        If ``dirty`` is None, every MMSI in the snapshot is treated as
        fresh (compat with unit-test callers that pass just the target
        under test).
        """
        for mmsi, tree in ais_snapshot.items():
            t = self._targets.get(mmsi) or _Target(mmsi=mmsi)
            if dirty is None or mmsi in dirty:
                t.last_delta_monotonic = now_monotonic
            self._read_position(tree, t)
            self._read_static(tree, t)
            self._targets[mmsi] = t

    def _read_position(self, tree: dict[str, Any], t: _Target) -> None:
        nav = tree.get("navigation") or {}
        pos = nav.get("position") or {}
        pv = pos.get("value") if isinstance(pos, dict) else None
        if isinstance(pv, dict):
            lat, lon = pv.get("latitude"), pv.get("longitude")
            if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
                t.position = {"latitude": float(lat), "longitude": float(lon)}
                ts = pos.get("timestamp") if isinstance(pos, dict) else None
                if isinstance(ts, str):
                    t.last_seen_iso = ts
        speed = nav.get("speedOverGround") if isinstance(nav, dict) else None
        if isinstance(speed, dict):
            sv = speed.get("value")
            if isinstance(sv, (int, float)):
                t.speed_over_ground_ms = float(sv)
        course = nav.get("courseOverGroundTrue") if isinstance(nav, dict) else None
        if isinstance(course, dict):
            cv = course.get("value")
            if isinstance(cv, (int, float)):
                t.course_over_ground_rad = float(cv)
        heading = nav.get("headingTrue") if isinstance(nav, dict) else None
        if isinstance(heading, dict):
            hv = heading.get("value")
            if isinstance(hv, (int, float)):
                t.heading_rad = float(hv)

    def _read_static(self, tree: dict[str, Any], t: _Target) -> None:
        # SK carries static AIS info at a mix of paths; canboatjs varies by
        # PGN version. Read the common ones and ignore misses.
        name_leaf = tree.get("name")
        if isinstance(name_leaf, dict):
            nv = name_leaf.get("value")
            if isinstance(nv, str):
                t.name = nv
        elif isinstance(name_leaf, str):
            t.name = name_leaf
        mmsi_leaf = tree.get("mmsi")
        if isinstance(mmsi_leaf, dict):
            mv = mmsi_leaf.get("value")
            if mv:
                t.mmsi = str(mv)
        comm = tree.get("communication") or {}
        cs = comm.get("callsignVhf") if isinstance(comm, dict) else None
        if isinstance(cs, dict):
            cv = cs.get("value")
            if isinstance(cv, str):
                t.callsign = cv
        design = tree.get("design") or {}
        if isinstance(design, dict):
            st = design.get("aisShipType")
            if isinstance(st, dict):
                sv = st.get("value")
                if isinstance(sv, dict):
                    # SK emits shipType as {id, name}
                    t.ship_type = sv.get("name") or sv.get("id")
                elif sv is not None:
                    t.ship_type = sv
            length = design.get("length")
            if isinstance(length, dict):
                lv = length.get("value")
                if isinstance(lv, dict):
                    overall = lv.get("overall")
                    if isinstance(overall, (int, float)):
                        t.length = float(overall)
                elif isinstance(lv, (int, float)):
                    t.length = float(lv)
            beam = design.get("beam")
            if isinstance(beam, dict):
                bv = beam.get("value")
                if isinstance(bv, (int, float)):
                    t.beam = float(bv)
            draft = design.get("draft") or design.get("draught")
            if isinstance(draft, dict):
                dv = draft.get("value")
                if isinstance(dv, dict):
                    max_d = dv.get("maximum") or dv.get("current")
                    if isinstance(max_d, (int, float)):
                        t.draft = float(max_d)
                elif isinstance(dv, (int, float)):
                    t.draft = float(dv)

    # ---- lifecycle -------------------------------------------------------

    def expire(self, now_monotonic: float) -> list[str]:
        """Drop targets past ``expire_seconds`` and return their MMSIs.

        Sticky targets never expire once registered. Also enforces
        ``max_targets`` by dropping oldest-observed non-sticky targets
        until the cap is met. The caller is expected to send empty-retained
        payloads to the discovery + state topics for every returned MMSI.
        """
        dropped: list[str] = []
        for mmsi, t in list(self._targets.items()):
            if mmsi in self.always_retain:
                continue
            if now_monotonic - t.last_delta_monotonic > self.expire_seconds:
                dropped.append(mmsi)
                self._targets.pop(mmsi, None)
        # Cap enforcement: keep sticky + freshest non-sticky.
        if len(self._targets) > self.max_targets:
            keep_sticky = {
                m: t for m, t in self._targets.items() if m in self.always_retain
            }
            non_sticky = sorted(
                (
                    (m, t)
                    for m, t in self._targets.items()
                    if m not in self.always_retain
                ),
                key=lambda kv: kv[1].last_delta_monotonic,
                reverse=True,
            )
            slots_left = max(0, self.max_targets - len(keep_sticky))
            kept_non_sticky = dict(non_sticky[:slots_left])
            dropped_by_cap = [m for m, _ in non_sticky[slots_left:]]
            dropped.extend(dropped_by_cap)
            self._targets = {**keep_sticky, **kept_non_sticky}
        return dropped

    # ---- render ----------------------------------------------------------

    def entities(self) -> dict[str, dict[str, Any]]:
        """Render every live target as one device_tracker entity dict.

        Entity keys are ``ais_<mmsi>`` (slugified). The dict shape matches
        the resolvers in :mod:`app`, so the existing publish loop consumes
        them without any AIS-specific plumbing.
        """
        out: dict[str, dict[str, Any]] = {}
        for mmsi, t in self._targets.items():
            if t.position is None:
                # No lat/lon means HA has nothing to render on the map;
                # keep the target in the registry (static data may still
                # be arriving) but don't create a tracker yet.
                continue
            key = f"ais_{slugify(mmsi)}"
            display_name = t.name or f"AIS {mmsi}"
            attrs: dict[str, Any] = {
                "latitude": t.position["latitude"],
                "longitude": t.position["longitude"],
                "mmsi": mmsi,
            }
            # Converted from Signal K's native SI (m/s → knots, radians →
            # degrees) at the edge so consumers can read a bearing without
            # remembering it's in radians.
            if t.speed_over_ground_ms is not None:
                attrs["speed_over_ground"] = round(
                    t.speed_over_ground_ms * _MS_TO_KNOTS, 2
                )
            if t.course_over_ground_rad is not None:
                attrs["course_over_ground"] = round(
                    _rad_to_deg_bearing(t.course_over_ground_rad), 1
                )
            if t.heading_rad is not None:
                attrs["heading"] = round(_rad_to_deg_bearing(t.heading_rad), 1)
            if t.name is not None:
                attrs["name"] = t.name
            if t.callsign is not None:
                attrs["callsign"] = t.callsign
            if t.ship_type is not None:
                attrs["ship_type"] = t.ship_type
            if t.length is not None:
                attrs["length"] = t.length
            if t.beam is not None:
                attrs["beam"] = t.beam
            if t.draft is not None:
                attrs["draft"] = t.draft
            if t.last_seen_iso is not None:
                attrs["last_seen"] = t.last_seen_iso
            attrs["sticky"] = mmsi in self.always_retain
            out[key] = {
                "path": f"vessels.urn:mrn:imo:mmsi:{mmsi}.navigation.position",
                "component": "device_tracker",
                "name": display_name,
                "state": None,
                "group_id": f"ais.{mmsi}",
                "group_label": display_name,
                "unit": None,
                "device_class": None,
                "state_class": None,
                "icon": "mdi:ferry",
                "attributes": attrs,
            }
        return out

    def inventory_entity(self, max_attr_bytes: int = 15000) -> dict[str, Any]:
        """A single ``sensor.signalk_ais_inventory`` whose state is the
        current tracked-target count and whose attributes list per-target
        summaries (most-recently-seen first, truncated to fit in an HA
        attribute payload).
        """
        rows = sorted(
            self._targets.values(),
            key=lambda t: t.last_delta_monotonic,
            reverse=True,
        )
        summaries: list[dict[str, Any]] = []
        truncated = False
        approx_bytes = 0
        for t in rows:
            row: dict[str, Any] = {"mmsi": t.mmsi}
            if t.name is not None:
                row["name"] = t.name
            if t.position is not None:
                row["latitude"] = t.position["latitude"]
                row["longitude"] = t.position["longitude"]
            if t.speed_over_ground_ms is not None:
                row["speed_over_ground"] = round(
                    t.speed_over_ground_ms * _MS_TO_KNOTS, 2
                )
            if t.course_over_ground_rad is not None:
                row["course_over_ground"] = round(
                    _rad_to_deg_bearing(t.course_over_ground_rad), 1
                )
            if t.heading_rad is not None:
                row["heading"] = round(_rad_to_deg_bearing(t.heading_rad), 1)
            if t.last_seen_iso is not None:
                row["last_seen"] = t.last_seen_iso
            # Rough estimate; ~60 bytes per compact row on average.
            approx_bytes += len(str(row)) + 2
            if approx_bytes > max_attr_bytes:
                truncated = True
                break
            summaries.append(row)

        return {
            "path": "signalk.ais_inventory",
            "component": "sensor",
            "name": "AIS targets tracked",
            "state": str(len(self._targets)),
            "group_id": "ais.fleet",
            "group_label": "AIS Fleet",
            "unit": None,
            "device_class": None,
            "state_class": "measurement",
            "icon": "mdi:radar",
            "attributes": {
                "targets": summaries,
                "truncated": truncated,
                "total_tracked": len(self._targets),
            },
        }

    def mmsis(self) -> list[str]:
        """MMSIs currently in the registry (for tests / diagnostics)."""
        return list(self._targets.keys())
