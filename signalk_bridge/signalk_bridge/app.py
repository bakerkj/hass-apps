# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Bridge Signal K marine data into Home Assistant as MQTT Discovery entities.

The bridge runs on a single asyncio event loop: aiohttp for the SK REST calls,
aiomqtt for the MQTT client, and asyncio.sleep for the poll cadence. Every
sync-blocking call (urllib, paho.loop_start's thread, threading.Event) was
removed so a slow SK response or MQTT reconnect cannot starve the other side.
Pure computation (entity resolution, path flattening, staleness scoring) stays
sync -- it's microseconds and adding coroutines around it would only add noise.
"""

import argparse
import asyncio
import json
import logging
import re
import signal
import sys
from datetime import UTC, datetime
from typing import Any

import aiohttp
import aiomqtt

from . import __version__, auth, paths, staleness
from .busstats import BusStats
from .client import (
    SignalKAuthError,
    SignalKError,
    get_self,
    get_server_info,
    get_sources,
)
from .mqtt import (
    attributes_topic,
    availability_topic,
    publish_discovery,
    state_topic,
)
from .paths import PATH_MAP, flatten, match_path, resolve_group, slugify

log = logging.getLogger(__name__)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level={
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
        }.get(level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def render(value: float) -> str:
    if isinstance(value, float):
        s = f"{value:.4f}".rstrip("0").rstrip(".")
        # A negative value that rounds to zero formats as "-0"; normalise it.
        return "0" if s == "-0" else s
    return str(value)


def _entity(
    path: str,
    name: str,
    state: str | None,
    group_id: str,
    group_label: str,
    *,
    component: str = "sensor",
    attributes: dict[str, Any] | None = None,
    **fields: Any,
) -> dict[str, Any]:
    ent = {
        "path": path,
        "component": component,
        "name": name,
        "state": state,
        "group_id": group_id,
        "group_label": group_label,
        "unit": None,
        "device_class": None,
        "state_class": None,
        "icon": None,
        "attributes": attributes,
    }
    ent.update(fields)
    return ent


def _entity_key(
    pattern: str, spec: dict[str, Any], path: str, per_group_instance_count: int
) -> str:
    """Slugified entity key; drops path wildcards for single-instance
    (no-``*``-in-group) devices, so tanks read as
    ``sensor.signalk_tanks_freshwater_level``. Falls back to the full
    path when the group has more than one instance, to avoid collisions
    on multi-tank boats."""
    if "*" not in spec["group"] and per_group_instance_count <= 1:
        segs = path.split(".")
        pat_segs = pattern.split(".")
        kept = [s for s, p in zip(segs, pat_segs) if p != "*"]
        return slugify(".".join(kept))
    return slugify(path)


def resolve_entities(
    tree: dict[str, Any],
    source_tags: dict[str, str] | None = None,
    suppress_paths: tuple[str, ...] | list[str] | None = None,
    suppress_primary_on_fanout: bool = False,
) -> dict[str, dict[str, Any]]:
    """Map a vessel tree onto numeric sensor definitions.

    Only paths present in the tree produce entities, so PATH_MAP can cover more
    equipment than any single boat carries without inventing sensors. When
    ``source_tags`` is provided, multi-source leaves fan out to per-source
    entities (see :func:`paths.flatten`).
    """
    return _entities_from_flat(
        flatten(
            tree,
            source_tags=source_tags,
            suppress_paths=suppress_paths,
            suppress_primary_on_fanout=suppress_primary_on_fanout,
        )
    )


def _entities_from_flat(flat: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Numeric sensor definitions from an already-flattened vessel tree.

    Split from :func:`resolve_entities` so the poll loop can share one flatten
    across the numeric and special resolvers instead of re-walking per call.
    """
    # Count instances per group (not per pattern): two tank leaves
    # -- level and capacity -- for the same fluid_type collapse to one
    # instance, and a second tank of the same type bumps the count so
    # the entity key + group label re-include the instance segment.
    group_instances: dict[str, set[str]] = {}
    for path in flat:
        for pattern, spec in PATH_MAP.items():
            caps = match_path(path, pattern)
            if caps is not None:
                group_instances.setdefault(spec["group"], set()).add(".".join(caps))
                break

    entities: dict[str, dict[str, Any]] = {}
    for path, raw in flat.items():
        for pattern, spec in PATH_MAP.items():
            caps = match_path(path, pattern)
            if caps is None:
                continue
            convert = spec.get("convert")
            if convert is None or not isinstance(raw, (int, float, bool)):
                # Numeric-only here; text/binary/position handled elsewhere.
                break
            group_pattern = spec["group"]
            n_instances = len(group_instances.get(spec["group"], set()))
            # No-wildcard group patterns (tanks) collapse per-fluid-type
            # devices into one; re-inject the wildcard on multi-instance
            # so two fresh-water tanks become two HA devices, not one
            # with silently overwritten entities.
            if "*" not in group_pattern and n_instances > 1:
                group_pattern = f"{group_pattern}.*"
            group_id, group_label = resolve_group(group_pattern, caps)
            key = _entity_key(pattern, spec, path, n_instances)
            entities[key] = _entity(
                path,
                spec["name"],
                render(convert(float(raw))),
                group_id,
                group_label,
                unit=spec.get("unit"),
                device_class=spec.get("device_class"),
                state_class=spec.get("state_class"),
                icon=spec.get("icon"),
                suggested_display_precision=spec.get("suggested_display_precision"),
            )
            break
    # Collapse duplicates that resolve to the same device + entity name (e.g. a
    # bank that reports stateOfCharge both at its root and under capacity).
    seen: set[tuple[str, str]] = set()
    deduped: dict[str, dict[str, Any]] = {}
    for key, ent in entities.items():
        ident = (ent["group_id"], ent["name"])
        if ident in seen:
            continue
        seen.add(ident)
        deduped[key] = ent
    return deduped


def resolve_special(
    tree: dict[str, Any],
    source_tags: dict[str, str] | None = None,
    suppress_paths: tuple[str, ...] | list[str] | None = None,
    suppress_primary_on_fanout: bool = False,
) -> dict[str, dict[str, Any]]:
    """Map the non-numeric paths: text/enum states, switch banks, notification
    alarms, and the vessel's position (as a device_tracker)."""
    return _special_from_flat(
        flatten(
            tree,
            source_tags=source_tags,
            suppress_paths=suppress_paths,
            suppress_primary_on_fanout=suppress_primary_on_fanout,
        )
    )


def _special_from_flat(flat: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Non-numeric entities (text/enum states, switch and alarm binary sensors,
    the vessel position device_tracker, and the derived tank/battery/current
    sensors) from an already-flattened vessel tree."""
    from .paths import (
        NOTIFICATION_PREFIX,
        POSITION_PATH,
        SWITCH_PATTERN,
        TEXT_MAP,
        TEXT_PATTERN_MAP,
        notification_is_active,
    )

    entities: dict[str, dict[str, Any]] = {}
    for path, raw in flat.items():
        key = slugify(path)

        # Position -> device_tracker (boat on the HA map).
        if (
            path == POSITION_PATH
            and isinstance(raw, dict)
            and isinstance(raw.get("latitude"), (int, float))
            and isinstance(raw.get("longitude"), (int, float))
        ):
            attrs = {"latitude": raw["latitude"], "longitude": raw["longitude"]}
            entities[key] = _entity(
                path,
                "Position",
                None,
                "vessel",
                "Vessel",
                component="device_tracker",
                icon="mdi:sail-boat",
                attributes=attrs,
            )
            continue

        # Enum / free-text state sensors.
        if path in TEXT_MAP and isinstance(raw, (str, int, float)):
            spec = TEXT_MAP[path]
            gid, label = resolve_group(spec["group"], [])
            entities[key] = _entity(
                path,
                spec["name"],
                str(raw),
                gid,
                label,
                icon=spec.get("icon"),
            )
            continue

        # Enum / free-text state on a wildcard (per-instance) path -> a plain
        # sensor grouped under the captured instance (e.g. charger mode).
        matched_text = False
        for pattern, spec in TEXT_PATTERN_MAP.items():
            caps = match_path(path, pattern)
            if caps is not None and isinstance(raw, (str, int, float)):
                gid, label = resolve_group(spec["group"], caps)
                entities[key] = _entity(
                    path, spec["name"], str(raw), gid, label, icon=spec.get("icon")
                )
                matched_text = True
                break
        if matched_text:
            continue

        # Digital switch banks -> binary_sensor per channel.
        caps = match_path(path, SWITCH_PATTERN)
        if caps is not None and isinstance(raw, (int, float, bool)):
            bank, channel = caps
            entities[key] = _entity(
                path,
                f"Switch {channel}",
                "ON" if raw else "OFF",
                f"switches.bank.{bank}",
                f"Digital switches bank {bank}",
                component="binary_sensor",
                icon="mdi:toggle-switch-variant",
            )
            continue

        # GNSS satellites in view: composite {count, satellites[...]} -> count.
        # Servers emit count as 12 or 12.0; accept both, reject bool.
        sat_count = raw.get("count") if isinstance(raw, dict) else None
        if (
            path == "navigation.gnss.satellitesInView"
            and isinstance(sat_count, (int, float))
            and not isinstance(sat_count, bool)
        ):
            gid, label = resolve_group("gps", [])
            entities[key] = _entity(
                path,
                "Satellites in view",
                str(int(sat_count)),
                gid,
                label,
                icon="mdi:satellite-variant",
            )
            continue

        # Notifications -> binary_sensor alarms.
        if path.startswith(NOTIFICATION_PREFIX) and isinstance(raw, dict):
            name = str(raw.get("message") or path.split(".")[-1])
            entities[key] = _entity(
                path,
                name,
                "ON" if notification_is_active(raw) else "OFF",
                "alarms",
                "Alarms",
                component="binary_sensor",
                device_class="problem",
                icon="mdi:alarm-light",
            )
            continue

    entities.update(_tank_derived_entities(flat))
    entities.update(_battery_derived_entities(flat))
    entities.update(_current_entities(flat))
    return entities


def _camel_to_snake(s: str) -> str:
    """freshWater -> fresh_water; matches Victron's fluid_type enum values."""
    out: list[str] = []
    for i, c in enumerate(s):
        if c.isupper() and i > 0:
            out.append("_")
        out.append(c.lower())
    return "".join(out)


def _tank_derived_entities(flat: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Per-tank ``remaining`` (gal) and ``fluid_type`` (enum), on top of
    the ``level``/``capacity`` PATH_MAP entries -- Victron-parity fields
    that aren't direct SignalK leaves."""
    from collections import Counter

    from .paths import GROUP_LABELS, m3_to_gal

    tanks: dict[tuple[str, str], dict[str, float]] = {}
    for path, raw in flat.items():
        parts = path.split(".")
        if len(parts) != 4 or parts[0] != "tanks":
            continue
        if parts[3] not in ("currentLevel", "capacity"):
            continue
        if not isinstance(raw, (int, float)) or isinstance(raw, bool):
            continue
        tanks.setdefault((parts[1], parts[2]), {})[parts[3]] = float(raw)

    per_type_count: Counter[str] = Counter()
    for fluid_type, _instance in tanks:
        per_type_count[fluid_type] += 1

    entities: dict[str, dict[str, Any]] = {}
    for (fluid_type, instance), fields in tanks.items():
        type_slug = slugify(fluid_type)
        if per_type_count[fluid_type] <= 1:
            group_id = f"tank.{fluid_type}"
            group_label = GROUP_LABELS.get(group_id, group_id)
            key_base = f"tanks_{type_slug}"
        else:
            group_id = f"tank.{fluid_type}.{instance}"
            base_label = GROUP_LABELS.get(f"tank.{fluid_type}", f"tank.{fluid_type}")
            group_label = f"{base_label} {instance}"
            key_base = f"tanks_{type_slug}_{instance}"

        # State keeps _camel_to_snake so downstream automations can key on the
        # same fluid_type string the Victron integration publishes.
        entities[f"{key_base}_fluid_type"] = _entity(
            f"tanks.{fluid_type}.{instance}",
            "Fluid type",
            _camel_to_snake(fluid_type),
            group_id,
            group_label,
            icon="mdi:water-opacity",
        )

        current = fields.get("currentLevel")
        capacity = fields.get("capacity")
        if current is None or capacity is None:
            continue
        entities[f"{key_base}_remaining"] = _entity(
            f"tanks.{fluid_type}.{instance}.remaining",
            "Remaining",
            render(m3_to_gal(capacity * current)),
            group_id,
            group_label,
            unit="gal",
            device_class="volume_storage",
            state_class="measurement",
            icon="mdi:cup-water",
        )

    return entities


def _battery_derived_entities(flat: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Per-battery ``power`` (W) = voltage × current. Not on N2K as a
    scalar leaf, but derivable everywhere the two source leaves are.
    Naturally covers the fanout instances too (Battery House, Battery
    Engine, Battery Solar) since those emit voltage + current under
    their own instance segment."""
    banks: dict[str, dict[str, float]] = {}
    for path, raw in flat.items():
        parts = path.split(".")
        if len(parts) != 4 or parts[0] != "electrical" or parts[1] != "batteries":
            continue
        if parts[3] not in ("voltage", "current"):
            continue
        if not isinstance(raw, (int, float)) or isinstance(raw, bool):
            continue
        banks.setdefault(parts[2], {})[parts[3]] = float(raw)

    entities: dict[str, dict[str, Any]] = {}
    for instance, fields in banks.items():
        # Skip when the device reports its own power (BMV/SmartShunt);
        # V*I is a strictly worse estimate than the shunt's averaged
        # reading, and overwriting it silently is a regression.
        if f"electrical.batteries.{instance}.power" in flat:
            continue
        v = fields.get("voltage")
        i = fields.get("current")
        if v is None or i is None:
            continue
        instance_label = (
            instance.title() if instance.islower() and instance.isalpha() else instance
        )
        entities[f"electrical_batteries_{slugify(instance)}_power"] = _entity(
            f"electrical.batteries.{instance}.power",
            "Power",
            render(v * i),
            f"battery.{instance}",
            f"Battery {instance_label}",
            unit="W",
            device_class="power",
            state_class="measurement",
            icon="mdi:flash",
        )
    return entities


def _current_entities(flat: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Water set & drift arrive as a single Signal K composite leaf
    (``environment.current`` = {setTrue, drift, ...}); HA states are scalar,
    so split it into a set-direction (°) and a drift-speed (m/s) sensor."""
    from .paths import rad_to_deg_positive

    cur = flat.get("environment.current")
    if not isinstance(cur, dict):
        return {}
    entities: dict[str, dict[str, Any]] = {}
    # Prefer true set; fall back to magnetic when a device reports only that.
    set_dir = cur.get("setTrue")
    set_path = "environment.current.setTrue"
    if not (isinstance(set_dir, (int, float)) and not isinstance(set_dir, bool)):
        set_dir = cur.get("setMagnetic")
        set_path = "environment.current.setMagnetic"
    if isinstance(set_dir, (int, float)) and not isinstance(set_dir, bool):
        entities["environment_current_set"] = _entity(
            set_path,
            "Current set",
            render(rad_to_deg_positive(float(set_dir))),
            "environment",
            "Environment",
            unit="°",
            state_class="measurement",
            icon="mdi:sync",
        )
    drift = cur.get("drift")
    if isinstance(drift, (int, float)) and not isinstance(drift, bool):
        entities["environment_current_drift"] = _entity(
            "environment.current.drift",
            "Current drift",
            render(float(drift)),
            "environment",
            "Environment",
            unit="m/s",
            device_class="speed",
            state_class="measurement",
            icon="mdi:waves-arrow-right",
        )
    return entities


def _map_tree(
    tracker: staleness.StalenessTracker | None,
    tree: dict[str, Any],
    source_tags: dict[str, str],
    base_suppress: tuple[str, ...],
    suppress_primary_on_fanout: bool,
    now: datetime,
    publish_unmapped: bool = False,
) -> dict[str, dict[str, Any]]:
    """Flatten the vessel tree ONCE and map it to entity definitions.

    Sharing a single flatten across the numeric and special resolvers (and the
    staleness scan) keeps a low ``interval_seconds`` cheap. Paths the staleness
    tracker reports stale are withheld so Home Assistant's ``expire_after``
    clears them; ``navigation.position`` is exempt (a device_tracker with no
    ``expire_after`` -- withholding it would freeze the last fix on the map
    rather than clear it).

    When ``publish_unmapped`` is set, the diagnostic catch-all is built from the
    SAME staleness-filtered ``flat`` -- so a withheld stale path stays withheld
    from the ``(other)`` sensors too, instead of being republished every cycle
    (which would reset its ``expire_after`` and defeat staleness).
    """
    meta = paths.flatten_with_meta(
        tree,
        source_tags=source_tags,
        suppress_paths=base_suppress,
        suppress_primary_on_fanout=suppress_primary_on_fanout,
    )
    stale: set[str] = set()
    if tracker is not None:
        ts_map = {
            p: parsed
            for p, (_v, raw_ts) in meta.items()
            if (parsed := staleness.parse_ts(raw_ts)) is not None
        }
        tracker.observe(ts_map)
        stale = tracker.stale_paths(ts_map, now)
        stale.discard(paths.POSITION_PATH)
        if stale:
            log.debug(
                "Staleness: withholding %d path(s) past their cadence", len(stale)
            )
    flat = {p: v for p, (v, _ts) in meta.items() if p not in stale}
    entities = {**_entities_from_flat(flat), **_special_from_flat(flat)}
    if publish_unmapped:
        emitted = {e["path"] for e in entities.values()}
        entities.update(_unmapped_entities(flat, emitted))
    return entities


def _humanize_path(path: str) -> str:
    """A readable entity name from a dotted, camelCase Signal K path."""
    rest = path.split(".")
    rest = rest[1:] if len(rest) > 1 else rest
    spaced = " ".join(
        re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", seg).replace("_", " ") for seg in rest
    ).strip()
    return spaced[:1].upper() + spaced[1:] if spaced else path


def _is_mapped(path: str) -> bool:
    """True if any explicit mapper claims this path -- even one whose entity was
    later dropped by the (group_id, name) dedup in :func:`_entities_from_flat`.

    Checking the maps directly (not just the surviving entities) keeps the
    catch-all from republishing a value the mapper deliberately collapsed, e.g. a
    battery that reports state-of-charge at two paths.
    """
    from .paths import POSITION_PATH, SWITCH_PATTERN, TEXT_MAP, TEXT_PATTERN_MAP

    if path == POSITION_PATH or path in TEXT_MAP:
        return True
    if match_path(path, SWITCH_PATTERN) is not None:
        return True
    return any(match_path(path, pat) is not None for pat in PATH_MAP) or any(
        match_path(path, pat) is not None for pat in TEXT_PATTERN_MAP
    )


def _consumed_tank_leaves(flat: dict[str, Any]) -> set[str]:
    """Tank ``currentLevel``/``capacity`` leaves that the derived "Remaining"
    sensor actually consumes -- i.e. where BOTH are present and numeric for the
    same ``(fluid_type, instance)``. Mirrors :func:`_tank_derived_entities`'
    gather so the catch-all suppresses exactly what the derived sensor covers,
    and nothing more (a tank with only one field keeps surfacing)."""
    fields: dict[tuple[str, str], set[str]] = {}
    for path, raw in flat.items():
        parts = path.split(".")
        if len(parts) != 4 or parts[0] != "tanks":
            continue
        if parts[3] not in ("currentLevel", "capacity"):
            continue
        if not isinstance(raw, (int, float)) or isinstance(raw, bool):
            continue
        fields.setdefault((parts[1], parts[2]), set()).add(parts[3])
    return {
        f"tanks.{ft}.{inst}.{leaf}"
        for (ft, inst), present in fields.items()
        if {"currentLevel", "capacity"} <= present
        for leaf in ("currentLevel", "capacity")
    }


def _unmapped_entities(
    flat: dict[str, Any], emitted_paths: set[str]
) -> dict[str, dict[str, Any]]:
    """Every remaining SCALAR leaf as a diagnostic sensor (``publish_unmapped``).

    Paths already covered by an explicit mapping are skipped, as are ``.name``
    device labels, composite (dict/list), and null leaves. Each is grouped under
    a per-branch ``<Top> (other)`` diagnostic device so nothing is dropped on a
    judgement call, without cluttering the primary devices.
    """
    # A tank's currentLevel/capacity feeds the derived "Remaining" sensor only
    # when BOTH are present for that (fluid_type, instance); those source leaves
    # are then already represented, so skip them. A tank reporting just one of
    # the two gets no derived entity, so it must still surface as a diagnostic
    # rather than vanish -- the whole point of publish_unmapped.
    consumed_tank_leaves = _consumed_tank_leaves(flat)

    entities: dict[str, dict[str, Any]] = {}
    for path, raw in flat.items():
        if path in emitted_paths or path.startswith("notifications."):
            continue
        if path in consumed_tank_leaves:
            continue
        if _is_mapped(path):
            continue  # mapped (possibly deduped away) -- don't duplicate it
        if path.endswith(".name"):
            continue  # device labels, not telemetry
        top = path.split(".")[0]
        gid, label = f"other.{top}", f"{top.title()} (other)"
        # Bool leaves are on/off, not numbers: a binary_sensor keeps HA's
        # is_state(...,'on') and template bool checks working.
        if isinstance(raw, bool):
            entities[slugify(path)] = _entity(
                path,
                _humanize_path(path),
                "ON" if raw else "OFF",
                gid,
                label,
                component="binary_sensor",
                entity_category="diagnostic",
            )
            continue
        if not isinstance(raw, (int, float, str)):
            continue
        # render() floats for parity with mapped sensors; clamp to HA's 255-char
        # state limit (a longer state is dropped and the entity stays unknown).
        state = render(raw) if isinstance(raw, float) else str(raw)
        entities[slugify(path)] = _entity(
            path,
            _humanize_path(path),
            state[:255],
            gid,
            label,
            entity_category="diagnostic",
        )
    return entities


# ---- runtime -----------------------------------------------------------------


async def _obtain_token(
    session: aiohttp.ClientSession,
    stop: asyncio.Event,
    base_url: str,
    data_dir: str,
) -> str | None:
    """Run the request-and-approve handshake until we have a token, ``stop`` is
    set, or the request is denied. Returns the token (also saved to disk), or
    None on ``stop``/DENIED so the caller re-enters the loop.

    Kept as its own coroutine so :func:`_run` reads as a normal poll loop
    instead of interleaving the handshake state machine.
    """
    cid = auth.client_id(data_dir)
    access_href: str | None = None
    while not stop.is_set():
        if access_href is None:
            try:
                access_href = await auth.request_access(
                    session, base_url, cid, "Signal K to Home Assistant bridge"
                )
                log.warning(
                    "Requested access from Signal K. APPROVE IT: Signal K -> "
                    "Security -> Access Requests -> approve (read, no expiry)."
                )
            except SignalKError as exc:
                log.warning("Could not submit access request: %s", exc)
                await _wait_or_stop(stop, 5)
                continue
        try:
            state, tok = await auth.poll_request(session, base_url, access_href)
        except SignalKError as exc:
            log.debug("poll error: %s", exc)
            await _wait_or_stop(stop, 3)
            continue
        if state == "APPROVED" and tok:
            auth.save_token(data_dir, tok)
            log.warning("Access approved -- token saved. Bridge is authenticated.")
            return tok
        if state == "DENIED":
            log.error("Access request was DENIED in Signal K; re-requesting shortly.")
            access_href = None
            await _wait_or_stop(stop, 30)
            continue
        log.info("Waiting for approval in Signal K -> Security -> Access Requests...")
        await _wait_or_stop(stop, 3)
    return None


async def _wait_or_stop(stop: asyncio.Event, seconds: float) -> None:
    """Sleep for ``seconds`` unless ``stop`` is set. Returns as soon as either
    happens. Used everywhere the poll loop would sleep so shutdown is prompt."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        return


async def _run(
    session: aiohttp.ClientSession,
    mq: aiomqtt.Client,
    stop: asyncio.Event,
    opts_summary: dict[str, Any],
) -> None:
    """One MQTT session's worth of the poll loop.

    Returns cleanly on ``stop`` and returns (letting the ``aiomqtt.MqttError``
    catch in :func:`main_async` reconnect) on any broker disruption. State that
    should survive reconnects (``announced``, staleness, source_tags) is held
    in :func:`main_async` and passed in via ``opts_summary``.
    """
    base_url = opts_summary["base_url"]
    data_dir = opts_summary["data_dir"]
    discovery_prefix = opts_summary["discovery_prefix"]
    base_topic = opts_summary["base_topic"]
    interval = opts_summary["interval"]
    expire_after_s = opts_summary["expire_after_s"]
    suppress_paths = opts_summary["suppress_paths"]
    suppress_primary_on_fanout = opts_summary["suppress_primary_on_fanout"]
    publish_unmapped = opts_summary["publish_unmapped"]
    tracker: staleness.StalenessTracker | None = opts_summary["tracker"]
    announced: set[str] = opts_summary["announced"]
    bus: BusStats = opts_summary["bus"]
    use_access_flow: bool = opts_summary["use_access_flow"]

    token: str | None = opts_summary.get("token")
    sources_cache: dict[str, Any] = {}
    source_tags: dict[str, str] = {}
    cycle = 0
    warned_empty = False

    save_every = max(1, 300 // max(1, interval))
    sources_every = max(1, 60 // max(1, interval))

    while not stop.is_set():
        started = asyncio.get_running_loop().time()

        if use_access_flow and not token:
            token = await _obtain_token(session, stop, base_url, data_dir)
            opts_summary["token"] = token
            if token is None:
                continue  # stop or DENIED loop -- re-check stop and retry

        try:
            tree = await get_self(session, base_url, token)
        except SignalKAuthError:
            if use_access_flow:
                log.warning("Token rejected by Signal K; requesting access again.")
                auth.clear_token(data_dir)
                token = None
                opts_summary["token"] = None
            else:
                log.warning("Signal K rejected the configured signalk_token (401/403).")
            await mq.publish(
                availability_topic(base_topic), payload=b"offline", qos=1, retain=True
            )
            await _wait_or_stop(stop, 2)
            continue
        except SignalKError as exc:
            log.warning("%s", exc)
            await mq.publish(
                availability_topic(base_topic), payload=b"offline", qos=1, retain=True
            )
            await _wait_or_stop(stop, max(1, interval))
            continue

        # Refresh /sources first so resolve_entities can disambiguate
        # multi-source leaves (two BMVs on the same batteries.0 path, etc.)
        # via friendly source tags.
        if cycle % sources_every == 0:
            try:
                sources_cache = await get_sources(session, base_url, token)
                source_tags = paths.build_source_tags(sources_cache)
            except SignalKError as exc:
                log.debug("sources fetch failed: %s", exc)

        try:
            data_entities = _map_tree(
                tracker,
                tree,
                source_tags,
                suppress_paths,
                suppress_primary_on_fanout,
                datetime.now(UTC),
                publish_unmapped,
            )
        except Exception:
            log.exception("Error mapping Signal K data, skipping cycle")
            await _wait_or_stop(stop, max(1, interval))
            continue

        if not data_entities and not warned_empty:
            # Almost always means no NMEA 2000 traffic rather than a bridge
            # fault, so say so explicitly instead of sitting silent.
            log.warning(
                "Signal K reachable but produced no mapped values. If this "
                "persists, check that can0 is receiving frames "
                "(cat /sys/class/net/can0/statistics/rx_packets) and that a "
                "canbus connection is configured in Signal K."
            )
            warned_empty = True
        elif data_entities:
            warned_empty = False

        bus_entities: dict[str, dict[str, Any]] = {}
        if bus.available():
            try:
                bus_entities = bus.sample(sources_cache)
            except Exception as exc:  # noqa: BLE001 - bus stats must not kill the daemon
                log.debug("bus stats sample failed: %s", exc)
        cycle += 1
        if tracker is not None and cycle % save_every == 0:
            tracker.save(datetime.now(UTC))

        entities = {**data_entities, **bus_entities}

        for key, ent in entities.items():
            if key not in announced:
                await publish_discovery(
                    mq, discovery_prefix, base_topic, key, ent, expire_after_s
                )
                announced.add(key)
                log.info(
                    "Discovered %s -> %s (%s)",
                    ent.get("path", key),
                    ent["name"],
                    ent["group_label"],
                )
            value = ent.get("state")
            if value is not None:
                await mq.publish(state_topic(base_topic, key), payload=value, qos=0)
            attrs = ent.get("attributes")
            if attrs is not None:
                await mq.publish(
                    attributes_topic(base_topic, key),
                    payload=json.dumps(attrs),
                    qos=0,
                )

        await mq.publish(
            availability_topic(base_topic), payload=b"online", qos=1, retain=True
        )
        log.debug("Published %d entities", len(entities))

        elapsed = asyncio.get_running_loop().time() - started
        await _wait_or_stop(stop, max(0.0, interval - elapsed))


async def main_async(opts_path: str) -> int:
    from .config import load_options_file, redact_options_for_log, resolve_mqtt_config

    ap = argparse.ArgumentParser(prog="signalk_bridge")
    opts = load_options_file(opts_path, ap)
    configure_logging(str(opts.get("log_level") or "INFO"))

    log.info("Signal K -> Home Assistant bridge v%s starting", __version__)
    log.info("Options:")
    for line in json.dumps(
        redact_options_for_log(opts), indent=2, sort_keys=True
    ).splitlines():
        log.info("  %s", line)

    base_url = str(opts.get("signalk_url") or "http://localhost:3000")
    data_dir = str(opts.get("data_dir") or "/data")
    token = str(opts.get("signalk_token") or "") or None
    use_access_flow = token is None
    if use_access_flow:
        token = auth.saved_token(data_dir)
    interval = int(opts.get("interval_seconds") or 10)
    discovery_prefix = str(opts.get("mqtt_discovery_prefix") or "homeassistant")
    base_topic = str(opts.get("mqtt_base_topic") or "signalk")
    client_id = str(opts.get("client_id") or "signalk-bridge")
    expire_after_s = max(5, interval * int(opts.get("expire_after_multiplier") or 4))
    raw_suppress = opts.get("suppress_paths") or []
    suppress_paths: tuple[str, ...] = tuple(
        s for s in raw_suppress if isinstance(s, str) and s
    )
    suppress_primary_on_fanout = bool(opts.get("suppress_primary_on_fanout"))
    publish_unmapped = bool(opts.get("publish_unmapped"))
    stale_after_s = int(opts.get("stale_after_seconds") or 0)
    stale_learning_max_age = int(opts.get("stale_learning_max_age") or 0)

    try:
        mqtt_cfg = resolve_mqtt_config(opts)
    except RuntimeError as exc:
        log.error("%s", exc)
        return 1

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    tracker: staleness.StalenessTracker | None = None
    if stale_after_s > 0:
        if stale_after_s < expire_after_s:
            log.warning(
                "stale_after_seconds (%ss) is below expire_after (%ss); the cap "
                "then floors every threshold and can flap slow sensors. Set it "
                "above the slowest device's broadcast interval.",
                stale_after_s,
                expire_after_s,
            )
        tracker = staleness.StalenessTracker(
            floor_s=expire_after_s,
            cap_s=stale_after_s,
            data_dir=data_dir,
            learning_max_age_s=stale_learning_max_age,
        )
        tracker.load(datetime.now(UTC))
        log.info(
            "Staleness detection on: cap=%ss, learned-cadence persistence=%s",
            stale_after_s,
            f"{stale_learning_max_age}s" if stale_learning_max_age > 0 else "off",
        )

    announced: set[str] = set()
    bus = BusStats()

    opts_summary: dict[str, Any] = {
        "base_url": base_url,
        "data_dir": data_dir,
        "discovery_prefix": discovery_prefix,
        "base_topic": base_topic,
        "interval": interval,
        "expire_after_s": expire_after_s,
        "suppress_paths": suppress_paths,
        "suppress_primary_on_fanout": suppress_primary_on_fanout,
        "publish_unmapped": publish_unmapped,
        "tracker": tracker,
        "announced": announced,
        "bus": bus,
        "use_access_flow": use_access_flow,
        "token": token,
    }

    async with aiohttp.ClientSession() as session:
        try:
            info = await get_server_info(session, base_url)
            ver = info.get("server", {}).get("version", "unknown")
            log.info("Signal K server at %s (version %s)", base_url, ver)
        except SignalKError as exc:
            log.warning("Signal K not reachable yet: %s", exc)

        will = aiomqtt.Will(
            topic=availability_topic(base_topic),
            payload=b"offline",
            qos=1,
            retain=True,
        )

        try:
            while not stop.is_set():
                try:
                    async with aiomqtt.Client(
                        hostname=mqtt_cfg["host"],
                        port=mqtt_cfg["port"],
                        username=mqtt_cfg["username"],
                        password=mqtt_cfg["password"],
                        identifier=client_id,
                        will=will,
                        keepalive=60,
                    ) as mq:
                        await _run(session, mq, stop, opts_summary)
                except aiomqtt.MqttError as exc:
                    log.warning("MQTT: %s; reconnecting in 3s", exc)
                    await _wait_or_stop(stop, 3)
        finally:
            log.info("Publishing offline availability and disconnecting")
            if tracker is not None:
                tracker.save(datetime.now(UTC))
            # Best-effort farewell on a fresh, short-lived connection: the
            # session above may already be torn down by the exception path.
            try:
                async with aiomqtt.Client(
                    hostname=mqtt_cfg["host"],
                    port=mqtt_cfg["port"],
                    username=mqtt_cfg["username"],
                    password=mqtt_cfg["password"],
                    identifier=f"{client_id}-shutdown",
                    timeout=1.5,
                ) as mq:
                    await mq.publish(
                        availability_topic(base_topic),
                        payload=b"offline",
                        qos=1,
                        retain=True,
                    )
            except Exception as exc:  # noqa: BLE001 - best effort on the way out
                log.debug("Error during MQTT shutdown publish: %s", exc)

    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="signalk_bridge")
    ap.add_argument("--options", default="/data/options.json")
    args = ap.parse_args(argv)
    try:
        return asyncio.run(main_async(args.options))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
