# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Per-device N2K bus health entities.

Signal K's ``/signalk/v1/api/sources`` returns one entry per N2K device on the
bus, each carrying the device's address-claim identity (canName, model,
manufacturer, deviceClass, deviceInstance) plus a ``pgns`` map of last-seen
timestamps for every PGN the device has transmitted. This module renders that
into one ``binary_sensor`` per device: ``ON`` if the freshest PGN is within
``stale_after_seconds``, ``OFF`` otherwise.

Useful for catching a partial fleet outage (e.g. the YDNG-03 gateway dies and
takes the ICOM's DSC path with it) that no other entity in the bridge would
surface -- the individual downstream sensors just stop updating and HA marks
them unavailable via ``expire_after`` without saying WHY.

Keyed by canName rather than address so an address collision after a bus
reset doesn't collide two entities under the same MQTT topic; the mutable
address lives in an attribute.
"""

from datetime import UTC, datetime
from typing import Any

from .paths import slugify


def _n2k_devices(sources: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """Flatten ``/sources`` into ``(source_key, n2k)`` pairs, skipping non-N2K
    entries (label/type strings, canName aliases) and any device without an
    address-claim canName."""
    out: list[tuple[str, dict[str, Any]]] = []
    if not isinstance(sources, dict):
        return out
    for bus_key, bus_val in sources.items():
        if not isinstance(bus_val, dict):
            continue
        for addr, info in bus_val.items():
            if not isinstance(info, dict):
                continue
            n2k = info.get("n2k")
            if not isinstance(n2k, dict):
                continue
            can_name = n2k.get("canName")
            if not isinstance(can_name, str) or not can_name:
                continue
            out.append((f"{bus_key}.{addr}", n2k))
    return out


def _parse_iso(ts: Any) -> datetime | None:
    if not isinstance(ts, str):
        return None
    try:
        # SK timestamps come in the "...Z" (UTC) shape; ``fromisoformat`` on
        # 3.11+ parses that directly. Strip Z first and force UTC so pre-3.11
        # behaviour stays consistent.
        return datetime.fromisoformat(ts.rstrip("Z")).replace(tzinfo=UTC)
    except ValueError:
        return None


def _display_name(n2k: dict[str, Any]) -> str:
    """Human-readable label for the fleet-card entity: prefer manufacturer
    + modelId, fall back to canName tail."""
    model = n2k.get("modelId") or n2k.get("modelVersion")
    mfr = n2k.get("manufacturerCode")
    can_name = str(n2k.get("canName") or "")
    tail = can_name[-4:] if can_name else "?"
    if isinstance(model, str) and model.strip():
        model_s = model.strip()
        if isinstance(mfr, str) and mfr.strip() and mfr not in model_s:
            return f"{mfr} {model_s}"
        return model_s
    if isinstance(mfr, str) and mfr.strip():
        return f"{mfr} device {tail}"
    return f"N2K device {tail}"


def health_entities(
    sources: dict[str, Any],
    now: datetime,
    stale_after_seconds: float,
) -> dict[str, dict[str, Any]]:
    """Render one binary_sensor per N2K device.

    ``sources`` is the raw ``/signalk/v1/api/sources`` payload. ``now`` is
    the current UTC time; a device is considered online if any of its PGN
    last-seens is within ``stale_after_seconds`` of ``now``. Attributes
    carry model / manufacturer / deviceClass / address / canName plus the
    freshest PGN and its age in seconds for at-a-glance diagnosis.
    """
    entities: dict[str, dict[str, Any]] = {}
    seen_keys: set[str] = set()
    for _src_key, n2k in _n2k_devices(sources):
        can_name = str(n2k["canName"])
        # canName is a 16-hex-char address-claim identifier; slug it verbatim
        # so the entity is stable across bus resets even if the address moves.
        key = f"n2k_dev_{slugify(can_name)}"
        if key in seen_keys:
            # A duplicate canName means the same device showed up under two
            # addresses (bus glitch during renumbering). Keep the first;
            # the second slot would just overwrite.
            continue
        seen_keys.add(key)

        raw_pgns = n2k.get("pgns")
        pgns: dict[str, Any] = raw_pgns if isinstance(raw_pgns, dict) else {}
        latest = None
        latest_pgn = None
        for pgn, ts in pgns.items():
            parsed = _parse_iso(ts)
            if parsed is None:
                continue
            if latest is None or parsed > latest:
                latest = parsed
                latest_pgn = pgn
        age_s: float | None = None
        online = False
        if latest is not None:
            age_s = (now - latest).total_seconds()
            online = age_s <= stale_after_seconds

        name = _display_name(n2k)
        attrs: dict[str, Any] = {
            "can_name": can_name,
            "address": _src_key.rsplit(".", 1)[1],
            "device_class": n2k.get("deviceClass"),
            "manufacturer": n2k.get("manufacturerCode"),
            "model": n2k.get("modelId"),
            "model_version": n2k.get("modelVersion"),
            "device_instance": n2k.get("deviceInstance"),
            "software_version": n2k.get("softwareVersionCode"),
        }
        if age_s is not None:
            attrs["last_seen_age_seconds"] = round(age_s, 1)
        if latest is not None:
            attrs["last_seen"] = latest.isoformat()
        if latest_pgn is not None:
            attrs["last_seen_pgn"] = latest_pgn
        # Strip None-valued attrs so the HA card isn't polluted with empty
        # rows for devices that don't advertise a given field.
        attrs = {k: v for k, v in attrs.items() if v is not None}

        entities[key] = {
            # Synthetic path so the rate limiter can slot per-device without
            # colliding with any real SK leaf.
            "path": f"bus.health.{can_name}",
            "component": "binary_sensor",
            "name": name,
            "state": "ON" if online else "OFF",
            "group_id": "n2k.fleet",
            "group_label": "N2K Fleet",
            "unit": None,
            "device_class": "connectivity",
            "state_class": None,
            "icon": "mdi:hub",
            "attributes": attrs,
            "entity_category": "diagnostic",
        }
    return entities
