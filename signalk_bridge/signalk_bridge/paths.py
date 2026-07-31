# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Signal K path -> Home Assistant entity mapping.

Signal K reports everything in SI base units: angles in radians, temperatures in
kelvin, speeds in m/s, pressures in Pa, ratios as 0..1, and engine revolutions in
Hz. Publishing those raw would give unreadable entities, so each mapping carries
an optional conversion.

Paths may contain ``*`` wildcards, which match a single segment and capture the
instance id (battery bank, engine, tank, switch). Only paths actually present in
the vessel tree produce entities, so this map can cover more equipment than any
one boat has without creating phantom sensors.
"""

import math
from typing import Any

# --- unit conversions -------------------------------------------------------


def rad_to_deg(v: float) -> float:
    return math.degrees(v)


def rad_to_deg_positive(v: float) -> float:
    """Radians to 0..360 degrees.

    Apparent/true wind angle is signed in Signal K (negative to port). Compass
    style bearings read better wrapped into 0..360.
    """
    return math.degrees(v) % 360.0


def kelvin_to_celsius(v: float) -> float:
    return v - 273.15


def ratio_to_percent(v: float) -> float:
    return v * 100.0


def hz_to_rpm(v: float) -> float:
    return v * 60.0


def pa_to_hpa(v: float) -> float:
    return v / 100.0


def pa_to_kpa(v: float) -> float:
    return v / 1000.0


def identity(v: float) -> float:
    return v


# --- mapping table ----------------------------------------------------------
#
# Keys:
#   name       entity name (within its device)
#   unit       unit_of_measurement published to HA
#   device_class / state_class / icon  passed through to discovery
#   convert    callable applied to the Signal K value
#   group      which HA device the entity belongs to; "*" segments are
#              substituted from the wildcard captures

SPEED = {"device_class": "speed", "state_class": "measurement"}
TEMP = {"device_class": "temperature", "state_class": "measurement"}
VOLT = {"device_class": "voltage", "state_class": "measurement"}
CURRENT = {"device_class": "current", "state_class": "measurement"}
PRESSURE = {"device_class": "pressure", "state_class": "measurement"}
ANGLE = {"state_class": "measurement"}
DISTANCE = {"device_class": "distance", "state_class": "measurement"}

PATH_MAP: dict[str, dict[str, Any]] = {
    # ---- navigation (MFD / GPS) ----
    "navigation.speedOverGround": {
        "name": "Speed over ground",
        "unit": "m/s",
        "convert": identity,
        "icon": "mdi:speedometer",
        "group": "navigation",
        **SPEED,
    },
    "navigation.speedThroughWater": {
        "name": "Speed through water",
        "unit": "m/s",
        "convert": identity,
        "icon": "mdi:speedometer-medium",
        "group": "navigation",
        **SPEED,
    },
    "navigation.courseOverGroundTrue": {
        "name": "Course over ground",
        "unit": "°",
        "convert": rad_to_deg_positive,
        "icon": "mdi:compass-outline",
        "group": "navigation",
        **ANGLE,
    },
    "navigation.headingTrue": {
        "name": "Heading true",
        "unit": "°",
        "convert": rad_to_deg_positive,
        "icon": "mdi:compass",
        "group": "navigation",
        **ANGLE,
    },
    "navigation.headingMagnetic": {
        "name": "Heading magnetic",
        "unit": "°",
        "convert": rad_to_deg_positive,
        "icon": "mdi:compass",
        "group": "navigation",
        **ANGLE,
    },
    "navigation.rateOfTurn": {
        "name": "Rate of turn",
        "unit": "°/s",
        "convert": rad_to_deg,
        "icon": "mdi:rotate-right",
        "group": "navigation",
        **ANGLE,
    },
    "navigation.attitude.roll": {
        "name": "Roll",
        "unit": "°",
        "convert": rad_to_deg,
        "icon": "mdi:angle-acute",
        "group": "navigation",
        **ANGLE,
    },
    "navigation.attitude.pitch": {
        "name": "Pitch",
        "unit": "°",
        "convert": rad_to_deg,
        "icon": "mdi:angle-acute",
        "group": "navigation",
        **ANGLE,
    },
    "navigation.log": {
        "name": "Log (total distance)",
        "unit": "m",
        "convert": identity,
        "icon": "mdi:map-marker-distance",
        "group": "navigation",
        "device_class": "distance",
        "state_class": "total_increasing",
    },
    "navigation.trip.log": {
        "name": "Trip distance",
        "unit": "m",
        "convert": identity,
        "icon": "mdi:map-marker-distance",
        "group": "navigation",
        "device_class": "distance",
        "state_class": "total",
    },
    "navigation.courseOverGroundMagnetic": {
        "name": "Course over ground (mag)",
        "unit": "°",
        "convert": rad_to_deg_positive,
        "icon": "mdi:compass-outline",
        "group": "navigation",
        **ANGLE,
    },
    "navigation.magneticVariation": {
        "name": "Magnetic variation",
        "unit": "°",
        "convert": rad_to_deg,
        "icon": "mdi:compass",
        "group": "navigation",
        **ANGLE,
    },
    # ---- steering (autopilot / rudder) ----
    "steering.rudderAngle": {
        "name": "Rudder angle",
        "unit": "°",
        "convert": rad_to_deg,
        "icon": "mdi:angle-acute",
        "group": "steering",
        **ANGLE,
    },
    # ---- GPS / GNSS ----
    "navigation.gnss.satellites": {
        "name": "Satellites in use",
        "unit": None,
        "convert": identity,
        "icon": "mdi:satellite-variant",
        "state_class": "measurement",
        "group": "gps",
    },
    "navigation.gnss.antennaAltitude": {
        "name": "Antenna altitude",
        "unit": "m",
        "convert": identity,
        "icon": "mdi:arrow-up-down",
        "group": "gps",
        "device_class": "distance",
        "state_class": "measurement",
    },
    "navigation.gnss.horizontalDilution": {
        "name": "HDOP",
        "unit": None,
        "convert": identity,
        "icon": "mdi:crosshairs-gps",
        "state_class": "measurement",
        "group": "gps",
    },
    # ---- depth (DST transducer) ----
    "environment.depth.belowTransducer": {
        "name": "Depth below transducer",
        "unit": "m",
        "convert": identity,
        "icon": "mdi:waves-arrow-down",
        "group": "environment",
        **DISTANCE,
    },
    "environment.depth.belowKeel": {
        "name": "Depth below keel",
        "unit": "m",
        "convert": identity,
        "icon": "mdi:waves-arrow-down",
        "group": "environment",
        **DISTANCE,
    },
    "environment.depth.belowSurface": {
        "name": "Depth below surface",
        "unit": "m",
        "convert": identity,
        "icon": "mdi:waves-arrow-down",
        "group": "environment",
        **DISTANCE,
    },
    # ---- wind ----
    "environment.wind.speedApparent": {
        "name": "Apparent wind speed",
        "unit": "m/s",
        "convert": identity,
        "icon": "mdi:weather-windy",
        "group": "environment",
        **SPEED,
    },
    "environment.wind.speedTrue": {
        "name": "True wind speed",
        "unit": "m/s",
        "convert": identity,
        "icon": "mdi:weather-windy",
        "group": "environment",
        **SPEED,
    },
    "environment.wind.angleApparent": {
        "name": "Apparent wind angle",
        "unit": "°",
        "convert": rad_to_deg,
        "icon": "mdi:windsock",
        "group": "environment",
        **ANGLE,
    },
    "environment.wind.angleTrueWater": {
        "name": "True wind angle",
        "unit": "°",
        "convert": rad_to_deg,
        "icon": "mdi:windsock",
        "group": "environment",
        **ANGLE,
    },
    "environment.wind.directionTrue": {
        "name": "True wind direction",
        "unit": "°",
        "convert": rad_to_deg_positive,
        "icon": "mdi:windsock",
        "group": "environment",
        **ANGLE,
    },
    # ---- water / air ----
    "environment.water.temperature": {
        "name": "Sea temperature",
        "unit": "°C",
        "convert": kelvin_to_celsius,
        "icon": "mdi:coolant-temperature",
        "group": "environment",
        **TEMP,
    },
    "environment.outside.temperature": {
        "name": "Outside temperature",
        "unit": "°C",
        "convert": kelvin_to_celsius,
        "group": "environment",
        **TEMP,
    },
    "environment.outside.pressure": {
        "name": "Barometric pressure",
        "unit": "hPa",
        "convert": pa_to_hpa,
        "group": "environment",
        **PRESSURE,
    },
    "environment.inside.temperature": {
        "name": "Cabin temperature",
        "unit": "°C",
        "convert": kelvin_to_celsius,
        "group": "environment",
        **TEMP,
    },
    "environment.inside.humidity": {
        "name": "Cabin humidity",
        "unit": "%",
        "convert": ratio_to_percent,
        "device_class": "humidity",
        "state_class": "measurement",
        "group": "environment",
    },
    # ---- propulsion (engine gateway) ----
    "propulsion.*.revolutions": {
        "name": "Engine speed",
        "unit": "rpm",
        "convert": hz_to_rpm,
        "icon": "mdi:engine",
        "state_class": "measurement",
        "group": "engine.*",
    },
    "propulsion.*.temperature": {
        "name": "Coolant temperature",
        "unit": "°C",
        "convert": kelvin_to_celsius,
        "icon": "mdi:coolant-temperature",
        "group": "engine.*",
        **TEMP,
    },
    "propulsion.*.oilTemperature": {
        "name": "Oil temperature",
        "unit": "°C",
        "convert": kelvin_to_celsius,
        "icon": "mdi:oil-temperature",
        "group": "engine.*",
        **TEMP,
    },
    "propulsion.*.oilPressure": {
        "name": "Oil pressure",
        "unit": "kPa",
        "convert": pa_to_kpa,
        "icon": "mdi:oil",
        "group": "engine.*",
        **PRESSURE,
    },
    "propulsion.*.alternatorVoltage": {
        "name": "Alternator voltage",
        "unit": "V",
        "convert": identity,
        "icon": "mdi:car-battery",
        "group": "engine.*",
        **VOLT,
    },
    "propulsion.*.runTime": {
        "name": "Engine hours",
        "unit": "h",
        "convert": lambda v: v / 3600.0,
        "icon": "mdi:timer-outline",
        "device_class": "duration",
        "state_class": "total_increasing",
        "group": "engine.*",
    },
    "propulsion.*.fuel.rate": {
        "name": "Fuel rate",
        "unit": "L/h",
        "convert": lambda v: v * 3_600_000.0,
        "icon": "mdi:fuel",
        "state_class": "measurement",
        "group": "engine.*",
    },
    "propulsion.*.engineLoad": {
        "name": "Engine load",
        "unit": "%",
        "convert": ratio_to_percent,
        "icon": "mdi:engine",
        "state_class": "measurement",
        "group": "engine.*",
    },
    "propulsion.*.coolantPressure": {
        "name": "Coolant pressure",
        "unit": "kPa",
        "convert": pa_to_kpa,
        "icon": "mdi:gauge",
        "group": "engine.*",
        **PRESSURE,
    },
    # ---- electrical: batteries (battery monitor) ----
    "electrical.batteries.*.voltage": {
        "name": "Voltage",
        "unit": "V",
        "convert": identity,
        "icon": "mdi:car-battery",
        "group": "battery.*",
        **VOLT,
    },
    "electrical.batteries.*.current": {
        "name": "Current",
        "unit": "A",
        "convert": identity,
        "icon": "mdi:current-dc",
        "group": "battery.*",
        **CURRENT,
    },
    "electrical.batteries.*.stateOfCharge": {
        "name": "State of charge",
        "unit": "%",
        "convert": ratio_to_percent,
        "icon": "mdi:battery",
        "device_class": "battery",
        "state_class": "measurement",
        "group": "battery.*",
    },
    # Some monitors report SoC nested under capacity rather than at the bank root.
    "electrical.batteries.*.capacity.stateOfCharge": {
        "name": "State of charge",
        "unit": "%",
        "convert": ratio_to_percent,
        "icon": "mdi:battery",
        "device_class": "battery",
        "state_class": "measurement",
        "group": "battery.*",
    },
    "electrical.batteries.*.temperature": {
        "name": "Temperature",
        "unit": "°C",
        "convert": kelvin_to_celsius,
        "group": "battery.*",
        **TEMP,
    },
    "electrical.batteries.*.capacity.timeRemaining": {
        "name": "Time remaining",
        "unit": "h",
        "convert": lambda v: v / 3600.0,
        "icon": "mdi:battery-clock",
        "device_class": "duration",
        "state_class": "measurement",
        "group": "battery.*",
    },
    "electrical.batteries.*.power": {
        "name": "Power",
        "unit": "W",
        "convert": identity,
        "device_class": "power",
        "state_class": "measurement",
        "group": "battery.*",
    },
    # ---- electrical: solar / chargers / inverters ----
    "electrical.solar.*.voltage": {
        "name": "Solar voltage",
        "unit": "V",
        "convert": identity,
        "icon": "mdi:solar-power",
        "group": "solar.*",
        **VOLT,
    },
    "electrical.solar.*.current": {
        "name": "Solar current",
        "unit": "A",
        "convert": identity,
        "icon": "mdi:solar-power",
        "group": "solar.*",
        **CURRENT,
    },
    "electrical.solar.*.panelPower": {
        "name": "Solar power",
        "unit": "W",
        "convert": identity,
        "icon": "mdi:solar-power",
        "device_class": "power",
        "state_class": "measurement",
        "group": "solar.*",
    },
    "electrical.inverters.*.dc.voltage": {
        "name": "Inverter DC voltage",
        "unit": "V",
        "convert": identity,
        "group": "inverter.*",
        **VOLT,
    },
    # ---- tanks (N2K senders) ----
    "tanks.freshWater.*.currentLevel": {
        "name": "Fresh water level",
        "unit": "%",
        "convert": ratio_to_percent,
        "icon": "mdi:water",
        "state_class": "measurement",
        "group": "tank.freshWater.*",
    },
    "tanks.fuel.*.currentLevel": {
        "name": "Fuel level",
        "unit": "%",
        "convert": ratio_to_percent,
        "icon": "mdi:fuel",
        "state_class": "measurement",
        "group": "tank.fuel.*",
    },
    "tanks.blackWater.*.currentLevel": {
        "name": "Black water level",
        "unit": "%",
        "convert": ratio_to_percent,
        "icon": "mdi:toilet",
        "state_class": "measurement",
        "group": "tank.blackWater.*",
    },
    "tanks.wasteWater.*.currentLevel": {
        "name": "Waste water level",
        "unit": "%",
        "convert": ratio_to_percent,
        "icon": "mdi:water-percent",
        "state_class": "measurement",
        "group": "tank.wasteWater.*",
    },
    "tanks.liveWell.*.currentLevel": {
        "name": "Live well level",
        "unit": "%",
        "convert": ratio_to_percent,
        "icon": "mdi:fishbowl",
        "state_class": "measurement",
        "group": "tank.liveWell.*",
    },
}


# Human-readable device names. "*" is replaced with the captured instance id.
GROUP_LABELS: dict[str, str] = {
    "navigation": "Navigation",
    "environment": "Environment",
    "engine": "Engine",
    "battery": "Battery",
    "solar": "Solar",
    "inverter": "Inverter",
    "charger": "Charger",
    "gps": "GPS",
    "steering": "Steering",
    "switches.bank": "Digital switches bank",
    "alarms": "Alarms",
    "n2k_bus": "NMEA 2000 Bus",
    "vessel": "Vessel",
    "tank.freshWater": "Fresh water tank",
    "tank.fuel": "Fuel tank",
    "tank.blackWater": "Black water tank",
    "tank.wasteWater": "Waste water tank",
    "tank.liveWell": "Live well",
}


# --- non-numeric mappings ---------------------------------------------------
#
# The bridge's default entity is a numeric sensor. These tables cover the paths
# that need a different Home Assistant entity type: plain-text state sensors,
# binary sensors, and the vessel's position as a device_tracker.

# Enum / free-text state -> plain sensor (state published verbatim).
TEXT_MAP: dict[str, dict[str, Any]] = {
    "steering.autopilot.state": {
        "name": "Autopilot state",
        "icon": "mdi:ship-wheel",
        "group": "steering",
    },
    "navigation.gnss.methodQuality": {
        "name": "GPS fix quality",
        "icon": "mdi:crosshairs-gps",
        "group": "gps",
    },
    "navigation.gnss.type": {
        "name": "GPS type",
        "icon": "mdi:satellite-variant",
        "group": "gps",
    },
}

# Enum / free-text state on a wildcard (per-instance) path -> plain sensor,
# grouped under the captured instance id. Distinct from TEXT_MAP because the
# path carries a "*" capture (e.g. which charger).
TEXT_PATTERN_MAP: dict[str, dict[str, Any]] = {
    "electrical.chargers.*.chargingMode": {
        "name": "Charging mode",
        "icon": "mdi:battery-charging",
        "group": "charger.*",
    },
}

# Digital switch banks: electrical.switches.bank.<bank>.<channel>.state = 0|1.
SWITCH_PATTERN = "electrical.switches.bank.*.*.state"

# The vessel's position -> HA device_tracker (shows the boat on the map).
POSITION_PATH = "navigation.position"

# A Signal K notification is a dict {state, message, method}; treat anything not
# "normal"/"nominal" as an active alarm (binary_sensor ON).
NOTIFICATION_PREFIX = "notifications."
_ALARM_CLEAR_STATES = {"normal", "nominal"}


def notification_is_active(value: Any) -> bool:
    state = value.get("state") if isinstance(value, dict) else None
    return bool(state) and str(state).lower() not in _ALARM_CLEAR_STATES


def _slugify_tag(s: str) -> str:
    """Turn a free-form source label (an N2K installationDescription, a
    hex canName tail, etc.) into a lowercase entity-safe slug."""
    return "".join(c if c.isalnum() else "_" for c in s.lower()).strip("_")


def build_source_tags(sources: Any) -> dict[str, str]:
    """Build ``{$source_string: friendly_tag}`` from a Signal K ``/sources``
    payload.

    Prefers the N2K ``installationDescription1`` field (set by the boat
    owner on each device via manufacturer tooling and broadcast on the
    bus), falling back to the last four hex chars of the ``canName``.
    Returns ``{}`` if the payload is not the expected shape -- callers
    with no map get single-source behavior.
    """
    out: dict[str, str] = {}
    if not isinstance(sources, dict):
        return out
    # First pass: collect (source_string, preferred_tag, canName). Two
    # devices with the same installationDescription1 -- easy to leave
    # laying around if you clone one Victron device's config to another --
    # would otherwise collapse to the same tag and one would silently
    # overwrite the other in the fanout dict, which defeats the whole
    # point of splitting them.
    candidates: list[tuple[str, str, str]] = []
    for bus_key, bus_val in sources.items():
        if not isinstance(bus_val, dict):
            continue
        for info in bus_val.values():
            if not isinstance(info, dict):
                continue
            n2k = info.get("n2k")
            if not isinstance(n2k, dict):
                continue
            can_name = n2k.get("canName")
            if not isinstance(can_name, str) or not can_name:
                continue
            desc = n2k.get("installationDescription1")
            preferred = (
                _slugify_tag(desc) if isinstance(desc, str) and desc else can_name[-4:]
            )
            candidates.append((f"{bus_key}.{can_name}", preferred, can_name))
    # Detect collisions and disambiguate by appending the canName tail.
    tag_counts: dict[str, int] = {}
    for _, tag, _ in candidates:
        tag_counts[tag] = tag_counts.get(tag, 0) + 1
    for src_key, tag, can_name in candidates:
        out[src_key] = f"{tag}_{can_name[-4:]}" if tag_counts[tag] > 1 else tag
    return out


def _fanout_paths(
    path: str, values: dict[str, Any], source_tags: dict[str, str]
) -> list[tuple[str, Any]]:
    """Yield per-source ``(alt_path, value)`` pairs for a multi-source leaf.

    Returns empty when there are fewer than two genuinely-distinct
    readings -- a "values" dict with all-equal payloads is not a real
    collision, just Signal K bookkeeping.

    The instance segment of the path (the last numeric segment, if any)
    is replaced with the friendly source tag, so
    ``electrical.batteries.0.voltage`` fans out to
    ``electrical.batteries.house.voltage`` and
    ``electrical.batteries.engine.voltage``. Downstream ``PATH_MAP``
    matching is unchanged: the ``*`` wildcard captures the tag the same
    way it would capture ``"0"``.
    """
    distinct: list[tuple[str, Any]] = []
    seen: set[Any] = set()
    for src, sub in values.items():
        if not isinstance(sub, dict):
            continue
        v = sub.get("value")
        # Only scalars fan out: dicts, lists, and other composites don't
        # round-trip through an HA state string, and their presence in a
        # ``values`` dict is rare enough not to justify special handling.
        # (Restricting the guard to plain scalars also keeps the
        # ``v in seen`` check hashable.)
        if not isinstance(v, (int, float, str, bool)):
            continue
        if v in seen:
            continue
        seen.add(v)
        distinct.append((src, v))

    if len(distinct) < 2:
        return []

    segs = path.split(".")
    inst_idx: int | None = None
    for i in range(len(segs) - 1, -1, -1):
        if segs[i].isdigit():
            inst_idx = i
            break

    fanouts: list[tuple[str, Any]] = []
    for src, v in distinct:
        tag = source_tags.get(src) or src.rsplit(".", 1)[-1][-4:]
        if inst_idx is not None:
            alt_segs = list(segs)
            alt_segs[inst_idx] = tag
            alt_path = ".".join(alt_segs)
        else:
            # Singleton path (no instance segment) -- synthesize one just
            # before the leaf so PATH_MAP entries can pick these up via a
            # companion ``x.*.y`` pattern.
            alt_path = ".".join(segs[:-1] + [tag, segs[-1]])
        fanouts.append((alt_path, v))
    return fanouts


def flatten(
    tree: Any,
    prefix: str = "",
    source_tags: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Flatten a Signal K vessel tree into ``dotted.path -> value``.

    Signal K leaves are objects carrying a ``value`` key alongside metadata
    (``$source``, ``timestamp``, ``pgn``). Anything without a ``value`` is an
    intermediate node and is walked into.

    When ``source_tags`` is provided (a ``{$source: friendly_tag}`` map
    from :func:`build_source_tags`), leaves that carry a ``values`` dict
    with two-or-more distinct source readings also emit per-source
    fanout paths -- disambiguating same-schema devices (e.g. two BMVs
    both publishing under ``electrical.batteries.0``). The primary path
    is preserved either way so existing entity IDs never break.
    """
    tags = source_tags or {}
    out: dict[str, Any] = {}
    if not isinstance(tree, dict):
        return out

    for key, node in tree.items():
        if key.startswith("$") or key in (
            "meta",
            "timestamp",
            "pgn",
            "sentence",
            "values",
        ):
            continue
        path = f"{prefix}{key}"
        if isinstance(node, dict) and "value" in node:
            out[path] = node["value"]
            values = node.get("values")
            if isinstance(values, dict) and tags:
                for alt_path, alt_val in _fanout_paths(path, values, tags):
                    out[alt_path] = alt_val
            # Some leaves nest further (e.g. propulsion.x.fuel.rate sits under a
            # node that itself has no value); keep walking siblings.
            for sub, subnode in node.items():
                if sub in (
                    "value",
                    "$source",
                    "timestamp",
                    "pgn",
                    "meta",
                    "sentence",
                    "values",
                ):
                    continue
                out.update(flatten({sub: subnode}, f"{path}.", source_tags=tags))
        elif isinstance(node, dict):
            out.update(flatten(node, f"{path}.", source_tags=tags))
    return out


def match_path(actual: str, pattern: str) -> list[str] | None:
    """Match a concrete path against a pattern with ``*`` segments.

    Returns the captured segments, or None if it does not match. An empty list
    means an exact (wildcard-free) match.
    """
    a = actual.split(".")
    p = pattern.split(".")
    if len(a) != len(p):
        return None
    captures: list[str] = []
    for seg_a, seg_p in zip(a, p):
        if seg_p == "*":
            captures.append(seg_a)
        elif seg_a != seg_p:
            return None
    return captures


def resolve_group(group_pattern: str, captures: list[str]) -> tuple[str, str]:
    """Turn a group pattern plus captures into ``(group_id, display_label)``.

    ``battery.*`` + ``["house"]`` -> ``("battery.house", "Battery house")``
    """
    parts = group_pattern.split(".")
    caps = list(captures)
    resolved = [caps.pop(0) if seg == "*" else seg for seg in parts]
    group_id = ".".join(resolved)

    base = ".".join(seg for seg in parts if seg != "*")
    label = GROUP_LABELS.get(base, base.replace(".", " ").title())
    # Title-case the instance portion only when it's all-lowercase
    # letters, so friendly source tags read as names ("Battery House"
    # not "Battery house"). Mixed-case Signal K instance ids
    # ("engineStart") must pass through unchanged; numeric ("0") and
    # hex-fallback ("01f5") tags likewise.
    instance = " ".join(
        r.title() if r.islower() and r.isalpha() else r
        for r, seg in zip(resolved, parts)
        if seg == "*"
    )
    return group_id, (f"{label} {instance}" if instance else label)
