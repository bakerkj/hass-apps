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


# Joules to kilowatt-hours -- Victron MPPT yield counters report SI joules; HA's
# Energy dashboard wants kWh.
def j_to_kwh(v: float) -> float:
    return v / 3_600_000.0


# Coulombs to amp-hours -- Signal K battery capacity fields (consumedCharge) are
# SI coulombs; a battery monitor's headline "Consumed Ah" is amp-hours.
def coulombs_to_ah(v: float) -> float:
    return v / 3600.0


# Cubic metres to US gallons -- N2K PGN 127505 reports tank capacity in m3;
# HA users on any US boat expect gallons.
_M3_TO_GAL = 264.172052


def m3_to_gal(v: float) -> float:
    return v * _M3_TO_GAL


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
# Voltage defaults to 2 decimals in HA (12.84 V, not 12.8 or 12.8442); the raw
# state keeps full precision, this only sets the displayed rounding.
VOLT = {
    "device_class": "voltage",
    "state_class": "measurement",
    "suggested_display_precision": 2,
}
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
    "steering.autopilot.target.headingMagnetic": {
        "name": "Target heading (mag)",
        "unit": "°",
        "convert": rad_to_deg_positive,
        "icon": "mdi:ship-wheel",
        "group": "steering",
        **ANGLE,
    },
    "steering.autopilot.target.headingTrue": {
        "name": "Target heading (true)",
        "unit": "°",
        "convert": rad_to_deg_positive,
        "icon": "mdi:ship-wheel",
        "group": "steering",
        **ANGLE,
    },
    "steering.autopilot.target.windAngleApparent": {
        "name": "Target wind angle",
        "unit": "°",
        "convert": rad_to_deg,
        "icon": "mdi:windsock",
        "group": "steering",
        **ANGLE,
    },
    # ---- active waypoint / course (MFD navigating to a mark or route) ----
    # Present only while a GOTO/route is active; N2K PGN 129283/129284/129285.
    "navigation.courseGreatCircle.crossTrackError": {
        # Signed: negative = vessel left of track. No distance device_class,
        # which HA treats as non-negative.
        "name": "Cross track error",
        "unit": "m",
        "convert": identity,
        "icon": "mdi:arrow-expand-horizontal",
        "state_class": "measurement",
        "group": "course",
    },
    "navigation.courseGreatCircle.bearingTrackTrue": {
        "name": "Track bearing",
        "unit": "°",
        "convert": rad_to_deg_positive,
        "icon": "mdi:compass-outline",
        "group": "course",
        **ANGLE,
    },
    "navigation.courseGreatCircle.nextPoint.distance": {
        "name": "Distance to waypoint",
        "unit": "m",
        "convert": identity,
        "icon": "mdi:map-marker-distance",
        "group": "course",
        **DISTANCE,
    },
    "navigation.courseGreatCircle.nextPoint.bearingTrue": {
        "name": "Bearing to waypoint",
        "unit": "°",
        "convert": rad_to_deg_positive,
        "icon": "mdi:compass",
        "group": "course",
        **ANGLE,
    },
    "navigation.courseGreatCircle.nextPoint.velocityMadeGood": {
        # Signed: negative while losing ground to the mark. No speed
        # device_class, matching crossTrackError's signed-value handling.
        "name": "VMG to waypoint",
        "unit": "m/s",
        "convert": identity,
        "icon": "mdi:speedometer",
        "state_class": "measurement",
        "group": "course",
    },
    "navigation.courseGreatCircle.nextPoint.timeToGo": {
        "name": "Time to go",
        "unit": "s",
        "convert": identity,
        "icon": "mdi:timer-sand",
        "device_class": "duration",
        "state_class": "measurement",
        "group": "course",
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
        "name": "Horizontal dilution of precision",
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
    "electrical.batteries.*.capacity.consumedCharge": {
        # SI coulombs -> the battery monitor's headline "Consumed Ah".
        "name": "Consumed charge",
        "unit": "Ah",
        "convert": coulombs_to_ah,
        "icon": "mdi:battery-minus-variant",
        "state_class": "measurement",
        "group": "battery.*",
    },
    "electrical.batteries.*.capacity.dischargedEnergy": {
        "name": "Discharged energy",
        "unit": "kWh",
        "convert": j_to_kwh,
        "icon": "mdi:battery-arrow-down",
        "device_class": "energy",
        "state_class": "total_increasing",
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
    "electrical.solar.*.panelVoltage": {
        # PV-array side, distinct from the battery-side ``voltage`` above.
        "name": "Panel voltage",
        "unit": "V",
        "convert": identity,
        "icon": "mdi:solar-panel",
        "group": "solar.*",
        **VOLT,
    },
    "electrical.solar.*.yieldToday": {
        "name": "Yield today",
        "unit": "kWh",
        "convert": j_to_kwh,
        "icon": "mdi:solar-power",
        "device_class": "energy",
        "state_class": "total_increasing",
        "group": "solar.*",
    },
    "electrical.solar.*.yieldYesterday": {
        # A fixed daily figure, not a running meter -- plain total, not
        # total_increasing (it never rises through the day).
        "name": "Yield yesterday",
        "unit": "kWh",
        "convert": j_to_kwh,
        "icon": "mdi:solar-power",
        "device_class": "energy",
        "state_class": "total",
        "group": "solar.*",
    },
    "electrical.solar.*.systemYield": {
        "name": "Total yield",
        "unit": "kWh",
        "convert": j_to_kwh,
        "icon": "mdi:solar-power",
        "device_class": "energy",
        "state_class": "total_increasing",
        "group": "solar.*",
    },
    "electrical.inverters.*.dc.voltage": {
        "name": "Inverter DC voltage",
        "unit": "V",
        "convert": identity,
        "group": "inverter.*",
        **VOLT,
    },
    # ---- Victron GX system aggregate ----
    "electrical.venus.dcPower": {
        "name": "DC power",
        "unit": "W",
        "convert": identity,
        "icon": "mdi:flash",
        "device_class": "power",
        "state_class": "measurement",
        "group": "system",
    },
    # ---- tanks (N2K senders) ----
    # ``group`` intentionally has no ``*``: N2K's per-fluid-type instance
    # is bookkeeping, and single-instance boats read "Fresh water tank"
    # instead of "Fresh water tank 7". app.py's ``_entity_key`` also drops
    # the captured instance for these, falling back to the digit form
    # only when multiple tanks of the same fluid type exist.
    "tanks.freshWater.*.currentLevel": {
        "name": "Level",
        "unit": "%",
        "convert": ratio_to_percent,
        "icon": "mdi:water",
        "state_class": "measurement",
        "group": "tank.freshWater",
    },
    "tanks.freshWater.*.capacity": {
        "name": "Capacity",
        "unit": "gal",
        "convert": m3_to_gal,
        "icon": "mdi:water",
        "device_class": "volume_storage",
        "state_class": "measurement",
        "group": "tank.freshWater",
    },
    "tanks.fuel.*.currentLevel": {
        "name": "Level",
        "unit": "%",
        "convert": ratio_to_percent,
        "icon": "mdi:fuel",
        "state_class": "measurement",
        "group": "tank.fuel",
    },
    "tanks.fuel.*.capacity": {
        "name": "Capacity",
        "unit": "gal",
        "convert": m3_to_gal,
        "icon": "mdi:fuel",
        "device_class": "volume_storage",
        "state_class": "measurement",
        "group": "tank.fuel",
    },
    "tanks.blackWater.*.currentLevel": {
        "name": "Level",
        "unit": "%",
        "convert": ratio_to_percent,
        "icon": "mdi:toilet",
        "state_class": "measurement",
        "group": "tank.blackWater",
    },
    "tanks.blackWater.*.capacity": {
        "name": "Capacity",
        "unit": "gal",
        "convert": m3_to_gal,
        "icon": "mdi:toilet",
        "device_class": "volume_storage",
        "state_class": "measurement",
        "group": "tank.blackWater",
    },
    "tanks.wasteWater.*.currentLevel": {
        "name": "Level",
        "unit": "%",
        "convert": ratio_to_percent,
        "icon": "mdi:water-percent",
        "state_class": "measurement",
        "group": "tank.wasteWater",
    },
    "tanks.wasteWater.*.capacity": {
        "name": "Capacity",
        "unit": "gal",
        "convert": m3_to_gal,
        "icon": "mdi:water-percent",
        "device_class": "volume_storage",
        "state_class": "measurement",
        "group": "tank.wasteWater",
    },
    "tanks.liveWell.*.currentLevel": {
        "name": "Level",
        "unit": "%",
        "convert": ratio_to_percent,
        "icon": "mdi:fishbowl",
        "state_class": "measurement",
        "group": "tank.liveWell",
    },
    "tanks.liveWell.*.capacity": {
        "name": "Capacity",
        "unit": "gal",
        "convert": m3_to_gal,
        "icon": "mdi:fishbowl",
        "device_class": "volume_storage",
        "state_class": "measurement",
        "group": "tank.liveWell",
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
    "converter": "Converter",
    "gps": "GPS",
    "steering": "Steering",
    "course": "Course",
    "system": "System",
    "communication": "Communication",
    "n2k": "N2K Fleet",
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
    "navigation.gnss.integrity": {
        "name": "GPS integrity",
        "icon": "mdi:shield-check",
        "group": "gps",
    },
    # Active waypoint / route names (companion to the numeric course entries).
    "navigation.courseGreatCircle.nextPoint.name": {
        "name": "Next waypoint",
        "icon": "mdi:map-marker",
        "group": "course",
    },
    "navigation.courseGreatCircle.activeRoute.name": {
        "name": "Active route",
        "icon": "mdi:map-marker-path",
        "group": "course",
    },
    # Own vessel's VHF callsign (static, but useful on the dashboard for
    # quick reference and to confirm the DSC MMSI mapping is populated).
    "communication.callsignVhf": {
        "name": "VHF callsign",
        "icon": "mdi:radio-handheld",
        "group": "communication",
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
    # DC-DC / solar converters: N2K PGN 127507 reports operating state
    # (bulk/absorption/float/off). Two-wildcard capture matches
    # ``electrical.converter.<instance>.<sub>.operatingState`` which is
    # what canboatjs emits for SmartSolar chargers.
    "electrical.converter.*.*.operatingState": {
        "name": "Operating state",
        "icon": "mdi:solar-power-variant",
        "group": "converter.*",
    },
    # MPPT charge stage (bulk / absorption / float / off).
    "electrical.solar.*.controllerMode": {
        "name": "Charge mode",
        "icon": "mdi:solar-power-variant",
        "group": "solar.*",
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

# Safety-critical notification branch roots -- DSC calls, MOB events,
# distress relays -- get HA's ``safety`` device class so the mobile app
# and dashboards escalate them prominently instead of the generic
# ``problem`` class every other notification uses. Matched as full path
# segments (exact match OR ``<prefix>.<something>``) so a future path
# like ``notifications.mobileNetwork.lost`` does NOT collide with
# ``notifications.mob``.
SAFETY_NOTIFICATION_BRANCHES = (
    "notifications.mob",
    "notifications.dsc",
    "notifications.communications.dsc",
    "notifications.communication.dsc",
    "notifications.communications.mob",
    "notifications.communication.mob",
    "notifications.communications.distress",
    "notifications.communication.distress",
)


def notification_is_active(value: Any) -> bool:
    state = value.get("state") if isinstance(value, dict) else None
    return bool(state) and str(state).lower() not in _ALARM_CLEAR_STATES


def notification_device_class(path: str) -> str:
    """HA device_class for a notification path -- ``safety`` for DSC/MOB/
    distress branches, ``problem`` for everything else.

    Segment-boundary matching: ``notifications.mob`` matches exactly or
    as ``notifications.mob.<anything>``, but NOT
    ``notifications.mobileNetwork.lost``. Same pattern the file uses in
    :func:`_is_suppressed`, so behaviour is consistent.
    """
    # `path in branches` handles exact match; the second clause handles
    # any leaf under one of the branches. Together they give the exact
    # segment-boundary semantics without a per-prefix loop.
    if path in SAFETY_NOTIFICATION_BRANCHES:
        return "safety"
    return (
        "safety"
        if path.startswith(tuple(f"{p}." for p in SAFETY_NOTIFICATION_BRANCHES))
        else "problem"
    )


def slugify(path: str) -> str:
    """Signal K path -> MQTT/entity-safe key."""
    return "".join(c if c.isalnum() else "_" for c in path).strip("_").lower()


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
                slugify(desc) if isinstance(desc, str) and desc else can_name[-4:]
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
) -> list[tuple[str, Any, str | None]]:
    """Yield per-source ``(alt_path, value)`` pairs for a multi-source leaf.

    Returns empty when fewer than two sources report a scalar value.
    Fans out per source regardless of whether the readings currently
    agree -- two physical devices are still two devices, and gating on
    value equality would flap the per-source entities to Unavailable in
    HA every time the readings converged.

    The instance segment of the path (the last numeric segment, if any)
    is replaced with the friendly source tag, so
    ``electrical.batteries.0.voltage`` fans out to
    ``electrical.batteries.house.voltage`` and
    ``electrical.batteries.engine.voltage``. Downstream ``PATH_MAP``
    matching is unchanged: the ``*`` wildcard captures the tag the same
    way it would capture ``"0"``.
    """
    per_source: list[tuple[str, Any, str | None]] = []
    for src, sub in values.items():
        if not isinstance(sub, dict):
            continue
        v = sub.get("value")
        # Only scalars fan out: composites (position dicts, list-valued
        # readings) don't round-trip through an HA state string and are
        # rare enough not to justify special handling here.
        if not isinstance(v, (int, float, str, bool)):
            continue
        ts = sub.get("timestamp")
        per_source.append((src, v, ts if isinstance(ts, str) else None))

    if len(per_source) < 2:
        return []

    segs = path.split(".")
    inst_idx: int | None = None
    for i in range(len(segs) - 1, -1, -1):
        if segs[i].isdigit():
            inst_idx = i
            break

    fanouts: list[tuple[str, Any, str | None]] = []
    for src, v, ts in per_source:
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
        fanouts.append((alt_path, v, ts))
    return fanouts


def _is_suppressed(path: str, suppress: tuple[str, ...]) -> bool:
    """Return True when ``path`` is at or below any of the suppress-list
    prefixes (dotted-segment boundary respected, so ``batteries.2`` does
    not match ``batteries.239``)."""
    for pref in suppress:
        if path == pref or path.startswith(pref + "."):
            return True
    return False


def flatten_with_meta(
    tree: Any,
    prefix: str = "",
    source_tags: dict[str, str] | None = None,
    suppress_paths: tuple[str, ...] | list[str] | None = None,
    suppress_primary_on_fanout: bool = False,
) -> dict[str, tuple[Any, str | None]]:
    """Flatten a Signal K vessel tree into ``dotted.path -> (value, timestamp)``.

    The same walk as :func:`flatten`, but each Signal K ``timestamp`` string is
    carried through (``None`` when a leaf has no parseable timestamp) so callers
    can reason about freshness -- see :mod:`signalk_bridge.staleness`. Fanout
    paths carry their own per-source timestamp, so one source going stale is
    distinguishable from another under the same primary path.

    Signal K leaves are objects carrying a ``value`` key alongside metadata
    (``$source``, ``timestamp``, ``pgn``). Anything without a ``value`` is an
    intermediate node and is walked into.

    When ``source_tags`` is provided (a ``{$source: friendly_tag}`` map
    from :func:`build_source_tags`), leaves that carry a ``values`` dict
    with two-or-more distinct source readings also emit per-source
    fanout paths -- disambiguating same-schema devices (e.g. two BMVs
    both publishing under ``electrical.batteries.0``). The primary path
    is preserved either way so existing entity IDs never break.

    ``suppress_paths`` is an optional list of dotted-path prefixes. Any
    path at or below one of the prefixes -- primary or fanout -- is
    dropped entirely, for hiding schema duplicates (a N2K device that
    broadcasts under two instance IDs) or misnamed entities that the
    fanout gives better replacements for.

    ``suppress_primary_on_fanout``, when True, drops the primary path
    on any leaf that fanned out. The primary reflects whichever source
    Signal K arbitrarily promoted to canonical -- once the per-source
    entities exist, the primary is a noisy alias for one of them.
    Off by default so existing entity IDs stay stable across upgrades.
    """
    tags = source_tags or {}
    suppress = tuple(suppress_paths) if suppress_paths else ()
    out: dict[str, tuple[Any, str | None]] = {}
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
        if suppress and _is_suppressed(path, suppress):
            continue
        if isinstance(node, dict) and "value" in node:
            ts = node.get("timestamp")
            values = node.get("values")
            fanouts: list[tuple[str, Any, str | None]] = []
            if isinstance(values, dict) and tags:
                fanouts = _fanout_paths(path, values, tags)
            if not (fanouts and suppress_primary_on_fanout):
                out[path] = (node["value"], ts if isinstance(ts, str) else None)
            for alt_path, alt_val, alt_ts in fanouts:
                if not suppress or not _is_suppressed(alt_path, suppress):
                    out[alt_path] = (alt_val, alt_ts)
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
                out.update(
                    flatten_with_meta(
                        {sub: subnode},
                        f"{path}.",
                        source_tags=tags,
                        suppress_paths=suppress,
                        suppress_primary_on_fanout=suppress_primary_on_fanout,
                    )
                )
        elif isinstance(node, dict):
            out.update(
                flatten_with_meta(
                    node,
                    f"{path}.",
                    source_tags=tags,
                    suppress_paths=suppress,
                    suppress_primary_on_fanout=suppress_primary_on_fanout,
                )
            )
    return out


def flatten(
    tree: Any,
    prefix: str = "",
    source_tags: dict[str, str] | None = None,
    suppress_paths: tuple[str, ...] | list[str] | None = None,
    suppress_primary_on_fanout: bool = False,
) -> dict[str, Any]:
    """Flatten a Signal K vessel tree into ``dotted.path -> value``.

    Thin wrapper over :func:`flatten_with_meta` that drops the timestamp; see
    that function for the fanout and suppression semantics.
    """
    return {
        path: value
        for path, (value, _ts) in flatten_with_meta(
            tree,
            prefix,
            source_tags=source_tags,
            suppress_paths=suppress_paths,
            suppress_primary_on_fanout=suppress_primary_on_fanout,
        ).items()
    }


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
    # Collapse "Solar Solar" -> "Solar" when the venus plugin's device
    # CustomName duplicates the fluid/device category ("solar" instance
    # of a "solar" group).
    if instance and instance.casefold() == label.casefold():
        return group_id, label
    return group_id, (f"{label} {instance}" if instance else label)
