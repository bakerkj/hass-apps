# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for the non-numeric entity types: text/enum sensors, switch and alarm
binary sensors, and the vessel position device_tracker."""

from typing import Any

from signalk_bridge.app import resolve_entities, resolve_special
from signalk_bridge.paths import slugify


def test_state_of_charge_under_capacity_becomes_sensor(
    vessel_tree: dict[str, Any],
) -> None:
    ents = resolve_entities(vessel_tree)
    soc = ents[slugify("electrical.batteries.1.capacity.stateOfCharge")]
    assert soc["component"] == "sensor"
    assert soc["state"] == "55"
    assert soc["group_label"] == "Battery 1"


def test_rudder_and_gnss_are_numeric_sensors(vessel_tree: dict[str, Any]) -> None:
    ents = resolve_entities(vessel_tree)
    assert ents[slugify("steering.rudderAngle")]["state"] == "-5"
    assert ents[slugify("navigation.gnss.satellites")]["state"] == "9"
    assert ents[slugify("navigation.gnss.satellites")]["group_label"] == "GPS"
    assert ents[slugify("navigation.trip.log")]["group_label"] == "Navigation"


def test_autopilot_and_gps_quality_are_text_sensors(
    vessel_tree: dict[str, Any],
) -> None:
    spc = resolve_special(vessel_tree)
    ap = spc[slugify("steering.autopilot.state")]
    assert ap["component"] == "sensor"
    assert ap["state"] == "standby"
    assert ap["group_label"] == "Steering"
    assert spc[slugify("navigation.gnss.methodQuality")]["state"] == "DGNSS fix"


def test_switch_bank_becomes_binary_sensors(vessel_tree: dict[str, Any]) -> None:
    spc = resolve_special(vessel_tree)
    on = spc[slugify("electrical.switches.bank.0.1.state")]
    off = spc[slugify("electrical.switches.bank.0.2.state")]
    assert on["component"] == "binary_sensor"
    assert on["state"] == "ON"
    assert off["state"] == "OFF"
    assert on["name"] == "Switch 1"
    assert on["group_label"] == "Digital switches bank 0"


def test_position_becomes_device_tracker(vessel_tree: dict[str, Any]) -> None:
    spc = resolve_special(vessel_tree)
    pos = spc[slugify("navigation.position")]
    assert pos["component"] == "device_tracker"
    assert pos["state"] is None
    assert pos["attributes"]["latitude"] == 40.0
    assert pos["attributes"]["longitude"] == -70.0


def test_charging_mode_is_text_sensor(vessel_tree: dict[str, Any]) -> None:
    # A wildcard text path (electrical.chargers.*.chargingMode) must resolve to a
    # plain sensor grouped under the captured charger id -- not be silently
    # dropped the way a numeric-only mapping would drop a text value.
    spc = resolve_special(vessel_tree)
    mode = spc[slugify("electrical.chargers.ac1.chargingMode")]
    assert mode["component"] == "sensor"
    assert mode["state"] == "bulk"
    assert mode["group_label"] == "Charger ac1"


def test_active_alarm_is_on_normal_is_off(vessel_tree: dict[str, Any]) -> None:
    spc = resolve_special(vessel_tree)
    alarm = spc[slugify("notifications.instrument.PilotOffCourse")]
    normal = spc[slugify("notifications.navigation.anchor")]
    assert alarm["component"] == "binary_sensor"
    assert alarm["device_class"] == "problem"
    assert alarm["state"] == "ON"
    assert alarm["name"] == "Pilot Off Course"
    assert normal["state"] == "OFF"
