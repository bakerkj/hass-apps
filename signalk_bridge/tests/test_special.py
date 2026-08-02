# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for the non-numeric entity types: text/enum sensors, switch and alarm
binary sensors, and the vessel position device_tracker."""

from typing import Any

import pytest
from signalk_bridge.app import resolve_entities, resolve_special
from signalk_bridge.paths import slugify


def _tank_tree(**tanks: dict) -> dict:
    """Build a minimal SK vessel tree with just tank leaves."""

    def leaf(v):
        return {"value": v}

    result: dict = {}
    for path, fields in tanks.items():
        node = result
        for seg in path.split("."):
            node = node.setdefault(seg, {})
        for k, v in fields.items():
            node[k] = leaf(v)
    return result


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


def test_course_waypoint_numeric_sensors(vessel_tree: dict[str, Any]) -> None:
    ents = resolve_entities(vessel_tree)
    xte = ents[slugify("navigation.courseGreatCircle.crossTrackError")]
    assert xte["state"] == "28.81"
    assert xte["unit"] == "m"
    assert xte["device_class"] is None  # signed; not an HA distance
    assert xte["group_label"] == "Course"
    btw = ents[slugify("navigation.courseGreatCircle.nextPoint.bearingTrue")]
    assert btw["unit"] == "°"
    assert float(btw["state"]) == pytest.approx(320.4, abs=0.2)  # 5.592 rad -> 0..360
    dtw = ents[slugify("navigation.courseGreatCircle.nextPoint.distance")]
    assert dtw["state"] == "233"
    ttg = ents[slugify("navigation.courseGreatCircle.nextPoint.timeToGo")]
    assert ttg["device_class"] == "duration"
    assert ttg["unit"] == "s"


def test_course_names_are_text_sensors(vessel_tree: dict[str, Any]) -> None:
    spc = resolve_special(vessel_tree)
    wp = spc[slugify("navigation.courseGreatCircle.nextPoint.name")]
    assert wp["component"] == "sensor"
    assert wp["state"] == "GOTO CURSOR"
    assert wp["group_label"] == "Course"
    route = spc[slugify("navigation.courseGreatCircle.activeRoute.name")]
    assert route["state"] == "Route"


def test_autopilot_target_heading_is_numeric(vessel_tree: dict[str, Any]) -> None:
    ents = resolve_entities(vessel_tree)
    tgt = ents[slugify("steering.autopilot.target.headingMagnetic")]
    assert tgt["unit"] == "°"
    assert float(tgt["state"]) == pytest.approx(62.0, abs=0.2)  # 1.0825 rad
    assert tgt["group_label"] == "Steering"


def test_water_current_splits_into_set_and_drift(vessel_tree: dict[str, Any]) -> None:
    # environment.current is one composite leaf ({setTrue, drift}); it must
    # surface as two scalar sensors, not be dropped like other dict values.
    spc = resolve_special(vessel_tree)
    cset = spc["environment_current_set"]
    cdrift = spc["environment_current_drift"]
    assert cset["unit"] == "°"
    assert float(cset["state"]) == pytest.approx(90.0, abs=0.1)  # 1.5708 rad
    assert cset["group_label"] == "Environment"
    assert cdrift["unit"] == "m/s"
    assert cdrift["device_class"] == "speed"
    assert float(cdrift["state"]) == pytest.approx(0.5, abs=0.01)


def _tree_from(paths_: dict[str, Any]) -> dict:
    """Build a minimal SK vessel tree from {dotted.path: value}."""
    root: dict = {}
    for dotted, v in paths_.items():
        node = root
        segs = dotted.split(".")
        for seg in segs[:-1]:
            node = node.setdefault(seg, {})
        node[segs[-1]] = {"value": v}
    return root


def test_vmg_is_signed_and_has_no_device_class() -> None:
    # VMG goes negative when losing ground to the mark; it must keep the sign
    # and (like crossTrackError) carry no speed device_class.
    tree = _tree_from({"navigation.courseGreatCircle.nextPoint.velocityMadeGood": -0.5})
    vmg = resolve_entities(tree)[
        slugify("navigation.courseGreatCircle.nextPoint.velocityMadeGood")
    ]
    assert vmg["state"] == "-0.5"
    assert vmg["unit"] == "m/s"
    assert vmg["device_class"] is None
    assert vmg["group_label"] == "Course"


def test_cross_track_error_preserves_negative() -> None:
    tree = _tree_from({"navigation.courseGreatCircle.crossTrackError": -28.81})
    xte = resolve_entities(tree)[
        slugify("navigation.courseGreatCircle.crossTrackError")
    ]
    assert xte["state"] == "-28.81"
    assert xte["device_class"] is None


def test_time_to_go_value_passes_through_in_seconds() -> None:
    tree = _tree_from({"navigation.courseGreatCircle.nextPoint.timeToGo": 3881.839})
    ttg = resolve_entities(tree)[
        slugify("navigation.courseGreatCircle.nextPoint.timeToGo")
    ]
    assert ttg["state"] == "3881.839"
    assert ttg["unit"] == "s"
    assert ttg["device_class"] == "duration"


def test_current_set_wraps_past_180() -> None:
    # 5.7596 rad = 330 deg. Signed conversion would give -30, so this is the
    # case that actually exercises the 0..360 wrap (the fixture's 90 deg cannot).
    tree = _tree_from({"environment.current": {"setTrue": 5.7596, "drift": 0.5}})
    spc = resolve_special(tree)
    assert float(spc["environment_current_set"]["state"]) == pytest.approx(
        330.0, abs=0.2
    )


def test_current_set_falls_back_to_set_magnetic() -> None:
    # A device reporting only magnetic set must still yield a set sensor.
    tree = _tree_from({"environment.current": {"setMagnetic": 1.5708, "drift": 0.5}})
    spc = resolve_special(tree)
    assert float(spc["environment_current_set"]["state"]) == pytest.approx(
        90.0, abs=0.1
    )
    assert float(spc["environment_current_drift"]["state"]) == pytest.approx(0.5)


def test_current_composite_not_leaked_as_numeric_sensor() -> None:
    # The composite leaf must split into set+drift, never leak as a phantom
    # numeric sensor keyed on the composite path itself.
    tree = _tree_from({"environment.current": {"setTrue": 1.5708, "drift": 0.5}})
    assert "environment_current" not in resolve_entities(tree)
    spc = resolve_special(tree)
    assert {k for k in spc if k.startswith("environment_current")} == {
        "environment_current_set",
        "environment_current_drift",
    }


def test_current_absent_or_non_dict_yields_nothing() -> None:
    assert not any(
        k.startswith("environment_current")
        for k in resolve_special(
            {"environment": {"water": {"temperature": {"value": 288.0}}}}
        )
    )
    scalar = _tree_from({"environment.current": 1.23})  # not a dict -> ignored
    assert not any(k.startswith("environment_current") for k in resolve_special(scalar))


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


# --------------------------------------------------------------------------
# tank parity: rename + derived remaining/fluid_type sensors
# --------------------------------------------------------------------------


def test_single_tank_key_drops_instance_digit() -> None:
    tree = _tank_tree(**{"tanks.freshWater.7": {"currentLevel": 0.62}})
    ents = resolve_entities(tree)
    # No "_7_" in the key, and the device label is "Fresh water tank" flat.
    assert "tanks_freshwater_currentlevel" in ents
    assert ents["tanks_freshwater_currentlevel"]["group_label"] == "Fresh water tank"


def test_multi_instance_tank_keeps_instance_digit() -> None:
    tree = _tank_tree(
        **{
            "tanks.freshWater.7": {"currentLevel": 0.62},
            "tanks.freshWater.8": {"currentLevel": 0.10},
        }
    )
    ents = resolve_entities(tree)
    # With two fresh-water tanks, stripping the instance would collide;
    # the raw path-based key survives.
    assert "tanks_freshwater_7_currentlevel" in ents
    assert "tanks_freshwater_8_currentlevel" in ents


def test_tank_capacity_and_remaining_are_emitted_in_gallons() -> None:
    tree = _tank_tree(**{"tanks.fuel.0": {"currentLevel": 0.5, "capacity": 0.2839}})
    ents = {**resolve_entities(tree), **resolve_special(tree)}
    cap = ents["tanks_fuel_capacity"]
    rem = ents["tanks_fuel_remaining"]
    assert cap["unit"] == "gal"
    assert float(cap["state"]) == pytest.approx(75.0, abs=0.1)
    assert rem["unit"] == "gal"
    assert float(rem["state"]) == pytest.approx(37.5, abs=0.1)


def test_tank_fluid_type_matches_victron_enum() -> None:
    tree = _tank_tree(
        **{
            "tanks.freshWater.7": {"currentLevel": 0.5},
            "tanks.blackWater.6": {"currentLevel": 0.9},
        }
    )
    ents = resolve_special(tree)
    # Key uses slugify (matches sibling tanks_<type>_currentlevel keys),
    # but the state value keeps the snake_case fluid_type Victron publishes.
    assert ents["tanks_freshwater_fluid_type"]["state"] == "fresh_water"
    assert ents["tanks_blackwater_fluid_type"]["state"] == "black_water"


def test_tank_remaining_skipped_when_capacity_missing() -> None:
    tree = _tank_tree(**{"tanks.fuel.0": {"currentLevel": 0.5}})
    ents = resolve_special(tree)
    # Fluid type still emitted; remaining needs both fields so it's skipped.
    assert "tanks_fuel_fluid_type" in ents
    assert "tanks_fuel_remaining" not in ents


# --------------------------------------------------------------------------
# battery + solar parity: derived power, converter operating state
# --------------------------------------------------------------------------


def _battery_tree(**banks: dict[str, float]) -> dict:
    def leaf(v):
        return {"value": v}

    result: dict = {"electrical": {"batteries": {}}}
    for instance, fields in banks.items():
        node: dict = {}
        for k, v in fields.items():
            node[k] = leaf(v)
        result["electrical"]["batteries"][instance] = node
    return result


def test_battery_power_derived_from_voltage_and_current() -> None:
    tree = _battery_tree(house={"voltage": 13.32, "current": 12.7})
    ents = resolve_special(tree)
    p = ents["electrical_batteries_house_power"]
    assert p["unit"] == "W"
    assert p["device_class"] == "power"
    assert float(p["state"]) == pytest.approx(169.16, abs=0.1)
    assert p["group_label"] == "Battery House"


def test_battery_power_emitted_per_instance() -> None:
    tree = _battery_tree(
        house={"voltage": 12.8, "current": -5.0},
        engine={"voltage": 12.4, "current": 0.2},
    )
    ents = resolve_special(tree)
    assert float(ents["electrical_batteries_house_power"]["state"]) == pytest.approx(
        -64.0
    )
    assert float(ents["electrical_batteries_engine_power"]["state"]) == pytest.approx(
        2.48, abs=0.01
    )


def test_battery_power_skipped_when_current_missing() -> None:
    tree = _battery_tree(house={"voltage": 12.8})
    ents = resolve_special(tree)
    assert "electrical_batteries_house_power" not in ents


def test_battery_power_yields_to_native_power_leaf() -> None:
    # BMV/SmartShunt publishes its own averaged power; V*I is a strictly
    # worse estimate. The derived sensor must not clobber it.
    tree = _battery_tree(house={"voltage": 12.8, "current": -2.5, "power": -32.4})
    ents = {**resolve_entities(tree), **resolve_special(tree)}
    # Native leaf survives via PATH_MAP; derived is suppressed.
    assert float(ents["electrical_batteries_house_power"]["state"]) == pytest.approx(
        -32.4
    )


def test_converter_operating_state_is_text_sensor() -> None:
    tree = {
        "electrical": {
            "converter": {"36": {"0": {"operatingState": {"value": "bulk"}}}}
        }
    }
    ents = resolve_special(tree)
    s = ents[slugify("electrical.converter.36.0.operatingState")]
    assert s["component"] == "sensor"
    assert s["state"] == "bulk"
    assert s["group_label"] == "Converter 36"


def test_tank_instance_count_shared_across_field_patterns() -> None:
    # Regression: freshWater.7 has ONLY level, freshWater.8 has ONLY capacity.
    # Per-pattern counting would say each pattern matches once (single-instance)
    # and both would land under the same "Fresh water tank" HA device --
    # silently merging two physical tanks. Per-group counting correctly sees
    # two instances and re-injects the wildcard.
    tree = _tank_tree(
        **{
            "tanks.freshWater.7": {"currentLevel": 0.62},
            "tanks.freshWater.8": {"capacity": 0.5},
        }
    )
    ents = {**resolve_entities(tree), **resolve_special(tree)}
    assert "tanks_freshwater_7_currentlevel" in ents
    assert "tanks_freshwater_8_capacity" in ents
    # And derived entities agree: each instance gets its own fluid_type.
    assert "tanks_freshwater_7_fluid_type" in ents
    assert "tanks_freshwater_8_fluid_type" in ents
