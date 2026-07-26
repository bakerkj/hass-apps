# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for the Signal K path mapping.

These exist because the mapping was written speculatively, against a boat that
was not yet connected to its NMEA 2000 backbone. Verifying the conversions and
grouping here is what makes that safe: SI-to-readable conversion is easy to get
subtly wrong, and a wrong factor looks plausible on a dashboard.
"""

from typing import Any

import pytest

from signalk_bridge import paths

# --------------------------------------------------------------------------
# flatten
# --------------------------------------------------------------------------


def test_flatten_extracts_leaf_values(vessel_tree: dict[str, Any]) -> None:
    flat = paths.flatten(vessel_tree)
    assert flat["navigation.speedOverGround"] == pytest.approx(3.086)
    assert flat["environment.depth.belowTransducer"] == pytest.approx(12.4)
    assert flat["propulsion.port.revolutions"] == pytest.approx(29.17)
    assert flat["electrical.batteries.house.stateOfCharge"] == pytest.approx(0.87)
    assert flat["tanks.freshWater.0.currentLevel"] == pytest.approx(0.62)


def test_flatten_drops_signalk_metadata(vessel_tree: dict[str, Any]) -> None:
    flat = paths.flatten(vessel_tree)
    for key in flat:
        assert "$source" not in key
        assert not key.endswith(".timestamp")
        assert not key.endswith(".pgn")


def test_flatten_handles_nested_leaf(vessel_tree: dict[str, Any]) -> None:
    # capacity.timeRemaining sits under an intermediate node
    flat = paths.flatten(vessel_tree)
    assert flat["electrical.batteries.house.capacity.timeRemaining"] == 36000.0


def test_flatten_keeps_composite_values(vessel_tree: dict[str, Any]) -> None:
    # position is a dict, not a scalar; it must survive for callers to handle
    flat = paths.flatten(vessel_tree)
    assert isinstance(flat["navigation.position"], dict)


def test_flatten_skips_multisource_values() -> None:
    # A multi-source leaf carries per-source copies under "values"; only the
    # canonical "value" should surface, never phantom per-source paths.
    tree = {
        "electrical": {
            "batteries": {
                "0": {
                    "voltage": {
                        "value": 12.8,
                        "values": {
                            "n2k-can0.abc": {"value": 12.9, "timestamp": "t"},
                            "n2k-can0.def": {"value": 12.1, "timestamp": "t"},
                        },
                    }
                }
            }
        }
    }
    flat = paths.flatten(tree)
    assert flat["electrical.batteries.0.voltage"] == 12.8
    assert not any(".values." in k for k in flat)


# --------------------------------------------------------------------------
# pattern matching
# --------------------------------------------------------------------------


def test_exact_match_returns_empty_captures() -> None:
    assert (
        paths.match_path("navigation.speedOverGround", "navigation.speedOverGround")
        == []
    )


def test_wildcard_captures_instance() -> None:
    assert paths.match_path(
        "electrical.batteries.house.voltage", "electrical.batteries.*.voltage"
    ) == ["house"]


def test_mismatched_depth_does_not_match() -> None:
    assert paths.match_path("a.b", "a.b.c") is None


def test_wildcard_matches_one_segment_only() -> None:
    assert (
        paths.match_path(
            "electrical.batteries.house.capacity.timeRemaining",
            "electrical.batteries.*.voltage",
        )
        is None
    )


# --------------------------------------------------------------------------
# unit conversions -- the part most likely to be silently wrong
# --------------------------------------------------------------------------


def test_engine_rpm_from_hz() -> None:
    # Engine gateways report Hz; 29.17 Hz is 1750 rpm
    assert paths.hz_to_rpm(29.17) == pytest.approx(1750.2, abs=0.5)


def test_coolant_temperature_from_kelvin() -> None:
    assert paths.kelvin_to_celsius(355.37) == pytest.approx(82.22, abs=0.01)


def test_state_of_charge_to_percent() -> None:
    assert paths.ratio_to_percent(0.87) == pytest.approx(87.0)


def test_barometric_pressure_to_hpa() -> None:
    assert paths.pa_to_hpa(101325.0) == pytest.approx(1013.25)


def test_engine_pressures_use_kpa() -> None:
    # Oil/coolant pressures read better in kPa than hPa (~310 kPa vs ~3100 hPa).
    assert paths.pa_to_kpa(310264.0) == pytest.approx(310.264)
    for p in ("propulsion.*.oilPressure", "propulsion.*.coolantPressure"):
        assert paths.PATH_MAP[p]["unit"] == "kPa"
        assert paths.PATH_MAP[p]["convert"] is paths.pa_to_kpa


def test_wind_angle_stays_signed() -> None:
    # -45 deg apparent means 45 deg to port; wrapping it to 315 would be wrong
    # for a wind angle gauge, so angleApparent must use the signed conversion.
    assert paths.rad_to_deg(-0.7854) == pytest.approx(-45.0, abs=0.01)
    assert paths.PATH_MAP["environment.wind.angleApparent"]["convert"] is (
        paths.rad_to_deg
    )


def test_bearings_wrap_to_0_360() -> None:
    # Course/heading are compass bearings; negative would be nonsense.
    assert paths.rad_to_deg_positive(-0.7854) == pytest.approx(315.0, abs=0.01)
    for path in (
        "navigation.courseOverGroundTrue",
        "navigation.headingMagnetic",
        "environment.wind.directionTrue",
    ):
        assert paths.PATH_MAP[path]["convert"] is paths.rad_to_deg_positive


def test_engine_hours_from_seconds() -> None:
    conv = paths.PATH_MAP["propulsion.*.runTime"]["convert"]
    assert conv(4_320_000.0) == pytest.approx(1200.0)


def test_battery_time_remaining_from_seconds() -> None:
    conv = paths.PATH_MAP["electrical.batteries.*.capacity.timeRemaining"]["convert"]
    assert conv(36000.0) == pytest.approx(10.0)


def test_soc_maps_both_at_root_and_under_capacity() -> None:
    # Different devices report state of charge at different depths; both map.
    assert paths.match_path(
        "electrical.batteries.house.stateOfCharge",
        "electrical.batteries.*.stateOfCharge",
    ) == ["house"]
    assert paths.match_path(
        "electrical.batteries.0.capacity.stateOfCharge",
        "electrical.batteries.*.capacity.stateOfCharge",
    ) == ["0"]


def test_rudder_angle_stays_signed() -> None:
    # -5 deg to port must not wrap to 355.
    assert paths.PATH_MAP["steering.rudderAngle"]["convert"] is paths.rad_to_deg
    assert paths.rad_to_deg(-0.0873) == pytest.approx(-5.0, abs=0.01)


# --------------------------------------------------------------------------
# device grouping
# --------------------------------------------------------------------------


def test_group_resolution_for_battery() -> None:
    gid, label = paths.resolve_group("battery.*", ["house"])
    assert gid == "battery.house"
    assert label == "Battery house"


def test_group_resolution_for_engine() -> None:
    gid, label = paths.resolve_group("engine.*", ["port"])
    assert gid == "engine.port"
    assert label == "Engine port"


def test_group_resolution_for_tank() -> None:
    gid, label = paths.resolve_group("tank.freshWater.*", ["0"])
    assert gid == "tank.freshWater.0"
    assert label == "Fresh water tank 0"


def test_group_resolution_without_wildcard() -> None:
    gid, label = paths.resolve_group("navigation", [])
    assert gid == "navigation"
    assert label == "Navigation"


# --------------------------------------------------------------------------
# instance-agnostic mapping invariants
#
# The mapping must hold for ANY instance id, not just the ones this boat's
# fixture happens to carry -- a regression that only shows up for a different
# bank/engine/tank instance must not slip through.
# --------------------------------------------------------------------------


@pytest.mark.parametrize("inst", ["house", "start", "0", "1", "engineStart", "aux_2"])
def test_battery_paths_map_for_any_instance(inst: str) -> None:
    for leafname in ("voltage", "stateOfCharge", "capacity.stateOfCharge"):
        path = f"electrical.batteries.{inst}.{leafname}"
        pattern = f"electrical.batteries.*.{leafname}"
        assert paths.match_path(path, pattern) == [inst]
    gid, label = paths.resolve_group("battery.*", [inst])
    assert gid == f"battery.{inst}"
    assert label == f"Battery {inst}"


@pytest.mark.parametrize("inst", ["port", "starboard", "0", "1", "main"])
def test_engine_paths_map_for_any_instance(inst: str) -> None:
    assert paths.match_path(
        f"propulsion.{inst}.revolutions", "propulsion.*.revolutions"
    ) == [inst]
    gid, label = paths.resolve_group("engine.*", [inst])
    assert gid == f"engine.{inst}"
    assert label == f"Engine {inst}"


@pytest.mark.parametrize(
    ("ttype", "inst", "label"),
    [
        ("freshWater", "0", "Fresh water tank 0"),
        ("fuel", "2", "Fuel tank 2"),
        ("blackWater", "aft", "Black water tank aft"),
    ],
)
def test_tank_paths_map_for_any_instance(ttype: str, inst: str, label: str) -> None:
    assert paths.match_path(
        f"tanks.{ttype}.{inst}.currentLevel", f"tanks.{ttype}.*.currentLevel"
    ) == [inst]
    gid, glabel = paths.resolve_group(f"tank.{ttype}.*", [inst])
    assert gid == f"tank.{ttype}.{inst}"
    assert glabel == label


# --------------------------------------------------------------------------
# end-to-end against the synthetic boat
# --------------------------------------------------------------------------


def _resolve_all(tree: dict[str, Any]) -> dict[str, tuple[float, str, str]]:
    """Map a vessel tree through PATH_MAP -> {path: (value, unit, group)}."""
    flat = paths.flatten(tree)
    out: dict[str, tuple[float, str, str]] = {}
    for actual, raw in flat.items():
        for pattern, spec in paths.PATH_MAP.items():
            caps = paths.match_path(actual, pattern)
            if caps is None:
                continue
            conv = spec.get("convert")
            if conv is None or not isinstance(raw, (int, float)):
                continue
            gid, _label = paths.resolve_group(spec["group"], caps)
            out[actual] = (conv(float(raw)), spec["unit"], gid)
            break
    return out


def test_whole_boat_resolves_to_expected_values(vessel_tree: dict[str, Any]) -> None:
    got = _resolve_all(vessel_tree)

    expected = {
        "navigation.speedOverGround": (3.086, "m/s", "navigation"),
        "navigation.courseOverGroundTrue": (90.0, "°", "navigation"),
        "environment.depth.belowTransducer": (12.4, "m", "environment"),
        "environment.wind.angleApparent": (-45.0, "°", "environment"),
        "environment.water.temperature": (15.0, "°C", "environment"),
        "environment.outside.pressure": (1013.25, "hPa", "environment"),
        "propulsion.port.revolutions": (1750.2, "rpm", "engine.port"),
        "propulsion.port.temperature": (82.22, "°C", "engine.port"),
        "propulsion.port.runTime": (1200.0, "h", "engine.port"),
        "electrical.batteries.house.stateOfCharge": (87.0, "%", "battery.house"),
        "electrical.batteries.house.voltage": (12.84, "V", "battery.house"),
        "electrical.batteries.start.voltage": (12.61, "V", "battery.start"),
        "electrical.solar.mppt1.panelPower": (77.5, "W", "solar.mppt1"),
        "tanks.freshWater.0.currentLevel": (62.0, "%", "tank.freshWater.0"),
        "tanks.fuel.0.currentLevel": (45.0, "%", "tank.fuel.0"),
    }

    for path, (value, unit, group) in expected.items():
        assert path in got, f"{path} produced no entity"
        gv, gu, gg = got[path]
        assert gv == pytest.approx(value, abs=0.5), f"{path} value"
        assert gu == unit, f"{path} unit"
        assert gg == group, f"{path} group"


def test_two_battery_banks_become_two_devices(vessel_tree: dict[str, Any]) -> None:
    groups = {g for _v, _u, g in _resolve_all(vessel_tree).values()}
    assert "battery.house" in groups
    assert "battery.start" in groups


def test_absent_equipment_produces_no_entities() -> None:
    # A boat with only depth should yield exactly one entity -- the map covering
    # more gear than is installed must not invent sensors.
    tree = {"environment": {"depth": {"belowTransducer": {"value": 3.3}}}}
    got = _resolve_all(tree)
    assert list(got) == ["environment.depth.belowTransducer"]


def test_every_mapping_has_required_metadata() -> None:
    for pattern, spec in paths.PATH_MAP.items():
        assert "name" in spec, f"{pattern} missing name"
        assert "group" in spec, f"{pattern} missing group"
        assert "unit" in spec, f"{pattern} missing unit key"
        # Wildcards in the group must be satisfiable from the path's wildcards.
        assert spec["group"].count("*") <= pattern.count("*"), pattern
