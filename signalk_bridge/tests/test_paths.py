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


def test_flatten_with_meta_carries_timestamp(vessel_tree: dict[str, Any]) -> None:
    meta = paths.flatten_with_meta(vessel_tree)
    value, ts = meta["navigation.speedOverGround"]
    assert value == pytest.approx(3.086)
    assert ts == "2026-07-22T04:00:00.000Z"  # the fixture leaf() timestamp


def test_flatten_matches_meta_values(vessel_tree: dict[str, Any]) -> None:
    # flatten is a thin wrapper; its values must equal meta's, timestamp aside.
    flat = paths.flatten(vessel_tree)
    meta = paths.flatten_with_meta(vessel_tree)
    assert flat == {p: v for p, (v, _ts) in meta.items()}


def test_flatten_with_meta_fanout_carries_per_source_timestamps() -> None:
    # Each fanned-out source keeps its OWN timestamp, so one source going stale
    # is distinguishable from another under the same primary path.
    tree = {
        "electrical": {
            "batteries": {
                "0": {
                    "voltage": {
                        "value": 12.8,
                        "values": {
                            "n2k-can0.abc": {
                                "value": 12.9,
                                "timestamp": "2026-01-01T00:00:01Z",
                            },
                            "n2k-can0.def": {
                                "value": 12.1,
                                "timestamp": "2026-01-01T00:00:05Z",
                            },
                        },
                    }
                }
            }
        }
    }
    tags = {"n2k-can0.abc": "house", "n2k-can0.def": "engine"}
    meta = paths.flatten_with_meta(tree, source_tags=tags)
    assert meta["electrical.batteries.house.voltage"] == (12.9, "2026-01-01T00:00:01Z")
    assert meta["electrical.batteries.engine.voltage"] == (12.1, "2026-01-01T00:00:05Z")


_MULTISOURCE_BATTERY = {
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


def test_flatten_without_source_tags_keeps_only_primary() -> None:
    # No source_tags map -> backwards-compat behavior: only the canonical
    # ``value`` surfaces. Callers that don't wire /sources through never
    # see a per-source fanout, so nothing breaks for them.
    flat = paths.flatten(_MULTISOURCE_BATTERY)
    assert flat["electrical.batteries.0.voltage"] == 12.8
    assert not any(".values." in k for k in flat)
    assert list(flat) == ["electrical.batteries.0.voltage"]


def test_flatten_with_source_tags_fans_out_multisource() -> None:
    tags = {"n2k-can0.abc": "house", "n2k-can0.def": "engine"}
    flat = paths.flatten(_MULTISOURCE_BATTERY, source_tags=tags)
    # Primary path preserved -- existing HA entity IDs never break.
    assert flat["electrical.batteries.0.voltage"] == 12.8
    # Per-source entities appear with the instance segment substituted
    # for the friendly tag; downstream PATH_MAP wildcards match them
    # transparently.
    assert flat["electrical.batteries.house.voltage"] == 12.9
    assert flat["electrical.batteries.engine.voltage"] == 12.1


def test_flatten_fans_out_even_when_source_readings_coincide() -> None:
    # Two physical devices are still two devices even when they report
    # the same value for a cycle. Gating fanout on value-distinctness
    # would drop per-source entities from the publish dict whenever
    # readings converge, tripping expire_after_s and flipping HA to
    # Unavailable -- flapping the fanout is meant to prevent.
    tree = {
        "electrical": {
            "batteries": {
                "0": {
                    "voltage": {
                        "value": 12.8,
                        "values": {
                            "n2k-can0.abc": {"value": 12.8, "timestamp": "t"},
                            "n2k-can0.def": {"value": 12.8, "timestamp": "t"},
                        },
                    }
                }
            }
        }
    }
    flat = paths.flatten(
        tree, source_tags={"n2k-can0.abc": "house", "n2k-can0.def": "engine"}
    )
    assert flat["electrical.batteries.0.voltage"] == 12.8
    assert flat["electrical.batteries.house.voltage"] == 12.8
    assert flat["electrical.batteries.engine.voltage"] == 12.8


def test_flatten_falls_back_to_hex_tail_for_untagged_sources() -> None:
    # A source not in source_tags gets the last 4 chars of its $source
    # string as a fallback tag; the point is disambiguation, and a hex
    # canName tail is stable enough for that.
    tags = {"n2k-can0.aaaaaaaa": "house"}
    tree = {
        "electrical": {
            "batteries": {
                "0": {
                    "voltage": {
                        "value": 12.8,
                        "values": {
                            "n2k-can0.aaaaaaaa": {"value": 12.9, "timestamp": "t"},
                            "n2k-can0.bbbb01f5": {"value": 12.1, "timestamp": "t"},
                        },
                    }
                }
            }
        }
    }
    flat = paths.flatten(tree, source_tags=tags)
    assert flat["electrical.batteries.house.voltage"] == 12.9
    assert flat["electrical.batteries.01f5.voltage"] == 12.1


def test_flatten_synthesizes_instance_for_singleton_multisource() -> None:
    # A singleton path (no numeric instance) with multiple sources
    # synthesizes an instance segment just before the leaf so PATH_MAP
    # companion patterns can pick these up. Not currently in PATH_MAP,
    # but the structural guarantee matters for future coverage.
    tree = {
        "navigation": {
            "speedOverGround": {
                "value": 3.0,
                "values": {
                    "n2k-can0.gps1": {"value": 3.1, "timestamp": "t"},
                    "n2k-can0.gps2": {"value": 2.9, "timestamp": "t"},
                },
            }
        }
    }
    tags = {"n2k-can0.gps1": "primary", "n2k-can0.gps2": "backup"}
    flat = paths.flatten(tree, source_tags=tags)
    assert flat["navigation.speedOverGround"] == 3.0
    assert flat["navigation.primary.speedOverGround"] == 3.1
    assert flat["navigation.backup.speedOverGround"] == 2.9


def test_flatten_drops_paths_in_suppress_list() -> None:
    tree = {
        "electrical": {
            "batteries": {
                "0": {"voltage": {"value": 12.8}},
                "239": {
                    "voltage": {"value": 12.8},
                    "current": {"value": -4.1},
                },
            }
        }
    }
    flat = paths.flatten(tree, suppress_paths=["electrical.batteries.239"])
    assert "electrical.batteries.0.voltage" in flat
    # Whole subtree gone -- both leaves under the suppressed prefix.
    assert not any(k.startswith("electrical.batteries.239") for k in flat)


def test_flatten_suppress_respects_segment_boundary() -> None:
    # ``batteries.2`` must not swallow ``batteries.239``; segment
    # boundary matters.
    tree = {
        "electrical": {
            "batteries": {
                "2": {"voltage": {"value": 12.8}},
                "239": {"voltage": {"value": 12.9}},
            }
        }
    }
    flat = paths.flatten(tree, suppress_paths=["electrical.batteries.2"])
    assert "electrical.batteries.239.voltage" in flat
    assert "electrical.batteries.2.voltage" not in flat


def test_flatten_suppress_applies_to_fanout_paths_too() -> None:
    tree = {
        "electrical": {
            "batteries": {
                "0": {
                    "voltage": {
                        "value": 12.8,
                        "values": {
                            "n2k-can0.abc": {"value": 12.9},
                            "n2k-can0.def": {"value": 12.1},
                        },
                    }
                }
            }
        }
    }
    tags = {"n2k-can0.abc": "solar", "n2k-can0.def": "house"}
    flat = paths.flatten(
        tree,
        source_tags=tags,
        suppress_paths=["electrical.batteries.solar"],
    )
    assert flat["electrical.batteries.0.voltage"] == 12.8
    assert flat["electrical.batteries.house.voltage"] == 12.1
    assert "electrical.batteries.solar.voltage" not in flat


def test_flatten_suppress_primary_on_fanout_drops_primary() -> None:
    tree = {
        "electrical": {
            "batteries": {
                "0": {
                    "voltage": {
                        "value": 12.8,
                        "values": {
                            "n2k-can0.abc": {"value": 12.9},
                            "n2k-can0.def": {"value": 12.1},
                        },
                    }
                }
            }
        }
    }
    tags = {"n2k-can0.abc": "house", "n2k-can0.def": "engine"}
    flat = paths.flatten(tree, source_tags=tags, suppress_primary_on_fanout=True)
    # Primary gone; per-source entities stay.
    assert "electrical.batteries.0.voltage" not in flat
    assert flat["electrical.batteries.house.voltage"] == 12.9
    assert flat["electrical.batteries.engine.voltage"] == 12.1


def test_flatten_suppress_primary_on_fanout_keeps_single_source_leaves() -> None:
    # A leaf with only one source never fans out, so the flag is a no-op
    # for it -- the primary must still surface. Otherwise turning the
    # flag on would delete every non-multi-source entity on the boat.
    tree = {
        "electrical": {
            "batteries": {
                "0": {"voltage": {"value": 12.8}},
            }
        }
    }
    flat = paths.flatten(tree, suppress_primary_on_fanout=True)
    assert flat["electrical.batteries.0.voltage"] == 12.8


def test_build_source_tags_prefers_installation_description() -> None:
    sources = {
        "n2k-can0": {
            "36": {
                "n2k": {
                    "canName": "c046a0002cc001f6",
                    "installationDescription1": "Solar",
                }
            },
            "224": {
                "n2k": {
                    "canName": "c046aa002cc001f5",
                    # no installationDescription1 -> hex fallback
                }
            },
        }
    }
    tags = paths.build_source_tags(sources)
    assert tags["n2k-can0.c046a0002cc001f6"] == "solar"
    assert tags["n2k-can0.c046aa002cc001f5"] == "01f5"


def test_build_source_tags_handles_missing_payload() -> None:
    # A missing/empty /sources payload -> {} rather than a crash; the
    # bridge continues in single-source mode.
    assert paths.build_source_tags(None) == {}
    assert paths.build_source_tags({}) == {}
    assert paths.build_source_tags({"n2k-can0": "not-a-dict"}) == {}


def test_build_source_tags_disambiguates_collisions() -> None:
    # Two devices with the same installationDescription1 must not
    # collapse to the same tag -- one would silently overwrite the
    # other in the fanout dict, defeating the whole point of splitting
    # them.
    sources = {
        "n2k-can0": {
            "224": {
                "n2k": {
                    "canName": "c046aa002cc001f5",
                    "installationDescription1": "House",
                }
            },
            "225": {
                "n2k": {
                    "canName": "c046aa002cc001f7",
                    "installationDescription1": "House",
                }
            },
        }
    }
    tags = paths.build_source_tags(sources)
    assert tags["n2k-can0.c046aa002cc001f5"] != tags["n2k-can0.c046aa002cc001f7"]
    # Collision resolution appends the canName tail as a disambiguator.
    assert tags["n2k-can0.c046aa002cc001f5"] == "house_01f5"
    assert tags["n2k-can0.c046aa002cc001f7"] == "house_01f7"


def test_flatten_skips_non_scalar_source_values() -> None:
    # Signal K composite leaves (positions, list-valued state) show up
    # inside per-source ``values`` dicts too. Attempting ``v in seen``
    # on a list would raise TypeError and drop the whole poll cycle;
    # non-scalars must be skipped instead.
    tree = {
        "electrical": {
            "batteries": {
                "0": {
                    "voltage": {
                        "value": 12.8,
                        "values": {
                            "n2k-can0.abc": {"value": [12.9, 13.0]},
                            "n2k-can0.def": {"value": {"nested": 1}},
                            "n2k-can0.ghi": {"value": 12.1},
                        },
                    }
                }
            }
        }
    }
    tags = {
        "n2k-can0.abc": "a",
        "n2k-can0.def": "b",
        "n2k-can0.ghi": "c",
    }
    # Only one scalar survives -> no fanout, no crash.
    flat = paths.flatten(tree, source_tags=tags)
    assert flat["electrical.batteries.0.voltage"] == 12.8
    # Nothing under an alt-tag path (the two non-scalar sources are
    # silently skipped, and one remaining scalar is not a "collision").
    assert list(flat) == ["electrical.batteries.0.voltage"]


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
        "navigation.courseGreatCircle.bearingTrackTrue",
        "navigation.courseGreatCircle.nextPoint.bearingTrue",
        "steering.autopilot.target.headingMagnetic",
        "steering.autopilot.target.headingTrue",
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
    # Alpha instance captures come from installationDescription1 tags and
    # are title-cased for readability; numeric and hex fallbacks stay as-is.
    assert label == "Battery House"


def test_group_resolution_for_engine() -> None:
    gid, label = paths.resolve_group("engine.*", ["port"])
    assert gid == "engine.port"
    assert label == "Engine Port"


def test_group_resolution_keeps_numeric_instance() -> None:
    gid, label = paths.resolve_group("battery.*", ["0"])
    assert gid == "battery.0"
    assert label == "Battery 0"


def test_group_resolution_keeps_hex_fallback_tag() -> None:
    # Devices without an installationDescription1 fall back to the last
    # four hex chars of the canName; those are alphanumeric but not
    # alpha-only, so must not be title-cased.
    gid, label = paths.resolve_group("battery.*", ["01f5"])
    assert gid == "battery.01f5"
    assert label == "Battery 01f5"


def test_group_resolution_preserves_camelcase_instance() -> None:
    # Signal K schema-style instance ids ("engineStart", "portAux") already
    # read correctly and must not be flattened to "Enginestart" by title().
    gid, label = paths.resolve_group("battery.*", ["engineStart"])
    assert gid == "battery.engineStart"
    assert label == "Battery engineStart"


def test_group_resolution_collapses_repeated_label() -> None:
    # signalk-venus-plugin's "Use the device names for paths" mode turns
    # a MPPT named "Solar" into path ``electrical.solar.solar`` -> device
    # "Solar Solar". Collapse to just "Solar" so the duplicated word
    # doesn't survive to HA.
    gid, label = paths.resolve_group("solar.*", ["solar"])
    assert gid == "solar.solar"
    assert label == "Solar"


def test_group_resolution_for_tank() -> None:
    gid, label = paths.resolve_group("tank.freshWater.*", ["0"])
    assert gid == "tank.freshWater.0"
    assert label == "Fresh water tank 0"


def test_group_resolution_for_tank_without_wildcard() -> None:
    gid, label = paths.resolve_group("tank.freshWater", [])
    assert gid == "tank.freshWater"
    assert label == "Fresh water tank"


def test_m3_to_gal_conversion() -> None:
    assert paths.m3_to_gal(1.0) == pytest.approx(264.172, abs=0.01)
    assert paths.m3_to_gal(0.2839) == pytest.approx(75.0, abs=0.05)


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


def _expected_instance_label(inst: str) -> str:
    # resolve_group title-cases all-lowercase alpha captures (friendly
    # source tags read as names); mixed-case, numeric, and mixed-alnum
    # captures pass through unchanged.
    return inst.title() if inst.islower() and inst.isalpha() else inst


@pytest.mark.parametrize("inst", ["house", "start", "0", "1", "engineStart", "aux_2"])
def test_battery_paths_map_for_any_instance(inst: str) -> None:
    for leafname in ("voltage", "stateOfCharge", "capacity.stateOfCharge"):
        path = f"electrical.batteries.{inst}.{leafname}"
        pattern = f"electrical.batteries.*.{leafname}"
        assert paths.match_path(path, pattern) == [inst]
    gid, label = paths.resolve_group("battery.*", [inst])
    assert gid == f"battery.{inst}"
    assert label == f"Battery {_expected_instance_label(inst)}"


@pytest.mark.parametrize("inst", ["port", "starboard", "0", "1", "main"])
def test_engine_paths_map_for_any_instance(inst: str) -> None:
    assert paths.match_path(
        f"propulsion.{inst}.revolutions", "propulsion.*.revolutions"
    ) == [inst]
    gid, label = paths.resolve_group("engine.*", [inst])
    assert gid == f"engine.{inst}"
    assert label == f"Engine {_expected_instance_label(inst)}"


@pytest.mark.parametrize(
    ("ttype", "inst", "label"),
    [
        ("freshWater", "0", "Fresh water tank 0"),
        ("fuel", "2", "Fuel tank 2"),
        ("blackWater", "aft", "Black water tank Aft"),
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
        # Tank group has no wildcard by design: N2K's per-fluid-type
        # instance ID isn't human-meaningful, so the device is grouped
        # per fluid type ("Fresh water tank") and the app-side entity
        # key drops the instance too on single-tank boats. See
        # signalk_bridge/signalk_bridge/paths.py PATH_MAP tank block.
        "tanks.freshWater.0.currentLevel": (62.0, "%", "tank.freshWater"),
        "tanks.fuel.0.currentLevel": (45.0, "%", "tank.fuel"),
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
