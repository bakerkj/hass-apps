# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for pure/utility functions in turbostat_mqtt.turbostat_mqtt."""

from __future__ import annotations


import turbostat_mqtt as tm


# ---------------------------------------------------------------------------
# sanitize_key
# ---------------------------------------------------------------------------


def test_sanitize_key_percent():
    assert tm.sanitize_key("Busy%") == "busy_pct"


def test_sanitize_key_slash():
    assert tm.sanitize_key("GFX%") == "gfx_pct"


def test_sanitize_key_slash_word():
    assert tm.sanitize_key("J/s") == "j_per_s"


def test_sanitize_key_dash():
    assert tm.sanitize_key("Pkg-Tmp") == "pkg_tmp"


def test_sanitize_key_leading_trailing_spaces():
    assert tm.sanitize_key("  PkgWatt  ") == "pkgwatt"


def test_sanitize_key_collapses_underscores():
    # Multiple consecutive separators collapse to one underscore
    assert tm.sanitize_key("A%%B") == "a_pct_pctb"


def test_sanitize_key_lowercases():
    assert tm.sanitize_key("PkgWatt") == "pkgwatt"


def test_sanitize_key_strips_leading_underscore():
    # A key that starts with a non-alnum char should not start with '_'
    result = tm.sanitize_key("%usage")
    assert not result.startswith("_"), repr(result)


def test_sanitize_key_strips_trailing_underscore():
    result = tm.sanitize_key("usage%")
    assert not result.endswith("_"), repr(result)


def test_sanitize_key_numeric_retained():
    assert tm.sanitize_key("CPU%c1") == "cpu_pctc1"


# ---------------------------------------------------------------------------
# friendly_name
# ---------------------------------------------------------------------------


def test_friendly_name_known_key():
    assert tm.friendly_name("PkgWatt") == "CPU Package Power"


def test_friendly_name_known_percent_key():
    assert tm.friendly_name("Busy%") == "CPU Busy"


def test_friendly_name_unknown_key():
    result = tm.friendly_name("Zorblax")
    assert result == "Turbostat Zorblax"


def test_friendly_name_all_mapped_keys_do_not_start_with_turbostat():
    # Every key explicitly in the replacements dict should return a known name
    known = [
        "PkgWatt",
        "CorWatt",
        "GFXWatt",
        "RAMWatt",
        "PkgTmp",
        "Busy%",
        "GFX%",
        "CoreTmp",
        "Bzy_MHz",
        "Avg_MHz",
        "TSC_MHz",
        "IRQ",
    ]
    for key in known:
        name = tm.friendly_name(key)
        assert not name.startswith("Turbostat "), f"{key!r} -> {name!r}"


# ---------------------------------------------------------------------------
# guess_meta
# ---------------------------------------------------------------------------


def test_guess_meta_percent_col():
    unit, dc, icon, sdp = tm.guess_meta("Busy%")
    assert unit == "%"
    assert dc is None
    assert icon == "mdi:percent"
    assert sdp == 1


def test_guess_meta_temperature():
    unit, dc, icon, sdp = tm.guess_meta("PkgTmp")
    assert unit == "°C"
    assert dc == "temperature"
    assert icon == "mdi:thermometer"
    assert sdp == 0


def test_guess_meta_mhz():
    unit, dc, icon, sdp = tm.guess_meta("Bzy_MHz")
    assert unit == "MHz"
    assert dc == "frequency"


def test_guess_meta_watt():
    unit, dc, icon, sdp = tm.guess_meta("PkgWatt")
    assert unit == "W"
    assert dc == "power"
    assert sdp == 1


def test_guess_meta_joule():
    unit, dc, icon, sdp = tm.guess_meta("Pkg_J")
    assert unit == "J"


def test_guess_meta_rps():
    unit, dc, icon, sdp = tm.guess_meta("LLCkRPS")
    assert unit == "1/s"


def test_guess_meta_sec():
    unit, dc, icon, sdp = tm.guess_meta("SomeSec")
    assert unit == "s"
    assert sdp == 1


def test_guess_meta_irq():
    unit, dc, icon, sdp = tm.guess_meta("IRQ")
    assert unit is None
    assert dc is None


def test_guess_meta_unknown_falls_back():
    unit, dc, icon, sdp = tm.guess_meta("Zorblax")
    assert unit is None
    assert dc is None
    assert sdp == 2


def test_guess_meta_cpu_percent_special():
    # "CPU%" contains "%" so should match percent branch
    unit, dc, icon, sdp = tm.guess_meta("CPU%")
    assert unit == "%"


# ---------------------------------------------------------------------------
# TurbostatParser
# ---------------------------------------------------------------------------


def test_parser_ignores_blank_lines():
    p = tm.TurbostatParser()
    assert p.parse_line("") is None
    assert p.parse_line("   \n") is None


def test_parser_sets_header_on_first_non_numeric_line():
    p = tm.TurbostatParser()
    result = p.parse_line("PkgWatt CorWatt GFXWatt\n")
    assert result is None
    assert p.header == ["PkgWatt", "CorWatt", "GFXWatt"]


def test_parser_returns_none_before_header():
    p = tm.TurbostatParser()
    # All-numeric line before a header has been seen → ignored
    result = p.parse_line("1.0 2.0 3.0\n")
    assert result is None


def test_parser_parses_data_line_after_header():
    p = tm.TurbostatParser()
    p.parse_line("PkgWatt CorWatt GFXWatt\n")
    result = p.parse_line("12.5 10.1 0.3\n")
    assert result is not None
    header, values, raw = result
    assert header == ["PkgWatt", "CorWatt", "GFXWatt"]
    assert values == {"PkgWatt": "12.5", "CorWatt": "10.1", "GFXWatt": "0.3"}


def test_parser_replaces_header_on_new_header_line():
    p = tm.TurbostatParser()
    p.parse_line("PkgWatt CorWatt\n")
    p.parse_line("1.0 2.0\n")
    # New all-text line should replace header
    p.parse_line("A B C\n")
    assert p.header == ["A", "B", "C"]


def test_parser_ignores_mismatched_column_count():
    p = tm.TurbostatParser()
    p.parse_line("PkgWatt CorWatt GFXWatt\n")
    result = p.parse_line("1.0 2.0\n")  # only 2 values for 3 columns
    assert result is None


def test_parser_reset_clears_header():
    p = tm.TurbostatParser()
    p.parse_line("PkgWatt CorWatt\n")
    assert p.header is not None
    p.reset()
    assert p.header is None


def test_parser_handles_integer_values():
    p = tm.TurbostatParser()
    p.parse_line("IRQ SMI\n")
    result = p.parse_line("100 0\n")
    assert result is not None
    _, values, _ = result
    assert values["IRQ"] == "100"


def test_parser_negative_number_is_numeric():
    p = tm.TurbostatParser()
    p.parse_line("A B\n")
    result = p.parse_line("-1.5 2.0\n")
    assert result is not None


# ---------------------------------------------------------------------------
# build_discovery_payloads
# ---------------------------------------------------------------------------


def test_build_discovery_payloads_structure():
    cols = {"PkgWatt": "pkgwatt", "Bzy_MHz": "bzy_mhz"}
    payloads = tm.build_discovery_payloads(
        discovery_prefix="homeassistant",
        device_id="turbostat",
        device_name="Turbostat",
        state_topic="turbostat/state",
        base_topic="turbostat",
        availability_topic="turbostat/availability",
        cols=cols,
        sample_timeout_s=180,
    )

    assert len(payloads) == 2

    watt_key = "homeassistant/sensor/turbostat/pkgwatt/config"
    assert watt_key in payloads
    p = payloads[watt_key]
    assert p["name"] == "CPU Package Power"
    assert p["unique_id"] == "turbostat_pkgwatt"
    assert p["state_topic"] == "turbostat/pkgwatt/state"
    assert p["unit_of_measurement"] == "W"
    assert p["device_class"] == "power"
    assert p["expire_after"] == 180
    assert p["device"]["identifiers"] == ["turbostat"]


def test_build_discovery_payloads_expire_after_minimum():
    cols = {"PkgWatt": "pkgwatt"}
    payloads = tm.build_discovery_payloads(
        discovery_prefix="homeassistant",
        device_id="turbostat",
        device_name="Turbostat",
        state_topic="turbostat/state",
        base_topic="turbostat",
        availability_topic="turbostat/availability",
        cols=cols,
        sample_timeout_s=1,  # below minimum of 5
    )
    p = list(payloads.values())[0]
    assert p["expire_after"] == 5


def test_build_discovery_payloads_no_unit_for_unknown():
    # A column whose guess_meta returns unit=None should not include
    # unit_of_measurement key.
    cols = {"IRQ": "irq"}
    payloads = tm.build_discovery_payloads(
        discovery_prefix="ha",
        device_id="ts",
        device_name="TS",
        state_topic="ts/state",
        base_topic="ts",
        availability_topic="ts/avail",
        cols=cols,
        sample_timeout_s=60,
    )
    p = list(payloads.values())[0]
    assert "unit_of_measurement" not in p
    assert "device_class" not in p


def test_build_discovery_payloads_empty_cols():
    payloads = tm.build_discovery_payloads(
        discovery_prefix="ha",
        device_id="ts",
        device_name="TS",
        state_topic="ts/state",
        base_topic="ts",
        availability_topic="ts/avail",
        cols={},
        sample_timeout_s=60,
    )
    assert payloads == {}
