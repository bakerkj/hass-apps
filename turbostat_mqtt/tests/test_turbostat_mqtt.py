# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for pure/utility functions in turbostat_mqtt.turbostat_mqtt."""

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


def test_sanitize_key_trailing_minus_becomes_shallow():
    # cpuidle mispredict column: trailing ``-`` = should-have-been-shallower
    assert tm.sanitize_key("C1ACPI-") == "c1acpi_shallow"


def test_sanitize_key_trailing_plus_becomes_deep():
    # cpuidle mispredict column: trailing ``+`` = should-have-been-deeper
    assert tm.sanitize_key("C1ACPI+") == "c1acpi_deep"


def test_sanitize_key_cpuidle_variants_are_distinct():
    # The three-way distinction between base, ``-`` and ``+`` cannot collapse:
    # each variant's row-value is different, and losing them to the same key
    # would silently overwrite in the payload dict.
    keys = {
        tm.sanitize_key("C1ACPI"),
        tm.sanitize_key("C1ACPI-"),
        tm.sanitize_key("C1ACPI+"),
    }
    assert len(keys) == 3, keys


def test_sanitize_key_internal_dash_unchanged():
    # The trailing-only ``-`` rewrite must not touch mid-string dashes.
    assert tm.sanitize_key("Pkg-Tmp") == "pkg_tmp"


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


def test_friendly_name_cpuidle_variants_are_distinct():
    # If any two variants collapse to the same name, the diagnostic sensors
    # become indistinguishable in the HA UI.
    keys = ["C1ACPI", "C1ACPI-", "C1ACPI+", "POLL", "POLL-"]
    names = {tm.friendly_name(k) for k in keys}
    assert len(names) == len(keys), sorted(names)


def test_friendly_name_cpuidle_all_mapped():
    # Every DIAGNOSTIC_COLS entry must have a real friendly name so we don't
    # end up publishing diagnostic sensors called "Turbostat C1ACPI-".
    for key in tm.DIAGNOSTIC_COLS:
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
    unit, dc, _icon, _sdp = tm.guess_meta("Bzy_MHz")
    assert unit == "MHz"
    assert dc == "frequency"


def test_guess_meta_watt():
    unit, dc, _icon, sdp = tm.guess_meta("PkgWatt")
    assert unit == "W"
    assert dc == "power"
    assert sdp == 1


def test_guess_meta_joule():
    unit, _dc, _icon, _sdp = tm.guess_meta("Pkg_J")
    assert unit == "J"


def test_guess_meta_rps_llc_overridden_to_megaref():
    """LLCkRPS / L2kRPS have explicit unit overrides because the parser
    scales the value into mega-refs/sec; the rps-suffix default ("1/s")
    would mislabel the magnitude."""
    unit, _dc, _icon, sdp = tm.guess_meta("LLCkRPS")
    assert unit == "M/s"
    assert sdp == 2
    unit, _, _, _ = tm.guess_meta("L2kRPS")
    assert unit == "M/s"


def test_guess_meta_rps_default_for_other_columns():
    """Non-overridden rps-suffix columns still get the generic 1/s."""
    unit, _, _, _ = tm.guess_meta("SomeRPS")
    assert unit == "1/s"


def test_guess_meta_sec():
    unit, _dc, _icon, sdp = tm.guess_meta("SomeSec")
    assert unit == "s"
    assert sdp == 1


def test_guess_meta_irq():
    unit, dc, _icon, _sdp = tm.guess_meta("IRQ")
    assert unit is None
    assert dc is None


def test_guess_meta_unknown_falls_back():
    unit, dc, _icon, sdp = tm.guess_meta("Zorblax")
    assert unit is None
    assert dc is None
    assert sdp == 2


def test_guess_meta_count_col_is_integer_count():
    # COUNT_COLS members get 0-decimal display and a counter icon so HA
    # doesn't render "898.00" for a value that arrives as int 898.
    for col in tm.COUNT_COLS:
        unit, dc, icon, sdp = tm.guess_meta(col)
        assert unit is None, col
        assert dc is None, col
        assert sdp == 0, col
        assert icon == "mdi:counter", col


def test_count_cols_and_diagnostic_cols_are_independent_concepts():
    # Today they happen to hold the same members, but the sets exist for
    # orthogonal reasons: DIAGNOSTIC_COLS drives ``enabled_by_default: false``
    # (should this sensor be opt-in?), COUNT_COLS drives 0-decimal display
    # (is the value an integer?). Assert we're not silently aliasing them.
    assert tm.COUNT_COLS is not tm.DIAGNOSTIC_COLS


def test_expected_cols_contains_historical_set():
    # Guard against future refactors accidentally dropping a column from
    # EXPECTED_COLS — the whole point of that set is to fire
    # ``missing_expected_columns`` when a supported column vanishes from
    # turbostat's output, so silently narrowing it defeats the check.
    # (Caught by pre-push review: the metadata consolidation initially
    # forgot ``expected=True`` on the IPC entry.)
    historical = {
        "PkgWatt",
        "CorWatt",
        "Busy%",
        "Bzy_MHz",
        "Avg_MHz",
        "TSC_MHz",
        "IPC",
        "LLCkRPS",
        "LLC%hit",
        "L2kRPS",
        "L2%hit",
    }
    assert historical.issubset(tm.EXPECTED_COLS), (
        f"EXPECTED_COLS lost historical members: "
        f"{sorted(historical - tm.EXPECTED_COLS)}"
    )


def test_guess_meta_cpu_percent_special():
    # "CPU%" contains "%" so should match percent branch
    unit, _dc, _icon, _sdp = tm.guess_meta("CPU%")
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
    header, values, _raw = result
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
        base_topic="turbostat",
        availability_topic="turbostat/availability",
        cols=cols,
        expire_after_s=120,
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
    assert p["expire_after"] == 120
    assert p["device"]["identifiers"] == ["turbostat"]


def test_build_discovery_payloads_expire_after_minimum():
    cols = {"PkgWatt": "pkgwatt"}
    payloads = tm.build_discovery_payloads(
        discovery_prefix="homeassistant",
        device_id="turbostat",
        device_name="Turbostat",
        base_topic="turbostat",
        availability_topic="turbostat/availability",
        cols=cols,
        expire_after_s=60,  # minimum value
    )
    p = next(iter(payloads.values()))
    assert p["expire_after"] == 60


def test_build_discovery_payloads_diagnostic_col_disabled_by_default():
    # Diagnostic sensors (cpuidle counters + governor mispredicts) get
    # ``enabled_by_default: false`` so they're opt-in per-sensor.
    cols = {"C1ACPI-": "c1acpi_shallow", "PkgWatt": "pkgwatt"}
    payloads = tm.build_discovery_payloads(
        discovery_prefix="ha",
        device_id="ts",
        device_name="TS",
        base_topic="ts",
        availability_topic="ts/avail",
        cols=cols,
        expire_after_s=120,
    )
    diag = payloads["ha/sensor/ts/c1acpi_shallow/config"]
    assert diag.get("enabled_by_default") is False
    # A non-diagnostic column must not gain the flag (HA defaults to True).
    prod = payloads["ha/sensor/ts/pkgwatt/config"]
    assert "enabled_by_default" not in prod


def test_build_discovery_payloads_no_unit_for_unknown():
    # A column whose guess_meta returns unit=None should not include
    # unit_of_measurement key.
    cols = {"IRQ": "irq"}
    payloads = tm.build_discovery_payloads(
        discovery_prefix="ha",
        device_id="ts",
        device_name="TS",
        base_topic="ts",
        availability_topic="ts/avail",
        cols=cols,
        expire_after_s=120,
    )
    p = next(iter(payloads.values()))
    assert "unit_of_measurement" not in p
    assert "device_class" not in p


def test_build_discovery_payloads_empty_cols():
    payloads = tm.build_discovery_payloads(
        discovery_prefix="ha",
        device_id="ts",
        device_name="TS",
        base_topic="ts",
        availability_topic="ts/avail",
        cols={},
        expire_after_s=120,
    )
    assert payloads == {}


# ---------------------------------------------------------------------------
# log
# ---------------------------------------------------------------------------


def test_log_debug_skipped_when_min_level_info(capsys):
    tm.log("DEBUG", "should not appear", min_level="INFO")
    assert capsys.readouterr().out == ""


def test_log_info_printed_when_min_level_info(capsys):
    tm.log("INFO", "hello", min_level="INFO")
    out = capsys.readouterr().out
    assert "[INFO] hello" in out


def test_log_warning_printed_when_min_level_info(capsys):
    tm.log("WARNING", "warn msg", min_level="INFO")
    out = capsys.readouterr().out
    assert "[WARNING] warn msg" in out


def test_log_error_printed_when_min_level_warning(capsys):
    tm.log("ERROR", "err msg", min_level="WARNING")
    out = capsys.readouterr().out
    assert "[ERROR] err msg" in out


def test_log_info_skipped_when_min_level_warning(capsys):
    tm.log("INFO", "should not appear", min_level="WARNING")
    assert capsys.readouterr().out == ""


def test_log_debug_printed_when_min_level_debug(capsys):
    tm.log("DEBUG", "dbg msg", min_level="DEBUG")
    out = capsys.readouterr().out
    assert "[DEBUG] dbg msg" in out


# ---------------------------------------------------------------------------
# MqttHealth
# ---------------------------------------------------------------------------


def test_mqtt_health_initial_state():
    h = tm.MqttHealth()
    assert h.connected is False
    assert h.last_connect_ok == 0.0
    assert h.last_disconnect == 0.0
    assert h.last_state_publish_ok == 0.0


# ---------------------------------------------------------------------------
# guess_meta — additional branches
# ---------------------------------------------------------------------------


def test_guess_meta_gfx_percent():
    unit, _dc, icon, _sdp = tm.guess_meta("GFX%")
    assert unit == "%"
    assert icon == "mdi:percent"


def test_guess_meta_temp_in_name():
    unit, dc, _icon, _sdp = tm.guess_meta("SomeTemp")
    assert unit == "°C"
    assert dc == "temperature"


def test_guess_meta_per_s_in_name():
    unit, _dc, _icon, _sdp = tm.guess_meta("Ops/s")
    assert unit == "1/s"


def test_guess_meta_trailing_s():
    unit, _dc, _icon, _sdp = tm.guess_meta("rate_s")
    assert unit == "1/s"


def test_guess_meta_seconds_exact():
    unit, _dc, _icon, sdp = tm.guess_meta("seconds")
    assert unit == "s"
    assert sdp == 1


def test_guess_meta_sec_exact():
    unit, _dc, _icon, _sdp = tm.guess_meta("sec")
    assert unit == "s"


def test_guess_meta_joule_single_j():
    unit, _dc, _icon, _sdp = tm.guess_meta("CorJ")
    assert unit == "J"


def test_guess_meta_nmi():
    # NMI and SMI are interrupt counts, semantically the same as IRQ. Give
    # them the same sdp=0 treatment via an explicit COLUMNS entry so we
    # don't inherit a stale sdp=2 from the pre-consolidation heuristic
    # (only "irq" matched a count-like branch back then).
    unit, _dc, _icon, sdp = tm.guess_meta("NMI")
    assert unit is None
    assert sdp == 0


# ---------------------------------------------------------------------------
# friendly_name — more coverage
# ---------------------------------------------------------------------------


def test_friendly_name_cpu_percent():
    assert tm.friendly_name("CPU%") == "CPU Busy"


def test_friendly_name_core_tmp():
    assert tm.friendly_name("CoreTmp") == "CPU Core Temperature"


def test_friendly_name_tsc_mhz():
    assert tm.friendly_name("TSC_MHz") == "CPU Time Stamp Counter Frequency"


def test_friendly_name_avg_mhz():
    assert tm.friendly_name("Avg_MHz") == "CPU Average Frequency"


def test_friendly_name_ram_watt():
    assert tm.friendly_name("RAMWatt") == "CPU DRAM Power"


def test_friendly_name_gfx_watt():
    assert tm.friendly_name("GFXWatt") == "CPU iGPU Power"


def test_friendly_name_gfx_c0():
    assert tm.friendly_name("GFX%C0") == "GPU C0 (Active)"


def test_friendly_name_pkg_c_states():
    assert tm.friendly_name("Pkg%pc7") == "CPU Package C7 Residency"
    assert tm.friendly_name("Pkg%pc9") == "CPU Package C9 Residency"
    assert tm.friendly_name("Pkg%pc10") == "CPU Package C10 Residency"


def test_friendly_name_cpu_c_states():
    assert tm.friendly_name("CPU%c1") == "CPU C1 Residency"
    assert tm.friendly_name("CPU%c6") == "CPU C6 Residency"
    assert tm.friendly_name("CPU%c7") == "CPU C7 Residency"


def test_friendly_name_lpi():
    assert tm.friendly_name("CPU%LPI") == "CPU Low Power Idle Residency"
    assert tm.friendly_name("SYS%LPI") == "System Low Power Idle Residency"


def test_friendly_name_gpu_freq():
    assert tm.friendly_name("GFXAMHz") == "GPU Frequency (Actual)"
    assert tm.friendly_name("GFXMHz") == "GPU Frequency (Requested)"


def test_friendly_name_ipc():
    assert tm.friendly_name("IPC") == "Instructions per Cycle"


def test_friendly_name_smi():
    assert tm.friendly_name("SMI") == "System Management Interrupt Rate"


def test_friendly_name_nmi():
    assert tm.friendly_name("NMI") == "Non-maskable Interrupt Rate"


def test_friendly_name_acpi():
    assert tm.friendly_name("C1ACPI%") == "ACPI C1 Residency"
    assert tm.friendly_name("C2ACPI%") == "ACPI C2 Residency"
    assert tm.friendly_name("C3ACPI%") == "ACPI C3 Residency"


def test_friendly_name_poll():
    assert tm.friendly_name("POLL%") == "CPU Polling Time"


def test_friendly_name_gfx_rc6():
    assert tm.friendly_name("GFX%rc6") == "GPU RC6 Residency"


def test_friendly_name_llc():
    assert tm.friendly_name("LLCkRPS") == "CPU Last-Level Cache References"
    assert tm.friendly_name("LLC%hi") == "CPU Last-Level Cache Hit Rate"
    assert tm.friendly_name("LLC%hit") == "CPU Last-Level Cache Hit Rate"


def test_friendly_name_l2():
    assert tm.friendly_name("L2kRPS") == "CPU L2 Cache References"
    assert tm.friendly_name("L2%hit") == "CPU L2 Cache Hit Rate"


def test_missing_expected_columns_all_present():
    header = sorted(tm.EXPECTED_COLS) + ["Extra1", "Extra2"]
    assert tm.missing_expected_columns(header) == []


def test_missing_expected_columns_flags_dropped():
    header = [c for c in sorted(tm.EXPECTED_COLS) if c not in {"LLCkRPS", "L2kRPS"}]
    assert tm.missing_expected_columns(header) == ["L2kRPS", "LLCkRPS"]


def test_missing_expected_columns_empty_header():
    assert sorted(tm.missing_expected_columns([])) == sorted(tm.EXPECTED_COLS)


def test_rps_suffix_canonicals_have_unit_override():
    """Any COLUMN_RENAMES canonical that ends in 'rps' would silently
    fall through guess_meta's rps-suffix branch to '1/s'. A rename only
    exists because the column's unit scale needs translating, so every
    such canonical must have an explicit COLUMNS entry with a non-None
    unit to express the actual published unit — keying on scale != 1.0
    alone misses the scale=1.0 mega-input path (caught by PR #274 review)."""
    from turbostat_mqtt.metadata import COLUMN_RENAMES, COLUMNS

    rps_canonicals = {
        canonical
        for canonical, _ in COLUMN_RENAMES.values()
        if canonical.lower().endswith("rps")
    }
    for canonical in rps_canonicals:
        assert canonical in COLUMNS and COLUMNS[canonical].unit is not None, (
            f"{canonical} ends in 'rps' (would default to '1/s') but "
            f"has no explicit COLUMNS entry with a unit"
        )


def test_expected_cols_subset_of_friendly_name():
    """Every column we expect must have a friendly_name mapping, or else the
    discovery filter would silently drop it even when present."""
    unmapped = [c for c in tm.EXPECTED_COLS if tm.friendly_name(c) == f"Turbostat {c}"]
    assert unmapped == [], (
        f"EXPECTED_COLS entries missing from friendly_name: {unmapped}"
    )


def test_friendly_name_totl_any_cpugfx():
    assert tm.friendly_name("Totl%C0") == "CPU Total C0 (Active)"
    assert tm.friendly_name("Any%C0") == "CPU Any Core C0 (Active)"
    assert tm.friendly_name("CPUGFX%") == "CPU+GPU C0 (Active)"


# ---------------------------------------------------------------------------
# build_discovery_payloads — more variations
# ---------------------------------------------------------------------------


def test_build_discovery_payloads_percent_col():
    cols = {"Busy%": "busy_pct"}
    payloads = tm.build_discovery_payloads(
        discovery_prefix="ha",
        device_id="ts",
        device_name="TS",
        base_topic="ts",
        availability_topic="ts/avail",
        cols=cols,
        expire_after_s=90,
    )
    key = "ha/sensor/ts/busy_pct/config"
    assert key in payloads
    p = payloads[key]
    assert p["unit_of_measurement"] == "%"
    assert "device_class" not in p
    assert p["suggested_display_precision"] == 1
    assert p["availability_topic"] == "ts/avail"
    assert p["state_topic"] == "ts/busy_pct/state"
    assert "json_attributes_topic" not in p


def test_build_discovery_payloads_temperature_col():
    cols = {"PkgTmp": "pkgtmp"}
    payloads = tm.build_discovery_payloads(
        discovery_prefix="ha",
        device_id="ts",
        device_name="TS",
        base_topic="ts",
        availability_topic="ts/avail",
        cols=cols,
        expire_after_s=120,
    )
    p = next(iter(payloads.values()))
    assert p["device_class"] == "temperature"
    assert p["unit_of_measurement"] == "°C"


def test_build_discovery_payloads_multiple_cols_share_device():
    cols = {"PkgWatt": "pkgwatt", "CorWatt": "corwatt", "PkgTmp": "pkgtmp"}
    payloads = tm.build_discovery_payloads(
        discovery_prefix="ha",
        device_id="ts",
        device_name="TS",
        base_topic="ts",
        availability_topic="ts/avail",
        cols=cols,
        expire_after_s=120,
    )
    devices = [p["device"] for p in payloads.values()]
    # All should reference the same device dict
    assert all(d["identifiers"] == ["ts"] for d in devices)
    assert all(d["name"] == "TS" for d in devices)


# ---------------------------------------------------------------------------
# TurbostatParser — additional edge cases
# ---------------------------------------------------------------------------


def test_parser_consecutive_headers_replace():
    """Two header lines in a row: second replaces first."""
    p = tm.TurbostatParser()
    p.parse_line("A B C\n")
    assert p.header == ["A", "B", "C"]
    p.parse_line("X Y\n")
    assert p.header == ["X", "Y"]


def test_parser_mixed_numeric_text_is_data():
    """A line with some text and some numbers after header is data if counts match."""
    p = tm.TurbostatParser()
    # Header must be all non-numeric
    p.parse_line("Name Value\n")
    # A line where at least one token is numeric is treated as data
    result = p.parse_line("cpu0 42\n")
    assert result is not None
    _, values, _ = result
    assert values == {"Name": "cpu0", "Value": "42"}


def test_parser_plus_sign_number():
    p = tm.TurbostatParser()
    p.parse_line("A\n")
    result = p.parse_line("+3.14\n")
    assert result is not None


def test_parser_aliases_mega_to_kilo_in_first_header():
    """Upstream turbostat renamed LLCkRPS→LLCMRPS and L2kRPS→L2MRPS;
    the parser aliases them back so downstream code keys on the historical
    names and HA entity IDs stay stable across the rename."""
    p = tm.TurbostatParser()
    result = p.parse_line("IPC LLCMRPS LLC%hit L2MRPS L2%hit\n")
    assert result is None
    assert p.header == ["IPC", "LLCkRPS", "LLC%hit", "L2kRPS", "L2%hit"]


def test_parser_aliases_mega_to_kilo_in_replacement_header():
    p = tm.TurbostatParser()
    p.parse_line("A B C\n")
    p.parse_line("LLCMRPS L2MRPS Busy%\n")
    assert p.header == ["LLCkRPS", "L2kRPS", "Busy%"]


def test_parser_data_row_keys_use_aliased_names_mega_input_unscaled():
    """New turbostat emits LLCMRPS — already in mega-refs/sec — so the
    parser aliases the name to LLCkRPS for entity ID continuity but
    leaves the value alone (scale=1.0)."""
    p = tm.TurbostatParser()
    p.parse_line("LLCMRPS L2MRPS\n")
    result = p.parse_line("54.19 73.5\n")
    assert result is not None
    _, values, _ = result
    assert values == {"LLCkRPS": "54.19", "L2kRPS": "73.5"}


def test_parser_data_row_kilo_input_rescaled_to_megaref():
    """Pre-rename turbostat emits LLCkRPS natively (in thousands-of-refs);
    parser converges to the canonical mega scale by dividing by 1000.
    Regression guard: catches drift back to no-op identity for kilo input,
    which would publish 1000× too large given the M/s unit override."""
    p = tm.TurbostatParser()
    p.parse_line("LLCkRPS L2kRPS\n")
    result = p.parse_line("54194 73500\n")
    assert result is not None
    _, values, _ = result
    assert values == {"LLCkRPS": "54.194", "L2kRPS": "73.5"}


def test_parser_kilo_rescale_suppresses_binary_float_noise():
    """Many integer kilo inputs produce ugly binary-float noise after the
    ×0.001 scale (e.g. 50495 * 0.001 = 50.495000000000005). The parser
    rounds before stringifying so MQTT JSON and downstream consumers see
    clean values."""
    p = tm.TurbostatParser()
    p.parse_line("LLCkRPS\n")
    result = p.parse_line("50495\n")
    assert result is not None
    _, values, _ = result
    assert values["LLCkRPS"] == "50.495"


def test_parser_exposes_original_header_for_raw_consumers():
    """parser.original_header preserves the pre-alias names so callers
    that zip it against the verbatim turbostat line (e.g. _raw_header
    against _raw_line) get consistent name→value pairs without a
    reverse-alias map."""
    p = tm.TurbostatParser()
    p.parse_line("IPC LLCMRPS L2MRPS\n")
    assert p.header == ["IPC", "LLCkRPS", "L2kRPS"]
    assert p.original_header == ["IPC", "LLCMRPS", "L2MRPS"]


def test_parser_reset_clears_original_header_and_scales():
    p = tm.TurbostatParser()
    p.parse_line("LLCMRPS\n")
    assert p.original_header is not None
    p.reset()
    assert p.original_header is None


# ---------------------------------------------------------------------------
# log — edge cases
# ---------------------------------------------------------------------------


def test_log_unknown_level_treated_as_info(capsys):
    # Unknown levels default to order 20 (same as INFO)
    tm.log("CUSTOM", "custom msg", min_level="INFO")
    out = capsys.readouterr().out
    assert "[CUSTOM] custom msg" in out


def test_log_unknown_min_level_treated_as_info(capsys):
    tm.log("ERROR", "err msg", min_level="BOGUS")
    out = capsys.readouterr().out
    assert "[ERROR] err msg" in out


# ---------------------------------------------------------------------------
# sanitize_key — additional edge cases
# ---------------------------------------------------------------------------


def test_sanitize_key_special_chars():
    assert tm.sanitize_key("A@B#C") == "a_b_c"


def test_sanitize_key_empty_string():
    assert tm.sanitize_key("") == ""


def test_sanitize_key_all_special():
    result = tm.sanitize_key("@#$")
    assert result == ""
