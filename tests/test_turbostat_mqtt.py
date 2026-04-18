# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for pure/utility functions in turbostat_mqtt.turbostat_mqtt."""

from __future__ import annotations

import subprocess

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
        state_topic="turbostat/state",
        base_topic="turbostat",
        availability_topic="turbostat/availability",
        cols=cols,
        expire_after_s=60,  # minimum value
    )
    p = list(payloads.values())[0]
    assert p["expire_after"] == 60


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
        expire_after_s=120,
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
# mqtt_publish
# ---------------------------------------------------------------------------


def _make_mock_client(rc=0):
    """Return a mock MQTT client whose publish() returns an info object."""
    from unittest.mock import MagicMock

    client = MagicMock()
    info = MagicMock()
    info.rc = rc
    client.publish.return_value = info
    return client


def test_mqtt_publish_success():
    client = _make_mock_client(rc=0)
    health = tm.MqttHealth()
    result = tm.mqtt_publish(
        client,
        "test/topic",
        "payload",
        qos=0,
        retain=False,
        log_level="INFO",
        health=health,
        mark_state=False,
    )
    assert result is True
    client.publish.assert_called_once_with(
        "test/topic", payload="payload", qos=0, retain=False
    )


def test_mqtt_publish_success_mark_state_updates_health():
    client = _make_mock_client(rc=0)
    health = tm.MqttHealth()
    assert health.last_state_publish_ok == 0.0
    result = tm.mqtt_publish(
        client,
        "t",
        "p",
        qos=1,
        retain=True,
        log_level="INFO",
        health=health,
        mark_state=True,
    )
    assert result is True
    assert health.last_state_publish_ok > 0.0


def test_mqtt_publish_success_no_mark_state_leaves_health():
    client = _make_mock_client(rc=0)
    health = tm.MqttHealth()
    tm.mqtt_publish(
        client,
        "t",
        "p",
        qos=0,
        retain=False,
        log_level="INFO",
        health=health,
        mark_state=False,
    )
    assert health.last_state_publish_ok == 0.0


def test_mqtt_publish_failure_rc():
    client = _make_mock_client(rc=4)  # non-zero rc
    health = tm.MqttHealth()
    result = tm.mqtt_publish(
        client,
        "t",
        "p",
        qos=0,
        retain=False,
        log_level="INFO",
        health=health,
    )
    assert result is False


def test_mqtt_publish_exception():
    from unittest.mock import MagicMock

    client = MagicMock()
    client.publish.side_effect = ConnectionError("boom")
    health = tm.MqttHealth()
    result = tm.mqtt_publish(
        client,
        "t",
        "p",
        qos=0,
        retain=False,
        log_level="INFO",
        health=health,
    )
    assert result is False


# ---------------------------------------------------------------------------
# connect_mqtt_with_retry
# ---------------------------------------------------------------------------


def test_connect_mqtt_with_retry_success_first_try():
    from unittest.mock import MagicMock

    client = MagicMock()
    client.connect.return_value = None  # success, no exception
    tm.connect_mqtt_with_retry(client, "localhost", 1883, "INFO")
    client.connect.assert_called_once_with("localhost", 1883, keepalive=60)


def test_connect_mqtt_with_retry_retries_then_succeeds():
    from unittest.mock import MagicMock, patch

    client = MagicMock()
    client.connect.side_effect = [
        ConnectionRefusedError("refused"),
        OSError("network down"),
        None,  # success on third attempt
    ]
    with patch("turbostat_mqtt.time.sleep") as mock_sleep:
        tm.connect_mqtt_with_retry(client, "broker", 1883, "INFO")

    assert client.connect.call_count == 3
    # First retry delay=5, second retry delay=10 (5*2)
    assert mock_sleep.call_count == 2
    mock_sleep.assert_any_call(5)
    mock_sleep.assert_any_call(10)


def test_connect_mqtt_with_retry_delay_caps_at_60():
    from unittest.mock import MagicMock, patch

    client = MagicMock()
    # Fail 6 times: delays should be 5, 10, 20, 40, 60, 60
    client.connect.side_effect = [OSError("fail") for _ in range(6)] + [None]
    with patch("turbostat_mqtt.time.sleep") as mock_sleep:
        tm.connect_mqtt_with_retry(client, "broker", 1883, "INFO")

    assert client.connect.call_count == 7
    delays = [c.args[0] for c in mock_sleep.call_args_list]
    assert delays == [5, 10, 20, 40, 60, 60]


# ---------------------------------------------------------------------------
# start_turbostat
# ---------------------------------------------------------------------------


def test_start_turbostat_command_construction():
    from unittest.mock import patch, MagicMock

    mock_proc = MagicMock()
    with patch("turbostat_mqtt.subprocess.Popen", return_value=mock_proc) as mock_popen:
        result = tm.start_turbostat(10.0)

    assert result is mock_proc
    mock_popen.assert_called_once_with(
        [
            "turbostat",
            "--Summary",
            "--quiet",
            "--enable",
            "all",
            "--interval",
            "10.0",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        bufsize=1,
        universal_newlines=True,
    )


def test_start_turbostat_fractional_interval():
    from unittest.mock import patch, MagicMock

    mock_proc = MagicMock()
    with patch("turbostat_mqtt.subprocess.Popen", return_value=mock_proc) as mock_popen:
        result = tm.start_turbostat(2.5)

    assert result is mock_proc
    cmd = mock_popen.call_args[0][0]
    assert cmd[-1] == "2.5"
    assert cmd[0] == "turbostat"


# ---------------------------------------------------------------------------
# mqtt_publish — additional edge cases
# ---------------------------------------------------------------------------


def test_mqtt_publish_failure_rc_does_not_mark_state():
    client = _make_mock_client(rc=4)
    health = tm.MqttHealth()
    tm.mqtt_publish(
        client,
        "t",
        "p",
        qos=0,
        retain=False,
        log_level="INFO",
        health=health,
        mark_state=True,
    )
    assert health.last_state_publish_ok == 0.0


def test_mqtt_publish_exception_does_not_mark_state():
    from unittest.mock import MagicMock

    client = MagicMock()
    client.publish.side_effect = RuntimeError("boom")
    health = tm.MqttHealth()
    result = tm.mqtt_publish(
        client,
        "t",
        "p",
        qos=1,
        retain=True,
        log_level="INFO",
        health=health,
        mark_state=True,
    )
    assert result is False
    assert health.last_state_publish_ok == 0.0


def test_mqtt_publish_with_retain_and_qos1():
    client = _make_mock_client(rc=0)
    health = tm.MqttHealth()
    result = tm.mqtt_publish(
        client,
        "avail/topic",
        "online",
        qos=1,
        retain=True,
        log_level="DEBUG",
        health=health,
    )
    assert result is True
    client.publish.assert_called_once_with(
        "avail/topic",
        payload="online",
        qos=1,
        retain=True,
    )


# ---------------------------------------------------------------------------
# guess_meta — additional branches
# ---------------------------------------------------------------------------


def test_guess_meta_gfx_percent():
    unit, dc, icon, sdp = tm.guess_meta("GFX%")
    assert unit == "%"
    assert icon == "mdi:percent"


def test_guess_meta_temp_in_name():
    unit, dc, icon, sdp = tm.guess_meta("SomeTemp")
    assert unit == "°C"
    assert dc == "temperature"


def test_guess_meta_per_s_in_name():
    unit, dc, icon, sdp = tm.guess_meta("Ops/s")
    assert unit == "1/s"


def test_guess_meta_trailing_s():
    unit, dc, icon, sdp = tm.guess_meta("rate_s")
    assert unit == "1/s"


def test_guess_meta_seconds_exact():
    unit, dc, icon, sdp = tm.guess_meta("seconds")
    assert unit == "s"
    assert sdp == 1


def test_guess_meta_sec_exact():
    unit, dc, icon, sdp = tm.guess_meta("sec")
    assert unit == "s"


def test_guess_meta_joule_single_j():
    unit, dc, icon, sdp = tm.guess_meta("CorJ")
    assert unit == "J"


def test_guess_meta_nmi():
    # "nmi" doesn't have "irq" in it but is otherwise unknown
    unit, dc, icon, sdp = tm.guess_meta("NMI")
    assert unit is None
    assert sdp == 2


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
        state_topic="ts/state",
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
    assert p["json_attributes_topic"] == "ts/state"


def test_build_discovery_payloads_temperature_col():
    cols = {"PkgTmp": "pkgtmp"}
    payloads = tm.build_discovery_payloads(
        discovery_prefix="ha",
        device_id="ts",
        device_name="TS",
        state_topic="ts/state",
        base_topic="ts",
        availability_topic="ts/avail",
        cols=cols,
        expire_after_s=120,
    )
    p = list(payloads.values())[0]
    assert p["device_class"] == "temperature"
    assert p["unit_of_measurement"] == "°C"


def test_build_discovery_payloads_multiple_cols_share_device():
    cols = {"PkgWatt": "pkgwatt", "CorWatt": "corwatt", "PkgTmp": "pkgtmp"}
    payloads = tm.build_discovery_payloads(
        discovery_prefix="ha",
        device_id="ts",
        device_name="TS",
        state_topic="ts/state",
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
