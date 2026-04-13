# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for pure/utility functions in container_info_mqtt.container_info_mqtt."""

from __future__ import annotations

import json
import logging
import subprocess
from typing import Any
from unittest.mock import patch

import pytest

import container_info_mqtt as cim

_LOG = logging.getLogger("test")


# ---------------------------------------------------------------------------
# slugify
# ---------------------------------------------------------------------------


def test_slugify_simple():
    assert cim.slugify("my_container") == "my_container"


def test_slugify_spaces_become_underscores():
    assert cim.slugify("My Container") == "my_container"


def test_slugify_strips_leading_trailing():
    assert cim.slugify("  hello  ") == "hello"


def test_slugify_special_chars_replaced():
    assert cim.slugify("foo.bar!baz") == "foo_bar_baz"


def test_slugify_consecutive_separators_collapsed():
    # Hyphens are allowed by the regex so "foo--bar" stays as "foo--bar";
    # what collapses is runs of the *replaced* underscore character.
    result = cim.slugify("foo--bar")
    assert "__" not in result
    assert result == "foo--bar"


def test_slugify_empty_returns_unknown():
    assert cim.slugify("") == "unknown"


def test_slugify_only_special_chars_returns_unknown():
    assert cim.slugify("!!!") == "unknown"


def test_slugify_lowercases():
    assert cim.slugify("MyApp") == "myapp"


# ---------------------------------------------------------------------------
# container_display_name
# ---------------------------------------------------------------------------


def test_container_display_name_plain():
    assert cim.container_display_name("mycontainer") == "Mycontainer"


def test_container_display_name_empty_returns_unknown():
    assert cim.container_display_name("") == "Unknown"
    assert cim.container_display_name("   ") == "Unknown"


def test_container_display_name_addon_prefix_strips_hash():
    # addon_<hex>_<name> → <name> formatted
    name = cim.container_display_name("addon_abc123_my_camera")
    assert "abc123" not in name
    assert "Camera" in name


def test_container_display_name_addon_prefix_non_hex_left_alone():
    # If the token after addon_ is not all hex, the name is not stripped further
    name = cim.container_display_name("addon_notahex_something")
    assert name  # just should not crash and return non-empty
    assert name != "Unknown"


def test_container_display_name_capitalizes_words():
    result = cim.container_display_name("my_cool_app")
    assert result == "My Cool App"


def test_container_display_name_hyphens_become_spaces():
    result = cim.container_display_name("my-cool-app")
    assert result == "My Cool App"


# ---------------------------------------------------------------------------
# safe_float (container_info_mqtt version)
# ---------------------------------------------------------------------------


def test_safe_float_int():
    assert cim.safe_float(10) == 10.0


def test_safe_float_float():
    assert cim.safe_float(3.5) == pytest.approx(3.5)


def test_safe_float_string_numeric():
    assert cim.safe_float("2.5") == pytest.approx(2.5)


def test_safe_float_string_non_numeric():
    assert cim.safe_float("abc") is None


def test_safe_float_bool_returns_none():
    # container_info_mqtt explicitly rejects bools
    assert cim.safe_float(True) is None
    assert cim.safe_float(False) is None


def test_safe_float_none_returns_none():
    assert cim.safe_float(None) is None


def test_safe_float_dict_returns_none():
    assert cim.safe_float({}) is None


# ---------------------------------------------------------------------------
# safe_int (container_info_mqtt version)
# ---------------------------------------------------------------------------


def test_safe_int_int():
    assert cim.safe_int(7) == 7


def test_safe_int_float_truncates():
    assert cim.safe_int(3.9) == 3


def test_safe_int_string():
    assert cim.safe_int("42") == 42


def test_safe_int_string_float():
    assert cim.safe_int("3.7") == 3


def test_safe_int_bool_returns_none():
    assert cim.safe_int(True) is None


def test_safe_int_empty_string():
    assert cim.safe_int("") is None


def test_safe_int_non_numeric_string():
    assert cim.safe_int("abc") is None


# ---------------------------------------------------------------------------
# safe_text
# ---------------------------------------------------------------------------


def test_safe_text_string():
    assert cim.safe_text("hello") == "hello"


def test_safe_text_strips_whitespace():
    assert cim.safe_text("  hi  ") == "hi"


def test_safe_text_empty_returns_none():
    assert cim.safe_text("") is None
    assert cim.safe_text("   ") is None


def test_safe_text_none():
    assert cim.safe_text(None) is None


def test_safe_text_dict_returns_none():
    assert cim.safe_text({}) is None


def test_safe_text_list_returns_none():
    assert cim.safe_text([]) is None


def test_safe_text_int_converts():
    assert cim.safe_text(42) == "42"


# ---------------------------------------------------------------------------
# deep_get
# ---------------------------------------------------------------------------


def test_deep_get_single_level():
    assert cim.deep_get({"a": 1}, ("a",)) == 1


def test_deep_get_nested():
    d: dict[str, Any] = {"network": {"cumulative_rx": 500}}
    assert cim.deep_get(d, ("network", "cumulative_rx")) == 500


def test_deep_get_missing_key():
    assert cim.deep_get({"a": 1}, ("b",)) is None


def test_deep_get_intermediate_not_dict():
    d: dict[str, Any] = {"a": 42}
    assert cim.deep_get(d, ("a", "b")) is None


def test_deep_get_empty_path():
    d: dict[str, Any] = {"a": 1}
    assert cim.deep_get(d, ()) == d


# ---------------------------------------------------------------------------
# cpu_percent_from_stats
# ---------------------------------------------------------------------------


def _make_stats(
    total: float,
    pre_total: float,
    system: float,
    pre_system: float,
    online_cpus: int = 2,
) -> dict[str, Any]:
    return {
        "cpu_stats": {
            "cpu_usage": {"total_usage": total},
            "system_cpu_usage": system,
            "online_cpus": online_cpus,
        },
        "precpu_stats": {
            "cpu_usage": {"total_usage": pre_total},
            "system_cpu_usage": pre_system,
        },
    }


def test_cpu_percent_basic():
    stats = _make_stats(
        total=200_000_000,
        pre_total=100_000_000,
        system=2_000_000_000,
        pre_system=1_000_000_000,
        online_cpus=2,
    )
    result = cim.cpu_percent_from_stats(stats)
    # (100e6 / 1000e6) * 2 * 100 = 20.0 %
    assert result == pytest.approx(20.0)


def test_cpu_percent_zero_cpu_delta_returns_zero():
    # cpu_delta == 0 means the container used no CPU — valid 0% reading
    stats = _make_stats(100, 100, 1000, 500)
    assert cim.cpu_percent_from_stats(stats) == pytest.approx(0.0)


def test_cpu_percent_negative_system_delta_returns_none():
    # system_usage went backwards (impossible in real data, but defensive)
    stats = _make_stats(200, 100, 900, 1000)
    assert cim.cpu_percent_from_stats(stats) is None


def test_cpu_percent_missing_cpu_stats_returns_none():
    assert cim.cpu_percent_from_stats({}) is None


def test_cpu_percent_fallback_percpu():
    # online_cpus missing → fall back to len(percpu_usage)
    stats = {
        "cpu_stats": {
            "cpu_usage": {
                "total_usage": 200_000_000,
                "percpu_usage": [0, 0, 0, 0],  # 4 CPUs
            },
            "system_cpu_usage": 2_000_000_000,
        },
        "precpu_stats": {
            "cpu_usage": {"total_usage": 100_000_000},
            "system_cpu_usage": 1_000_000_000,
        },
    }
    result = cim.cpu_percent_from_stats(stats)
    # (100e6 / 1000e6) * 4 * 100 = 40.0%
    assert result == pytest.approx(40.0)


# ---------------------------------------------------------------------------
# sum_network_totals
# ---------------------------------------------------------------------------


def test_sum_network_totals_single_iface():
    payload: dict[str, Any] = {
        "networks": {
            "eth0": {"rx_bytes": 1000, "tx_bytes": 500},
        }
    }
    rx, tx = cim.sum_network_totals(payload)
    assert rx == pytest.approx(1000.0)
    assert tx == pytest.approx(500.0)


def test_sum_network_totals_multiple_ifaces():
    payload: dict[str, Any] = {
        "networks": {
            "eth0": {"rx_bytes": 1000, "tx_bytes": 200},
            "eth1": {"rx_bytes": 500, "tx_bytes": 100},
        }
    }
    rx, tx = cim.sum_network_totals(payload)
    assert rx == pytest.approx(1500.0)
    assert tx == pytest.approx(300.0)


def test_sum_network_totals_no_networks_key():
    rx, tx = cim.sum_network_totals({})
    assert rx is None
    assert tx is None


def test_sum_network_totals_networks_not_dict():
    rx, tx = cim.sum_network_totals({"networks": []})
    assert rx is None
    assert tx is None


def test_sum_network_totals_clamps_negative():
    payload: dict[str, Any] = {
        "networks": {
            "eth0": {"rx_bytes": -100, "tx_bytes": 500},
        }
    }
    rx, tx = cim.sum_network_totals(payload)
    assert rx == pytest.approx(0.0)  # clamped
    assert tx == pytest.approx(500.0)


# ---------------------------------------------------------------------------
# sum_blkio_totals
# ---------------------------------------------------------------------------


def test_sum_blkio_totals_basic():
    payload: dict[str, Any] = {
        "blkio_stats": {
            "io_service_bytes_recursive": [
                {"op": "Read", "value": 4096},
                {"op": "Write", "value": 8192},
                {"op": "Sync", "value": 100},  # ignored op
            ]
        }
    }
    read, write = cim.sum_blkio_totals(payload)
    assert read == pytest.approx(4096.0)
    assert write == pytest.approx(8192.0)


def test_sum_blkio_totals_no_blkio_key():
    read, write = cim.sum_blkio_totals({})
    assert read is None
    assert write is None


def test_sum_blkio_totals_empty_records():
    payload: dict[str, Any] = {"blkio_stats": {"io_service_bytes_recursive": []}}
    read, write = cim.sum_blkio_totals(payload)
    assert read is None
    assert write is None


def test_sum_blkio_totals_records_not_list():
    payload: dict[str, Any] = {"blkio_stats": {"io_service_bytes_recursive": {}}}
    read, write = cim.sum_blkio_totals(payload)
    assert read is None
    assert write is None


def test_sum_blkio_totals_multiple_read_entries():
    payload: dict[str, Any] = {
        "blkio_stats": {
            "io_service_bytes_recursive": [
                {"op": "Read", "value": 1024},
                {"op": "Read", "value": 2048},
                {"op": "Write", "value": 512},
            ]
        }
    }
    read, write = cim.sum_blkio_totals(payload)
    assert read == pytest.approx(3072.0)
    assert write == pytest.approx(512.0)


# ---------------------------------------------------------------------------
# compute_rate_metrics
# ---------------------------------------------------------------------------


def test_compute_rate_metrics_first_call_no_rates():
    container: dict[str, Any] = {
        "network": {"cumulative_rx": 1000, "cumulative_tx": 200},
        "io": {"cumulative_ior": 500, "cumulative_iow": 100},
        "network_rx_total": 1000,
        "network_tx_total": 200,
        "io_read_total": 500,
        "io_write_total": 100,
    }
    totals: dict[str, dict[str, float]] = {}
    rates = cim.compute_rate_metrics("slug", container, 1000.0, totals)
    # First call: no previous data → no rates
    assert rates == {}
    assert "slug" in totals


def test_compute_rate_metrics_second_call_yields_rates():
    container: dict[str, Any] = {
        "network_rx_total": 2000,
        "network_tx_total": 400,
        "io_read_total": 1000,
        "io_write_total": 200,
    }
    totals: dict[str, dict[str, float]] = {}

    cim.compute_rate_metrics("slug", container, 1000.0, totals)

    container2: dict[str, Any] = {
        "network_rx_total": 3000,
        "network_tx_total": 600,
        "io_read_total": 1500,
        "io_write_total": 300,
    }
    rates = cim.compute_rate_metrics("slug", container2, 1010.0, totals)

    # dt = 10 s; delta_rx = 1000 → rate = 100 B/s
    assert rates["network_rx_rate"] == pytest.approx(100.0)
    assert rates["network_tx_rate"] == pytest.approx(20.0)
    assert rates["io_read_rate"] == pytest.approx(50.0)
    assert rates["io_write_rate"] == pytest.approx(10.0)


def test_compute_rate_metrics_counter_reset_skipped():
    # If current total < previous total, the rate for that metric is skipped
    totals: dict[str, dict[str, float]] = {}
    c1: dict[str, Any] = {"network_rx_total": 5000}
    cim.compute_rate_metrics("slug", c1, 1000.0, totals)
    c2: dict[str, Any] = {"network_rx_total": 100}  # counter reset
    rates = cim.compute_rate_metrics("slug", c2, 1010.0, totals)
    assert "network_rx_rate" not in rates


# ---------------------------------------------------------------------------
# parse_include_metrics
# ---------------------------------------------------------------------------


def test_parse_include_metrics_valid_keys():
    result = cim.parse_include_metrics("cpu_percent,memory_usage", _LOG)
    assert result == ["cpu_percent", "memory_usage"]


def test_parse_include_metrics_unknown_key_skipped():
    result = cim.parse_include_metrics("cpu_percent,nonexistent", _LOG)
    assert "nonexistent" not in result
    assert "cpu_percent" in result


def test_parse_include_metrics_empty_returns_all():
    result = cim.parse_include_metrics("", _LOG)
    assert set(result) == set(cim.METRIC_DEFS.keys())


def test_parse_include_metrics_all_unknown_returns_all():
    result = cim.parse_include_metrics("garbage,junk", _LOG)
    assert set(result) == set(cim.METRIC_DEFS.keys())


def test_parse_include_metrics_strips_whitespace():
    result = cim.parse_include_metrics(" cpu_percent , memory_usage ", _LOG)
    assert "cpu_percent" in result
    assert "memory_usage" in result


# ---------------------------------------------------------------------------
# fetch_ps_containers — name normalisation
# ---------------------------------------------------------------------------


def _fake_ps_proc(rows: list[dict[str, Any]]) -> subprocess.CompletedProcess[str]:
    stdout = "\n".join(json.dumps(row) for row in rows)
    return subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr="")


def test_fetch_ps_strips_leading_slash():
    row = {"ID": "abc123", "Names": "/builder_foo", "Status": "Up", "State": "running"}
    with patch("container_info_mqtt.run_cmd", return_value=_fake_ps_proc([row])):
        result = cim.fetch_ps_containers(10, _LOG)
    assert len(result) == 1
    assert result[0]["name"] == "builder_foo"


def test_fetch_ps_name_without_slash_unchanged():
    row = {"ID": "abc123", "Names": "mycontainer", "Status": "Up", "State": "running"}
    with patch("container_info_mqtt.run_cmd", return_value=_fake_ps_proc([row])):
        result = cim.fetch_ps_containers(10, _LOG)
    assert result[0]["name"] == "mycontainer"


# ---------------------------------------------------------------------------
# summary_only_metrics derivation
# ---------------------------------------------------------------------------


def _summary_only(summary_raw: str, include_raw: str) -> list[str]:
    """Replicate the summary_only_metrics derivation from main()."""
    selected = set(cim.parse_include_metrics(include_raw, _LOG))
    if not summary_raw.strip():
        return []
    return [
        m for m in cim.parse_include_metrics(summary_raw, _LOG) if m not in selected
    ]


def test_summary_only_excludes_metrics_already_in_include():
    result = _summary_only("cpu_percent,cpu_shares", "cpu_percent,memory_usage")
    assert "cpu_percent" not in result
    assert "cpu_shares" in result


def test_summary_only_empty_summary_gives_empty_list():
    result = _summary_only("", "cpu_percent,memory_usage")
    assert result == []


def test_summary_only_whitespace_summary_gives_empty_list():
    result = _summary_only("   ", "cpu_percent")
    assert result == []


def test_summary_only_all_in_include_gives_empty():
    result = _summary_only("cpu_percent,memory_usage", "cpu_percent,memory_usage")
    assert result == []


def test_summary_only_no_overlap():
    result = _summary_only("cpu_shares,blkio_weight", "cpu_percent,memory_usage")
    assert set(result) == {"cpu_shares", "blkio_weight"}


def test_summary_only_unknown_keys_skipped():
    result = _summary_only("cpu_shares,nonexistent", "cpu_percent")
    assert "nonexistent" not in result
    assert "cpu_shares" in result


def test_summary_only_default_config_deduplicates_correctly():
    # Simulate the real defaults: summary is a superset of include.
    # Metrics shared with include_metrics should be deduped; only the
    # extra ones (cpu_shares, cpuset_cpus, blkio_weight) should appear.
    default_include = (
        "cpu_percent,memory_usage,network_rx_rate,network_tx_rate,"
        "io_read_rate,io_write_rate,uptime_seconds"
    )
    default_summary = (
        "cpu_percent,memory_usage,network_rx_rate,network_tx_rate,"
        "io_read_rate,io_write_rate,uptime_seconds,"
        "cpu_shares,cpuset_cpus,blkio_weight"
    )
    result = _summary_only(default_summary, default_include)
    assert set(result) == {"cpu_shares", "cpuset_cpus", "blkio_weight"}


# ---------------------------------------------------------------------------
# redact_options_for_log
# ---------------------------------------------------------------------------


def test_redact_options_password_hidden():
    opts: dict[str, Any] = {"mqtt_password": "s3cr3t", "mqtt_host": "localhost"}
    redacted = cim.redact_options_for_log(opts)
    assert redacted["mqtt_password"] == "***"
    assert redacted["mqtt_host"] == "localhost"


def test_redact_options_empty_password_not_hidden():
    opts: dict[str, Any] = {"mqtt_password": ""}
    redacted = cim.redact_options_for_log(opts)
    assert redacted["mqtt_password"] == ""


def test_redact_options_non_sensitive_keys_unchanged():
    opts: dict[str, Any] = {"interval_seconds": 10, "log_level": "DEBUG"}
    redacted = cim.redact_options_for_log(opts)
    assert redacted == opts
