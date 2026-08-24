# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for pure/utility functions in container_info_mqtt.container_info_mqtt."""

import argparse
import json
import logging
import subprocess
from datetime import UTC, datetime
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


def test_container_display_name_app_prefix_strips_hash():
    assert (
        cim.container_display_name("app_0f7b38ce_container_info_mqtt")
        == "Container Info Mqtt"
    )
    assert cim.container_display_name("app_3fd9e6b0_hamh") == "Hamh"


def test_container_display_name_app_core_prefix_not_stripped():
    assert cim.container_display_name("app_core_mosquitto") == "Core Mosquitto"
    assert cim.container_display_name("app_core_mariadb") == "Core Mariadb"


def test_container_display_name_bare_container_unchanged():
    assert cim.container_display_name("hassio_observer") == "Hassio Observer"
    assert cim.container_display_name("homeassistant") == "Homeassistant"


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
    with patch("container_info_mqtt.docker.run_cmd", return_value=_fake_ps_proc([row])):
        result = cim.fetch_ps_containers(10, _LOG)
    assert len(result) == 1
    assert result[0]["name"] == "builder_foo"


def test_fetch_ps_name_without_slash_unchanged():
    row = {"ID": "abc123", "Names": "mycontainer", "Status": "Up", "State": "running"}
    with patch("container_info_mqtt.docker.run_cmd", return_value=_fake_ps_proc([row])):
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
        "io_read_rate,io_write_rate,started_at"
    )
    default_summary = (
        "cpu_percent,memory_usage,network_rx_rate,network_tx_rate,"
        "io_read_rate,io_write_rate,started_at,"
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


# ---------------------------------------------------------------------------
# prune_stale_discovery
# ---------------------------------------------------------------------------


class _RecordingClient:
    """Minimal mqtt.Client stub that records publish() calls."""

    def __init__(self) -> None:
        self.published: list[tuple[str, str, int, bool]] = []

    def publish(
        self, topic: str, payload: str = "", qos: int = 0, retain: bool = False
    ) -> Any:
        self.published.append((topic, payload, qos, retain))
        return None


def _prune(
    retained: dict[str, set[str]], expected_by_slug: dict[str, set[str]]
) -> tuple[int, _RecordingClient]:
    client = _RecordingClient()
    pruned = cim.prune_stale_discovery(
        client,  # type: ignore[arg-type]
        "homeassistant",
        "container_info",
        "container-info-mqtt",
        retained,
        expected_by_slug,
        _LOG,
    )
    return pruned, client


def test_prune_no_retained_returns_zero():
    pruned, client = _prune({}, {"foo": set()})
    assert pruned == 0
    assert client.published == []


def test_prune_all_retained_active_returns_zero():
    retained = {"container-info-mqtt_foo": {"cpu_percent", "summary"}}
    pruned, client = _prune(retained, {"foo": {"cpu_percent", "summary"}})
    assert pruned == 0
    assert client.published == []


def test_prune_single_stale_clears_all_metrics_and_availability():
    retained = {
        "container-info-mqtt_builder_x": {"cpu_percent", "memory_usage", "summary"},
    }
    pruned, client = _prune(retained, {"other": set()})
    assert pruned == 1

    topics = [t for (t, _, _, _) in client.published]
    # Each metric's discovery config cleared with empty retained payload.
    assert (
        "homeassistant/sensor/container-info-mqtt_builder_x/cpu_percent/config"
        in topics
    )
    assert (
        "homeassistant/sensor/container-info-mqtt_builder_x/memory_usage/config"
        in topics
    )
    assert "homeassistant/sensor/container-info-mqtt_builder_x/summary/config" in topics
    # Per-slug availability cleared.
    assert "container_info/builder_x/availability" in topics

    # All publishes are empty + retained.
    for _topic, payload, _qos, retain in client.published:
        assert payload == ""
        assert retain is True


def test_prune_mix_stale_and_active_only_clears_stale():
    retained = {
        "container-info-mqtt_keepme": {"cpu_percent", "summary"},
        "container-info-mqtt_builder_x": {"cpu_percent"},
    }
    pruned, client = _prune(retained, {"keepme": {"cpu_percent", "summary"}})
    assert pruned == 1
    topics = {t for (t, _, _, _) in client.published}
    # Stale node's topics cleared.
    assert (
        "homeassistant/sensor/container-info-mqtt_builder_x/cpu_percent/config"
        in topics
    )
    assert "container_info/builder_x/availability" in topics
    # Active node's topics untouched.
    for topic in topics:
        assert "keepme" not in topic


def test_prune_zero_overlap_at_or_above_threshold_refuses_wipe():
    retained = {
        "container-info-mqtt_hamh": {"cpu_percent", "summary"},
        "container-info-mqtt_airsonos": {"summary"},
        "container-info-mqtt_core_mosquitto": {"summary"},
    }
    expected = {
        "app_3fd9e6b0_hamh": {"cpu_percent", "summary"},
        "app_605cee21_airsonos": {"summary"},
        "app_core_mosquitto": {"summary"},
    }
    pruned, client = _prune(retained, expected)
    assert pruned == 0
    assert client.published == []


def test_prune_empty_expected_at_or_above_threshold_refuses_wipe():
    retained = {
        "container-info-mqtt_hamh": {"summary"},
        "container-info-mqtt_airsonos": {"summary"},
        "container-info-mqtt_core_mosquitto": {"summary"},
    }
    pruned, client = _prune(retained, {})
    assert pruned == 0
    assert client.published == []


def test_prune_zero_overlap_below_threshold_still_prunes():
    retained = {
        "container-info-mqtt_old_a": {"summary"},
        "container-info-mqtt_old_b": {"summary"},
    }
    expected = {"new_a": {"summary"}, "new_b": {"summary"}}
    pruned, _client = _prune(retained, expected)
    assert pruned == 2


def test_prune_partial_overlap_still_prunes_the_stale_ones():
    retained = {
        "container-info-mqtt_hamh": {"summary"},
        "container-info-mqtt_removed_container": {"summary"},
    }
    expected = {"hamh": {"summary"}}
    pruned, client = _prune(retained, expected)
    assert pruned == 1
    topics = {t for (t, _, _, _) in client.published}
    assert (
        "homeassistant/sensor/container-info-mqtt_removed_container/summary/config"
        in topics
    )
    assert "container_info/removed_container/availability" in topics


def test_prune_multiple_stale_with_overlapping_active_still_prunes_the_stale():
    retained = {
        "container-info-mqtt_builder_a": {"summary"},
        "container-info-mqtt_builder_b": {"summary"},
        "container-info-mqtt_builder_c": {"cpu_percent", "summary"},
        "container-info-mqtt_keepme": {"summary"},
    }
    # keepme overlaps -> guard passes; the 3 builder_* nodes prune as before.
    pruned, client = _prune(retained, {"keepme": {"summary"}})
    assert pruned == 3
    # 3 nodes × (their metrics + 1 availability) publishes.
    # builder_a: 1+1; builder_b: 1+1; builder_c: 2+1 = 7 total.
    assert len(client.published) == 7


def test_reconcile_active_slug_clears_only_orphan_metric():
    # Snapshotter retained an old uptime_seconds config from a prior version;
    # this process now publishes started_at instead. uptime_seconds must be
    # tombstoned while cpu_percent/started_at/summary are left intact and the
    # container stays online (availability untouched, not counted as pruned).
    retained = {
        "container-info-mqtt_ffmpeg_snapshotter": {
            "cpu_percent",
            "uptime_seconds",
            "started_at",
            "summary",
        },
    }
    pruned, client = _prune(
        retained,
        {"ffmpeg_snapshotter": {"cpu_percent", "started_at", "summary"}},
    )
    assert pruned == 0  # active slug reconciliation is not counted

    cleared = {t for (t, _, _, _) in client.published}
    assert cleared == {
        "homeassistant/sensor/container-info-mqtt_ffmpeg_snapshotter/uptime_seconds/config"
    }
    # The orphan was cleared with an empty retained payload (a tombstone).
    _topic, payload, _qos, retain = client.published[0]
    assert payload == ""
    assert retain is True
    # Availability for an online container is never touched.
    assert "ffmpeg_snapshotter/availability" not in cleared


def test_reconcile_active_slug_no_orphans_is_noop():
    retained = {
        "container-info-mqtt_ffmpeg_snapshotter": {
            "cpu_percent",
            "started_at",
            "summary",
        },
    }
    pruned, client = _prune(
        retained,
        {"ffmpeg_snapshotter": {"cpu_percent", "started_at", "summary"}},
    )
    assert pruned == 0
    assert client.published == []


def test_reconcile_and_stale_prune_combined():
    retained = {
        # Active container with a dropped metric.
        "container-info-mqtt_ffmpeg_snapshotter": {
            "uptime_seconds",
            "started_at",
            "summary",
        },
        # Whole container gone (now excluded / removed).
        "container-info-mqtt_builder_x": {"cpu_percent", "summary"},
    }
    pruned, client = _prune(
        retained,
        {"ffmpeg_snapshotter": {"started_at", "summary"}},
    )
    assert pruned == 1  # only the fully-stale slug is counted

    cleared = {t for (t, _, _, _) in client.published}
    # Active slug: only the orphan metric, no availability change.
    assert (
        "homeassistant/sensor/container-info-mqtt_ffmpeg_snapshotter/uptime_seconds/config"
        in cleared
    )
    assert "container_info/ffmpeg_snapshotter/availability" not in cleared
    assert (
        "homeassistant/sensor/container-info-mqtt_ffmpeg_snapshotter/started_at/config"
        not in cleared
    )
    # Fully-stale slug: every metric + availability cleared.
    assert (
        "homeassistant/sensor/container-info-mqtt_builder_x/cpu_percent/config"
        in cleared
    )
    assert "container_info/builder_x/availability" in cleared


# ---------------------------------------------------------------------------
# metric_value
# ---------------------------------------------------------------------------


def test_metric_value_cpu_percent_number_type():
    container: dict[str, Any] = {"cpu_percent": 42.5}
    result = cim.metric_value(container, "cpu_percent")
    assert result == pytest.approx(42.5)


def test_metric_value_cpu_percent_string_coerced():
    container: dict[str, Any] = {"cpu_percent": "12.3"}
    result = cim.metric_value(container, "cpu_percent")
    assert result == pytest.approx(12.3)


def test_metric_value_container_status_string_type():
    # container_status is not in METRIC_DEFS — cpuset_cpus is a string type
    container: dict[str, Any] = {"cpuset_cpus": "0-3"}
    result = cim.metric_value(container, "cpuset_cpus")
    assert result == "0-3"


def test_metric_value_cpu_shares_integer_type():
    container: dict[str, Any] = {"cpu_shares": 1024}
    result = cim.metric_value(container, "cpu_shares")
    assert result == 1024


def test_metric_value_integer_type_from_float():
    container: dict[str, Any] = {"blkio_weight": 500.9}
    result = cim.metric_value(container, "blkio_weight")
    assert result == 500


def test_metric_value_timestamp_passthrough():
    container: dict[str, Any] = {"started_at": "2026-01-15T10:30:00+00:00"}
    result = cim.metric_value(container, "started_at")
    assert result == "2026-01-15T10:30:00+00:00"


def test_started_at_metric_def_is_timestamp_and_uptime_kept():
    started = cim.METRIC_DEFS["started_at"]
    assert started["device_class"] == "timestamp"
    # timestamp sensors take no state_class / unit_of_measurement.
    assert "state_class" not in started
    assert "unit" not in started
    # uptime_seconds stays registered as an opt-in metric.
    assert "uptime_seconds" in cim.METRIC_DEFS


def test_render_metric_state_timestamp_not_floated():
    # Regression: started_at is a timestamp ISO string. The buggy publish
    # path float()'d any non-string/integer value_type, so this raised
    # ValueError and crashed the main loop on the first running container.
    mdef = cim.METRIC_DEFS["started_at"]
    state_payload, attr = cim.render_metric_state(mdef, "2026-01-15T10:30:00+00:00")
    assert state_payload == "2026-01-15T10:30:00+00:00"
    assert attr == "2026-01-15T10:30:00+00:00"


def test_render_metric_state_number_rounds_and_stringifies():
    mdef = {"value_type": "number", "round_digits": 2}
    state_payload, attr = cim.render_metric_state(mdef, 3.14159)
    assert attr == pytest.approx(3.14)
    assert state_payload == "3.14"


def test_render_metric_state_integer_coerces():
    mdef = {"value_type": "integer"}
    state_payload, attr = cim.render_metric_state(mdef, 500.9)
    assert attr == 500
    assert state_payload == "500"


def test_render_metric_state_string_passthrough():
    mdef = {"value_type": "string"}
    state_payload, attr = cim.render_metric_state(mdef, "0-3")
    assert attr == "0-3"
    assert state_payload == "0-3"


def test_render_metric_state_default_value_type_is_number():
    # No value_type key -> treated as number (the publish-path default).
    state_payload, attr = cim.render_metric_state({}, 42.0)
    assert attr == pytest.approx(42.0)
    assert state_payload == "42.0"


def test_render_metric_state_every_metric_def_renders():
    # Every registered metric must render without raising for a value of its
    # own declared type — catches a new value_type the helper does not handle.
    sample_by_type = {
        "number": 1.5,
        "integer": 7,
        "string": "x",
        "timestamp": "2026-01-15T10:30:00+00:00",
    }
    for mdef in cim.METRIC_DEFS.values():
        sample = sample_by_type[mdef.get("value_type", "number")]
        cim.render_metric_state(mdef, sample)  # must not raise


def test_metric_value_unknown_key_returns_none():
    container: dict[str, Any] = {"cpu_percent": 42.5}
    result = cim.metric_value(container, "nonexistent_metric")
    assert result is None


def test_metric_value_missing_data_returns_none():
    container: dict[str, Any] = {}
    result = cim.metric_value(container, "cpu_percent")
    assert result is None


def test_metric_value_fallback_path():
    # network_rx_total has paths: [("network", "cumulative_rx"), ("network_rx_total",)]
    # First path missing, second path present.
    container: dict[str, Any] = {"network_rx_total": 9999.0}
    result = cim.metric_value(container, "network_rx_total")
    assert result == pytest.approx(9999.0)


def test_metric_value_first_path_wins():
    # Both paths present — first one should be used.
    container: dict[str, Any] = {
        "network": {"cumulative_rx": 1111.0},
        "network_rx_total": 2222.0,
    }
    result = cim.metric_value(container, "network_rx_total")
    assert result == pytest.approx(1111.0)


# ---------------------------------------------------------------------------
# cmd_error
# ---------------------------------------------------------------------------


def test_cmd_error_stderr_preferred():
    proc = subprocess.CompletedProcess(
        args=[], returncode=1, stdout="stdout msg", stderr="stderr msg"
    )
    assert cim.cmd_error(proc) == "stderr msg"


def test_cmd_error_stdout_fallback():
    proc = subprocess.CompletedProcess(
        args=[], returncode=1, stdout="stdout msg", stderr=""
    )
    assert cim.cmd_error(proc) == "stdout msg"


def test_cmd_error_both_empty():
    proc = subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="")
    assert cim.cmd_error(proc) == ""


def test_cmd_error_none_stderr():
    proc = subprocess.CompletedProcess(
        args=[], returncode=1, stdout="fallback", stderr=None
    )
    assert cim.cmd_error(proc) == "fallback"


def test_cmd_error_strips_whitespace():
    proc = subprocess.CompletedProcess(
        args=[], returncode=1, stdout="", stderr="  error  \n"
    )
    assert cim.cmd_error(proc) == "error"


# ---------------------------------------------------------------------------
# parse_docker_timestamp
# ---------------------------------------------------------------------------


def test_parse_docker_timestamp_full_iso8601():
    result = cim.parse_docker_timestamp("2026-01-15T10:30:00.123456789Z")
    assert result is not None
    assert isinstance(result, float)
    # Verify it parses to roughly the right time (Jan 15, 2026 10:30 UTC).
    dt = datetime.fromtimestamp(result, tz=UTC)
    assert dt.year == 2026
    assert dt.month == 1
    assert dt.day == 15
    assert dt.hour == 10
    assert dt.minute == 30


def test_parse_docker_timestamp_timezone_offset():
    result = cim.parse_docker_timestamp("2026-01-15T10:30:00.123456+05:00")
    assert result is not None
    # 10:30 +05:00 = 05:30 UTC
    dt = datetime.fromtimestamp(result, tz=UTC)
    assert dt.hour == 5
    assert dt.minute == 30


def test_parse_docker_timestamp_truncated_fractional():
    result = cim.parse_docker_timestamp("2026-01-15T10:30:00.12Z")
    assert result is not None
    assert isinstance(result, float)


def test_parse_docker_timestamp_no_fractional():
    result = cim.parse_docker_timestamp("2026-01-15T10:30:00Z")
    assert result is not None
    dt = datetime.fromtimestamp(result, tz=UTC)
    assert dt.year == 2026
    assert dt.second == 0


def test_parse_docker_timestamp_no_tz():
    # No timezone suffix — treated as UTC.
    result = cim.parse_docker_timestamp("2026-01-15T10:30:00")
    assert result is not None


def test_parse_docker_timestamp_invalid_string():
    assert cim.parse_docker_timestamp("not-a-timestamp") is None


def test_parse_docker_timestamp_empty_string():
    assert cim.parse_docker_timestamp("") is None


# ---------------------------------------------------------------------------
# load_options_file
# ---------------------------------------------------------------------------


def test_load_options_file_valid_json(tmp_path):
    opts_file = tmp_path / "options.json"
    opts_file.write_text(json.dumps({"mqtt_host": "broker.local", "mqtt_port": 1883}))
    ap = argparse.ArgumentParser()
    result = cim.load_options_file(str(opts_file), ap)
    assert result["mqtt_host"] == "broker.local"
    assert result["mqtt_port"] == 1883


def test_load_options_file_nested_options_key(tmp_path):
    opts_file = tmp_path / "options.json"
    # When the payload has an "options" key and no top-level OPTION_KEYS,
    # load_options_file should unwrap to the nested dict.
    opts_file.write_text(
        json.dumps({"options": {"mqtt_host": "nested.local", "interval_seconds": 30}})
    )
    ap = argparse.ArgumentParser()
    result = cim.load_options_file(str(opts_file), ap)
    assert result["mqtt_host"] == "nested.local"
    assert result["interval_seconds"] == 30


def test_load_options_file_top_level_wins_over_nested(tmp_path):
    # If top-level keys are already OPTION_KEYS, the "options" key is NOT unwrapped.
    opts_file = tmp_path / "options.json"
    opts_file.write_text(
        json.dumps(
            {
                "mqtt_host": "top.local",
                "options": {"mqtt_host": "nested.local"},
            }
        )
    )
    ap = argparse.ArgumentParser()
    result = cim.load_options_file(str(opts_file), ap)
    assert result["mqtt_host"] == "top.local"


# ---------------------------------------------------------------------------
# MqttHealth
# ---------------------------------------------------------------------------


def test_mqtt_health_defaults():
    h = cim.MqttHealth()
    assert h.connected is False
    assert h.last_connect_ok == 0.0
    assert h.last_disconnect == 0.0


def test_mqtt_health_mutable():
    h = cim.MqttHealth()
    h.connected = True
    h.last_connect_ok = 123.456
    h.last_disconnect = 789.0
    assert h.connected is True
    assert h.last_connect_ok == 123.456
    assert h.last_disconnect == 789.0


# ---------------------------------------------------------------------------
# publish_discovery
# ---------------------------------------------------------------------------


def test_publish_discovery_basic_payload():
    client = _RecordingClient()
    metric_def = cim.METRIC_DEFS["cpu_percent"]
    cim.publish_discovery(
        client,  # type: ignore[arg-type]
        "homeassistant",
        "container-info-mqtt",
        "container_info",
        "my_app",
        "My App",
        "cpu_percent",
        metric_def,
        120,
    )
    assert len(client.published) == 1
    topic, payload_str, qos, retain = client.published[0]
    assert topic == "homeassistant/sensor/container-info-mqtt_my_app/cpu_percent/config"
    assert qos == 1
    assert retain is True

    payload = json.loads(payload_str)
    assert payload["name"] == "CPU Usage"
    assert payload["has_entity_name"] is True
    assert payload["unique_id"] == "container-info-mqtt_my_app_cpu_percent"
    assert payload["state_topic"] == "container_info/my_app/cpu_percent/state"
    assert payload["availability_topic"] == "container_info/my_app/availability"
    assert payload["payload_available"] == "online"
    assert payload["payload_not_available"] == "offline"
    assert payload["expire_after"] == 120
    assert payload["unit_of_measurement"] == "%"
    assert payload["icon"] == "mdi:chip"
    assert payload["state_class"] == "measurement"
    assert payload["suggested_display_precision"] == 1
    assert payload["device"]["identifiers"] == ["container-info-mqtt_my_app"]
    assert payload["device"]["name"] == "Container My App"
    assert payload["device"]["manufacturer"] == "Docker"
    assert payload["device"]["model"] == "Container"


def test_publish_discovery_no_optional_fields():
    """Metric def without unit/icon/device_class/state_class omits those keys."""
    client = _RecordingClient()
    minimal_def: dict[str, Any] = {"name": "Custom", "paths": [("custom",)]}
    cim.publish_discovery(
        client,  # type: ignore[arg-type]
        "ha",
        "dev",
        "base",
        "slug",
        "Slug",
        "custom",
        minimal_def,
        60,
    )
    payload = json.loads(client.published[0][1])
    assert "unit_of_measurement" not in payload
    assert "icon" not in payload
    assert "device_class" not in payload
    assert "state_class" not in payload
    assert "suggested_display_precision" not in payload


def test_publish_discovery_expire_after_minimum_clamp():
    """expire_after is clamped to at least 5."""
    client = _RecordingClient()
    metric_def = {"name": "Test", "paths": [("t",)]}
    cim.publish_discovery(
        client,  # type: ignore[arg-type]
        "ha",
        "dev",
        "base",
        "slug",
        "Slug",
        "t",
        metric_def,
        1,
    )
    payload = json.loads(client.published[0][1])
    assert payload["expire_after"] == 5


def test_publish_discovery_with_device_class():
    """Metric def with device_class includes it in the payload."""
    client = _RecordingClient()
    metric_def = cim.METRIC_DEFS["memory_usage"]
    cim.publish_discovery(
        client,  # type: ignore[arg-type]
        "ha",
        "dev",
        "base",
        "mem_slug",
        "Mem",
        "memory_usage",
        metric_def,
        60,
    )
    payload = json.loads(client.published[0][1])
    assert payload["device_class"] == "data_size"


# ---------------------------------------------------------------------------
# publish_summary_discovery
# ---------------------------------------------------------------------------


def test_publish_summary_discovery_payload():
    client = _RecordingClient()
    cim.publish_summary_discovery(
        client,  # type: ignore[arg-type]
        "homeassistant",
        "container-info-mqtt",
        "container_info",
        "my_app",
        "My App",
        120,
    )
    assert len(client.published) == 1
    topic, payload_str, qos, retain = client.published[0]
    assert topic == "homeassistant/sensor/container-info-mqtt_my_app/summary/config"
    assert qos == 1
    assert retain is True

    payload = json.loads(payload_str)
    assert payload["name"] == "Summary"
    assert payload["has_entity_name"] is True
    assert payload["unique_id"] == "container-info-mqtt_my_app_summary"
    assert payload["default_entity_id"] == "sensor.container_my_app_summary"
    assert payload["state_topic"] == "container_info/my_app/summary/state"
    assert (
        payload["json_attributes_topic"] == "container_info/my_app/summary/attributes"
    )
    assert payload["availability_topic"] == "container_info/my_app/availability"
    assert payload["expire_after"] == 120
    assert payload["icon"] == "mdi:table"
    assert payload["device"]["identifiers"] == ["container-info-mqtt_my_app"]
    assert payload["device"]["name"] == "Container My App"


def test_publish_summary_discovery_expire_clamp():
    client = _RecordingClient()
    cim.publish_summary_discovery(
        client,  # type: ignore[arg-type]
        "ha",
        "dev",
        "base",
        "slug",
        "Slug",
        2,
    )
    payload = json.loads(client.published[0][1])
    assert payload["expire_after"] == 5


# ---------------------------------------------------------------------------
# clear_discovery
# ---------------------------------------------------------------------------


def test_clear_discovery_publishes_empty_retained():
    client = _RecordingClient()
    cim.clear_discovery(
        client,  # type: ignore[arg-type]
        "homeassistant",
        "container-info-mqtt",
        "my_app",
        "cpu_percent",
    )
    assert len(client.published) == 1
    topic, payload, qos, retain = client.published[0]
    assert topic == "homeassistant/sensor/container-info-mqtt_my_app/cpu_percent/config"
    assert payload == ""
    assert qos == 1
    assert retain is True


def test_clear_discovery_different_metric():
    client = _RecordingClient()
    cim.clear_discovery(client, "ha", "dev", "slug", "summary")  # type: ignore[arg-type]
    topic = client.published[0][0]
    assert topic == "ha/sensor/dev_slug/summary/config"


# ---------------------------------------------------------------------------
# compute_rate_metrics — additional edge cases
# ---------------------------------------------------------------------------


def test_compute_rate_metrics_dt_zero_no_rates():
    """When timestamps are identical (dt == 0), no rates are computed."""
    totals: dict[str, dict[str, float]] = {}
    c1: dict[str, Any] = {"network_rx_total": 1000, "network_tx_total": 200}
    cim.compute_rate_metrics("s", c1, 1000.0, totals)
    c2: dict[str, Any] = {"network_rx_total": 2000, "network_tx_total": 400}
    rates = cim.compute_rate_metrics("s", c2, 1000.0, totals)  # same ts
    assert rates == {}


def test_compute_rate_metrics_no_totals_clears_entry():
    """When container has no total metrics, slug is removed from totals dict."""
    totals: dict[str, dict[str, float]] = {}
    c1: dict[str, Any] = {"network_rx_total": 1000}
    cim.compute_rate_metrics("s", c1, 1000.0, totals)
    assert "s" in totals

    c2: dict[str, Any] = {}  # no totals at all
    cim.compute_rate_metrics("s", c2, 1010.0, totals)
    assert "s" not in totals


def test_compute_rate_metrics_partial_metrics():
    """Only metrics present in both calls produce rates."""
    totals: dict[str, dict[str, float]] = {}
    c1: dict[str, Any] = {"network_rx_total": 1000, "io_read_total": 500}
    cim.compute_rate_metrics("s", c1, 100.0, totals)
    # Second call only has network_rx_total
    c2: dict[str, Any] = {"network_rx_total": 2000}
    rates = cim.compute_rate_metrics("s", c2, 110.0, totals)
    assert "network_rx_rate" in rates
    assert rates["network_rx_rate"] == pytest.approx(100.0)
    assert "io_read_rate" not in rates


# ---------------------------------------------------------------------------
# redact_options_for_log — additional edge cases
# ---------------------------------------------------------------------------


def test_redact_options_whitespace_only_password_not_hidden():
    opts: dict[str, Any] = {"mqtt_password": "   "}
    redacted = cim.redact_options_for_log(opts)
    assert redacted["mqtt_password"] == "   "


def test_redact_options_no_keys():
    assert cim.redact_options_for_log({}) == {}


def test_redact_options_original_not_mutated():
    opts: dict[str, Any] = {"mqtt_password": "secret", "mqtt_host": "broker"}
    redacted = cim.redact_options_for_log(opts)
    assert opts["mqtt_password"] == "secret"
    assert redacted["mqtt_password"] == "***"


# ---------------------------------------------------------------------------
# fetch_stats_by_id (sync wrapper)
# ---------------------------------------------------------------------------


def test_fetch_stats_by_id_empty_list():
    result = cim.fetch_stats_by_id([], 10, _LOG)
    assert result == {}


def test_fetch_stats_by_id_delegates_to_async():
    """Mock the async helper and verify the sync wrapper calls it."""
    fake_result = {"abc123": {"cpu_percent": 42.0}}
    with patch(
        "container_info_mqtt.docker._fetch_stats_by_id_async",
        return_value=fake_result,
    ) as mock_async:
        result = cim.fetch_stats_by_id(["abc123"], 10, _LOG)
    assert result == fake_result
    mock_async.assert_called_once_with(["abc123"], 10, _LOG)


# ---------------------------------------------------------------------------
# _fetch_stats_by_id_async
# ---------------------------------------------------------------------------


def test_fetch_stats_by_id_async_empty():
    import asyncio

    result = asyncio.run(cim._fetch_stats_by_id_async([], 10, _LOG))
    assert result == {}


def test_fetch_stats_by_id_async_single_container():
    import asyncio

    fake_payload = {
        "cpu_stats": {
            "cpu_usage": {"total_usage": 200_000_000},
            "system_cpu_usage": 2_000_000_000,
            "online_cpus": 2,
        },
        "precpu_stats": {
            "cpu_usage": {"total_usage": 100_000_000},
            "system_cpu_usage": 1_000_000_000,
        },
        "memory_stats": {"usage": 1048576},
        "networks": {"eth0": {"rx_bytes": 500, "tx_bytes": 100}},
    }
    with patch(
        "container_info_mqtt.docker.docker_api_get_json", return_value=fake_payload
    ):
        result = asyncio.run(cim._fetch_stats_by_id_async(["abc123"], 10, _LOG))
    assert "abc123" in result
    assert result["abc123"]["cpu_percent"] == pytest.approx(20.0)
    assert result["abc123"]["memory_usage"] == 1048576
    assert result["abc123"]["network_rx_total"] == pytest.approx(500.0)
    assert result["abc123"]["network_tx_total"] == pytest.approx(100.0)


def test_fetch_stats_by_id_async_exception_skips():
    import asyncio

    with patch(
        "container_info_mqtt.docker.docker_api_get_json",
        side_effect=RuntimeError("fail"),
    ):
        result = asyncio.run(cim._fetch_stats_by_id_async(["abc123"], 10, _LOG))
    assert result == {}


def test_fetch_stats_by_id_async_bad_payload_type():
    import asyncio

    with patch(
        "container_info_mqtt.docker.docker_api_get_json", return_value="not a dict"
    ):
        result = asyncio.run(cim._fetch_stats_by_id_async(["abc123"], 10, _LOG))
    assert result == {}


# ---------------------------------------------------------------------------
# fetch_inspect_by_id
# ---------------------------------------------------------------------------


def test_fetch_inspect_by_id_empty_list():
    result = cim.fetch_inspect_by_id([], 10, _LOG)
    assert result == {}


def test_fetch_inspect_by_id_parses_state_and_host_config():
    inspect_payload = json.dumps(
        [
            {
                "Id": "abc123",
                "State": {
                    "Status": "running",
                    "StartedAt": "2026-01-15T10:30:00.000000000Z",
                },
                "HostConfig": {
                    "CpusetCpus": "0-3",
                    "CpuShares": 1024,
                    "BlkioWeight": 500,
                },
            }
        ]
    )
    proc = subprocess.CompletedProcess(
        args=[], returncode=0, stdout=inspect_payload, stderr=""
    )
    with patch("container_info_mqtt.docker.run_cmd", return_value=proc):
        result = cim.fetch_inspect_by_id(["abc123"], 10, _LOG)
    assert "abc123" in result
    info = result["abc123"]
    assert info["status"] == "running"
    assert info["cpuset_cpus"] == "0-3"
    assert info["cpu_shares"] == 1024
    assert info["blkio_weight"] == 500
    assert isinstance(info["started_at"], float)


def test_fetch_inspect_by_id_failure_raises():
    proc = subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="error")
    with (
        patch("container_info_mqtt.docker.run_cmd", return_value=proc),
        pytest.raises(RuntimeError, match="docker inspect failed"),
    ):
        cim.fetch_inspect_by_id(["abc123"], 10, _LOG)


def test_fetch_inspect_by_id_invalid_json_raises():
    proc = subprocess.CompletedProcess(
        args=[], returncode=0, stdout="not json", stderr=""
    )
    with (
        patch("container_info_mqtt.docker.run_cmd", return_value=proc),
        pytest.raises(RuntimeError, match="invalid JSON"),
    ):
        cim.fetch_inspect_by_id(["abc123"], 10, _LOG)


def test_fetch_inspect_by_id_non_list_raises():
    proc = subprocess.CompletedProcess(
        args=[], returncode=0, stdout='{"not": "a list"}', stderr=""
    )
    with (
        patch("container_info_mqtt.docker.run_cmd", return_value=proc),
        pytest.raises(TypeError, match="unexpected payload"),
    ):
        cim.fetch_inspect_by_id(["abc123"], 10, _LOG)


# ---------------------------------------------------------------------------
# run_cmd
# ---------------------------------------------------------------------------


def test_run_cmd_file_not_found_raises():
    with pytest.raises(RuntimeError, match="command not found"):
        cim.run_cmd(["__nonexistent_binary__"], 5)


def test_run_cmd_timeout_raises():
    with pytest.raises(RuntimeError, match="timed out"):
        cim.run_cmd(["sleep", "60"], 1)


# ---------------------------------------------------------------------------
# UnixSocketHTTPConnection
# ---------------------------------------------------------------------------


def test_unix_socket_http_connection_min_timeout():
    conn = cim.UnixSocketHTTPConnection("/tmp/test.sock", 0)
    assert conn.timeout == 1  # clamped to at least 1


# ---------------------------------------------------------------------------
# fetch_ps_containers — additional edge cases
# ---------------------------------------------------------------------------


def test_fetch_ps_empty_output():
    proc = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
    with patch("container_info_mqtt.docker.run_cmd", return_value=proc):
        result = cim.fetch_ps_containers(10, _LOG)
    assert result == []


def test_fetch_ps_skips_invalid_json_lines():
    stdout = "not json\n" + json.dumps(
        {"ID": "abc", "Names": "good", "Status": "Up", "State": "running"}
    )
    proc = subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr="")
    with patch("container_info_mqtt.docker.run_cmd", return_value=proc):
        result = cim.fetch_ps_containers(10, _LOG)
    assert len(result) == 1
    assert result[0]["name"] == "good"


def test_fetch_ps_failure_raises():
    proc = subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="error")
    with (
        patch("container_info_mqtt.docker.run_cmd", return_value=proc),
        pytest.raises(RuntimeError, match="docker ps failed"),
    ):
        cim.fetch_ps_containers(10, _LOG)


def test_fetch_ps_skips_missing_id():
    row = json.dumps({"Names": "noname", "Status": "Up", "State": "running"})
    proc = subprocess.CompletedProcess(args=[], returncode=0, stdout=row, stderr="")
    with patch("container_info_mqtt.docker.run_cmd", return_value=proc):
        result = cim.fetch_ps_containers(10, _LOG)
    assert result == []


# ---------------------------------------------------------------------------
# clear_container_discovery
# ---------------------------------------------------------------------------


def test_clear_container_discovery_clears_metrics_summary_and_availability():
    client = _RecordingClient()
    cim.clear_container_discovery(
        client,  # type: ignore[arg-type]
        "homeassistant",
        "container_info",
        "container-info-mqtt",
        "clever_buck",
        {"cpu_percent", "memory_usage"},
        clear_summary=True,
        log=_LOG,
    )
    topics = {t for (t, _, _, _) in client.published}
    assert (
        "homeassistant/sensor/container-info-mqtt_clever_buck/cpu_percent/config"
        in topics
    )
    assert (
        "homeassistant/sensor/container-info-mqtt_clever_buck/memory_usage/config"
        in topics
    )
    assert (
        "homeassistant/sensor/container-info-mqtt_clever_buck/summary/config" in topics
    )
    assert "container_info/clever_buck/availability" in topics
    # Everything cleared as empty + retained.
    for _t, payload, qos, retain in client.published:
        assert payload == ""
        assert qos == 1
        assert retain is True


def test_clear_container_discovery_without_summary_skips_summary_config():
    client = _RecordingClient()
    cim.clear_container_discovery(
        client,  # type: ignore[arg-type]
        "homeassistant",
        "container_info",
        "container-info-mqtt",
        "clever_buck",
        {"cpu_percent"},
        clear_summary=False,
        log=_LOG,
    )
    topics = [t for (t, _, _, _) in client.published]
    assert (
        "homeassistant/sensor/container-info-mqtt_clever_buck/summary/config"
        not in topics
    )
    assert "container_info/clever_buck/availability" in topics


# ---------------------------------------------------------------------------
# due_for_removal (debounced removal decision)
# ---------------------------------------------------------------------------


def test_due_for_removal_newly_absent_starts_clock_not_due():
    absent: dict[str, float] = {}
    due = cim.due_for_removal(["gone"], set(), absent, now=100.0, grace_seconds=300.0)
    assert due == set()
    assert absent == {"gone": 100.0}  # clock started, nothing cleared yet


def test_due_for_removal_absent_within_grace_not_due():
    absent = {"gone": 100.0}
    due = cim.due_for_removal(["gone"], set(), absent, now=350.0, grace_seconds=300.0)
    assert due == set()  # only 250s < 300s grace


def test_due_for_removal_absent_past_grace_is_due():
    absent = {"gone": 100.0}
    due = cim.due_for_removal(["gone"], set(), absent, now=401.0, grace_seconds=300.0)
    assert due == {"gone"}  # 301s >= grace


def test_due_for_removal_present_slug_never_due_and_clock_cleared():
    absent = {"running": 100.0}
    due = cim.due_for_removal(
        ["running"], {"running"}, absent, now=9999.0, grace_seconds=300.0
    )
    assert due == set()
    assert absent == {}  # reappearing clears the clock


def test_due_for_removal_restart_before_grace_resets_clock_no_churn():
    # A container that vanishes briefly (restart) and returns before the grace
    # must never be reported as removed, and its clock must reset.
    absent: dict[str, float] = {}
    # poll 1: absent
    assert cim.due_for_removal(["c"], set(), absent, 100.0, 300.0) == set()
    # poll 2: back (restart finished) -> clock cleared
    assert cim.due_for_removal(["c"], {"c"}, absent, 160.0, 300.0) == set()
    assert absent == {}
    # poll 3: absent again -> a fresh clock, still not due despite >300s elapsed
    assert cim.due_for_removal(["c"], set(), absent, 500.0, 300.0) == set()
    assert absent == {"c": 500.0}
