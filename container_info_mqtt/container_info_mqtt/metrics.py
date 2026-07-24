# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Metric definitions + per-metric value extraction + rate/aggregate helpers."""

import logging
import re
from datetime import datetime
from typing import Any

from .util import deep_get, safe_float, safe_int, safe_text

METRIC_DEFS: dict[str, dict[str, Any]] = {
    "cpu_percent": {
        "paths": [("cpu_percent",)],
        "name": "CPU Usage",
        "unit": "%",
        "icon": "mdi:chip",
        "state_class": "measurement",
        "suggested_display_precision": 1,
        "round_digits": 1,
    },
    "memory_usage": {
        "paths": [("memory_usage",)],
        "name": "Memory Used",
        "unit": "B",
        "icon": "mdi:memory",
        "device_class": "data_size",
        "state_class": "measurement",
    },
    "network_rx_total": {
        "paths": [("network", "cumulative_rx"), ("network_rx_total",)],
        "name": "Network RX Total",
        "unit": "B",
        "icon": "mdi:download",
        "device_class": "data_size",
        "state_class": "total_increasing",
    },
    "network_tx_total": {
        "paths": [("network", "cumulative_tx"), ("network_tx_total",)],
        "name": "Network TX Total",
        "unit": "B",
        "icon": "mdi:upload",
        "device_class": "data_size",
        "state_class": "total_increasing",
    },
    "io_read_total": {
        "paths": [("io", "cumulative_ior"), ("io_read_total",)],
        "name": "Disk Read Total",
        "unit": "B",
        "icon": "mdi:harddisk",
        "device_class": "data_size",
        "state_class": "total_increasing",
    },
    "io_write_total": {
        "paths": [("io", "cumulative_iow"), ("io_write_total",)],
        "name": "Disk Write Total",
        "unit": "B",
        "icon": "mdi:harddisk",
        "device_class": "data_size",
        "state_class": "total_increasing",
    },
    "network_rx_rate": {
        "name": "Network RX Rate",
        "unit": "B/s",
        "icon": "mdi:download",
        "device_class": "data_rate",
        "state_class": "measurement",
        "suggested_display_precision": 0,
        "round_digits": 0,
    },
    "network_tx_rate": {
        "name": "Network TX Rate",
        "unit": "B/s",
        "icon": "mdi:upload",
        "device_class": "data_rate",
        "state_class": "measurement",
        "suggested_display_precision": 0,
        "round_digits": 0,
    },
    "io_read_rate": {
        "name": "Disk Read Rate",
        "unit": "B/s",
        "icon": "mdi:harddisk",
        "device_class": "data_rate",
        "state_class": "measurement",
        "suggested_display_precision": 0,
        "round_digits": 0,
    },
    "io_write_rate": {
        "name": "Disk Write Rate",
        "unit": "B/s",
        "icon": "mdi:harddisk",
        "device_class": "data_rate",
        "state_class": "measurement",
        "suggested_display_precision": 0,
        "round_digits": 0,
    },
    "cpuset_cpus": {
        "paths": [("cpuset_cpus",)],
        "name": "CPU Set",
        "icon": "mdi:cpu-64-bit",
        "value_type": "string",
    },
    "cpu_shares": {
        "paths": [("cpu_shares",)],
        "name": "CPU Shares",
        "icon": "mdi:scale-balance",
        "value_type": "integer",
    },
    "blkio_weight": {
        "paths": [("blkio_weight",)],
        "name": "Block I/O Weight",
        "icon": "mdi:harddisk-plus",
        "value_type": "integer",
    },
    "uptime_seconds": {
        "paths": [("uptime_seconds",)],
        "name": "Uptime",
        "unit": "s",
        "icon": "mdi:timer-outline",
        "device_class": "duration",
        "state_class": "measurement",
        "suggested_display_precision": 0,
    },
    # Fixed container start time as an ISO 8601 timestamp. Unlike
    # ``uptime_seconds`` (which changes every poll and churns the HA
    # recorder), this only changes on a restart, while HA still renders a
    # ``timestamp`` sensor as relative time ("3 days ago").
    "started_at": {
        "paths": [("started_at",)],
        "name": "Started",
        "icon": "mdi:clock-start",
        "device_class": "timestamp",
        "value_type": "timestamp",
    },
}

RATE_SOURCE_METRICS: dict[str, str] = {
    "network_rx_rate": "network_rx_total",
    "network_tx_rate": "network_tx_total",
    "io_read_rate": "io_read_total",
    "io_write_rate": "io_write_total",
}

RATE_METRICS: tuple[str, ...] = tuple(RATE_SOURCE_METRICS.keys())


def metric_value(container: dict[str, Any], metric_key: str) -> Any:
    metric_def = METRIC_DEFS.get(metric_key)
    if metric_def is None:
        return None

    value_type = metric_def.get("value_type", "number")
    for path in metric_def["paths"]:
        raw_value = deep_get(container, path)
        if value_type in ("string", "timestamp"):
            text_value = safe_text(raw_value)
            if text_value is not None:
                return text_value
        elif value_type == "integer":
            int_value = safe_int(raw_value)
            if int_value is not None:
                return int_value
        else:
            number_value = safe_float(raw_value)
            if number_value is not None:
                return number_value
    return None


def render_metric_state(metric_def: dict[str, Any], value: Any) -> tuple[str, Any]:
    """Convert a raw metric value into ``(state_payload, attribute_value)``.

    ``state_payload`` is the string published to the MQTT state topic;
    ``attribute_value`` is the typed value embedded in the summary JSON
    attributes. Both publish call sites go through here so the value_type
    handling can never drift between them (a ``timestamp`` value must not be
    coerced with ``float()``).
    """
    value_type = metric_def.get("value_type", "number")
    if value_type in ("string", "timestamp"):
        text = str(value)
        return text, text
    if value_type == "integer":
        int_value = int(value)
        return str(int_value), int_value
    numeric_value = float(value)
    round_digits = metric_def.get("round_digits")
    if isinstance(round_digits, int):
        numeric_value = round(numeric_value, round_digits)
    return str(numeric_value), numeric_value


def compute_rate_metrics(
    container_slug: str,
    container: dict[str, Any],
    now: float,
    last_totals_by_container: dict[str, dict[str, float]],
) -> dict[str, float]:
    current_totals: dict[str, float] = {}
    for total_metric in RATE_SOURCE_METRICS.values():
        total_value = metric_value(container, total_metric)
        if total_value is not None:
            current_totals[total_metric] = float(total_value)

    previous = last_totals_by_container.get(container_slug)
    rate_values: dict[str, float] = {}

    if previous is not None:
        dt = now - previous.get("_ts", now)
        if dt > 0:
            for rate_metric, total_metric in RATE_SOURCE_METRICS.items():
                current_total = current_totals.get(total_metric)
                previous_total = previous.get(total_metric)
                if current_total is None or previous_total is None:
                    continue
                if current_total < previous_total:
                    continue
                rate_values[rate_metric] = (current_total - previous_total) / dt

    if current_totals:
        snapshot = {"_ts": now}
        snapshot.update(current_totals)
        last_totals_by_container[container_slug] = snapshot
    else:
        last_totals_by_container.pop(container_slug, None)

    return rate_values


def parse_include_metrics(raw: str, log: logging.Logger) -> list[str]:
    wanted = [item.strip() for item in raw.split(",") if item.strip()]
    selected: list[str] = []

    for metric in wanted:
        if metric in METRIC_DEFS:
            selected.append(metric)
        else:
            log.warning("Unknown metric key in include_metrics: %s", metric)

    if not selected:
        selected = list(METRIC_DEFS.keys())
    return selected


def cpu_percent_from_stats(payload: dict[str, Any]) -> float | None:
    cpu_stats = payload.get("cpu_stats")
    pre_cpu_stats = payload.get("precpu_stats")
    if not isinstance(cpu_stats, dict) or not isinstance(pre_cpu_stats, dict):
        return None

    cpu_usage = cpu_stats.get("cpu_usage")
    pre_cpu_usage = pre_cpu_stats.get("cpu_usage")
    if not isinstance(cpu_usage, dict) or not isinstance(pre_cpu_usage, dict):
        return None

    total_usage = safe_float(cpu_usage.get("total_usage"))
    pre_total_usage = safe_float(pre_cpu_usage.get("total_usage"))
    system_usage = safe_float(cpu_stats.get("system_cpu_usage"))
    pre_system_usage = safe_float(pre_cpu_stats.get("system_cpu_usage"))
    if (
        total_usage is None
        or pre_total_usage is None
        or system_usage is None
        or pre_system_usage is None
    ):
        return None

    cpu_delta = total_usage - pre_total_usage
    system_delta = system_usage - pre_system_usage
    if system_delta <= 0:
        return None
    if cpu_delta <= 0:
        return 0.0

    online_cpus = safe_int(cpu_stats.get("online_cpus"))
    if online_cpus is None or online_cpus <= 0:
        per_cpu = cpu_usage.get("percpu_usage")
        if isinstance(per_cpu, list) and per_cpu:
            online_cpus = len(per_cpu)
        else:
            online_cpus = 1

    return (cpu_delta / system_delta) * float(online_cpus) * 100.0


def sum_network_totals(
    payload: dict[str, Any],
) -> tuple[float | None, float | None]:
    networks = payload.get("networks")
    if not isinstance(networks, dict):
        return None, None

    rx_total = 0.0
    tx_total = 0.0
    saw_rx = False
    saw_tx = False

    for iface_data in networks.values():
        if not isinstance(iface_data, dict):
            continue

        rx_value = safe_float(iface_data.get("rx_bytes"))
        if rx_value is not None:
            saw_rx = True
            rx_total += max(0.0, rx_value)

        tx_value = safe_float(iface_data.get("tx_bytes"))
        if tx_value is not None:
            saw_tx = True
            tx_total += max(0.0, tx_value)

    return (rx_total if saw_rx else None, tx_total if saw_tx else None)


def sum_blkio_totals(
    payload: dict[str, Any],
) -> tuple[float | None, float | None]:
    blkio_stats = payload.get("blkio_stats")
    if not isinstance(blkio_stats, dict):
        return None, None

    records = blkio_stats.get("io_service_bytes_recursive")
    if not isinstance(records, list):
        return None, None

    read_total = 0.0
    write_total = 0.0
    saw_read = False
    saw_write = False

    for record in records:
        if not isinstance(record, dict):
            continue

        op = safe_text(record.get("op"))
        value = safe_float(record.get("value"))
        if op is None or value is None:
            continue

        op_norm = op.lower()
        if op_norm == "read":
            saw_read = True
            read_total += max(0.0, value)
        elif op_norm == "write":
            saw_write = True
            write_total += max(0.0, value)

    return (read_total if saw_read else None, write_total if saw_write else None)


def parse_docker_timestamp(ts_str: str) -> float | None:
    """Parse a Docker ISO 8601 timestamp (nanosecond precision) to a Unix timestamp."""
    m = re.match(
        r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d+)?(Z|[+-]\d{2}:\d{2})?$",
        ts_str,
    )
    if not m:
        return None
    base, frac, tz = m.group(1), m.group(2), m.group(3)
    if frac:
        frac = frac[:7]  # truncate nanoseconds to microseconds
    tz_part = "+00:00" if (tz is None or tz == "Z") else tz
    try:
        dt = datetime.fromisoformat(base + (frac or "") + tz_part)
        return dt.timestamp()
    except ValueError:
        return None
