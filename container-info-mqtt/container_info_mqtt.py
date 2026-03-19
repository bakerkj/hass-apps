# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import logging
import re
import socket
import subprocess
import time
from urllib.parse import quote
from typing import Any, Optional

import paho.mqtt.client as mqtt


METRIC_DEFS: dict[str, dict[str, Any]] = {
    "cpu_percent": {
        "paths": [("cpu_percent",)],
        "name": "CPU Usage",
        "unit": "%",
        "icon": "mdi:chip",
        "state_class": "measurement",
        "suggested_display_precision": 1,
        "round_digits": 3,
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
        "round_digits": 3,
    },
    "network_tx_rate": {
        "name": "Network TX Rate",
        "unit": "B/s",
        "icon": "mdi:upload",
        "device_class": "data_rate",
        "state_class": "measurement",
        "suggested_display_precision": 0,
        "round_digits": 3,
    },
    "io_read_rate": {
        "name": "Disk Read Rate",
        "unit": "B/s",
        "icon": "mdi:harddisk",
        "device_class": "data_rate",
        "state_class": "measurement",
        "suggested_display_precision": 0,
        "round_digits": 3,
    },
    "io_write_rate": {
        "name": "Disk Write Rate",
        "unit": "B/s",
        "icon": "mdi:harddisk",
        "device_class": "data_rate",
        "state_class": "measurement",
        "suggested_display_precision": 0,
        "round_digits": 3,
    },
    "status": {
        "paths": [("status",), ("state",)],
        "name": "Status",
        "icon": "mdi:information-outline",
        "value_type": "string",
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
}

RATE_SOURCE_METRICS: dict[str, str] = {
    "network_rx_rate": "network_rx_total",
    "network_tx_rate": "network_tx_total",
    "io_read_rate": "io_read_total",
    "io_write_rate": "io_write_total",
}

RATE_METRICS: tuple[str, ...] = tuple(RATE_SOURCE_METRICS.keys())

OPTION_KEYS: set[str] = {
    "interval_seconds",
    "docker_timeout_seconds",
    "include_metrics",
    "container_include_regex",
    "container_exclude_regex",
    "mqtt_host",
    "mqtt_port",
    "mqtt_username",
    "mqtt_password",
    "mqtt_discovery_prefix",
    "mqtt_base_topic",
    "client_id",
    "log_level",
    "heartbeat_interval_seconds",
}

SENSITIVE_OPTION_KEYS: set[str] = {"mqtt_password"}

DOCKER_SOCKET_PATH = "/var/run/docker.sock"


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_\-]+", "_", value.strip().lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    return slug or "unknown"


def container_display_name(value: str) -> str:
    display = value.strip()
    if not display:
        return "Unknown"

    if display.lower().startswith("addon_"):
        display = display[6:]
        parts = display.split("_", 1)
        if len(parts) == 2 and re.fullmatch(r"[0-9a-f]+", parts[0], re.IGNORECASE):
            display = parts[1]

    display = re.sub(r"[_\-]+", " ", display)
    display = re.sub(r"\s+", " ", display).strip()
    if not display:
        return "Unknown"

    return " ".join(part.capitalize() for part in display.split(" "))


def safe_float(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def safe_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(float(text))
        except ValueError:
            return None
    return None


def safe_text(value: Any) -> Optional[str]:
    if value is None or isinstance(value, (dict, list)):
        return None
    text = str(value).strip()
    return text or None


def deep_get(container: dict[str, Any], path: tuple[str, ...]) -> Any:
    cur: Any = container
    for part in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
        if cur is None:
            return None
    return cur


def metric_value(container: dict[str, Any], metric_key: str) -> Optional[Any]:
    metric_def = METRIC_DEFS.get(metric_key)
    if metric_def is None:
        return None

    value_type = metric_def.get("value_type", "number")
    for path in metric_def["paths"]:
        raw_value = deep_get(container, path)
        if value_type == "string":
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


def cmd_error(proc: subprocess.CompletedProcess[str]) -> str:
    return (proc.stderr or proc.stdout or "").strip()


def run_cmd(cmd: list[str], timeout_seconds: int) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            timeout=max(1, timeout_seconds),
        )
    except FileNotFoundError as exc:
        raise RuntimeError(f"command not found: {cmd[0]}") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"command timed out after {timeout_seconds}s: {' '.join(cmd)}"
        ) from exc


def fetch_ps_containers(
    docker_timeout_seconds: int,
    log: logging.Logger,
) -> list[dict[str, str]]:
    cmd = ["docker", "ps", "-a", "--no-trunc", "--format", "{{json .}}"]
    proc = run_cmd(cmd, docker_timeout_seconds)
    if proc.returncode != 0:
        raise RuntimeError(f"docker ps failed: {cmd_error(proc)}")

    containers: list[dict[str, str]] = []
    for line in proc.stdout.splitlines():
        text = line.strip()
        if not text:
            continue

        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            log.warning("Skipping unparsable docker ps line: %s", text)
            continue

        if not isinstance(payload, dict):
            continue

        container_id = safe_text(payload.get("ID") or payload.get("Id"))
        name = safe_text(payload.get("Names") or payload.get("Name"))
        if container_id is None or name is None:
            continue

        status_text = safe_text(payload.get("Status")) or ""
        state = safe_text(payload.get("State")) or ""
        containers.append(
            {
                "id": container_id,
                "name": name,
                "status_text": status_text,
                "state": state,
            }
        )

    return containers


class UnixSocketHTTPConnection(http.client.HTTPConnection):
    def __init__(self, unix_socket_path: str, timeout_seconds: int):
        super().__init__("localhost", timeout=max(1, timeout_seconds))
        self.unix_socket_path = unix_socket_path

    def connect(self) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect(self.unix_socket_path)
        self.sock = sock


def docker_api_get_json(path: str, docker_timeout_seconds: int) -> Any:
    conn = UnixSocketHTTPConnection(DOCKER_SOCKET_PATH, docker_timeout_seconds)
    try:
        conn.request("GET", path, headers={"Host": "localhost"})
        response = conn.getresponse()
        body = response.read()
    except OSError as exc:
        raise RuntimeError(
            f"docker engine API request failed for {path}: {exc}"
        ) from exc
    finally:
        conn.close()

    if response.status >= 400:
        error_text = body.decode("utf-8", errors="replace").strip()
        raise RuntimeError(
            f"docker engine API {path} failed: HTTP {response.status} "
            f"{response.reason}: {error_text}"
        )

    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"docker engine API {path} returned invalid JSON: {exc}"
        ) from exc


def cpu_percent_from_stats(payload: dict[str, Any]) -> Optional[float]:
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
    if cpu_delta <= 0 or system_delta <= 0:
        return None

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
) -> tuple[Optional[float], Optional[float]]:
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
) -> tuple[Optional[float], Optional[float]]:
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


def fetch_stats_by_id(
    container_ids: list[str],
    docker_timeout_seconds: int,
    log: logging.Logger,
) -> dict[str, dict[str, float]]:
    stats_by_id: dict[str, dict[str, float]] = {}

    for container_id in container_ids:
        endpoint = f"/containers/{quote(container_id, safe='')}/stats?stream=false"
        try:
            payload = docker_api_get_json(endpoint, docker_timeout_seconds)
        except Exception as exc:
            log.warning("Skipping docker stats for %s: %s", container_id[:12], exc)
            continue

        if not isinstance(payload, dict):
            log.warning(
                "Skipping docker stats for %s: unexpected payload type",
                container_id[:12],
            )
            continue

        container_stats: dict[str, float] = {}

        cpu_percent = cpu_percent_from_stats(payload)
        if cpu_percent is not None:
            container_stats["cpu_percent"] = cpu_percent

        memory_stats = payload.get("memory_stats")
        if isinstance(memory_stats, dict):
            memory_usage = safe_float(memory_stats.get("usage"))
            if memory_usage is not None:
                container_stats["memory_usage"] = memory_usage

        network_rx_total, network_tx_total = sum_network_totals(payload)
        if network_rx_total is not None:
            container_stats["network_rx_total"] = network_rx_total
        if network_tx_total is not None:
            container_stats["network_tx_total"] = network_tx_total

        io_read_total, io_write_total = sum_blkio_totals(payload)
        if io_read_total is not None:
            container_stats["io_read_total"] = io_read_total
        if io_write_total is not None:
            container_stats["io_write_total"] = io_write_total

        stats_by_id[container_id] = container_stats

    return stats_by_id


def fetch_inspect_by_id(
    container_ids: list[str],
    docker_timeout_seconds: int,
    log: logging.Logger,
) -> dict[str, dict[str, Any]]:
    if not container_ids:
        return {}

    cmd = ["docker", "inspect", *container_ids]
    proc = run_cmd(cmd, docker_timeout_seconds)
    if proc.returncode != 0:
        raise RuntimeError(f"docker inspect failed: {cmd_error(proc)}")

    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"docker inspect returned invalid JSON: {exc}") from exc

    if not isinstance(payload, list):
        raise RuntimeError("docker inspect returned an unexpected payload")

    inspect_by_id: dict[str, dict[str, Any]] = {}
    for item in payload:
        if not isinstance(item, dict):
            continue

        container_id = safe_text(item.get("Id") or item.get("ID"))
        if container_id is None:
            continue

        state_obj = item.get("State")
        host_cfg = item.get("HostConfig")

        status = None
        if isinstance(state_obj, dict):
            status = safe_text(state_obj.get("Status"))

        cpuset_cpus = None
        cpu_shares = None
        blkio_weight = None
        if isinstance(host_cfg, dict):
            cpuset_cpus = safe_text(host_cfg.get("CpusetCpus"))
            cpu_shares = safe_int(host_cfg.get("CpuShares"))
            blkio_weight = safe_int(host_cfg.get("BlkioWeight"))

        inspect_by_id[container_id] = {
            "status": status,
            "cpuset_cpus": cpuset_cpus,
            "cpu_shares": cpu_shares,
            "blkio_weight": blkio_weight,
        }

    if len(inspect_by_id) != len(container_ids):
        log.debug(
            "docker inspect returned %d records for %d IDs",
            len(inspect_by_id),
            len(container_ids),
        )

    return inspect_by_id


def fetch_containers(
    docker_timeout_seconds: int,
    log: logging.Logger,
) -> list[dict[str, Any]]:
    ps_containers = fetch_ps_containers(docker_timeout_seconds, log)
    if not ps_containers:
        return []

    container_ids = [entry["id"] for entry in ps_containers]
    inspect_by_id = fetch_inspect_by_id(container_ids, docker_timeout_seconds, log)
    stats_by_id = fetch_stats_by_id(container_ids, docker_timeout_seconds, log)

    containers: list[dict[str, Any]] = []
    for entry in ps_containers:
        container_id = entry["id"]
        name = entry["name"]
        inspect_info = inspect_by_id.get(container_id, {})
        stats_info = stats_by_id.get(container_id, {})

        status = (
            safe_text(inspect_info.get("status"))
            or safe_text(entry.get("state"))
            or safe_text(entry.get("status_text"))
            or "unknown"
        )

        cpuset_cpus = safe_text(inspect_info.get("cpuset_cpus")) or "all"
        cpu_shares = safe_int(inspect_info.get("cpu_shares"))
        blkio_weight = safe_int(inspect_info.get("blkio_weight"))

        container: dict[str, Any] = {
            "id": container_id,
            "name": name,
            "status": status.lower(),
            "cpuset_cpus": cpuset_cpus,
            "cpu_shares": cpu_shares if cpu_shares is not None else 0,
            "blkio_weight": blkio_weight if blkio_weight is not None else 0,
        }

        cpu_percent = safe_float(stats_info.get("cpu_percent"))
        memory_usage = safe_float(stats_info.get("memory_usage"))
        network_rx_total = safe_float(stats_info.get("network_rx_total"))
        network_tx_total = safe_float(stats_info.get("network_tx_total"))
        io_read_total = safe_float(stats_info.get("io_read_total"))
        io_write_total = safe_float(stats_info.get("io_write_total"))

        if cpu_percent is not None:
            container["cpu_percent"] = cpu_percent
        if memory_usage is not None:
            container["memory_usage"] = memory_usage

        network: dict[str, float] = {}
        if network_rx_total is not None:
            network["cumulative_rx"] = network_rx_total
            container["network_rx_total"] = network_rx_total
        if network_tx_total is not None:
            network["cumulative_tx"] = network_tx_total
            container["network_tx_total"] = network_tx_total
        if network:
            container["network"] = network

        io: dict[str, float] = {}
        if io_read_total is not None:
            io["cumulative_ior"] = io_read_total
            container["io_read_total"] = io_read_total
        if io_write_total is not None:
            io["cumulative_iow"] = io_write_total
            container["io_write_total"] = io_write_total
        if io:
            container["io"] = io

        containers.append(container)

    return containers


def load_options_file(path: str, ap: argparse.ArgumentParser) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except OSError as exc:
        ap.error(f"unable to read options file {path}: {exc}")
    except json.JSONDecodeError as exc:
        ap.error(f"invalid JSON in options file {path}: {exc}")

    if not isinstance(payload, dict):
        ap.error("options file must contain a JSON object")

    nested = payload.get("options")
    if isinstance(nested, dict) and not any(key in payload for key in OPTION_KEYS):
        opts = nested
    else:
        opts = payload

    if not isinstance(opts, dict):
        ap.error("options object must be a JSON object")

    known_keys = sorted(key for key in opts.keys() if key in OPTION_KEYS)
    if not known_keys:
        ap.error(
            "options file does not contain recognized add-on keys; "
            "expected top-level keys or an 'options' object"
        )

    return opts


def redact_options_for_log(opts: dict[str, Any]) -> dict[str, Any]:
    redacted: dict[str, Any] = {}
    for key, value in opts.items():
        if key in SENSITIVE_OPTION_KEYS and str(value).strip():
            redacted[key] = "***"
        else:
            redacted[key] = value
    return redacted


def publish_discovery(
    client: mqtt.Client,
    discovery_prefix: str,
    device_id: str,
    base_topic: str,
    container_slug: str,
    container_display_name_text: str,
    metric_key: str,
    metric_def: dict[str, Any],
    expire_after_s: int,
) -> None:
    sensor_id = f"{container_slug}_{metric_key}"
    config_topic = f"{discovery_prefix}/sensor/{device_id}/{sensor_id}/config"
    state_topic = f"{base_topic}/{container_slug}/{metric_key}/state"
    friendly_container_name = f"Container {container_display_name_text}"

    payload: dict[str, Any] = {
        "name": metric_def["name"],
        "has_entity_name": True,
        "unique_id": f"{device_id}_{sensor_id}",
        "default_entity_id": f"sensor.container_{container_slug}_{metric_key}",
        "state_topic": state_topic,
        "availability_topic": f"{base_topic}/{container_slug}/availability",
        "payload_available": "online",
        "payload_not_available": "offline",
        "expire_after": max(5, int(expire_after_s)),
        "device": {
            "identifiers": [f"{device_id}_{container_slug}"],
            "name": friendly_container_name,
            "manufacturer": "Docker",
            "model": "Container",
        },
    }

    if metric_def.get("unit"):
        payload["unit_of_measurement"] = metric_def["unit"]
    if metric_def.get("icon"):
        payload["icon"] = metric_def["icon"]
    if metric_def.get("device_class"):
        payload["device_class"] = metric_def["device_class"]
    if metric_def.get("state_class"):
        payload["state_class"] = metric_def["state_class"]
    if "suggested_display_precision" in metric_def:
        payload["suggested_display_precision"] = metric_def[
            "suggested_display_precision"
        ]

    client.publish(config_topic, json.dumps(payload), qos=1, retain=True)


def publish_summary_discovery(
    client: mqtt.Client,
    discovery_prefix: str,
    device_id: str,
    base_topic: str,
    container_slug: str,
    container_display_name_text: str,
    expire_after_s: int,
) -> None:
    sensor_id = f"{container_slug}_summary"
    config_topic = f"{discovery_prefix}/sensor/{device_id}/{sensor_id}/config"
    state_topic = f"{base_topic}/{container_slug}/summary/state"
    attributes_topic = f"{base_topic}/{container_slug}/summary/attributes"
    friendly_container_name = f"Container {container_display_name_text}"

    payload: dict[str, Any] = {
        "name": "Summary",
        "has_entity_name": True,
        "unique_id": f"{device_id}_{sensor_id}",
        "default_entity_id": f"sensor.container_{container_slug}_summary",
        "state_topic": state_topic,
        "json_attributes_topic": attributes_topic,
        "availability_topic": f"{base_topic}/{container_slug}/availability",
        "payload_available": "online",
        "payload_not_available": "offline",
        "expire_after": max(5, int(expire_after_s)),
        "icon": "mdi:table",
        "device": {
            "identifiers": [f"{device_id}_{container_slug}"],
            "name": friendly_container_name,
            "manufacturer": "Docker",
            "model": "Container",
        },
    }

    client.publish(config_topic, json.dumps(payload), qos=1, retain=True)


def clear_discovery(
    client: mqtt.Client,
    discovery_prefix: str,
    device_id: str,
    container_slug: str,
    metric_key: str,
) -> None:
    sensor_id = f"{container_slug}_{metric_key}"
    config_topic = f"{discovery_prefix}/sensor/{device_id}/{sensor_id}/config"
    client.publish(config_topic, "", qos=1, retain=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--options", default="/data/options.json")
    ap.add_argument("--interval-seconds", type=int, default=None)
    ap.add_argument("--docker-timeout-seconds", type=int, default=None)
    ap.add_argument("--include-metrics", default=None)
    ap.add_argument("--container-include-regex", default=None)
    ap.add_argument("--container-exclude-regex", default=None)

    ap.add_argument("--mqtt-host", default=None)
    ap.add_argument("--mqtt-port", type=int, default=None)
    ap.add_argument("--mqtt-username", default=None)
    ap.add_argument("--mqtt-password", default=None)
    ap.add_argument("--mqtt-discovery-prefix", default=None)
    ap.add_argument("--mqtt-base-topic", default=None)
    ap.add_argument("--client-id", default=None)
    ap.add_argument("--log-level", default=None)
    ap.add_argument("--heartbeat-interval-seconds", type=int, default=None)

    args = ap.parse_args()

    opts = load_options_file(args.options, ap)

    def resolve(cli_value: Any, key: str, default: Any, cast=None) -> Any:
        value = cli_value if cli_value is not None else opts.get(key, default)
        if value is None:
            value = default
        if cast is not None:
            return cast(value)
        return value

    args.interval_seconds = resolve(args.interval_seconds, "interval_seconds", 10, int)
    args.docker_timeout_seconds = resolve(
        args.docker_timeout_seconds,
        "docker_timeout_seconds",
        10,
        int,
    )
    args.include_metrics = resolve(
        args.include_metrics,
        "include_metrics",
        "cpu_percent,memory_usage,network_rx_total,network_tx_total,"
        "io_read_total,io_write_total,network_rx_rate,network_tx_rate,"
        "io_read_rate,io_write_rate,status,cpuset_cpus,cpu_shares,blkio_weight",
        str,
    )
    args.container_include_regex = resolve(
        args.container_include_regex, "container_include_regex", "", str
    )
    args.container_exclude_regex = resolve(
        args.container_exclude_regex, "container_exclude_regex", "", str
    )

    args.mqtt_host = resolve(args.mqtt_host, "mqtt_host", "core-mosquitto", str)
    args.mqtt_port = resolve(args.mqtt_port, "mqtt_port", 1883, int)
    args.mqtt_username = resolve(args.mqtt_username, "mqtt_username", "", str)
    args.mqtt_password = resolve(args.mqtt_password, "mqtt_password", "", str)
    args.mqtt_discovery_prefix = resolve(
        args.mqtt_discovery_prefix, "mqtt_discovery_prefix", "homeassistant", str
    )
    args.mqtt_base_topic = resolve(
        args.mqtt_base_topic,
        "mqtt_base_topic",
        "container_info",
        str,
    )
    args.client_id = resolve(args.client_id, "client_id", "container-info-mqtt", str)
    args.log_level = resolve(args.log_level, "log_level", "INFO", str)
    args.heartbeat_interval_seconds = resolve(
        args.heartbeat_interval_seconds, "heartbeat_interval_seconds", 30, int
    )

    if not args.mqtt_host:
        ap.error("mqtt_host is required (via --mqtt-host or --options)")

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    log = logging.getLogger("container_info_mqtt")
    unknown_option_keys = sorted(key for key in opts.keys() if key not in OPTION_KEYS)
    if unknown_option_keys:
        log.warning("Unknown keys in options file: %s", ", ".join(unknown_option_keys))
    log.info(
        "Raw options file values: %s",
        json.dumps(redact_options_for_log(opts), sort_keys=True),
    )
    log.info(
        "Effective configuration: %s",
        json.dumps(
            {
                "options_file": args.options,
                "interval_seconds": args.interval_seconds,
                "docker_timeout_seconds": args.docker_timeout_seconds,
                "include_metrics": args.include_metrics,
                "container_include_regex": args.container_include_regex,
                "container_exclude_regex": args.container_exclude_regex,
                "mqtt_host": args.mqtt_host,
                "mqtt_port": args.mqtt_port,
                "mqtt_username": args.mqtt_username,
                "mqtt_password": "***" if args.mqtt_password else "",
                "mqtt_discovery_prefix": args.mqtt_discovery_prefix,
                "mqtt_base_topic": args.mqtt_base_topic,
                "client_id": args.client_id,
                "log_level": args.log_level,
                "heartbeat_interval_seconds": args.heartbeat_interval_seconds,
            },
            sort_keys=True,
        ),
    )

    try:
        include_rx = (
            re.compile(args.container_include_regex, re.IGNORECASE)
            if args.container_include_regex
            else None
        )
    except re.error as exc:
        ap.error(f"invalid container_include_regex: {exc}")

    try:
        exclude_rx = (
            re.compile(args.container_exclude_regex, re.IGNORECASE)
            if args.container_exclude_regex
            else None
        )
    except re.error as exc:
        ap.error(f"invalid container_exclude_regex: {exc}")

    selected_metrics = parse_include_metrics(args.include_metrics, log)
    selected_metric_set = set(selected_metrics)

    client = mqtt.Client(client_id=args.client_id, clean_session=True)
    if args.mqtt_username:
        client.username_pw_set(args.mqtt_username, args.mqtt_password)

    base_topic = args.mqtt_base_topic
    container_online: dict[str, bool] = {}

    def on_connect(_client, _userdata, _flags, rc):
        if rc == 0:
            log.info("MQTT connected to %s:%d", args.mqtt_host, args.mqtt_port)
            _client.publish(f"{base_topic}/availability", "online", qos=1, retain=True)
            for slug, is_online in container_online.items():
                _client.publish(
                    f"{base_topic}/{slug}/availability",
                    "online" if is_online else "offline",
                    qos=1,
                    retain=True,
                )
        else:
            log.error("MQTT connect failed rc=%s", rc)

    def on_disconnect(_client, _userdata, rc):
        if rc == 0:
            log.warning("MQTT disconnected (clean)")
        else:
            log.warning("MQTT disconnected rc=%s", rc)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    client.will_set(f"{base_topic}/availability", "offline", qos=1, retain=True)
    client.connect(args.mqtt_host, args.mqtt_port, keepalive=60)
    client.loop_start()

    discovered: dict[str, set[str]] = {}
    summary_discovered: set[str] = set()
    last_totals_by_container: dict[str, dict[str, float]] = {}
    last_heartbeat = 0.0

    interval_seconds = max(1, args.interval_seconds)
    expire_after_seconds = max(5, int(interval_seconds * 3))

    def sleep_to_interval(start_monotonic: float) -> None:
        remaining = interval_seconds - (time.monotonic() - start_monotonic)
        if remaining > 0:
            time.sleep(remaining)

    while True:
        loop_start_monotonic = time.monotonic()
        now = time.time()

        if now - last_heartbeat >= args.heartbeat_interval_seconds:
            last_heartbeat = now
            hb = {
                "ts": now,
                "source": "docker_engine_api",
                "selected_metrics": selected_metrics,
            }
            client.publish(
                f"{base_topic}/heartbeat",
                json.dumps(hb, sort_keys=True),
                qos=0,
                retain=False,
            )

        try:
            containers = fetch_containers(args.docker_timeout_seconds, log)
        except Exception as exc:
            log.error("Failed to collect Docker container info: %s", exc)
            sleep_to_interval(loop_start_monotonic)
            continue

        seen_slugs: set[str] = set()
        for container in containers:
            container_name = safe_text(container.get("name")) or safe_text(
                container.get("id")
            )
            if not container_name:
                continue

            if include_rx and not include_rx.search(container_name):
                continue
            if exclude_rx and exclude_rx.search(container_name):
                continue

            display_name = container_display_name(container_name)
            base_slug = slugify(display_name)
            container_slug = base_slug

            # Prefer stable, name-based slugs so entity IDs survive container restarts.
            # Only fall back when multiple containers normalize to the same display slug.
            if container_slug in seen_slugs:
                alt_slug = slugify(container_name)
                if alt_slug and alt_slug not in seen_slugs:
                    container_slug = alt_slug
                else:
                    name_hash = hashlib.sha1(
                        container_name.encode("utf-8")
                    ).hexdigest()[:8]
                    container_slug = f"{base_slug}_{name_hash}"

            seen_slugs.add(container_slug)

            if container_slug not in discovered:
                discovered[container_slug] = set()

            if container_online.get(container_slug) is not True:
                client.publish(
                    f"{base_topic}/{container_slug}/availability",
                    "online",
                    qos=1,
                    retain=True,
                )
                container_online[container_slug] = True

            if container_slug not in summary_discovered:
                publish_summary_discovery(
                    client,
                    args.mqtt_discovery_prefix,
                    args.client_id,
                    base_topic,
                    container_slug,
                    display_name,
                    expire_after_seconds,
                )
                summary_discovered.add(container_slug)

            has_network_rx_total = (
                metric_value(container, "network_rx_total") is not None
            )
            has_network_tx_total = (
                metric_value(container, "network_tx_total") is not None
            )

            def network_metric_enabled(metric_key: str) -> bool:
                if metric_key in {"network_rx_total", "network_rx_rate"}:
                    return has_network_rx_total
                if metric_key in {"network_tx_total", "network_tx_rate"}:
                    return has_network_tx_total
                return True

            stale_for_container = discovered[container_slug] - selected_metric_set
            stale_for_container |= {
                metric_key
                for metric_key in discovered[container_slug]
                if not network_metric_enabled(metric_key)
            }
            for stale_metric in stale_for_container:
                clear_discovery(
                    client,
                    args.mqtt_discovery_prefix,
                    args.client_id,
                    container_slug,
                    stale_metric,
                )
                discovered[container_slug].discard(stale_metric)

            rate_values = compute_rate_metrics(
                container_slug,
                container,
                now,
                last_totals_by_container,
            )

            summary_attributes: dict[str, Any] = {}

            for metric_key in selected_metrics:
                metric_def = METRIC_DEFS[metric_key]
                if not network_metric_enabled(metric_key):
                    continue

                if metric_key not in discovered[container_slug]:
                    publish_discovery(
                        client,
                        args.mqtt_discovery_prefix,
                        args.client_id,
                        base_topic,
                        container_slug,
                        display_name,
                        metric_key,
                        metric_def,
                        expire_after_seconds,
                    )
                    discovered[container_slug].add(metric_key)

                if metric_key in RATE_METRICS:
                    value = rate_values.get(metric_key)
                else:
                    value = metric_value(container, metric_key)

                if value is None:
                    continue

                state_topic = f"{base_topic}/{container_slug}/{metric_key}/state"

                value_type = metric_def.get("value_type", "number")
                if value_type == "string":
                    summary_value: Any = str(value)
                    state_payload = summary_value
                elif value_type == "integer":
                    int_value = int(value)
                    summary_value = int_value
                    state_payload = str(int_value)
                else:
                    numeric_value = float(value)
                    round_digits = metric_def.get("round_digits")
                    if isinstance(round_digits, int):
                        numeric_value = round(numeric_value, round_digits)
                    summary_value = numeric_value
                    state_payload = repr(numeric_value)

                summary_attributes[metric_key] = summary_value
                client.publish(state_topic, state_payload, qos=0, retain=False)

            summary_state = str(summary_attributes.get("status", "unknown"))
            summary_state_topic = f"{base_topic}/{container_slug}/summary/state"
            summary_attributes_topic = (
                f"{base_topic}/{container_slug}/summary/attributes"
            )
            client.publish(summary_state_topic, summary_state, qos=0, retain=False)
            client.publish(
                summary_attributes_topic,
                json.dumps(summary_attributes, sort_keys=True),
                qos=0,
                retain=False,
            )

        stale = set(discovered.keys()) - seen_slugs
        for stale_slug in stale:
            if container_online.get(stale_slug) is not False:
                client.publish(
                    f"{base_topic}/{stale_slug}/availability",
                    "offline",
                    qos=1,
                    retain=True,
                )
                container_online[stale_slug] = False
            last_totals_by_container.pop(stale_slug, None)

        sleep_to_interval(loop_start_monotonic)


if __name__ == "__main__":
    raise SystemExit(main())
