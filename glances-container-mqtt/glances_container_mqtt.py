# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

from __future__ import annotations

import argparse
import json
import logging
import re
import time
from typing import Any, Optional
from urllib.parse import urlsplit, urlunsplit

import paho.mqtt.client as mqtt
import requests


METRIC_DEFS: dict[str, dict[str, Any]] = {
    "cpu_percent": {
        "paths": [("cpu_percent",), ("cpu", "total")],
        "name": "CPU Usage",
        "unit": "%",
        "icon": "mdi:chip",
        "state_class": "measurement",
    },
    "memory_usage": {
        "paths": [("memory_usage",), ("memory", "usage")],
        "name": "Memory Used",
        "unit": "B",
        "icon": "mdi:memory",
        "device_class": "data_size",
        "state_class": "measurement",
    },
    "network_rx_total": {
        "paths": [("network", "cumulative_rx"), ("cumulative_rx",)],
        "name": "Network RX Total",
        "unit": "B",
        "icon": "mdi:download",
        "device_class": "data_size",
        "state_class": "total_increasing",
    },
    "network_tx_total": {
        "paths": [("network", "cumulative_tx"), ("cumulative_tx",)],
        "name": "Network TX Total",
        "unit": "B",
        "icon": "mdi:upload",
        "device_class": "data_size",
        "state_class": "total_increasing",
    },
    "io_read_total": {
        "paths": [("io", "cumulative_ior"), ("cumulative_ior",)],
        "name": "Disk Read Total",
        "unit": "B",
        "icon": "mdi:harddisk",
        "device_class": "data_size",
        "state_class": "total_increasing",
    },
    "io_write_total": {
        "paths": [("io", "cumulative_iow"), ("cumulative_iow",)],
        "name": "Disk Write Total",
        "unit": "B",
        "icon": "mdi:harddisk",
        "device_class": "data_size",
        "state_class": "total_increasing",
    },
}


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_\-]+", "_", value.strip().lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    return slug or "unknown"


def container_display_name(value: str) -> str:
    display = value.strip()
    match = re.match(r"^addon_[0-9a-f]+_(.+)$", display, re.IGNORECASE)
    if match:
        display = match.group(1).strip()

    if not display:
        return "Unknown"

    return display[0].upper() + display[1:]


def safe_float(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def deep_get(container: dict[str, Any], path: tuple[str, ...]) -> Any:
    cur: Any = container
    for part in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
        if cur is None:
            return None
    return cur


def first_nonempty(container: dict[str, Any], keys: list[str]) -> Optional[str]:
    for key in keys:
        value = container.get(key)
        if value is None:
            continue
        if isinstance(value, list):
            if not value:
                continue
            value = value[0]
        if isinstance(value, dict):
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def metric_value(container: dict[str, Any], metric_key: str) -> Optional[float]:
    metric_def = METRIC_DEFS.get(metric_key)
    if metric_def is None:
        return None

    for path in metric_def["paths"]:
        value = safe_float(deep_get(container, path))
        if value is not None:
            return value
    return None


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


def fetch_containers(
    base_url: str,
    endpoint: str,
    timeout_seconds: int,
    auth: Optional[tuple[str, str]],
) -> list[dict[str, Any]]:
    raw_base = (base_url or "").strip()
    parsed = urlsplit(raw_base)

    if parsed.scheme and parsed.netloc:
        # glances_url is host/base URL; glances_endpoint controls API path.
        base = urlunsplit((parsed.scheme, parsed.netloc, "", "", "")).rstrip("/")
    else:
        base = raw_base.rstrip("/")

    normalized_endpoint = "/" + (endpoint or "").strip().lstrip("/")
    if normalized_endpoint == "/":
        normalized_endpoint = "/api/3/containers"

    candidates: list[str] = []
    for ep in (
        normalized_endpoint,
        "/api/3/containers",
        "/api/4/containers",
    ):
        norm = "/" + ep.lstrip("/")
        if norm not in candidates:
            candidates.append(norm)

    last_error = ""

    for ep in candidates:
        url = f"{base}/{ep.lstrip('/')}"
        try:
            res = requests.get(url, timeout=timeout_seconds, auth=auth)
            res.raise_for_status()
            payload = res.json()

            if isinstance(payload, list):
                return [x for x in payload if isinstance(x, dict)]

            if isinstance(payload, dict):
                if isinstance(payload.get("containers"), list):
                    return [x for x in payload["containers"] if isinstance(x, dict)]
                if isinstance(payload.get("container"), list):
                    return [x for x in payload["container"] if isinstance(x, dict)]

            last_error = f"Unexpected payload shape from {url}"
        except Exception as exc:
            last_error = f"{url}: {exc}"

    raise RuntimeError(f"Unable to fetch Glances containers: {last_error}")


def publish_discovery(
    client: mqtt.Client,
    discovery_prefix: str,
    device_id: str,
    base_topic: str,
    container_slug: str,
    container_name: str,
    container_display_name: str,
    container_ident: str,
    metric_key: str,
    metric_def: dict[str, Any],
) -> None:
    sensor_id = f"{container_slug}_{metric_key}"
    config_topic = f"{discovery_prefix}/sensor/{device_id}/{sensor_id}/config"
    state_topic = f"{base_topic}/{container_slug}/{metric_key}/state"
    attr_topic = f"{base_topic}/{container_slug}/{metric_key}/attributes"

    payload: dict[str, Any] = {
        "name": f"{container_name} {metric_def['name']}",
        "unique_id": f"{device_id}_{sensor_id}",
        "object_id": f"{device_id}_{sensor_id}",
        "state_topic": state_topic,
        "json_attributes_topic": attr_topic,
        "availability_topic": f"{base_topic}/availability",
        "payload_available": "online",
        "payload_not_available": "offline",
        "unit_of_measurement": metric_def.get("unit", ""),
        "device": {
            "identifiers": [f"{device_id}_{container_slug}"],
            "name": f"Container {container_display_name}",
            "manufacturer": "Glances",
            "model": "Container",
            "serial_number": container_ident,
        },
    }

    if metric_def.get("icon"):
        payload["icon"] = metric_def["icon"]
    if metric_def.get("device_class"):
        payload["device_class"] = metric_def["device_class"]
    if metric_def.get("state_class"):
        payload["state_class"] = metric_def["state_class"]
    if metric_def.get("unit") == "%":
        payload["suggested_display_precision"] = 1

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
    ap.add_argument("--glances-url", default=None)
    ap.add_argument("--glances-endpoint", default=None)
    ap.add_argument("--glances-username", default=None)
    ap.add_argument("--glances-password", default=None)
    ap.add_argument("--glances-timeout-seconds", type=int, default=None)
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

    try:
        with open(args.options, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except OSError as exc:
        ap.error(f"unable to read options file {args.options}: {exc}")

    if not isinstance(payload, dict):
        ap.error("options file must contain a JSON object")

    opts: dict[str, Any] = payload

    def resolve(cli_value: Any, key: str, default: Any, cast=None) -> Any:
        value = cli_value if cli_value is not None else opts.get(key, default)
        if value is None:
            value = default
        if cast is not None:
            return cast(value)
        return value

    args.interval_seconds = resolve(args.interval_seconds, "interval_seconds", 10, int)
    args.glances_url = resolve(
        args.glances_url, "glances_url", "http://localhost:61209", str
    )
    args.glances_endpoint = resolve(
        args.glances_endpoint, "glances_endpoint", "/api/3/containers", str
    )
    args.glances_username = resolve(args.glances_username, "glances_username", "", str)
    args.glances_password = resolve(args.glances_password, "glances_password", "", str)
    args.glances_timeout_seconds = resolve(
        args.glances_timeout_seconds, "glances_timeout_seconds", 10, int
    )
    args.include_metrics = resolve(
        args.include_metrics,
        "include_metrics",
        "cpu_percent,memory_usage,network_rx_total,network_tx_total,io_read_total,io_write_total",
        str,
    )
    args.container_include_regex = resolve(
        args.container_include_regex, "container_include_regex", "", str
    )
    args.container_exclude_regex = resolve(
        args.container_exclude_regex, "container_exclude_regex", "", str
    )

    args.mqtt_host = resolve(args.mqtt_host, "mqtt_host", "", str)
    args.mqtt_port = resolve(args.mqtt_port, "mqtt_port", 1883, int)
    args.mqtt_username = resolve(args.mqtt_username, "mqtt_username", "", str)
    args.mqtt_password = resolve(args.mqtt_password, "mqtt_password", "", str)
    args.mqtt_discovery_prefix = resolve(
        args.mqtt_discovery_prefix, "mqtt_discovery_prefix", "homeassistant", str
    )
    args.mqtt_base_topic = resolve(
        args.mqtt_base_topic, "mqtt_base_topic", "glances_containers", str
    )
    args.client_id = resolve(args.client_id, "client_id", "glances-container-mqtt", str)
    args.log_level = resolve(args.log_level, "log_level", "INFO", str)
    args.heartbeat_interval_seconds = resolve(
        args.heartbeat_interval_seconds, "heartbeat_interval_seconds", 30, int
    )

    args.glances_url = args.glances_url.strip()
    args.glances_endpoint = "/" + args.glances_endpoint.strip().lstrip("/")

    if not args.glances_url:
        ap.error("glances_url is required (via --glances-url or --options)")
    if not args.mqtt_host:
        ap.error("mqtt_host is required (via --mqtt-host or --options)")

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    log = logging.getLogger("glances_container_mqtt")
    log.info(
        "Config: glances_url=%s glances_endpoint=%s options_file=%s",
        args.glances_url,
        args.glances_endpoint,
        args.options,
    )

    include_rx = (
        re.compile(args.container_include_regex, re.IGNORECASE)
        if args.container_include_regex
        else None
    )
    exclude_rx = (
        re.compile(args.container_exclude_regex, re.IGNORECASE)
        if args.container_exclude_regex
        else None
    )

    selected_metrics = parse_include_metrics(args.include_metrics, log)
    selected_metric_set = set(selected_metrics)

    auth = None
    if args.glances_username:
        auth = (args.glances_username, args.glances_password)

    client = mqtt.Client(client_id=args.client_id, clean_session=True)
    if args.mqtt_username:
        client.username_pw_set(args.mqtt_username, args.mqtt_password)

    base_topic = args.mqtt_base_topic
    client.will_set(f"{base_topic}/availability", "offline", qos=1, retain=True)
    client.connect(args.mqtt_host, args.mqtt_port, keepalive=60)
    client.loop_start()
    client.publish(f"{base_topic}/availability", "online", qos=1, retain=True)

    discovered: dict[str, set[str]] = {}
    last_heartbeat = 0.0

    interval_seconds = max(1, args.interval_seconds)

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
                "source": args.glances_url,
                "endpoint": args.glances_endpoint,
                "selected_metrics": selected_metrics,
            }
            client.publish(
                f"{base_topic}/heartbeat", json.dumps(hb), qos=0, retain=False
            )

        try:
            containers = fetch_containers(
                args.glances_url,
                args.glances_endpoint,
                args.glances_timeout_seconds,
                auth,
            )
        except Exception as exc:
            log.error("Failed to fetch Glances containers: %s", exc)
            sleep_to_interval(loop_start_monotonic)
            continue

        seen_slugs: set[str] = set()

        for container in containers:
            container_name = first_nonempty(
                container, ["name", "container_name", "Name", "id", "Id"]
            )
            container_ident = first_nonempty(
                container, ["id", "Id", "container_id", "name", "Name"]
            )

            if not container_name or not container_ident:
                continue

            if include_rx and not include_rx.search(container_name):
                continue
            if exclude_rx and exclude_rx.search(container_name):
                continue

            short_id = container_ident.replace("/", "_")[:12]
            container_slug = slugify(f"{container_name}_{short_id}")
            display_name = container_display_name(container_name)
            seen_slugs.add(container_slug)

            if container_slug not in discovered:
                discovered[container_slug] = set()

            stale_for_container = discovered[container_slug] - selected_metric_set
            for stale_metric in stale_for_container:
                clear_discovery(
                    client,
                    args.mqtt_discovery_prefix,
                    args.client_id,
                    container_slug,
                    stale_metric,
                )
                discovered[container_slug].discard(stale_metric)

            attrs = {
                "container_name": container_name,
                "container_id": container_ident,
                "image": first_nonempty(container, ["image", "Image", "image_name"]),
                "status": first_nonempty(
                    container, ["status", "Status", "state", "State"]
                ),
                "engine": first_nonempty(container, ["engine", "Engine"]),
                "source": args.glances_url,
                "ts": now,
            }

            for metric_key in selected_metrics:
                metric_def = METRIC_DEFS[metric_key]
                value = metric_value(container, metric_key)
                if value is None:
                    continue

                if metric_key not in discovered[container_slug]:
                    publish_discovery(
                        client,
                        args.mqtt_discovery_prefix,
                        args.client_id,
                        base_topic,
                        container_slug,
                        container_name,
                        display_name,
                        container_ident,
                        metric_key,
                        metric_def,
                    )
                    discovered[container_slug].add(metric_key)

                state_topic = f"{base_topic}/{container_slug}/{metric_key}/state"
                attr_topic = f"{base_topic}/{container_slug}/{metric_key}/attributes"

                client.publish(attr_topic, json.dumps(attrs), qos=0, retain=False)
                client.publish(state_topic, repr(float(value)), qos=0, retain=False)

        stale = set(discovered.keys()) - seen_slugs
        for stale_slug in stale:
            for metric_key in discovered[stale_slug]:
                clear_discovery(
                    client,
                    args.mqtt_discovery_prefix,
                    args.client_id,
                    stale_slug,
                    metric_key,
                )
            del discovered[stale_slug]

        sleep_to_interval(loop_start_monotonic)


if __name__ == "__main__":
    raise SystemExit(main())
