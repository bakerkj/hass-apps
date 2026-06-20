# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Entry-point orchestration: option resolution, MQTT lifecycle, main loop."""

import argparse
import hashlib
import json
import logging
import re
import signal
import time
from typing import Any

import paho.mqtt.client as mqtt

from . import __version__
from .config import OPTION_KEYS, load_options_file
from .docker import fetch_containers, run_cmd
from .metrics import (
    METRIC_DEFS,
    RATE_METRICS,
    compute_rate_metrics,
    metric_value,
    parse_include_metrics,
    render_metric_state,
)
from .mqtt import (
    MqttHealth,
    clear_discovery,
    prune_stale_discovery,
    publish_discovery,
    publish_summary_discovery,
)
from .util import cmd_error, container_display_name, safe_text, slugify


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--options", default="/data/options.json")
    ap.add_argument("--interval-seconds", type=int, default=None)
    ap.add_argument("--docker-timeout-seconds", type=int, default=None)
    ap.add_argument("--include-metrics", default=None)
    ap.add_argument("--summary-metrics", default=None)
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
    ap.add_argument("--expire-after-multiplier", type=int, default=None)
    ap.add_argument("--mqtt-disconnect-timeout-seconds", type=int, default=None)

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
        "cpu_percent,memory_usage,network_rx_rate,network_tx_rate,"
        "io_read_rate,io_write_rate,started_at",
        str,
    )
    args.summary_metrics = resolve(
        args.summary_metrics,
        "summary_metrics",
        "cpu_percent,memory_usage,network_rx_rate,network_tx_rate,"
        "io_read_rate,io_write_rate,started_at,"
        "cpu_shares,cpuset_cpus,blkio_weight",
        str,
    )
    args.container_include_regex = resolve(
        args.container_include_regex, "container_include_regex", "", str
    )
    args.container_exclude_regex = resolve(
        args.container_exclude_regex,
        "container_exclude_regex",
        "^(?:addon_(?:[0-9a-f]+_)?)?builder_",
        str,
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
    args.expire_after_multiplier = max(
        2,
        min(
            10, resolve(args.expire_after_multiplier, "expire_after_multiplier", 4, int)
        ),
    )
    args.mqtt_disconnect_timeout_seconds = resolve(
        args.mqtt_disconnect_timeout_seconds,
        "mqtt_disconnect_timeout_seconds",
        300,
        int,
    )

    if not args.mqtt_host:
        ap.error("mqtt_host is required (via --mqtt-host or --options)")

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    log = logging.getLogger("container_info_mqtt")
    log.info("Container Info MQTT v%s starting", __version__)
    interval_seconds = max(1, args.interval_seconds)
    expire_after_s = max(60, int(interval_seconds) * args.expire_after_multiplier)
    unknown_option_keys = sorted(key for key in opts.keys() if key not in OPTION_KEYS)
    if unknown_option_keys:
        log.warning("Unknown keys in options file: %s", ", ".join(unknown_option_keys))
    log.info(
        "Configuration:\n"
        "  base_topic:         %s\n"
        "  client_id:          %s\n"
        "  container_exclude:  %s\n"
        "  container_include:  %s\n"
        "  disconnect_timeout: %ds\n"
        "  discovery_prefix:   %s\n"
        "  docker_timeout:     %ds\n"
        "  include_metrics:    %s\n"
        "  summary_metrics:    %s\n"
        "  interval:           %ds\n"
        "  log_level:          %s\n"
        "  mqtt_host:          %s:%d\n"
        "  mqtt_username:      %s\n"
        "  expire_after:       %ds",
        args.mqtt_base_topic,
        args.client_id,
        args.container_exclude_regex or "(none)",
        args.container_include_regex or "(all)",
        args.mqtt_disconnect_timeout_seconds,
        args.mqtt_discovery_prefix,
        args.docker_timeout_seconds,
        args.include_metrics,
        args.summary_metrics or "(none)",
        args.interval_seconds,
        args.log_level,
        args.mqtt_host,
        args.mqtt_port,
        args.mqtt_username or "(none)",
        expire_after_s,
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
    summary_only_metrics = (
        [
            m
            for m in parse_include_metrics(args.summary_metrics, log)
            if m not in selected_metric_set
        ]
        if args.summary_metrics.strip()
        else []
    )

    proc = run_cmd(["docker", "info"], args.docker_timeout_seconds)
    if proc.returncode != 0:
        err = cmd_error(proc).lower()
        if (
            "docker.sock" in err
            or "connect" in err
            or "permission denied" in err
            or "no such file" in err
        ):
            log.error(
                "Cannot connect to the Docker API at unix:///var/run/docker.sock. "
                "Disable Protection Mode for this addon in the Home Assistant UI "
                "(Addon → Info → Protection mode) and restart."
            )
        else:
            log.error("Docker API check failed: %s", cmd_error(proc))
        return 1

    client = mqtt.Client(client_id=args.client_id, clean_session=True)
    if args.mqtt_username:
        client.username_pw_set(args.mqtt_username, args.mqtt_password)

    base_topic = args.mqtt_base_topic
    container_online: dict[str, bool] = {}
    health = MqttHealth()

    # One-shot scan of retained discovery configs so we can prune entities for
    # containers that no longer exist (or now match the exclude filter).
    discovery_topic_prefix = f"{args.mqtt_discovery_prefix}/sensor/"
    discovery_topic_suffix = "/config"
    device_node_prefix = f"{args.client_id}_"
    discovery_filter = f"{args.mqtt_discovery_prefix}/sensor/+/+/config"
    retained_configs: dict[str, set[str]] = {}
    prune_pending = {"v": True}
    retained_subscribe_time = {"v": 0.0}
    # Wait this long after subscribing before pruning, to let the broker flush
    # its retained backlog. Generous to tolerate slow brokers and large
    # retained sets without prematurely pruning live entries.
    retained_scan_seconds = 60.0

    def on_connect(_client, _userdata, _flags, rc):
        if rc == 0:
            log.info("MQTT connected to %s:%d", args.mqtt_host, args.mqtt_port)
            health.connected = True
            health.last_connect_ok = time.time()
            _client.publish(f"{base_topic}/availability", "online", qos=1, retain=True)
            for slug, is_online in container_online.items():
                _client.publish(
                    f"{base_topic}/{slug}/availability",
                    "online" if is_online else "offline",
                    qos=1,
                    retain=True,
                )
            _client.subscribe(f"{args.mqtt_discovery_prefix}/status", qos=1)
            if prune_pending["v"]:
                _client.subscribe(discovery_filter, qos=0)
                retained_subscribe_time["v"] = time.time()
        else:
            log.error("MQTT connect failed rc=%s", rc)

    def on_disconnect(_client, _userdata, rc):
        health.connected = False
        health.last_disconnect = time.time()
        if rc == 0:
            log.warning("MQTT disconnected (clean)")
        else:
            log.warning("MQTT disconnected rc=%s", rc)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    client.will_set(f"{base_topic}/availability", "offline", qos=1, retain=True)
    _retry_delay = 5
    while True:
        try:
            client.connect(args.mqtt_host, args.mqtt_port, keepalive=60)
            break
        except Exception as exc:
            log.warning(
                "Cannot connect to MQTT broker %s:%s: %s — retrying in %ds",
                args.mqtt_host,
                args.mqtt_port,
                exc,
                _retry_delay,
            )
            time.sleep(_retry_delay)
            _retry_delay = min(_retry_delay * 2, 60)
    client.loop_start()

    stop = {"v": False}

    def _handle_sig(_sig: int, _frame: object) -> None:
        stop["v"] = True

    signal.signal(signal.SIGTERM, _handle_sig)
    signal.signal(signal.SIGINT, _handle_sig)

    discovered: dict[str, set[str]] = {}
    summary_discovered: set[str] = set()
    needs_rediscovery = {"v": False}
    last_totals_by_container: dict[str, dict[str, float]] = {}

    def on_message(_client, _userdata, msg):
        topic = msg.topic
        # Retained discovery config under our discovery prefix.
        if topic.startswith(discovery_topic_prefix) and topic.endswith(
            discovery_topic_suffix
        ):
            middle = topic[len(discovery_topic_prefix) : -len(discovery_topic_suffix)]
            node_id, sep, metric_key = middle.partition("/")
            if sep and "/" not in metric_key and node_id.startswith(device_node_prefix):
                # Empty payload = tombstone (already cleared); ignore.
                if msg.payload:
                    retained_configs.setdefault(node_id, set()).add(metric_key)
            return
        # HA birth message on the discovery status topic.
        if topic == f"{args.mqtt_discovery_prefix}/status":
            if msg.payload.decode(errors="replace").strip() == "online":
                log.info("HA birth message received — will republish discovery")
                needs_rediscovery["v"] = True

    client.on_message = on_message
    last_heartbeat = 0.0
    last_sample_time = 0.0

    def sleep_to_interval(start_monotonic: float) -> None:
        remaining = interval_seconds - (time.monotonic() - start_monotonic)
        if remaining > 0:
            time.sleep(remaining)
        elif remaining < -1:
            log.warning("Collection loop overran interval by %.2fs", -remaining)

    while not stop["v"]:
        loop_start_monotonic = time.monotonic()
        now = time.time()

        if needs_rediscovery["v"]:
            discovered.clear()
            summary_discovered.clear()
            needs_rediscovery["v"] = False
            log.info("Discovery state cleared — will republish for all containers")

        # MQTT disconnect watchdog
        if not health.connected and health.last_disconnect > 0:
            if (now - health.last_disconnect) > args.mqtt_disconnect_timeout_seconds:
                log.error(
                    "MQTT disconnected for %.1fs (> %ss). Exiting for supervisor restart.",
                    now - health.last_disconnect,
                    args.mqtt_disconnect_timeout_seconds,
                )
                return 11

        # Sample stall watchdog
        if last_sample_time > 0 and (now - last_sample_time) > expire_after_s:
            log.error(
                "No Docker samples published for %.1fs (> %ss). Exiting for supervisor restart.",
                now - last_sample_time,
                expire_after_s,
            )
            return 12

        if now - last_heartbeat >= interval_seconds:
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
                    expire_after_s,
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
                        expire_after_s,
                    )
                    discovered[container_slug].add(metric_key)

                if metric_key in RATE_METRICS:
                    value = rate_values.get(metric_key)
                else:
                    value = metric_value(container, metric_key)

                if value is None:
                    continue

                state_topic = f"{base_topic}/{container_slug}/{metric_key}/state"

                state_payload, summary_value = render_metric_state(metric_def, value)
                summary_attributes[metric_key] = summary_value
                client.publish(state_topic, state_payload, qos=0, retain=False)

            for metric_key in summary_only_metrics:
                if not network_metric_enabled(metric_key):
                    continue
                metric_def = METRIC_DEFS[metric_key]
                if metric_key in RATE_METRICS:
                    value = rate_values.get(metric_key)
                else:
                    value = metric_value(container, metric_key)
                if value is None:
                    continue
                _, summary_attributes[metric_key] = render_metric_state(
                    metric_def, value
                )

            summary_state = container.get("status", "unknown")
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

        last_sample_time = now

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

        # One-shot prune of retained discovery for slugs that no longer exist
        # (e.g. removed containers, or containers now matching the exclude
        # filter). Gated on: at least one container actually seen this poll
        # AND the retained-scan window has elapsed since subscribe — so a
        # transient Docker failure on the very first poll cannot wipe state.
        if (
            prune_pending["v"]
            and seen_slugs
            and retained_subscribe_time["v"] > 0
            and (now - retained_subscribe_time["v"]) >= retained_scan_seconds
        ):
            # Snapshot retained_configs to avoid races with the network thread.
            retained_snapshot = {
                node_id: set(metrics) for node_id, metrics in retained_configs.items()
            }
            # For each active slug, the discovery configs we actually publish:
            # the per-metric sensors in `discovered` plus the always-on
            # "summary" sensor. Anything else retained on the broker for an
            # active slug is an orphan from a prior config/version and gets
            # reconciled away.
            expected_by_slug = {
                slug: (
                    set(discovered.get(slug, set()))
                    | ({"summary"} if slug in summary_discovered else set())
                )
                for slug in seen_slugs
            }
            prune_stale_discovery(
                client,
                args.mqtt_discovery_prefix,
                base_topic,
                args.client_id,
                retained_snapshot,
                expected_by_slug,
                log,
            )
            client.unsubscribe(discovery_filter)
            retained_configs.clear()
            prune_pending["v"] = False

        sleep_to_interval(loop_start_monotonic)

    log.info("Shutting down")
    try:
        client.publish(f"{base_topic}/availability", "offline", qos=1, retain=True)
    except Exception:
        pass
    client.loop_stop()
    return 0
