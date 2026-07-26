# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Bridge Signal K marine data into Home Assistant as MQTT Discovery entities."""

import argparse
import json
import logging
import signal
import sys
import threading
import time
from types import FrameType
from typing import Any

import paho.mqtt.client as mqtt

from . import __version__, auth
from .busstats import BusStats
from .client import (
    SignalKAuthError,
    SignalKError,
    get_self,
    get_server_info,
    get_sources,
)
from .mqtt import (
    attributes_topic,
    availability_topic,
    publish_discovery,
    state_topic,
)
from .paths import PATH_MAP, flatten, match_path, resolve_group

log = logging.getLogger(__name__)

_stop = threading.Event()


def _handle_signal(signum: int, _frame: FrameType | None) -> None:
    log.info("Received signal %d; shutting down", signum)
    _stop.set()


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level={
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
        }.get(level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def slugify(path: str) -> str:
    """Signal K path -> MQTT/entity-safe key."""
    return "".join(c if c.isalnum() else "_" for c in path).strip("_").lower()


def render(value: float) -> str:
    if isinstance(value, float):
        s = f"{value:.4f}".rstrip("0").rstrip(".")
        # A negative value that rounds to zero formats as "-0"; normalise it.
        return "0" if s == "-0" else s
    return str(value)


def _entity(
    path: str,
    name: str,
    state: str | None,
    group_id: str,
    group_label: str,
    *,
    component: str = "sensor",
    attributes: dict[str, Any] | None = None,
    **fields: Any,
) -> dict[str, Any]:
    ent = {
        "path": path,
        "component": component,
        "name": name,
        "state": state,
        "group_id": group_id,
        "group_label": group_label,
        "unit": None,
        "device_class": None,
        "state_class": None,
        "icon": None,
        "attributes": attributes,
    }
    ent.update(fields)
    return ent


def resolve_entities(tree: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Map a vessel tree onto numeric sensor definitions.

    Only paths present in the tree produce entities, so PATH_MAP can cover more
    equipment than any single boat carries without inventing sensors.
    """
    entities: dict[str, dict[str, Any]] = {}
    for path, raw in flatten(tree).items():
        for pattern, spec in PATH_MAP.items():
            caps = match_path(path, pattern)
            if caps is None:
                continue
            convert = spec.get("convert")
            if convert is None or not isinstance(raw, (int, float, bool)):
                # Numeric-only here; text/binary/position handled elsewhere.
                break
            group_id, group_label = resolve_group(spec["group"], caps)
            entities[slugify(path)] = _entity(
                path,
                spec["name"],
                render(convert(float(raw))),
                group_id,
                group_label,
                unit=spec.get("unit"),
                device_class=spec.get("device_class"),
                state_class=spec.get("state_class"),
                icon=spec.get("icon"),
            )
            break
    # Collapse duplicates that resolve to the same device + entity name (e.g. a
    # bank that reports stateOfCharge both at its root and under capacity).
    seen: set[tuple[str, str]] = set()
    deduped: dict[str, dict[str, Any]] = {}
    for key, ent in entities.items():
        ident = (ent["group_id"], ent["name"])
        if ident in seen:
            continue
        seen.add(ident)
        deduped[key] = ent
    return deduped


def resolve_special(tree: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Map the non-numeric paths: text/enum states, switch banks, notification
    alarms, and the vessel's position (as a device_tracker)."""
    from .paths import (
        NOTIFICATION_PREFIX,
        POSITION_PATH,
        SWITCH_PATTERN,
        TEXT_MAP,
        TEXT_PATTERN_MAP,
        notification_is_active,
    )

    entities: dict[str, dict[str, Any]] = {}
    for path, raw in flatten(tree).items():
        key = slugify(path)

        # Position -> device_tracker (boat on the HA map).
        if (
            path == POSITION_PATH
            and isinstance(raw, dict)
            and isinstance(raw.get("latitude"), (int, float))
            and isinstance(raw.get("longitude"), (int, float))
        ):
            attrs = {"latitude": raw["latitude"], "longitude": raw["longitude"]}
            entities[key] = _entity(
                path,
                "Position",
                None,
                "vessel",
                "Vessel",
                component="device_tracker",
                icon="mdi:sail-boat",
                attributes=attrs,
            )
            continue

        # Enum / free-text state sensors.
        if path in TEXT_MAP and isinstance(raw, (str, int, float)):
            spec = TEXT_MAP[path]
            gid, label = resolve_group(spec["group"], [])
            entities[key] = _entity(
                path,
                spec["name"],
                str(raw),
                gid,
                label,
                icon=spec.get("icon"),
            )
            continue

        # Enum / free-text state on a wildcard (per-instance) path -> a plain
        # sensor grouped under the captured instance (e.g. charger mode).
        matched_text = False
        for pattern, spec in TEXT_PATTERN_MAP.items():
            caps = match_path(path, pattern)
            if caps is not None and isinstance(raw, (str, int, float)):
                gid, label = resolve_group(spec["group"], caps)
                entities[key] = _entity(
                    path, spec["name"], str(raw), gid, label, icon=spec.get("icon")
                )
                matched_text = True
                break
        if matched_text:
            continue

        # Digital switch banks -> binary_sensor per channel.
        caps = match_path(path, SWITCH_PATTERN)
        if caps is not None and isinstance(raw, (int, float, bool)):
            bank, channel = caps
            entities[key] = _entity(
                path,
                f"Switch {channel}",
                "ON" if raw else "OFF",
                f"switches.bank.{bank}",
                f"Digital switches bank {bank}",
                component="binary_sensor",
                icon="mdi:toggle-switch-variant",
            )
            continue

        # Notifications -> binary_sensor alarms.
        if path.startswith(NOTIFICATION_PREFIX) and isinstance(raw, dict):
            name = str(raw.get("message") or path.split(".")[-1])
            entities[key] = _entity(
                path,
                name,
                "ON" if notification_is_active(raw) else "OFF",
                "alarms",
                "Alarms",
                component="binary_sensor",
                device_class="problem",
                icon="mdi:alarm-light",
            )
            continue

    return entities


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="signalk_bridge")
    ap.add_argument("--options", default="/data/options.json")
    args = ap.parse_args(argv)

    from .config import load_options_file, redact_options_for_log, resolve_mqtt_config

    opts = load_options_file(args.options, ap)
    configure_logging(str(opts.get("log_level") or "INFO"))

    log.info("Signal K -> Home Assistant bridge v%s starting", __version__)
    log.info("Options:")
    for line in json.dumps(
        redact_options_for_log(opts), indent=2, sort_keys=True
    ).splitlines():
        log.info("  %s", line)

    base_url = str(opts.get("signalk_url") or "http://localhost:3000")
    data_dir = str(opts.get("data_dir") or "/data")
    # An explicit signalk_token wins. Otherwise use the request-and-approve flow,
    # reusing a previously granted (persisted) token if we have one.
    token = str(opts.get("signalk_token") or "") or None
    use_access_flow = token is None
    if use_access_flow:
        token = auth.saved_token(data_dir)
    interval = int(opts.get("interval_seconds") or 10)
    discovery_prefix = str(opts.get("mqtt_discovery_prefix") or "homeassistant")
    base_topic = str(opts.get("mqtt_base_topic") or "signalk")
    client_id = str(opts.get("client_id") or "signalk-bridge")
    expire_after_s = max(5, interval * int(opts.get("expire_after_multiplier") or 4))

    try:
        mqtt_cfg = resolve_mqtt_config(opts)
    except RuntimeError as exc:
        log.error("%s", exc)
        return 1

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    try:
        info = get_server_info(base_url)
        ver = info.get("server", {}).get("version", "unknown")
        log.info("Signal K server at %s (version %s)", base_url, ver)
    except SignalKError as exc:
        log.warning("Signal K not reachable yet: %s", exc)

    client = mqtt.Client(client_id=client_id)
    if mqtt_cfg["username"]:
        client.username_pw_set(mqtt_cfg["username"], mqtt_cfg["password"])
    client.will_set(availability_topic(base_topic), "offline", qos=1, retain=True)
    try:
        client.connect(mqtt_cfg["host"], mqtt_cfg["port"], keepalive=60)
    except OSError as exc:
        log.error("Could not connect to MQTT broker: %s", exc)
        return 1
    client.loop_start()

    announced: set[str] = set()
    warned_empty = False
    access_href: str | None = None
    bus = BusStats()
    sources_cache: dict[str, Any] = {}
    cycle = 0
    # /sources is a large payload; refresh the device inventory ~once a minute.
    sources_every = max(1, 60 // max(1, interval))

    # Polling model, not the delta websocket: each cycle pulls a full REST
    # snapshot and republishes. Marine data is human-timescale, a snapshot is
    # internally consistent, and there is no reconnect/backfill state machine to
    # get wrong. `interval` governs freshness; the delta stream is the upgrade
    # path only if sub-second latency is ever needed.
    try:
        while not _stop.is_set():
            started = time.monotonic()

            # Obtain a token via the request-and-approve flow if we don't have one.
            if use_access_flow and not token:
                if access_href is None:
                    try:
                        cid = auth.client_id(data_dir)
                        access_href = auth.request_access(
                            base_url, cid, "Signal K to Home Assistant bridge"
                        )
                        log.warning(
                            "Requested access from Signal K. APPROVE IT: Signal K "
                            "-> Security -> Access Requests -> approve (read, no expiry)."
                        )
                    except SignalKError as exc:
                        log.warning("Could not submit access request: %s", exc)
                        _stop.wait(max(2, interval))
                    continue
                try:
                    state, tok = auth.poll_request(base_url, access_href)
                except SignalKError as exc:
                    log.debug("poll error: %s", exc)
                    _stop.wait(3)
                    continue
                if state == "APPROVED" and tok:
                    token = tok
                    auth.save_token(data_dir, tok)
                    access_href = None
                    log.warning(
                        "Access approved -- token saved. Bridge is authenticated."
                    )
                elif state == "DENIED":
                    log.error(
                        "Access request was DENIED in Signal K; re-requesting shortly."
                    )
                    access_href = None
                    _stop.wait(30)
                    continue
                else:
                    log.info(
                        "Waiting for approval in Signal K -> Security -> Access Requests..."
                    )
                    _stop.wait(3)
                    continue

            try:
                tree = get_self(base_url, token)
            except SignalKAuthError:
                if use_access_flow:
                    log.warning("Token rejected by Signal K; requesting access again.")
                    auth.clear_token(data_dir)
                    token = None
                    access_href = None
                else:
                    log.warning(
                        "Signal K rejected the configured signalk_token (401/403)."
                    )
                client.publish(
                    availability_topic(base_topic), "offline", qos=1, retain=True
                )
                _stop.wait(2)
                continue
            except SignalKError as exc:
                log.warning("%s", exc)
                client.publish(
                    availability_topic(base_topic), "offline", qos=1, retain=True
                )
                _stop.wait(max(1, interval))
                continue

            try:
                data_entities = {**resolve_entities(tree), **resolve_special(tree)}
            except Exception:
                log.exception("Error mapping Signal K data, skipping cycle")
                _stop.wait(max(1, interval))
                continue

            if not data_entities and not warned_empty:
                # Almost always means no NMEA 2000 traffic rather than a bridge
                # fault, so say so explicitly instead of sitting silent.
                log.warning(
                    "Signal K reachable but produced no mapped values. If this "
                    "persists, check that can0 is receiving frames "
                    "(cat /sys/class/net/can0/statistics/rx_packets) and that a "
                    "canbus connection is configured in Signal K."
                )
                warned_empty = True
            elif data_entities:
                warned_empty = False

            bus_entities: dict[str, dict[str, Any]] = {}
            if bus.available():
                if cycle % sources_every == 0:
                    try:
                        sources_cache = get_sources(base_url, token)
                    except SignalKError as exc:
                        log.debug("sources fetch failed: %s", exc)
                try:
                    bus_entities = bus.sample(sources_cache)
                except Exception as exc:  # noqa: BLE001 - bus stats must not kill the daemon
                    log.debug("bus stats sample failed: %s", exc)
            cycle += 1

            entities = {**data_entities, **bus_entities}

            for key, ent in entities.items():
                if key not in announced:
                    publish_discovery(
                        client, discovery_prefix, base_topic, key, ent, expire_after_s
                    )
                    announced.add(key)
                    log.info(
                        "Discovered %s -> %s (%s)",
                        ent.get("path", key),
                        ent["name"],
                        ent["group_label"],
                    )
                value = ent.get("state")
                if value is not None:
                    client.publish(state_topic(base_topic, key), value, qos=0)
                attrs = ent.get("attributes")
                if attrs is not None:
                    client.publish(
                        attributes_topic(base_topic, key), json.dumps(attrs), qos=0
                    )

            client.publish(availability_topic(base_topic), "online", qos=1, retain=True)
            log.debug("Published %d entities", len(entities))

            _stop.wait(max(0.0, interval - (time.monotonic() - started)))
    finally:
        log.info("Publishing offline availability and disconnecting")
        try:
            pub = client.publish(
                availability_topic(base_topic), "offline", qos=1, retain=True
            )
            pub.wait_for_publish(timeout=0.3)  # best-effort flush; LWT covers the rest
            client.loop_stop()
            client.disconnect()
        except Exception as exc:  # noqa: BLE001 - best effort on the way out
            log.debug("Error during MQTT shutdown: %s", exc)

    return 0


if __name__ == "__main__":
    sys.exit(main())
