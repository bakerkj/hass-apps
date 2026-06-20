# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Entry-point orchestration: CLI/options parsing, MQTT lifecycle, main loop."""

import argparse
import json
import logging
import os
import signal
import time
from typing import Any

import paho.mqtt.client as mqtt

from . import __version__
from .device import (
    auto_select_device_arg,
    list_intel_gpu_top_devices,
    start_intel_gpu_top,
)
from .metrics import build_metrics
from .mqtt import MqttHealth, publish_discovery
from .util import extract_latest_json_object


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--options", default="")
    ap.add_argument("--interval-seconds", type=int, default=None)
    ap.add_argument("--mqtt-host", default=None)
    ap.add_argument("--mqtt-port", type=int, default=None)
    ap.add_argument("--mqtt-username", default=None)
    ap.add_argument("--mqtt-password", default=None)
    ap.add_argument("--mqtt-discovery-prefix", default=None)
    ap.add_argument("--mqtt-base-topic", default=None)
    ap.add_argument("--client-id", default=None)
    ap.add_argument("--preferred-device-regex", default=None)
    ap.add_argument("--log-level", default=None)
    ap.add_argument("--publish-raw-sample", default=None)

    ap.add_argument("--expire-after-multiplier", type=int, default=None)
    ap.add_argument("--mqtt-disconnect-timeout-seconds", type=int, default=None)
    ap.add_argument("--intel-restart-grace-seconds", type=int, default=None)

    args = ap.parse_args()

    opts: dict[str, Any] = {}
    if args.options:
        with open(args.options, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if not isinstance(payload, dict):
            raise ValueError("options file must contain a JSON object")
        opts = payload

    def resolve(cli_value: Any, key: str, default: Any, cast=None) -> Any:
        value = cli_value if cli_value is not None else opts.get(key, default)
        if value is None:
            value = default
        if cast is not None:
            return cast(value)
        return value

    def parse_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

    args.interval_seconds = resolve(args.interval_seconds, "interval_seconds", 5, int)
    args.mqtt_host = resolve(args.mqtt_host, "mqtt_host", "", str)
    args.mqtt_port = resolve(args.mqtt_port, "mqtt_port", 1883, int)
    args.mqtt_username = resolve(args.mqtt_username, "mqtt_username", "", str)
    args.mqtt_password = resolve(args.mqtt_password, "mqtt_password", "", str)
    args.mqtt_discovery_prefix = resolve(
        args.mqtt_discovery_prefix, "mqtt_discovery_prefix", "homeassistant", str
    )
    args.mqtt_base_topic = resolve(
        args.mqtt_base_topic, "mqtt_base_topic", "intel_gpu_top", str
    )
    args.client_id = resolve(args.client_id, "client_id", "intel-gpu-top-addon", str)
    args.preferred_device_regex = resolve(
        args.preferred_device_regex, "preferred_device_regex", "", str
    )
    args.log_level = resolve(args.log_level, "log_level", "INFO", str)
    args.publish_raw_sample = parse_bool(
        resolve(args.publish_raw_sample, "publish_raw_sample", True)
    )

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
    args.intel_restart_grace_seconds = resolve(
        args.intel_restart_grace_seconds, "intel_restart_grace_seconds", 10, int
    )

    if not args.mqtt_host:
        ap.error("mqtt_host is required (via --mqtt-host or --options)")

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    log = logging.getLogger("intel_gpu_mqtt")
    log.info("Intel GPU Top MQTT v%s starting", __version__)

    interval_s = max(1, args.interval_seconds)
    interval_ms = interval_s * 1000
    expire_after_s = max(60, interval_s * args.expire_after_multiplier)

    # Device selection
    listing = list_intel_gpu_top_devices(log)
    log.info("intel_gpu_top -L output:\n%s", listing if listing else "(none)")
    dev_arg, dev_path = auto_select_device_arg(
        listing, args.preferred_device_regex, log
    )
    log.info("Selected device arg: %s", dev_arg or "(none)")
    if dev_path:
        log.info("Selected render node: %s", dev_path)

    log.info(
        "Configuration:\n"
        "  base_topic:         %s\n"
        "  client_id:          %s\n"
        "  disconnect_timeout: %ds\n"
        "  discovery_prefix:   %s\n"
        "  interval:           %ds\n"
        "  log_level:          %s\n"
        "  mqtt_host:          %s:%d\n"
        "  mqtt_username:      %s\n"
        "  preferred_device:   %s\n"
        "  publish_raw:        %s\n"
        "  restart_grace:      %ds\n"
        "  expire_after:       %ds",
        args.mqtt_base_topic,
        args.client_id,
        args.mqtt_disconnect_timeout_seconds,
        args.mqtt_discovery_prefix,
        interval_s,
        args.log_level,
        args.mqtt_host,
        args.mqtt_port,
        args.mqtt_username or "(none)",
        args.preferred_device_regex or "(auto)",
        args.publish_raw_sample,
        args.intel_restart_grace_seconds,
        expire_after_s,
    )

    # MQTT setup with reconnect logic
    health = MqttHealth()
    base_topic = args.mqtt_base_topic

    client = mqtt.Client(client_id=args.client_id, clean_session=True)
    if args.mqtt_username:
        client.username_pw_set(args.mqtt_username, args.mqtt_password)

    client.will_set(f"{base_topic}/availability", "offline", qos=1, retain=True)

    # Backoff for reconnect attempts
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    def on_connect(_client, _userdata, _flags, rc):
        if rc == 0:
            health.connected = True
            health.last_connect_ok = time.time()
            log.info("MQTT connected successfully")
            # Mark available on every connect (retained)
            _client.publish(f"{base_topic}/availability", "online", qos=1, retain=True)
            _client.subscribe(f"{args.mqtt_discovery_prefix}/status", qos=1)
        else:
            health.connected = False
            log.error("MQTT connection failed rc=%s", rc)

    def on_disconnect(_client, _userdata, rc):
        health.connected = False
        health.last_disconnect = time.time()
        # rc==0 is clean disconnect; nonzero implies unexpected
        if rc == 0:
            log.warning("MQTT disconnected (clean)")
        else:
            log.warning("MQTT disconnected rc=%s (will retry)", rc)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    log.info("Connecting MQTT to %s:%d", args.mqtt_host, args.mqtt_port)
    _retry_delay = 5
    while True:
        try:
            client.connect(args.mqtt_host, args.mqtt_port, keepalive=60)
            break
        except Exception as e:
            log.warning(
                "Cannot connect to MQTT broker %s:%d: %s — retrying in %ds",
                args.mqtt_host,
                args.mqtt_port,
                e,
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

    # Start intel_gpu_top
    try:
        proc = start_intel_gpu_top(interval_ms, dev_arg, log)
    except FileNotFoundError:
        return 2

    device_id = "intel_gpu_top"
    device_name = "Intel GPU Top"

    buf = ""
    discovery_published = False
    last_publish_time = 0.0

    def on_message(_client, _userdata, msg):
        nonlocal discovery_published
        if msg.payload.decode(errors="replace").strip() == "online":
            log.info("HA birth message received — will republish discovery")
            discovery_published = False

    client.on_message = on_message
    last_heartbeat_time = 0.0
    last_sample_time = 0.0
    # Monotonic mirror of last_sample_time used only by the stall watchdog:
    # the published last_sample_age_s stays on wall clock, but the restart
    # decision must survive an NTP step.
    last_sample_monotonic = 0.0
    last_intel_restart_attempt = 0.0
    samples_since_intel_start = 0

    def restart_intel_gpu_top(reason: str) -> None:
        nonlocal proc, buf, last_intel_restart_attempt, dev_arg, dev_path, listing
        nonlocal samples_since_intel_start
        # Monotonic: the restart-grace debounce is a pure duration and must
        # not be defeated by a wall-clock step.
        now = time.monotonic()
        if now - last_intel_restart_attempt < args.intel_restart_grace_seconds:
            log.warning(
                "Skipping intel_gpu_top restart (grace period) reason=%s", reason
            )
            return
        last_intel_restart_attempt = now

        log.warning("Restarting intel_gpu_top reason=%s", reason)
        try:
            if proc and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except Exception:
                    proc.kill()
        except Exception as e:
            log.warning("Error stopping intel_gpu_top: %s", e)

        # Re-select device in case GPU nodes changed
        listing = list_intel_gpu_top_devices(log)
        dev_arg, dev_path = auto_select_device_arg(
            listing, args.preferred_device_regex, log
        )
        log.info("Re-selected device arg: %s", dev_arg or "(none)")
        if dev_path:
            log.info("Re-selected render node: %s", dev_path)

        buf = ""
        samples_since_intel_start = 0
        proc = start_intel_gpu_top(interval_ms, dev_arg, log)

    try:
        if proc.stdout is None:
            log.error("intel_gpu_top stdout is None")
            return 2

        while not stop["v"]:
            # ----- Watchdogs -----

            now = time.time()
            now_mono = time.monotonic()

            # GPU disappearance / device node check
            if dev_path is not None and not os.path.exists(dev_path):
                log.error("GPU render node disappeared: %s", dev_path)
                restart_intel_gpu_top("render_node_disappeared")

            # Sample timeout watchdog. Monotonic so a wall-clock step can't
            # suppress restarting a dead intel_gpu_top; the published
            # last_sample_age_s below deliberately stays on wall clock.
            if (
                last_sample_monotonic > 0
                and (now_mono - last_sample_monotonic) > expire_after_s
            ):
                log.error(
                    "No intel_gpu_top samples for %.1fs",
                    now_mono - last_sample_monotonic,
                )
                # Try restart once; if it keeps failing, we'll exit via repeated timeout
                restart_intel_gpu_top("sample_timeout")

            # MQTT disconnect watchdog: exit nonzero so add-on supervisor restarts us
            if not health.connected and health.last_disconnect > 0:
                if (
                    now - health.last_disconnect
                ) > args.mqtt_disconnect_timeout_seconds:
                    log.error(
                        "MQTT disconnected for %.1fs (> %ss). Exiting for supervisor restart.",
                        now - health.last_disconnect,
                        args.mqtt_disconnect_timeout_seconds,
                    )
                    return 11

            # Heartbeat publish (independent of samples)
            if now - last_heartbeat_time >= interval_s:
                last_heartbeat_time = now
                hb_payload = json.dumps(
                    {
                        "ts": now,
                        "mqtt_connected": health.connected,
                        "last_sample_age_s": (now - last_sample_time)
                        if last_sample_time
                        else None,
                        "device": dev_path,
                    }
                )
                info = client.publish(
                    f"{base_topic}/heartbeat", hb_payload, qos=0, retain=False
                )
                log.debug(
                    "Heartbeat publish mid=%s rc=%s payload=%s",
                    info.mid,
                    info.rc,
                    hb_payload,
                )

            # ----- Read intel_gpu_top output line-by-line -----

            line = proc.stdout.readline()
            if line:
                # Log *each line* from intel_gpu_top
                log.debug("intel_gpu_top: %s", line.rstrip("\n"))

                buf += line
                obj, buf = extract_latest_json_object(buf)
                if not obj:
                    continue

                last_sample_time = time.time()
                last_sample_monotonic = time.monotonic()
                metrics = build_metrics(obj)
                samples_since_intel_start += 1
                log.debug("Parsed metrics keys=%s", list(metrics.keys()))

                # Publish discovery once we have our first sample (retained)
                if not discovery_published:
                    log.info("Publishing MQTT discovery for %d sensors", len(metrics))
                    publish_discovery(
                        client,
                        args.mqtt_discovery_prefix,
                        base_topic,
                        device_id,
                        device_name,
                        metrics,
                        expire_after_s,
                        log,
                    )
                    discovery_published = True

                # Rate-limit publishing to once per interval_seconds.
                # Monotonic: a wall-clock step must not stall or burst publishing.
                now_pub_mono = time.monotonic()
                if now_pub_mono - last_publish_time < interval_s:
                    continue
                last_publish_time = now_pub_mono

                warmup_sample = samples_since_intel_start <= 1
                if warmup_sample:
                    log.debug(
                        "Skipping state publish for warm-up sample #%d after "
                        "intel_gpu_top start/restart",
                        samples_since_intel_start,
                    )

                # Publish each sensor to its own state topic
                for key, m in metrics.items():
                    val = m["value"]
                    if warmup_sample or val is None:
                        continue

                    state_topic = f"{base_topic}/{key}/state"
                    # Publish full-precision numeric value; HA can format display using suggested_display_precision.
                    payload = str(float(val))
                    sinfo = client.publish(state_topic, payload, qos=0, retain=False)
                    log.debug(
                        "MQTT state %s=%s mid=%s rc=%s",
                        state_topic,
                        payload,
                        sinfo.mid,
                        sinfo.rc,
                    )

                if args.publish_raw_sample:
                    # Publish a raw sample snapshot for debugging (non-discovery)
                    raw_topic = f"{base_topic}/raw_sample"
                    rinfo = client.publish(
                        raw_topic, json.dumps(obj)[:200000], qos=0, retain=False
                    )
                    log.debug("MQTT raw_sample mid=%s rc=%s", rinfo.mid, rinfo.rc)

            else:
                # No line read. Check if process died.
                rc = proc.poll()
                if rc is not None:
                    err_tail = ""
                    if proc.stderr is not None:
                        try:
                            err_tail = proc.stderr.read()[-4000:]
                        except Exception:
                            err_tail = "(stderr read failed)"
                    log.error("intel_gpu_top exited rc=%s stderr_tail=%s", rc, err_tail)
                    restart_intel_gpu_top("intel_gpu_top_exited")
                else:
                    # Still running but no line available; small sleep
                    time.sleep(0.05)

    finally:
        try:
            client.publish(f"{base_topic}/availability", "offline", qos=1, retain=True)
        except Exception:
            pass
        client.loop_stop()
        try:
            if proc and proc.poll() is None:
                proc.terminate()
        except Exception:
            pass

    return 0
