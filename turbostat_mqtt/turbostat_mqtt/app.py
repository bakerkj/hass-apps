# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Entry-point orchestration: option parsing, MQTT lifecycle, turbostat loop."""

import argparse
import json
import re
import signal
import subprocess
import time
from typing import Any

import paho.mqtt.client as mqtt

from . import __version__
from .metadata import COLUMN_SCALES, friendly_name, missing_expected_columns
from .mqtt import (
    MqttHealth,
    build_discovery_payloads,
    connect_mqtt_with_retry,
    mqtt_publish,
)
from .parser import TurbostatParser, start_turbostat
from .util import log, sanitize_key


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--options", required=True)
    args = ap.parse_args()

    with open(args.options, "r", encoding="utf-8") as f:
        opts = json.load(f)

    log_level = (opts.get("log_level") or "INFO").upper()
    log("INFO", f"Turbostat to MQTT v{__version__} starting", log_level)

    interval = max(1.0, float(opts.get("interval_seconds", 10)))
    discovery_prefix = opts.get("mqtt_discovery_prefix", "homeassistant")
    base_topic = (opts.get("mqtt_base_topic") or "turbostat").rstrip("/")

    mqtt_host = opts.get("mqtt_host", "core-mosquitto")
    mqtt_port = int(opts.get("mqtt_port", 1883))
    mqtt_username = opts.get("mqtt_username", "") or ""
    mqtt_password = opts.get("mqtt_password", "") or ""
    client_id = opts.get("client_id") or "turbostat-app"

    publish_raw = bool(opts.get("publish_raw_sample", False))

    heartbeat_interval = int(interval)
    disconnect_timeout = max(5, int(opts.get("mqtt_disconnect_timeout_seconds", 300)))
    expire_after_multiplier = max(
        2, min(10, int(opts.get("expire_after_multiplier", 4)))
    )
    expire_after_s = max(60, int(interval) * expire_after_multiplier)

    raw_topic = f"{base_topic}/raw_sample"
    availability_topic = f"{base_topic}/availability"
    heartbeat_topic = f"{base_topic}/heartbeat"

    log(
        "INFO",
        "\n".join(
            [
                "Configuration:",
                f"  base_topic:         {base_topic}",
                f"  client_id:          {client_id}",
                f"  disconnect_timeout: {disconnect_timeout}s",
                f"  discovery_prefix:   {discovery_prefix}",
                f"  interval:           {interval}s",
                f"  log_level:          {log_level}",
                f"  mqtt_host:          {mqtt_host}:{mqtt_port}",
                f"  mqtt_username:      {mqtt_username or '(none)'}",
                f"  publish_raw:        {publish_raw}",
                f"  expire_after:       {expire_after_s}s",
            ]
        ),
        log_level,
    )

    health = MqttHealth()

    client = mqtt.Client(client_id=client_id, clean_session=True)
    if mqtt_username:
        client.username_pw_set(mqtt_username, mqtt_password)

    client.will_set(availability_topic, "offline", qos=1, retain=True)
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    def on_connect(_client, _userdata, _flags, rc):
        if rc == 0:
            health.connected = True
            health.last_connect_ok = time.time()
            log("INFO", f"MQTT connected to {mqtt_host}:{mqtt_port}", log_level)
            _client.subscribe(f"{discovery_prefix}/status", qos=1)
            mqtt_publish(
                _client,
                availability_topic,
                "online",
                qos=1,
                retain=True,
                log_level=log_level,
                health=health,
            )
        else:
            health.connected = False
            log("ERROR", f"MQTT connect failed rc={rc}", log_level)

    def on_disconnect(_client, _userdata, rc):
        health.connected = False
        health.last_disconnect = time.time()
        if rc == 0:
            log("WARNING", "MQTT disconnected (clean)", log_level)
        else:
            log("WARNING", f"MQTT disconnected rc={rc}", log_level)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    log("INFO", f"Connecting MQTT to {mqtt_host}:{mqtt_port}", log_level)
    connect_mqtt_with_retry(client, mqtt_host, mqtt_port, log_level)

    client.loop_start()

    stop = {"v": False}

    def handle(sig, frame):
        stop["v"] = True

    signal.signal(signal.SIGINT, handle)
    signal.signal(signal.SIGTERM, handle)

    device_id = "turbostat"
    device_name = "Turbostat"
    cols_map: dict[str, str] = {}
    filtered_out_sensor_keys: set[str] = set()
    discovered = False
    last_heartbeat = 0.0
    last_status_line = 0.0
    last_sample_time = 0.0
    # Monotonic mirror of last_sample_time used only by the stall watchdog:
    # the published last_sample_age_s stays on wall clock, but the restart
    # decision must survive an NTP step.
    last_sample_monotonic = 0.0
    first_sample_time = 0.0

    def on_message(_client, _userdata, msg):
        nonlocal discovered
        if msg.payload.decode(errors="replace").strip() == "online":
            log(
                "INFO",
                "HA birth message received — will republish discovery",
                log_level,
            )
            discovered = False

    client.on_message = on_message

    proc: subprocess.Popen | None = None

    try:
        parser = TurbostatParser()
        restart_grace_seconds = max(1.0, min(expire_after_s / 2.0, 30.0))
        last_turbostat_restart_attempt = 0.0
        turbostat_started_at = 0.0
        samples_since_turbostat_start = 0

        def restart_turbostat(reason: str) -> None:
            nonlocal proc, last_sample_time, last_sample_monotonic
            nonlocal first_sample_time
            nonlocal last_turbostat_restart_attempt, turbostat_started_at
            nonlocal samples_since_turbostat_start

            # Monotonic: the restart-grace debounce is a pure duration and
            # must not be defeated by a wall-clock step.
            now_local = time.monotonic()
            if (
                reason != "initial_start"
                and (now_local - last_turbostat_restart_attempt) < restart_grace_seconds
            ):
                log(
                    "WARNING",
                    f"Skipping turbostat restart (grace period) reason={reason}",
                    log_level,
                )
                return

            last_turbostat_restart_attempt = now_local

            try:
                if proc is not None and proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=3)
                    except Exception:
                        proc.kill()
            except Exception as e:
                log("WARNING", f"Error stopping turbostat: {e}", log_level)

            parser.reset()
            first_sample_time = 0.0
            last_sample_time = 0.0
            last_sample_monotonic = 0.0
            samples_since_turbostat_start = 0
            health.last_state_publish_ok = 0.0

            try:
                proc = start_turbostat(interval)
            except FileNotFoundError:
                log(
                    "ERROR",
                    "turbostat not found in container; check package install.",
                    log_level,
                )
                raise
            # Monotonic: only feeds the startup-no-samples restart watchdog,
            # never exposed as a wall-clock timestamp.
            turbostat_started_at = time.monotonic()
            log(
                "INFO",
                f"Started turbostat: interval={interval}s reason={reason}",
                log_level,
            )

        restart_turbostat("initial_start")

        while not stop["v"]:
            now = time.time()
            now_mono = time.monotonic()

            if (
                not health.connected
                and health.last_disconnect > 0
                and (now - health.last_disconnect) > disconnect_timeout
            ):
                log(
                    "ERROR",
                    f"MQTT disconnected for {now - health.last_disconnect:.1f}s (> {disconnect_timeout}s). Exiting for supervisor restart.",
                    log_level,
                )
                return 11

            if (
                samples_since_turbostat_start == 0
                and turbostat_started_at > 0
                and (now_mono - turbostat_started_at) > expire_after_s
            ):
                log(
                    "ERROR",
                    f"No turbostat samples since process start for {now_mono - turbostat_started_at:.1f}s",
                    log_level,
                )
                restart_turbostat("startup_no_samples")

            if (
                samples_since_turbostat_start > 0
                and last_sample_monotonic > 0
                and (now_mono - last_sample_monotonic) > expire_after_s
            ):
                log(
                    "ERROR",
                    f"No turbostat samples for {now_mono - last_sample_monotonic:.1f}s",
                    log_level,
                )
                restart_turbostat("sample_timeout")

            if (
                health.connected
                and last_sample_time > 0
                and (now - last_sample_time) <= max(expire_after_s, interval * 2)
            ):
                if (
                    health.last_state_publish_ok > 0
                    and (now - health.last_state_publish_ok) > expire_after_s
                ):
                    log(
                        "ERROR",
                        "Detected MQTT state publish stall while samples are active. Exiting for supervisor restart.",
                        log_level,
                    )
                    return 12
                if (
                    health.last_state_publish_ok == 0
                    and first_sample_time > 0
                    and (now - first_sample_time) > expire_after_s
                ):
                    log(
                        "ERROR",
                        "No successful MQTT state publish since first sample. Exiting for supervisor restart.",
                        log_level,
                    )
                    return 12

            if now - last_heartbeat >= heartbeat_interval:
                last_heartbeat = now
                hb = {
                    "ts_ms": int(now * 1000),
                    "connected": health.connected,
                    "last_sample_age_s": round(now - last_sample_time, 1)
                    if last_sample_time
                    else None,
                    "state_publish_age_s": round(now - health.last_state_publish_ok, 1)
                    if health.last_state_publish_ok
                    else None,
                }
                mqtt_publish(
                    client,
                    heartbeat_topic,
                    json.dumps(hb, separators=(",", ":")),
                    qos=0,
                    retain=False,
                    log_level=log_level,
                    health=health,
                )

            if proc is None or proc.stdout is None:
                restart_turbostat("stdout_missing")
                time.sleep(0.2)
                continue

            line = proc.stdout.readline()
            if not line:
                rc = proc.poll()
                if rc is not None:
                    log("ERROR", f"turbostat exited rc={rc}", log_level)
                    restart_turbostat("process_eof")
                else:
                    time.sleep(0.05)
                continue

            parsed = parser.parse_line(line)
            if parsed is None:
                continue

            header, values, raw_line = parsed
            now = time.time()

            samples_since_turbostat_start += 1
            last_sample_time = now
            last_sample_monotonic = time.monotonic()
            if first_sample_time == 0.0:
                first_sample_time = now

            if not cols_map:
                all_cols_map = {col: sanitize_key(col) for col in header}
                skip_cols = {
                    "IRQ",
                    "NMI",
                    "SMI",
                    "Pkg%pc2",
                    "Pkg%pc3",
                    "Pkg%pc6",
                    "Pkg%pc8",
                    "Pk%pc10",
                    "CPU%LPI",
                    "SYS%LPI",
                }
                cols_map = {
                    c: k
                    for c, k in all_cols_map.items()
                    if c not in skip_cols and friendly_name(c) != f"Turbostat {c}"
                }
                filtered_out_sensor_keys = {
                    k
                    for c, k in all_cols_map.items()
                    if c not in skip_cols and c not in cols_map
                }
                missing = missing_expected_columns(header)
                if missing:
                    log(
                        "WARNING",
                        f"turbostat is not emitting expected column(s): "
                        f"{missing}. Likely an upstream rename or kernel "
                        "change; HA entities for these will go unavailable. "
                        "Update EXPECTED_COLS and the friendly_name() "
                        "mapping in turbostat_mqtt/metadata.py if the "
                        "column moved.",
                        log_level,
                    )

            payload: dict[str, Any] = {}
            for col, val in values.items():
                if col not in cols_map:
                    continue
                key = cols_map.get(col) or sanitize_key(col)
                scale = COLUMN_SCALES.get(col)
                try:
                    if scale is not None:
                        payload[key] = float(val) * scale
                    elif re.fullmatch(r"[-+]?\d+", val):
                        payload[key] = int(val)
                    else:
                        payload[key] = float(val)
                except Exception:
                    payload[key] = val

            if not discovered and health.connected:
                mqtt_publish(
                    client,
                    availability_topic,
                    "online",
                    qos=1,
                    retain=True,
                    log_level=log_level,
                    health=health,
                )
                for stale_sensor_key in sorted(filtered_out_sensor_keys):
                    mqtt_publish(
                        client,
                        f"{discovery_prefix}/sensor/{device_id}/{stale_sensor_key}/config",
                        "",
                        qos=1,
                        retain=True,
                        log_level=log_level,
                        health=health,
                    )

                if filtered_out_sensor_keys:
                    log(
                        "INFO",
                        f"Removed discovery for {len(filtered_out_sensor_keys)} unmapped columns",
                        log_level,
                    )

                disc = build_discovery_payloads(
                    discovery_prefix=discovery_prefix,
                    device_id=device_id,
                    device_name=device_name,
                    base_topic=base_topic,
                    availability_topic=availability_topic,
                    cols=cols_map,
                    expire_after_s=expire_after_s,
                )
                for t, cfg in disc.items():
                    mqtt_publish(
                        client,
                        t,
                        json.dumps(cfg, separators=(",", ":")),
                        qos=1,
                        retain=False,
                        log_level=log_level,
                        health=health,
                    )

                for sensor_key in cols_map.values():
                    mqtt_publish(
                        client,
                        f"{base_topic}/{sensor_key}/availability",
                        "",
                        qos=1,
                        retain=True,
                        log_level=log_level,
                        health=health,
                    )

                discovered = True
                log(
                    "INFO",
                    f"Published discovery for {len(disc)} sensors",
                    log_level,
                )

            if publish_raw:
                # Keys in `_raw` are the aliased canonical names (matching
                # the regular sensor topics); apply COLUMN_SCALES here so
                # the value scale matches the unit the key implies, same as
                # the main publish loop. `_raw_line` stays verbatim — it's
                # the literal turbostat output, pre-alias and pre-rescale.
                def _raw_value(col: str) -> str:
                    scale = COLUMN_SCALES.get(col)
                    if scale is None:
                        return values[col]
                    try:
                        return str(float(values[col]) * scale)
                    except ValueError:
                        return values[col]

                raw_payload = {
                    "_ts_ms": int(now * 1000),
                    "_raw": {
                        cols_map[c]: _raw_value(c)
                        for c in values.keys()
                        if c in cols_map
                    },
                    "_raw_header": header,
                    "_raw_line": raw_line,
                }
                mqtt_publish(
                    client,
                    raw_topic,
                    json.dumps(raw_payload, separators=(",", ":")),
                    qos=0,
                    retain=False,
                    log_level=log_level,
                    health=health,
                )
            for k, v in payload.items():
                mqtt_publish(
                    client,
                    f"{base_topic}/{k}/state",
                    str(v),
                    qos=0,
                    retain=False,
                    log_level=log_level,
                    health=health,
                    mark_state=True,
                )

            if now - last_status_line >= 10.0:
                last_status_line = now
                bits = []
                for k in ("pkgwatt", "corwatt", "gfxwatt", "ramwatt"):
                    if k in payload:
                        bits.append(f"{k}={payload[k]}")
                log(
                    "INFO",
                    " | ".join(bits) if bits else f"Published {len(payload)} keys",
                    log_level,
                )

    except Exception as e:
        log("ERROR", f"Main loop exception: {e}", log_level)
        return 14
    finally:
        stop["v"] = True
        try:
            mqtt_publish(
                client,
                availability_topic,
                "offline",
                qos=1,
                retain=True,
                log_level=log_level,
                health=health,
            )
            time.sleep(0.2)
        except Exception:
            pass

        try:
            client.loop_stop()
            client.disconnect()
        except Exception:
            pass

        try:
            if proc is not None and proc.poll() is None:
                proc.terminate()
        except Exception:
            pass

    return 0
