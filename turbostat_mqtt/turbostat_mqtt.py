# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

import argparse
import json
import re
import select
import signal
import subprocess
import time
from typing import Any, Dict, Optional, Tuple

import paho.mqtt.client as mqtt


def log(level: str, msg: str, min_level: str = "INFO") -> None:
    order = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}
    if order.get(level, 20) < order.get(min_level, 20):
        return
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"{ts} [{level}] {msg}", flush=True)


def sanitize_key(k: str) -> str:
    k = k.strip()
    k = k.replace("%", "_pct")
    k = k.replace("/", "_per_")
    k = k.replace("-", "_")
    k = re.sub(r"[^A-Za-z0-9_]+", "_", k)
    k = re.sub(r"_+", "_", k).strip("_")
    return k.lower()


def friendly_name(col: str) -> str:
    replacements = {
        "PkgWatt": "CPU Package Power",
        "CorWatt": "CPU Cores Power",
        "GFXWatt": "CPU iGPU Power",
        "RAMWatt": "CPU DRAM Power",
        "PkgTmp": "CPU Package Temperature",
        "Busy%": "CPU Busy",
        "CPU%": "CPU Busy",
        "GFX%": "CPU iGPU Busy",
        "CorTmp": "CPU Cores Temperature",
        "Bzy_MHz": "CPU Busy Frequency",
        "Avg_MHz": "CPU Average Frequency",
        "TSC_MHz": "CPU Time Stamp Counter Frequency",
        "Totl%C0": "CPU Total C0 (Active)",
        "Pkg%pc2": "CPU Package C2 Residency",
        "Pkg%pc3": "CPU Package C3 Residency",
        "Pkg%pc6": "CPU Package C6 Residency",
        "Pkg%pc7": "CPU Package C7 Residency",
        "Pkg%pc8": "CPU Package C8 Residency",
        "Pkg%pc9": "CPU Package C9 Residency",
        "Pkg%pc10": "CPU Package C10 Residency",
        "Pk%pc10": "CPU Package C10 Residency",
        "C1ACPI%": "ACPI C1 Residency",
        "C2ACPI%": "ACPI C2 Residency",
        "C3ACPI%": "ACPI C3 Residency",
        "CPU%c1": "CPU C1 Residency",
        "CPU%c6": "CPU C6 Residency",
        "CPU%c7": "CPU C7 Residency",
        "CPU%LPI": "CPU Low Power Idle Residency",
        "SYS%LPI": "System Low Power Idle Residency",
        "GFX%rc6": "GPU RC6 Residency",
        "GFXAMHz": "GPU Frequency (Actual)",
        "GFXMHz": "GPU Frequency (Requested)",
        "IPC": "Instructions per Cycle",
        "LLCkRPS": "CPU Last-Level Cache References",
        "LLC%hi": "CPU Last-Level Cache Hit Rate",
        "LLC%hit": "CPU Last-Level Cache Hit Rate",
        "IRQ": "Interrupt Rate",
        "NMI": "Non-maskable Interrupt Rate",
        "SMI": "System Management Interrupt Rate",
        "POLL%": "CPU Polling Time",
    }
    return replacements.get(col, f"Turbostat {col}")


def guess_meta(original_col: str) -> Tuple[Optional[str], Optional[str], str, int]:
    col = original_col.strip()

    if "%" in col or col in ("CPU%", "GFX%"):
        return "%", None, "mdi:percent", 1

    if col.lower().endswith("tmp") or "temp" in col.lower():
        return "°C", "temperature", "mdi:thermometer", 0

    if "mhz" in col.lower():
        return "MHz", "frequency", "mdi:sine-wave", 0

    if "watt" in col.lower():
        return "W", "power", "mdi:flash", 1

    if col.lower().endswith("_j") or col.lower().endswith("j"):
        return "J", None, "mdi:counter", 0

    if col.lower().endswith("rps") or "/s" in col.lower() or col.lower().endswith("_s"):
        return "1/s", None, "mdi:chart-line", 0

    if col.lower() in {"sec", "seconds"} or col.lower().endswith("sec"):
        return "s", None, "mdi:timer-outline", 1

    if "irq" in col.lower():
        return None, None, "mdi:chart-line", 0

    return None, None, "mdi:chart-line", 2


class MqttHealth:
    def __init__(self) -> None:
        self.connected: bool = False
        self.last_connect_ok: float = 0.0
        self.last_disconnect: float = 0.0
        self.last_state_publish_ok: float = 0.0


def mqtt_publish(
    client: mqtt.Client,
    topic: str,
    payload: str,
    *,
    qos: int,
    retain: bool,
    log_level: str,
    health: MqttHealth,
    mark_state: bool = False,
) -> bool:
    try:
        info = client.publish(topic, payload=payload, qos=qos, retain=retain)
        if info.rc == mqtt.MQTT_ERR_SUCCESS:
            if mark_state:
                health.last_state_publish_ok = time.time()
            return True
        log("WARNING", f"MQTT publish rc={info.rc} topic={topic}", log_level)
    except Exception as e:
        log("WARNING", f"MQTT publish failed topic={topic}: {e}", log_level)
    return False


def connect_mqtt_with_retry(
    client: mqtt.Client,
    mqtt_host: str,
    mqtt_port: int,
    startup_timeout_s: int,
    log_level: str,
) -> bool:
    delay = 1.0
    deadline = time.time() + float(max(5, startup_timeout_s))
    attempt = 0

    while True:
        attempt += 1
        try:
            client.connect(mqtt_host, mqtt_port, keepalive=60)
            return True
        except Exception as e:
            now = time.time()
            remaining = deadline - now
            if remaining <= 0:
                log(
                    "ERROR",
                    f"Initial MQTT connect failed after {attempt} attempts: {e}",
                    log_level,
                )
                return False
            sleep_s = min(delay, remaining)
            log(
                "WARNING",
                f"Initial MQTT connect attempt {attempt} failed: {e}; retrying in {sleep_s:.1f}s",
                log_level,
            )
            time.sleep(sleep_s)
            delay = min(delay * 2, 30.0)


def build_discovery_payloads(
    discovery_prefix: str,
    device_id: str,
    device_name: str,
    state_topic: str,
    base_topic: str,
    availability_topic: str,
    cols: Dict[str, str],
    sample_timeout_s: int,
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}

    device = {
        "identifiers": [device_id],
        "name": device_name,
        "manufacturer": "turbostat",
        "model": "turbostat summary",
    }

    expire_after = max(5, int(sample_timeout_s))

    for original_col, json_key in cols.items():
        name = friendly_name(original_col)
        unit, device_class, icon, sdp = guess_meta(original_col)

        payload: Dict[str, Any] = {
            "name": name,
            "unique_id": f"{device_id}_{json_key}",
            "state_topic": f"{base_topic}/{json_key}/state",
            "json_attributes_topic": state_topic,
            "icon": icon,
            "device": device,
            "entity_category": "diagnostic",
            "state_class": "measurement",
            "suggested_display_precision": int(sdp),
            "availability_topic": availability_topic,
            "payload_available": "online",
            "payload_not_available": "offline",
            "expire_after": expire_after,
        }

        if unit is not None:
            payload["unit_of_measurement"] = unit
        if device_class is not None:
            payload["device_class"] = device_class

        disc_topic = f"{discovery_prefix}/sensor/{device_id}/{json_key}/config"
        out[disc_topic] = payload

    return out


def start_turbostat(interval_s: float) -> subprocess.Popen:
    cmd = ["turbostat", "--Summary", "--quiet", "--interval", str(interval_s)]
    return subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
    )


class TurbostatParser:
    def __init__(self) -> None:
        self.header: Optional[list[str]] = None
        self.num_re = re.compile(r"^[-+]?\d+(?:\.\d+)?$")

    def reset(self) -> None:
        self.header = None

    def parse_line(
        self, raw_line: str
    ) -> Optional[tuple[list[str], dict[str, str], str]]:
        line = raw_line.rstrip("\n")
        if not line.strip():
            return None

        parts = re.split(r"\s+", line.strip())

        def is_number(s: str) -> bool:
            return self.num_re.match(s) is not None

        if self.header is None:
            if all((not is_number(p)) for p in parts):
                self.header = parts
            return None

        if all((not is_number(p)) for p in parts):
            self.header = parts
            return None

        if len(parts) != len(self.header):
            return None

        values = dict(zip(self.header, parts))
        return self.header, values, line


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--options", required=True)
    args = ap.parse_args()

    with open(args.options, "r", encoding="utf-8") as f:
        opts = json.load(f)

    log_level = (opts.get("log_level") or "INFO").upper()

    interval = max(1.0, float(opts.get("interval_seconds", 10)))
    discovery_prefix = opts.get("mqtt_discovery_prefix", "homeassistant")
    base_topic = (opts.get("mqtt_base_topic") or "turbostat").rstrip("/")

    mqtt_host = opts.get("mqtt_host", "core-mosquitto")
    mqtt_port = int(opts.get("mqtt_port", 1883))
    mqtt_username = opts.get("mqtt_username", "") or ""
    mqtt_password = opts.get("mqtt_password", "") or ""
    client_id = opts.get("client_id") or "turbostat-app"

    publish_raw = bool(opts.get("publish_raw_sample", True))

    heartbeat_interval = max(1, int(opts.get("heartbeat_interval_seconds", 10)))
    disconnect_timeout = max(5, int(opts.get("mqtt_disconnect_timeout_seconds", 300)))
    sample_timeout = max(
        5, int(opts.get("sample_timeout_seconds", max(180, int(interval * 3))))
    )

    state_topic = f"{base_topic}/state"
    availability_topic = f"{base_topic}/availability"
    heartbeat_topic = f"{base_topic}/heartbeat"

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
    if not connect_mqtt_with_retry(
        client, mqtt_host, mqtt_port, disconnect_timeout, log_level
    ):
        return 10

    client.loop_start()

    stop = {"v": False}

    def handle(sig, frame):
        stop["v"] = True

    signal.signal(signal.SIGINT, handle)
    signal.signal(signal.SIGTERM, handle)

    device_id = "turbostat"
    device_name = "Turbostat"

    cols_map: Dict[str, str] = {}
    discovered = False
    last_heartbeat = 0.0
    last_status_line = 0.0
    last_sample_time = 0.0
    first_sample_time = 0.0

    proc: Optional[subprocess.Popen] = None

    try:
        parser = TurbostatParser()
        restart_grace_seconds = max(1.0, min(sample_timeout / 2.0, 30.0))
        last_turbostat_restart_attempt = 0.0
        turbostat_started_at = 0.0
        samples_since_turbostat_start = 0

        def restart_turbostat(reason: str) -> None:
            nonlocal proc, last_sample_time, first_sample_time
            nonlocal last_turbostat_restart_attempt, turbostat_started_at
            nonlocal samples_since_turbostat_start

            now_local = time.time()
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
            samples_since_turbostat_start = 0
            health.last_state_publish_ok = 0.0

            proc = start_turbostat(interval)
            turbostat_started_at = time.time()
            log(
                "INFO",
                f"Started turbostat: interval={interval}s reason={reason}",
                log_level,
            )

        restart_turbostat("initial_start")

        while not stop["v"]:
            now = time.time()

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
                and (now - turbostat_started_at) > sample_timeout
            ):
                log(
                    "ERROR",
                    f"No turbostat samples since process start for {now - turbostat_started_at:.1f}s",
                    log_level,
                )
                restart_turbostat("startup_no_samples")

            if (
                samples_since_turbostat_start > 0
                and last_sample_time > 0
                and (now - last_sample_time) > sample_timeout
            ):
                log(
                    "ERROR",
                    f"No turbostat samples for {now - last_sample_time:.1f}s",
                    log_level,
                )
                restart_turbostat("sample_timeout")

            if (
                health.connected
                and last_sample_time > 0
                and (now - last_sample_time) <= max(sample_timeout, interval * 2)
            ):
                if (
                    health.last_state_publish_ok > 0
                    and (now - health.last_state_publish_ok) > sample_timeout
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
                    and (now - first_sample_time) > sample_timeout
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

            try:
                ready, _, _ = select.select([proc.stdout], [], [], 0.5)
            except Exception as e:
                log("WARNING", f"select() failed: {e}", log_level)
                restart_turbostat("select_failed")
                continue

            if not ready:
                rc = proc.poll()
                if rc is not None:
                    log("ERROR", f"turbostat exited rc={rc}", log_level)
                    restart_turbostat("process_exited")
                continue

            line = proc.stdout.readline()
            if not line:
                rc = proc.poll()
                if rc is not None:
                    log("ERROR", f"turbostat exited rc={rc}", log_level)
                    restart_turbostat("process_eof")
                continue

            parsed = parser.parse_line(line)
            if parsed is None:
                continue

            header, values, raw_line = parsed
            now = time.time()

            samples_since_turbostat_start += 1
            last_sample_time = now
            if first_sample_time == 0.0:
                first_sample_time = now

            if not cols_map:
                cols_map = {col: sanitize_key(col) for col in header}
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
                cols_map = {c: k for c, k in cols_map.items() if c not in skip_cols}

            payload: Dict[str, Any] = {}
            for col, val in values.items():
                if col not in cols_map:
                    continue
                key = cols_map.get(col) or sanitize_key(col)
                try:
                    if re.fullmatch(r"[-+]?\d+", val):
                        payload[key] = int(val)
                    else:
                        payload[key] = float(val)
                except Exception:
                    payload[key] = val

            payload["_ts_ms"] = int(now * 1000)
            if publish_raw:
                payload["_raw"] = {
                    cols_map[c]: values[c] for c in values.keys() if c in cols_map
                }
                payload["_raw_header"] = header
                payload["_raw_line"] = raw_line

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

                disc = build_discovery_payloads(
                    discovery_prefix=discovery_prefix,
                    device_id=device_id,
                    device_name=device_name,
                    state_topic=state_topic,
                    base_topic=base_topic,
                    availability_topic=availability_topic,
                    cols=cols_map,
                    sample_timeout_s=sample_timeout,
                )
                for t, cfg in disc.items():
                    mqtt_publish(
                        client,
                        t,
                        json.dumps(cfg, separators=(",", ":")),
                        qos=1,
                        retain=True,
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
                log("INFO", f"Published discovery for {len(disc)} sensors", log_level)

            mqtt_publish(
                client,
                state_topic,
                json.dumps(payload, separators=(",", ":")),
                qos=0,
                retain=False,
                log_level=log_level,
                health=health,
                mark_state=True,
            )
            for k, v in payload.items():
                if k.startswith("_"):
                    continue
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


if __name__ == "__main__":
    raise SystemExit(main())
