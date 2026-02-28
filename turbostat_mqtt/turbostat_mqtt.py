#!/usr/bin/env python3

# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

import argparse
import json
import queue
import re
import signal
import subprocess
import threading
import time
from dataclasses import dataclass
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

    if col.lower().endswith("sec") or col.lower().endswith("s"):
        return "s", None, "mdi:timer-outline", 1

    if "irq" in col.lower():
        if "/s" in col.lower() or col.lower().endswith("_s"):
            return "1/s", None, "mdi:chart-line", 0
        return None, None, "mdi:chart-line", 0

    return None, None, "mdi:chart-line", 2


@dataclass
class MqttCfg:
    host: str
    port: int
    username: str
    password: str
    client_id: str


class MqttPublisher:
    def __init__(
        self,
        cfg: MqttCfg,
        retain: bool,
        queue_max: int,
        disconnect_timeout_s: int,
        log_level: str,
    ) -> None:
        self.cfg = cfg
        self.retain = retain
        self.disconnect_timeout_s = max(5, int(disconnect_timeout_s))
        self.log_level = log_level

        self._client = mqtt.Client(client_id=self.cfg.client_id, clean_session=True)
        if cfg.username:
            self._client.username_pw_set(cfg.username, cfg.password)

        self._connected = threading.Event()
        self._stop = threading.Event()

        # Queue holds (topic, payload, retain, enqueued_ts)
        self._q: "queue.Queue[Tuple[str, str, bool, float]]" = queue.Queue(maxsize=max(queue_max, 1))

        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect

        self._loop_thread = threading.Thread(target=self._loop, daemon=True)
        self._drain_thread = threading.Thread(target=self._drain, daemon=True)

    def start(self) -> None:
        self._loop_thread.start()
        self._drain_thread.start()

    def stop(self) -> None:
        self._stop.set()
        try:
            self._client.disconnect()
        except Exception:
            pass

    def is_connected(self) -> bool:
        return self._connected.is_set()

    def wait_connected(self, timeout: float = 10.0) -> bool:
        return self._connected.wait(timeout)

    def publish(self, topic: str, payload: str, retain: Optional[bool] = None) -> None:
        if retain is None:
            retain = self.retain
        item = (topic, payload, retain, time.time())
        try:
            self._q.put_nowait(item)
        except queue.Full:
            # Drop oldest then retry once
            try:
                _ = self._q.get_nowait()
            except queue.Empty:
                pass
            try:
                self._q.put_nowait(item)
            except queue.Full:
                pass

    def _on_connect(self, client: mqtt.Client, userdata: Any, flags: Dict[str, Any], rc: int) -> None:
        if rc == 0:
            self._connected.set()
            log("INFO", f"MQTT connected to {self.cfg.host}:{self.cfg.port}", self.log_level)
        else:
            log("ERROR", f"MQTT connect failed rc={rc}", self.log_level)

    def _on_disconnect(self, client: mqtt.Client, userdata: Any, rc: int) -> None:
        self._connected.clear()
        if self._stop.is_set():
            return
        log("WARNING", f"MQTT disconnected rc={rc}", self.log_level)

    def _loop(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                self._client.connect(self.cfg.host, self.cfg.port, keepalive=30)
                self._client.loop_forever(retry_first_connection=True)
            except Exception as e:
                log("WARNING", f"MQTT loop error: {e} (retry in {backoff:.1f}s)", self.log_level)
                time.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    def _drain(self) -> None:
        while not self._stop.is_set():
            try:
                topic, payload, retain, enq_ts = self._q.get(timeout=0.5)
            except queue.Empty:
                continue

            # Drop stale messages if we've been disconnected too long
            if not self._connected.is_set() and (time.time() - enq_ts) > self.disconnect_timeout_s:
                continue

            if not self._connected.is_set():
                # Requeue best-effort
                self.publish(topic, payload, retain)
                time.sleep(1.0)
                continue

            try:
                self._client.publish(topic, payload=payload, qos=0, retain=retain)
            except Exception as e:
                log("WARNING", f"MQTT publish failed: {e}", self.log_level)
                self.publish(topic, payload, retain)
                time.sleep(1.0)


def build_discovery_payloads(
    discovery_prefix: str,
    device_id: str,
    device_name: str,
    state_topic: str,
    base_topic: str,
    availability_topics: Dict[str, str],
    cols: Dict[str, str],
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}

    device = {
        "identifiers": [device_id],
        "name": device_name,
        "manufacturer": "turbostat",
        "model": "turbostat summary",
    }

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
            "availability_topic": availability_topics.get(json_key, ""),
            "payload_available": "online",
            "payload_not_available": "offline",
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


def iter_samples(proc: subprocess.Popen):
    header: Optional[list[str]] = None
    num_re = re.compile(r"^[-+]?\d+(?:\.\d+)?$")

    assert proc.stdout is not None
    for line in proc.stdout:
        line = line.rstrip("\n")
        if not line.strip():
            continue
        parts = re.split(r"\s+", line.strip())

        def is_number(s: str) -> bool:
            return num_re.match(s) is not None

        if header is None:
            if all((not is_number(p)) for p in parts):
                header = parts
            continue

        if all((not is_number(p)) for p in parts):
            header = parts
            continue

        if len(parts) != len(header):
            continue

        values = dict(zip(header, parts))
        yield header, values, line


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--options", required=True)
    args = ap.parse_args()

    with open(args.options, "r", encoding="utf-8") as f:
        opts = json.load(f)

    log_level = (opts.get("log_level") or "INFO").upper()

    interval = float(opts.get("interval_seconds", 10))
    discovery_prefix = opts.get("mqtt_discovery_prefix", "homeassistant")
    base_topic = (opts.get("mqtt_base_topic") or "turbostat").rstrip("/")

    client_id = opts.get("client_id") or "turbostat-app"

    mqtt_cfg = MqttCfg(
        host=opts.get("mqtt_host", "core-mosquitto"),
        port=int(opts.get("mqtt_port", 1883)),
        username=opts.get("mqtt_username", "") or "",
        password=opts.get("mqtt_password", "") or "",
        client_id=client_id,
    )

    publish_raw = bool(opts.get("publish_raw_sample", True))

    heartbeat_interval = int(opts.get("heartbeat_interval_seconds", 10))
    disconnect_timeout = int(opts.get("mqtt_disconnect_timeout_seconds", 60))

    state_topic = f"{base_topic}/state"
    availability_topic = f"{base_topic}/availability"
    availability_topics: Dict[str, str] = {}
    heartbeat_topic = f"{base_topic}/heartbeat"

    pub = MqttPublisher(
        cfg=mqtt_cfg,
        retain=True,
        queue_max=500,
        disconnect_timeout_s=disconnect_timeout,
        log_level=log_level,
    )
    pub.start()

    stop = {"v": False}

    def handle(sig, frame):
        stop["v"] = True

    signal.signal(signal.SIGINT, handle)
    signal.signal(signal.SIGTERM, handle)

    device_id = "turbostat"
    device_name = "Turbostat"

    cols_map: Dict[str, str] = {}
    discovered = False
    last_sample_ts = 0.0
    last_heartbeat = 0.0
    last_status_line = 0.0

    proc = start_turbostat(interval)
    log("INFO", f"Started turbostat: interval={interval}s", log_level)

    try:
        for header, values, raw_line in iter_samples(proc):
            if stop["v"]:
                break

            now = time.time()
            last_sample_ts = now

            if not cols_map:
                cols_map = {col: sanitize_key(col) for col in header}
            skip_cols = {'IRQ', 'NMI', 'SMI', 'Pkg%pc2', 'Pkg%pc3', 'Pkg%pc6', 'Pkg%pc8', 'Pk%pc10', 'CPU%LPI', 'SYS%LPI'}
            cols_map = {c: k for c, k in cols_map.items() if c not in skip_cols}
                availability_topics = {k: f"{base_topic}/{k}/availability" for k in cols_map.values()}

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
                payload["_raw"] = {cols_map[c]: values[c] for c in values.keys() if c in cols_map}
                payload["_raw_header"] = header
            if publish_raw:
                payload["_raw_line"] = raw_line

            # Publish discovery once connected and we have columns
            if not discovered and pub.wait_connected(0.1):
                # set availability online
                pub.publish(availability_topic, "online", retain=True)
                for t in availability_topics.values():
                    pub.publish(t, "online", retain=True)

                disc = build_discovery_payloads(
                    discovery_prefix=discovery_prefix,
                    device_id=device_id,
                    device_name=device_name,
                    state_topic=state_topic,
                    base_topic=base_topic,
                    availability_topics=availability_topics,
                    cols=cols_map,
                )
                for t, cfg in disc.items():
                    pub.publish(t, json.dumps(cfg, separators=(",", ":")), retain=True)
                discovered = True
                log("INFO", f"Published discovery for {len(disc)} sensors", log_level)

            pub.publish(state_topic, json.dumps(payload, separators=(",", ":")), retain=True)
            for k, v in payload.items():
                if k.startswith("_"):
                    continue
                pub.publish(f"{base_topic}/{k}/state", str(v), retain=True)

            # Heartbeat
            if now - last_heartbeat >= heartbeat_interval:
                last_heartbeat = now
                hb = {"ts_ms": int(now * 1000), "connected": pub.is_connected()}
                pub.publish(heartbeat_topic, json.dumps(hb, separators=(",", ":")), retain=True)

            # CLI status (like intel_gpu_top add-on)
            if now - last_status_line >= 10.0:
                last_status_line = now
                bits = []
                for k in ("pkgwatt", "corwatt", "gfxwatt", "ramwatt"):
                    if k in payload:
                        bits.append(f"{k}={payload[k]}")
                log("INFO", " | ".join(bits) if bits else f"Published {len(payload)} keys", log_level)

            # Stall watchdog: handled outside loop by periodic check

            # Non-blocking stall check: if turbostat stops producing output, loop will block.
            # We'll rely on external watchdog thread to restart it.
    except Exception as e:
        log("ERROR", f"Main loop exception: {e}", log_level)
    finally:
        stop["v"] = True

    # Clean shutdown
    try:
        pub.publish(availability_topic, "offline", retain=True)
        time.sleep(0.2)
    except Exception:
        pass
    try:
        if proc.poll() is None:
            proc.terminate()
    except Exception:
        pass
    pub.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
