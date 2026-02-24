#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import time
from typing import Any, Dict, Optional, Tuple

import paho.mqtt.client as mqtt


def extract_latest_json_object(buf: str) -> Tuple[Optional[dict], str]:
    """Parse the latest complete dict object from intel_gpu_top -J streaming output."""
    s = buf.lstrip()
    if s.startswith("["):
        s = s[1:]

    dec = json.JSONDecoder()
    i = 0
    last = None
    last_end = None

    def skip(j: int) -> int:
        while j < len(s) and s[j] in " \r\n\t,":
            j += 1
        return j

    i = skip(i)
    while i < len(s):
        try:
            obj, end = dec.raw_decode(s, i)
        except json.JSONDecodeError:
            break
        if isinstance(obj, dict):
            last = obj
            last_end = end
        i = skip(end)

    if last is None or last_end is None:
        return None, buf[-200_000:]

    remaining = s[last_end:]
    return last, remaining[-200_000:]


def safe_float(x: Any) -> Optional[float]:
    if isinstance(x, (int, float)):
        return float(x)
    return None


def dig(d: Dict[str, Any], path: list[str]) -> Any:
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
        if cur is None:
            return None
    return cur


def find_engine_field(raw: Dict[str, Any], engine_name: str, field: str) -> Optional[float]:
    engines = raw.get("engines")
    if isinstance(engines, dict):
        for k, v in engines.items():
            if k.lower() == engine_name.lower() and isinstance(v, dict):
                return safe_float(v.get(field))
    return None


def list_intel_gpu_top_devices(log: logging.Logger) -> str:
    """Return stdout of `intel_gpu_top -L` (or empty string)."""
    try:
        out = subprocess.check_output(
            ["intel_gpu_top", "-L"], text=True, stderr=subprocess.STDOUT, timeout=5
        )
        return out
    except Exception as e:
        log.warning("Failed to list devices with intel_gpu_top -L: %s", e)
        return ""


def auto_select_device_arg(device_listing: str, preferred_regex: str, log: logging.Logger) -> tuple[Optional[str], Optional[str]]:
    """Pick a -d argument for intel_gpu_top based on `intel_gpu_top -L` output.

    Returns:
      (device_arg, render_node_path)
      e.g. ("drm:/dev/dri/renderD128", "/dev/dri/renderD128")
    """
    lines = [ln.strip() for ln in device_listing.splitlines() if ln.strip()]
    render_candidates: list[tuple[str, str]] = []
    for ln in lines:
        m = re.search(r"(/dev/dri/renderD\d+)", ln)
        if m:
            render_candidates.append((ln, m.group(1)))

    if not render_candidates:
        return None, None

    if preferred_regex:
        try:
            rx = re.compile(preferred_regex, re.IGNORECASE)
            for ln, path in render_candidates:
                if rx.search(ln) or rx.search(path):
                    log.info("Auto-selected device by regex '%s': %s", preferred_regex, ln)
                    return f"drm:{path}", path
        except re.error as e:
            log.warning("Invalid preferred_device_regex '%s': %s", preferred_regex, e)

    ln, path = render_candidates[0]
    log.info("Auto-selected first available device: %s", ln)
    return f"drm:{path}", path


def build_metrics(raw: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Return metrics dict keyed by sensor key with fields:
       - value (numeric or None)
       - unit
       - attrs (dict)
       - name (human name)
    """
    ts = time.time()
    common_attrs: Dict[str, Any] = {"ts": ts}

    for k in ["pci_id", "device", "driver", "card", "gt", "timestamp"]:
        v = raw.get(k)
        if v is not None and isinstance(v, (str, int, float)):
            common_attrs[k] = v

    def metric(
        key: str,
        name: str,
        value: Optional[float],
        unit: str,
        extra_attrs: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        attrs = dict(common_attrs)
        if extra_attrs:
            attrs.update(extra_attrs)
        return {"key": key, "name": name, "value": value, "unit": unit, "attrs": attrs}

    # Note: intel_gpu_top JSON schema varies a bit by version.
    # On your system, power keys are capitalized: power.GPU and power.Package.
    rc6 = safe_float(dig(raw, ["rc6", "value"])) or safe_float(raw.get("rc6"))
    freq_actual = safe_float(dig(raw, ["frequency", "actual"]))
    freq_requested = safe_float(dig(raw, ["frequency", "requested"]))

    p_gpu = safe_float(dig(raw, ["power", "GPU"])) or safe_float(dig(raw, ["power", "gpu"]))
    p_pkg = (
        safe_float(dig(raw, ["power", "Package"]))
        or safe_float(dig(raw, ["power", "pkg"]))
        or safe_float(dig(raw, ["power", "package"]))
    )

    metrics: Dict[str, Dict[str, Any]] = {
        "rc6_percent": metric("rc6_percent", "Intel GPU RC6", rc6, "%"),
        "freq_mhz": metric("freq_mhz", "Intel GPU Frequency Actual", freq_actual, "MHz"),
        "freq_requested_mhz": metric("freq_requested_mhz", "Intel GPU Frequency Requested", freq_requested, "MHz"),
        "power_gpu_w": metric("power_gpu_w", "Intel GPU Power", p_gpu, "W"),
        "power_pkg_w": metric("power_pkg_w", "Intel Package Power", p_pkg, "W"),

        # Render/3D
        "engine_render_3d_busy_percent": metric(
            "engine_render_3d_busy_percent",
            "Intel GPU Engine Render/3D Busy",
            find_engine_field(raw, "Render/3D", "busy"),
            "%",
            {"engine": "Render/3D", "field": "busy"},
        ),
        "engine_render_3d_semaphore_percent": metric(
            "engine_render_3d_semaphore_percent",
            "Intel GPU Engine Render/3D Semaphore Wait",
            find_engine_field(raw, "Render/3D", "sema"),
            "%",
            {"engine": "Render/3D", "field": "semaphore_wait"},
        ),
        "engine_render_3d_wait_percent": metric(
            "engine_render_3d_wait_percent",
            "Intel GPU Engine Render/3D Wait",
            find_engine_field(raw, "Render/3D", "wait"),
            "%",
            {"engine": "Render/3D", "field": "wait"},
        ),

        # Video
        "engine_video_busy_percent": metric(
            "engine_video_busy_percent",
            "Intel GPU Engine Video Busy",
            find_engine_field(raw, "Video", "busy"),
            "%",
            {"engine": "Video", "field": "busy"},
        ),
        "engine_video_semaphore_percent": metric(
            "engine_video_semaphore_percent",
            "Intel GPU Engine Video Semaphore Wait",
            find_engine_field(raw, "Video", "sema"),
            "%",
            {"engine": "Video", "field": "semaphore_wait"},
        ),
        "engine_video_wait_percent": metric(
            "engine_video_wait_percent",
            "Intel GPU Engine Video Wait",
            find_engine_field(raw, "Video", "wait"),
            "%",
            {"engine": "Video", "field": "wait"},
        ),

        # VideoEnhance
        "engine_videoenhance_busy_percent": metric(
            "engine_videoenhance_busy_percent",
            "Intel GPU Engine VideoEnhance Busy",
            find_engine_field(raw, "VideoEnhance", "busy"),
            "%",
            {"engine": "VideoEnhance", "field": "busy"},
        ),
        "engine_videoenhance_semaphore_percent": metric(
            "engine_videoenhance_semaphore_percent",
            "Intel GPU Engine VideoEnhance Semaphore Wait",
            find_engine_field(raw, "VideoEnhance", "sema"),
            "%",
            {"engine": "VideoEnhance", "field": "semaphore_wait"},
        ),
        "engine_videoenhance_wait_percent": metric(
            "engine_videoenhance_wait_percent",
            "Intel GPU Engine VideoEnhance Wait",
            find_engine_field(raw, "VideoEnhance", "wait"),
            "%",
            {"engine": "VideoEnhance", "field": "wait"},
        ),

        # Blitter
        "engine_blitter_busy_percent": metric(
            "engine_blitter_busy_percent",
            "Intel GPU Engine Blitter Busy",
            find_engine_field(raw, "Blitter", "busy"),
            "%",
            {"engine": "Blitter", "field": "busy"},
        ),
        "engine_blitter_semaphore_percent": metric(
            "engine_blitter_semaphore_percent",
            "Intel GPU Engine Blitter Semaphore Wait",
            find_engine_field(raw, "Blitter", "sema"),
            "%",
            {"engine": "Blitter", "field": "semaphore_wait"},
        ),
        "engine_blitter_wait_percent": metric(
            "engine_blitter_wait_percent",
            "Intel GPU Engine Blitter Wait",
            find_engine_field(raw, "Blitter", "wait"),
            "%",
            {"engine": "Blitter", "field": "wait"},
        ),
    }

    return metrics



def publish_discovery(
    client: mqtt.Client,
    discovery_prefix: str,
    base_topic: str,
    device_id: str,
    device_name: str,
    metrics: Dict[str, Dict[str, Any]],
    log: logging.Logger,
) -> None:
    device = {
        "identifiers": [device_id],
        "name": device_name,
        "manufacturer": "Intel",
        "model": "intel_gpu_top",
    }

    availability_topic = f"{base_topic}/availability"

    engine_icons = {
        "Render/3D": "mdi:cube-outline",
        "Video": "mdi:video-outline",
        "VideoEnhance": "mdi:video-plus-outline",
        "Blitter": "mdi:image-move",
    }

    for key, m in metrics.items():
        state_topic = f"{base_topic}/{key}/state"
        attr_topic = f"{base_topic}/{key}/attributes"

        payload: Dict[str, Any] = {
            "name": m["name"],
            "unique_id": f"{device_id}_{key}",
            "object_id": f"intel_gpu_{key}",
            "state_topic": state_topic,
            "availability_topic": availability_topic,
            "payload_available": "online",
            "payload_not_available": "offline",
            "unit_of_measurement": m["unit"],
            "json_attributes_topic": attr_topic,
            "device": device,
        }

        # Suggested display precision in Home Assistant UI
        if m["unit"] == "W":
            payload["suggested_display_precision"] = 1
        elif m["unit"] == "%":
            payload["suggested_display_precision"] = 1
        elif m["unit"] == "MHz":
            payload["suggested_display_precision"] = 0

        if m["unit"] == "W":
            payload["device_class"] = "power"
            payload["icon"] = "mdi:flash-outline"
        elif key.startswith("freq_"):
            payload["icon"] = "mdi:speedometer"
        elif key.startswith("rc6_"):
            payload["icon"] = "mdi:sleep"

        # Icon per engine type
        engine = m.get("attrs", {}).get("engine")
        if engine in engine_icons:
            payload["icon"] = engine_icons[engine]

        config_topic = f"{discovery_prefix}/sensor/{device_id}/{key}/config"
        info = client.publish(config_topic, json.dumps(payload), qos=1, retain=True)
        log.debug("MQTT discovery publish %s mid=%s rc=%s", config_topic, info.mid, info.rc)



class MqttHealth:
    def __init__(self) -> None:
        self.connected: bool = False
        self.last_connect_ok: float = 0.0
        self.last_disconnect: float = 0.0


def start_intel_gpu_top(
    interval_ms: int,
    dev_arg: Optional[str],
    log: logging.Logger,
) -> subprocess.Popen:
    cmd = ["intel_gpu_top", "-J", "-s", str(interval_ms), "-o", "-"]
    if dev_arg:
        cmd += ["-d", dev_arg]
    log.info("Starting: %s", " ".join(cmd))
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,  # line-buffered in text mode
        )
    except FileNotFoundError:
        log.error("intel_gpu_top not found in container; check package install.")
        raise
    log.info("intel_gpu_top process started pid=%s", proc.pid)
    return proc


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval-seconds", type=int, default=5)
    ap.add_argument("--mqtt-host", required=True)
    ap.add_argument("--mqtt-port", type=int, default=1883)
    ap.add_argument("--mqtt-username", default="")
    ap.add_argument("--mqtt-password", default="")
    ap.add_argument("--mqtt-discovery-prefix", default="homeassistant")
    ap.add_argument("--mqtt-base-topic", default="intel_gpu")
    ap.add_argument("--client-id", default="intel-gpu-top-addon")
    ap.add_argument("--preferred-device-regex", default="")
    ap.add_argument("--log-level", default="INFO")
    ap.add_argument("--publish-raw-sample", type=lambda s: str(s).lower() in ["1", "true", "yes", "y", "on"], default=True)

    # New health/heartbeat knobs (defaults are sane; you can wire them into add-on options later if desired)
    ap.add_argument("--heartbeat-interval-seconds", type=int, default=10)
    ap.add_argument("--sample-timeout-seconds", type=int, default=20)
    ap.add_argument("--mqtt-disconnect-timeout-seconds", type=int, default=60)
    ap.add_argument("--intel-restart-grace-seconds", type=int, default=10)

    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    log = logging.getLogger("intel_gpu_mqtt")

    interval_s = max(1, args.interval_seconds)
    interval_ms = interval_s * 1000

    # Device selection
    listing = list_intel_gpu_top_devices(log)
    log.info("intel_gpu_top -L output:\n%s", listing if listing else "(none)")
    dev_arg, dev_path = auto_select_device_arg(listing, args.preferred_device_regex, log)
    log.info("Selected device arg: %s", dev_arg or "(none)")
    if dev_path:
        log.info("Selected render node: %s", dev_path)

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
    try:
        client.connect(args.mqtt_host, args.mqtt_port, keepalive=60)
    except Exception as e:
        log.error("Initial MQTT connect failed: %s", e)
        # Let supervisor restart us
        return 10

    client.loop_start()

    # Start intel_gpu_top
    try:
        proc = start_intel_gpu_top(interval_ms, dev_arg, log)
    except FileNotFoundError:
        return 2

    device_id = "intel_gpu_top"
    device_name = "Intel GPU (intel_gpu_top)"

    buf = ""
    discovery_published = False
    last_publish_time = 0.0
    last_heartbeat_time = 0.0
    last_sample_time = 0.0
    last_intel_restart_attempt = 0.0

    def restart_intel_gpu_top(reason: str) -> None:
        nonlocal proc, buf, last_intel_restart_attempt, dev_arg, dev_path, listing
        now = time.time()
        if now - last_intel_restart_attempt < args.intel_restart_grace_seconds:
            log.warning("Skipping intel_gpu_top restart (grace period) reason=%s", reason)
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
        dev_arg, dev_path = auto_select_device_arg(listing, args.preferred_device_regex, log)
        log.info("Re-selected device arg: %s", dev_arg or "(none)")
        if dev_path:
            log.info("Re-selected render node: %s", dev_path)

        buf = ""
        proc = start_intel_gpu_top(interval_ms, dev_arg, log)

    try:
        assert proc.stdout is not None

        while True:
            # ----- Watchdogs -----

            now = time.time()

            # GPU disappearance / device node check
            if dev_path is not None and not os.path.exists(dev_path):
                log.error("GPU render node disappeared: %s", dev_path)
                restart_intel_gpu_top("render_node_disappeared")

            # Sample timeout watchdog
            if last_sample_time > 0 and (now - last_sample_time) > args.sample_timeout_seconds:
                log.error("No intel_gpu_top samples for %.1fs", now - last_sample_time)
                # Try restart once; if it keeps failing, we'll exit via repeated timeout
                restart_intel_gpu_top("sample_timeout")

            # MQTT disconnect watchdog: exit nonzero so add-on supervisor restarts us
            if not health.connected and health.last_disconnect > 0:
                if (now - health.last_disconnect) > args.mqtt_disconnect_timeout_seconds:
                    log.error(
                        "MQTT disconnected for %.1fs (> %ss). Exiting for supervisor restart.",
                        now - health.last_disconnect,
                        args.mqtt_disconnect_timeout_seconds,
                    )
                    return 11

            # Heartbeat publish (independent of samples)
            if now - last_heartbeat_time >= args.heartbeat_interval_seconds:
                last_heartbeat_time = now
                hb_payload = json.dumps(
                    {
                        "ts": now,
                        "mqtt_connected": health.connected,
                        "last_sample_age_s": (now - last_sample_time) if last_sample_time else None,
                        "device": dev_path,
                    }
                )
                info = client.publish(f"{base_topic}/heartbeat", hb_payload, qos=0, retain=False)
                log.debug("Heartbeat publish mid=%s rc=%s payload=%s", info.mid, info.rc, hb_payload)

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
                metrics = build_metrics(obj)
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
                        log,
                    )
                    discovery_published = True

                # Rate-limit publishing to once per interval_seconds
                now = time.time()
                if now - last_publish_time < interval_s:
                    continue
                last_publish_time = now

                # Publish each sensor to its own state topic, plus attributes topic
                for key, m in metrics.items():
                    val = m["value"]

                    # Always update attributes (ts, etc.)
                    attr_topic = f"{base_topic}/{key}/attributes"
                    ainfo = client.publish(attr_topic, json.dumps(m["attrs"]), qos=0, retain=False)
                    log.debug("MQTT attrs %s mid=%s rc=%s", attr_topic, ainfo.mid, ainfo.rc)

                    if val is None:
                        continue

                    state_topic = f"{base_topic}/{key}/state"
                    # Publish full-precision numeric value; HA can format display using suggested_display_precision.
                    payload = repr(float(val))
                    sinfo = client.publish(state_topic, payload, qos=0, retain=False)
                    log.debug("MQTT state %s=%s mid=%s rc=%s", state_topic, payload, sinfo.mid, sinfo.rc)

                if args.publish_raw_sample:
                    # Publish a raw sample snapshot for debugging (non-discovery)
                    raw_topic = f"{base_topic}/raw_sample"
                    rinfo = client.publish(raw_topic, json.dumps(obj)[:200000], qos=0, retain=False)
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

    except KeyboardInterrupt:
        log.info("Shutting down (SIGINT)")
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


if __name__ == "__main__":
    sys.exit(main())
