#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
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


def find_engine_busy(raw: Dict[str, Any], engine_name: str) -> Optional[float]:
    engines = raw.get("engines")
    if isinstance(engines, dict):
        for k, v in engines.items():
            if k.lower() == engine_name.lower() and isinstance(v, dict):
                return safe_float(v.get("busy"))
    return None


def list_intel_gpu_top_devices(log: logging.Logger) -> str:
    """Return stdout of `intel_gpu_top -L` (or empty string)."""
    try:
        out = subprocess.check_output(["intel_gpu_top", "-L"], text=True, stderr=subprocess.STDOUT, timeout=5)
        return out
    except Exception as e:
        log.warning("Failed to list devices with intel_gpu_top -L: %s", e)
        return ""


def auto_select_device_arg(device_listing: str, preferred_regex: str, log: logging.Logger) -> Optional[str]:
    """Pick a -d argument for intel_gpu_top based on `intel_gpu_top -L` output.

    Strategy:
      1) If preferred_regex provided, choose first line matching it that also contains /dev/dri/renderD*
      2) Else choose first /dev/dri/renderD* found.
    Returns something like: 'drm:/dev/dri/renderD128' or None.
    """
    lines = [ln.strip() for ln in device_listing.splitlines() if ln.strip()]
    render_candidates = []
    for ln in lines:
        m = re.search(r"(/dev/dri/renderD\d+)", ln)
        if m:
            render_candidates.append((ln, m.group(1)))

    if not render_candidates:
        return None

    if preferred_regex:
        try:
            rx = re.compile(preferred_regex, re.IGNORECASE)
            for ln, path in render_candidates:
                if rx.search(ln) or rx.search(path):
                    log.info("Auto-selected device by regex '%s': %s", preferred_regex, ln)
                    return f"drm:{path}"
        except re.error as e:
            log.warning("Invalid preferred_device_regex '%s': %s", preferred_regex, e)

    ln, path = render_candidates[0]
    log.info("Auto-selected first available device: %s", ln)
    return f"drm:{path}"


def build_metrics(raw: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Return metrics dict keyed by sensor key with fields:
       - value (numeric or None)
       - unit
       - attrs (dict)
       - name (human name)
    """
    # base attributes that apply to all sensors
    ts = time.time()
    common_attrs = {
        "ts": ts,
    }

    # Some versions include a 'name'/'device' field; keep a subset as attributes
    for k in ["pci_id", "device", "driver", "card", "gt", "timestamp"]:
        v = raw.get(k)
        if v is not None and isinstance(v, (str, int, float)):
            common_attrs[k] = v

    def metric(key: str, name: str, value: Optional[float], unit: str, extra_attrs: Optional[Dict[str, Any]] = None):
        attrs = dict(common_attrs)
        if extra_attrs:
            attrs.update(extra_attrs)
        return {
            "key": key,
            "name": name,
            "value": value,
            "unit": unit,
            "attrs": attrs,
        }

    gpu_busy = safe_float(dig(raw, ["gpu", "busy"])) or safe_float(raw.get("busy"))
    rc6 = safe_float(dig(raw, ["rc6", "value"])) or safe_float(raw.get("rc6"))
    freq = safe_float(dig(raw, ["frequency", "actual"]))
    p_gpu = safe_float(dig(raw, ["power", "gpu"]))
    p_pkg = safe_float(dig(raw, ["power", "pkg"]))

    metrics = {
        "gpu_busy_percent": metric("gpu_busy_percent", "Intel GPU Busy", gpu_busy, "%"),
        "rc6_percent": metric("rc6_percent", "Intel GPU RC6", rc6, "%"),
        "freq_mhz": metric("freq_mhz", "Intel GPU Frequency", freq, "MHz"),
        "power_gpu_w": metric("power_gpu_w", "Intel GPU Power", p_gpu, "W"),
        "power_pkg_w": metric("power_pkg_w", "Intel Package Power", p_pkg, "W"),
        "engine_render_3d_busy_percent": metric(
            "engine_render_3d_busy_percent",
            "Intel GPU Engine Render/3D Busy",
            find_engine_busy(raw, "Render/3D"),
            "%",
            {"engine": "Render/3D"},
        ),
        "engine_video_busy_percent": metric(
            "engine_video_busy_percent",
            "Intel GPU Engine Video Busy",
            find_engine_busy(raw, "Video"),
            "%",
            {"engine": "Video"},
        ),
        "engine_videoenhance_busy_percent": metric(
            "engine_videoenhance_busy_percent",
            "Intel GPU Engine VideoEnhance Busy",
            find_engine_busy(raw, "VideoEnhance"),
            "%",
            {"engine": "VideoEnhance"},
        ),
        "engine_blitter_busy_percent": metric(
            "engine_blitter_busy_percent",
            "Intel GPU Engine Blitter Busy",
            find_engine_busy(raw, "Blitter"),
            "%",
            {"engine": "Blitter"},
        ),
    }

    # Add a few additional raw fields as attributes if present
    # Keep it small and useful.
    if isinstance(raw.get("engines"), dict):
        metrics["gpu_busy_percent"]["attrs"]["engines_present"] = list(raw["engines"].keys())

    return metrics


def publish_discovery(
    client: mqtt.Client,
    discovery_prefix: str,
    base_topic: str,
    device_id: str,
    device_name: str,
    metrics: Dict[str, Dict[str, Any]],
) -> None:
    device = {
        "identifiers": [device_id],
        "name": device_name,
        "manufacturer": "Intel",
        "model": "intel_gpu_top",
    }

    availability_topic = f"{base_topic}/availability"

    for key, m in metrics.items():
        # Only publish discovery for metrics we can actually produce (even if value None sometimes)
        state_topic = f"{base_topic}/{key}/state"
        attr_topic = f"{base_topic}/{key}/attributes"

        payload = {
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

        # Provide some hints for HA where applicable
        if m["unit"] == "W":
            payload["device_class"] = "power"

        config_topic = f"{discovery_prefix}/sensor/{device_id}/{key}/config"
        client.publish(config_topic, json.dumps(payload), qos=1, retain=True)


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
    ap.add_argument("--publish-raw-sample", type=lambda s: str(s).lower() in ["1","true","yes","y","on"], default=True)
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    log = logging.getLogger("intel_gpu_mqtt")

    interval_ms = max(1, args.interval_seconds) * 1000

    listing = list_intel_gpu_top_devices(log)
    dev_arg = auto_select_device_arg(listing, args.preferred_device_regex, log)

    cmd = ["intel_gpu_top", "-J", "-s", str(interval_ms), "-o", "-"]
    if dev_arg:
        cmd += ["-d", dev_arg]
    else:
        log.warning("No /dev/dri/renderD* found via intel_gpu_top -L; running without -d.")

    log.info("Starting: %s", " ".join(cmd))

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=0,
        )
    except FileNotFoundError:
        log.error("intel_gpu_top not found in container; check Dockerfile package install.")
        return 2

    device_id = "intel_gpu_top"
    device_name = "Intel GPU (intel_gpu_top)"
    base_topic = args.mqtt_base_topic

    client = mqtt.Client(client_id=args.client_id, clean_session=True)
    if args.mqtt_username:
        client.username_pw_set(args.mqtt_username, args.mqtt_password)

    client.will_set(f"{base_topic}/availability", "offline", qos=1, retain=True)

    log.info("Connecting MQTT to %s:%d", args.mqtt_host, args.mqtt_port)
    client.connect(args.mqtt_host, args.mqtt_port, keepalive=60)
    client.loop_start()

    client.publish(f"{base_topic}/availability", "online", qos=1, retain=True)

    buf = ""
    discovery_published = False
    last_publish_time = 0.0

    try:
        assert proc.stdout is not None
        while True:
            chunk = proc.stdout.read(4096)
            if chunk:
                buf += chunk
                obj, buf = extract_latest_json_object(buf)
                if not obj:
                    continue

                metrics = build_metrics(obj)

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
                    )
                    discovery_published = True

                # Rate-limit publishing to once per interval_seconds
                now = time.time()
                if now - last_publish_time < args.interval_seconds:
                    continue
                last_publish_time = now

                # Publish each sensor to its own state topic, plus attributes topic
                for key, m in metrics.items():
                    val = m["value"]
                    if val is None:
                        # Don't spam 'unknown'; just skip state update but still update attrs timestamp
                        client.publish(f"{base_topic}/{key}/attributes", json.dumps(m["attrs"]), qos=0, retain=False)
                        continue

                    client.publish(f"{base_topic}/{key}/state", f"{val:.3f}".rstrip("0").rstrip("."), qos=0, retain=False)
                    client.publish(f"{base_topic}/{key}/attributes", json.dumps(m["attrs"]), qos=0, retain=False)

                if args.publish_raw_sample:
                    # Publish a raw sample snapshot for debugging (non-discovery)
                    client.publish(f"{base_topic}/raw_sample", json.dumps(obj)[:200000], qos=0, retain=False)

            else:
                rc = proc.poll()
                if rc is not None:
                    err = ""
                    if proc.stderr is not None:
                        err = proc.stderr.read()[-4000:]
                    log.error("intel_gpu_top exited rc=%s stderr_tail=%s", rc, err)
                    return 3
                time.sleep(0.1)

    except KeyboardInterrupt:
        log.info("Shutting down")
    finally:
        try:
            client.publish(f"{base_topic}/availability", "offline", qos=1, retain=True)
        except Exception:
            pass
        client.loop_stop()
        try:
            proc.terminate()
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    sys.exit(main())
