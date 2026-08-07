# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Poll halpid and publish HALPI2 power telemetry as MQTT discovery entities."""

import argparse
import datetime
import json
import logging
import os
import signal
import sys
import time
from types import FrameType
from typing import Any

import paho.mqtt.client as mqtt

from . import __version__
from .config import (
    load_options_file,
    redact_options_for_log,
    resolve_mqtt_config,
)
from .halpid_client import HalpidError, get_values
from .mqtt import (
    MqttHealth,
    availability_topic,
    publish_all_discovery,
    state_topic,
)
from .sensors import (
    DERIVED_DEFS,
    POWER_OUTAGE_ELAPSED_KEY,
    POWER_STATE_DEF,
    POWER_STATE_KEY,
    SENSOR_DEFS,
    SHUTDOWN_IN_KEY,
    extract,
)

log = logging.getLogger(__name__)

# How often halpid is read, independent of how often we publish.
#
# Detection latency must be well under power_outage_time_limit or a power outage can
# begin and end between two reads: halpid runs a 100 ms loop and shuts down
# after power_outage_time_limit (5 s by default), so a 10 s read interval would miss
# roughly half of all events entirely. Reading is cheap -- one extra I2C
# transaction against a daemon already polling ten times a second.
POLL_INTERVAL_S = 0.5

# Supercapacitor discharge trace, written only while on cap power.
#
# The decay curve is the only way to learn the real hold-up time and effective
# capacitance under this installation's actual load -- the datasheet figure (6.25 F at
# 10.8 V) says nothing about conversion efficiency or the true cutoff. It goes
# to /share because the machine may be hard-cut at the end of the very event
# being measured, and MQTT/recorder history could be lost in that cut.
TRACE_PATH = "/share/halpi2_power/power-outage-trace.csv"

_running = True


def _handle_signal(signum: int, _frame: FrameType | None) -> None:
    global _running
    log.info("Received signal %d; shutting down", signum)
    _running = False


def configure_logging(level: str) -> None:
    levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "WARN": logging.WARNING,
        "ERROR": logging.ERROR,
    }
    logging.basicConfig(
        level=levels.get(level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def append_trace(
    elapsed: float, values: dict[str, Any], phase: str = "discharge"
) -> None:
    """Append one trace sample. Best effort -- never break the loop for it.

    phase "baseline" is the single pre-cut nominal row (its V_in/I_in give the
    load power); "discharge" is the on-cap samples.
    """
    try:
        new = not os.path.exists(TRACE_PATH)
        with open(TRACE_PATH, "a", encoding="utf-8") as fh:
            if new:
                fh.write("iso_time,phase,elapsed_s,V_in,V_cap,I_in,state\n")
            ts = datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds")
            fh.write(
                f"{ts},{phase},{elapsed:.1f},"
                f"{values.get('V_in')},{values.get('V_cap')},"
                f"{values.get('I_in')},{values.get('state')}\n"
            )
            fh.flush()
            os.fsync(fh.fileno())
    except OSError:
        pass


def in_power_outage(values: dict[str, Any], voltage_limit: float) -> bool:
    """Is the boat currently on supercapacitor power?

    True on either signal: the controller reporting a power-outage state (matched
    by the daemon's state-name prefix) or V_in below the threshold.
    """
    if str(values.get("state", "")).startswith("Blackout"):
        return True
    v_in = values.get("V_in")
    return isinstance(v_in, (int, float)) and float(v_in) < voltage_limit


def render_state(value: Any) -> str:
    """Format a value for an MQTT state topic."""
    if isinstance(value, float):
        s = f"{value:.4f}".rstrip("0").rstrip(".")
        # A tiny negative reading (e.g. I_in=-0.00004) formats to "-0"; publish
        # a plain "0" instead.
        return "0" if s == "-0" else s
    return str(value)


def build_client(
    mqtt_cfg: dict[str, Any],
    client_id: str,
    base_topic: str,
    discovery_prefix: str,
    health: MqttHealth,
) -> mqtt.Client:
    client = mqtt.Client(client_id=client_id, clean_session=True)
    if mqtt_cfg["username"]:
        client.username_pw_set(mqtt_cfg["username"], mqtt_cfg["password"])

    # The broker publishes this if we drop off without a clean disconnect, so
    # entities go unavailable instead of showing stale power readings.
    client.will_set(availability_topic(base_topic), "offline", qos=1, retain=True)
    # Cap reconnect back-off at 30 s (paho's default runs up to 120 s).
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    status_topic = f"{discovery_prefix}/status"

    def on_connect(
        _client: mqtt.Client, _userdata: Any, _flags: Any, rc: int, *_: Any
    ) -> None:
        if rc == 0:
            health.connected = True
            health.last_connect_ok = time.time()
            log.info("Connected to MQTT broker")
            # Re-assert availability on every (re)connect, and watch HA's birth
            # topic so a Home Assistant restart triggers a discovery republish.
            _client.subscribe(status_topic, qos=1)
            _client.publish(
                availability_topic(base_topic), "online", qos=1, retain=True
            )
        else:
            log.error("MQTT connection failed (rc=%s)", rc)

    def on_disconnect(_client: mqtt.Client, _userdata: Any, rc: int, *_: Any) -> None:
        health.connected = False
        health.last_disconnect = time.time()
        log.warning("Disconnected from MQTT broker (rc=%s)", rc)

    def on_message(_client: mqtt.Client, _userdata: Any, msg: mqtt.MQTTMessage) -> None:
        # HA's birth message ("online" on {prefix}/status) means it just
        # (re)started and dropped our retained discovery configs; flag a
        # republish so the entities come back without an add-on restart.
        if (
            msg.topic == status_topic
            and msg.payload.decode(errors="replace").strip() == "online"
        ):
            log.info("HA birth message received; will republish discovery")
            health.rediscover = True

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    return client


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="halpi2_power")
    ap.add_argument("--options", default="/data/options.json")
    ap.add_argument("--socket", default="/run/halpid/halpid.sock")
    args = ap.parse_args(argv)

    opts = load_options_file(args.options, ap)
    configure_logging(str(opts.get("log_level") or "INFO"))

    log.info("HALPI2 Power MQTT publisher v%s starting", __version__)
    if str(opts.get("mode") or "coop") == "solo":
        log.warning(
            "mode=solo: telemetry only. The controller manages power on its own "
            "thresholds and a power outage will end in an abrupt power cut."
        )
    log.info("Options:")
    for line in json.dumps(
        redact_options_for_log(opts), indent=2, sort_keys=True
    ).splitlines():
        log.info("  %s", line)

    mode = str(opts.get("mode") or "coop")
    interval = int(opts.get("interval_seconds") or 10)
    power_outage_voltage_limit = float(opts.get("power_outage_voltage_limit") or 9.0)
    power_outage_time_limit = float(opts.get("power_outage_time_limit") or 5.0)
    if interval < power_outage_time_limit:
        log.debug(
            "Publish interval (%ss) is shorter than power_outage_time_limit (%ss)",
            interval,
            power_outage_time_limit,
        )
    discovery_prefix = str(opts.get("mqtt_discovery_prefix") or "homeassistant")
    base_topic = str(opts.get("mqtt_base_topic") or "halpi2_power")
    client_id = str(opts.get("client_id") or "halpi2-power")
    expire_multiplier = int(opts.get("expire_after_multiplier") or 4)
    expire_after_s = max(5, interval * expire_multiplier)

    try:
        mqtt_cfg = resolve_mqtt_config(opts)
    except RuntimeError as exc:
        log.error("%s", exc)
        return 1

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    health = MqttHealth()
    client = build_client(mqtt_cfg, client_id, base_topic, discovery_prefix, health)

    # Retry the initial connect with back-off: the add-on can start before the
    # Mosquitto add-on is up, and exiting on the first failure would just make s6
    # crash-loop us. loop_start() handles reconnects after this first success.
    delay = 5
    while _running:
        try:
            client.connect(mqtt_cfg["host"], mqtt_cfg["port"], keepalive=60)
            break
        except OSError as exc:
            log.warning(
                "Cannot connect to MQTT broker %s:%s: %s -- retrying in %ds",
                mqtt_cfg["host"],
                mqtt_cfg["port"],
                exc,
                delay,
            )
            # Sleep in 1 s slices so a SIGTERM during back-off is honored
            # promptly instead of blocking for up to `delay` seconds.
            for _ in range(delay):
                if not _running:
                    break
                time.sleep(1)
            delay = min(delay * 2, 60)
    if not _running:
        return 0

    client.loop_start()

    discovery_published = False
    device_id = ""
    reads_without_device_id = 0
    consecutive_errors = 0
    marked_offline = False
    outage_since: float | None = None
    last_publish = 0.0
    # Most recent nominal (pre-power outage) reading. Captured as the trace baseline
    # so the load power at the moment of the cut is recoverable from the CSV.
    last_nominal: dict[str, Any] | None = None

    try:
        while _running:
            loop_started = time.monotonic()

            try:
                values = get_values(args.socket)
                consecutive_errors = 0
                if marked_offline:
                    # halpid readable again -- re-assert availability now rather
                    # than waiting for the next publish tick (up to `interval`s).
                    client.publish(
                        availability_topic(base_topic), "online", qos=1, retain=True
                    )
                    marked_offline = False
                    log.info("halpid readable again; entities back online")
            except HalpidError as exc:
                consecutive_errors += 1
                # halpid owns the I2C bus and exits on fatal errors; its own
                # service restart is the real recovery path. Log and keep
                # trying so a transient blip doesn't take the publisher down.
                log.warning("Could not read halpid values: %s", exc)
                if consecutive_errors == 3:
                    client.publish(
                        availability_topic(base_topic), "offline", qos=1, retain=True
                    )
                    marked_offline = True
                    log.error(
                        "halpid unreadable %d times; marked entities unavailable",
                        consecutive_errors,
                    )
                # Fixed 2 s, not max(1, interval): a longer back-off could reach
                # the watchdog timeout during a coop depletion measurement and
                # truncate the discharge curve.
                time.sleep(2)
                continue

            # Power outage countdown. halpid keeps its own timer internally and
            # does not expose it, so this is an independent measurement of the
            # same interval -- close, but not the daemon's authoritative value.
            now = time.monotonic()
            if in_power_outage(values, power_outage_voltage_limit):
                if outage_since is None:
                    outage_since = now
                    # Baseline: pre-cut load power, lost once V_in collapses.
                    if last_nominal is not None:
                        append_trace(0.0, last_nominal, phase="baseline")
                    log.warning(
                        "Power outage detected (V_in=%s, state=%s). Shutdown in ~%.0f s "
                        "unless input power returns.",
                        values.get("V_in"),
                        values.get("state"),
                        power_outage_time_limit,
                    )
                elapsed = now - outage_since
            else:
                last_nominal = values
                if outage_since is not None:
                    log.info(
                        "Input power restored after %.1f s; shutdown averted.",
                        now - outage_since,
                    )
                outage_since = None
                elapsed = 0.0

            remaining = max(0.0, power_outage_time_limit - elapsed)

            if outage_since is not None:
                # Every read, not every publish: the discharge curve is the
                # measurement, and it is only available once.
                append_trace(elapsed, values, phase="discharge")

            if health.rediscover:
                # HA restarted and dropped our retained discovery configs.
                health.rediscover = False
                discovery_published = False

            if not discovery_published:
                # Derive the device_id only on the first discovery; on an HA-birth
                # rediscovery reuse the known-good id so a transient malformed read
                # can't republish everything under the "halpi2" fallback.
                if not device_id:
                    device_id = str(values.get("device_id") or "")
                if not device_id:
                    # halpid returns a stable hardware device_id on every read; a
                    # missing one means a transient/malformed payload. Wait a few
                    # reads for a real id rather than freezing every entity under
                    # a "halpi2" fallback that never re-groups -- but fall back
                    # eventually so telemetry still appears on odd firmware.
                    reads_without_device_id += 1
                    if reads_without_device_id < 3:
                        time.sleep(
                            max(
                                0.0, POLL_INTERVAL_S - (time.monotonic() - loop_started)
                            )
                        )
                        continue
                    device_id = "halpi2"
                publish_all_discovery(
                    client,
                    discovery_prefix,
                    device_id,
                    base_topic,
                    values,
                    expire_after_s,
                )
                discovery_published = True
                log.info(
                    "Published discovery for %d entities (device_id=%s)",
                    len(SENSOR_DEFS) + 1 + len(DERIVED_DEFS),
                    device_id,
                )
                # Assert availability the moment discovery lands, not on the next
                # publish-cadence tick, so entities come up "online" immediately.
                client.publish(
                    availability_topic(base_topic), "online", qos=1, retain=True
                )

            # Read every cycle (above) but publish only on the configured
            # cadence -- unless a power outage is running, when every read is
            # published so the countdown is recorded at full resolution.
            now_pub = time.monotonic()
            due = (now_pub - last_publish) >= interval
            if not (due or outage_since is not None):
                time.sleep(
                    max(0.0, POLL_INTERVAL_S - (time.monotonic() - loop_started))
                )
                continue
            last_publish = now_pub

            client.publish(availability_topic(base_topic), "online", qos=1, retain=True)

            for key, definition in SENSOR_DEFS.items():
                value = extract(values, definition)
                if value is not None:
                    client.publish(
                        state_topic(base_topic, key),
                        render_state(value),
                        qos=0,
                        retain=False,
                    )

            power_state = extract(values, POWER_STATE_DEF)
            if power_state:
                client.publish(
                    state_topic(base_topic, POWER_STATE_KEY),
                    str(power_state),
                    qos=0,
                    retain=False,
                )
                # Solo means nothing is coordinating with the controller, so a
                # power outage ends in a hard power cut. That is expected when the
                # user asked for mode=solo; it is a real problem in coop mode.
                if power_state == "OperationalSolo" and mode != "solo":
                    log.warning(
                        "Controller is in OperationalSolo despite mode=coop: "
                        "graceful power outage shutdown is NOT active. Co-op "
                        "requires the hardware watchdog to be armed."
                    )

            client.publish(
                state_topic(base_topic, POWER_OUTAGE_ELAPSED_KEY),
                render_state(elapsed),
                qos=0,
            )
            client.publish(
                state_topic(base_topic, SHUTDOWN_IN_KEY), render_state(remaining), qos=0
            )

            log.debug("Published: %s", values)

            time.sleep(max(0.0, POLL_INTERVAL_S - (time.monotonic() - loop_started)))
    finally:
        log.info("Publishing offline availability and disconnecting")
        try:
            info = client.publish(
                availability_topic(base_topic), "offline", qos=1, retain=True
            )
            info.wait_for_publish(timeout=0.3)  # best-effort flush; LWT covers the rest
            # No loop_stop(): it joins the network thread, which may be
            # mid-connect() to an unreachable broker or holding an unacked
            # qos=1 message. The thread is a daemon; the OS reaps it on exit.
            client.disconnect()
        except Exception as exc:  # noqa: BLE001 - best effort on the way out
            log.debug("Error during MQTT shutdown: %s", exc)

    return 0


if __name__ == "__main__":
    sys.exit(main())
