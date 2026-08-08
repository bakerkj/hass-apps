# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""MQTT side of the add-on: discovery, states, heartbeat, watchdogs.

Publishes are async against an ``aiomqtt.Client``: the whole add-on runs on a
single event loop, so a sync send would stall the loop that drains
intel_gpu_top.

Connection lifetime belongs to app.py -- this publishes into whatever session it
is handed, and a broker disruption surfaces as ``aiomqtt.MqttError`` out of the
publish so the caller can reconnect. Split from app.py so the decisions here --
when a watchdog is overdue, whether a sample is still warming up -- are reachable
without a broker or a running intel_gpu_top.
"""

import json
import logging
import os
import time
from enum import Enum
from typing import Any, Protocol

from .config import Options
from .metrics import build_metrics
from .mqtt import MqttHealth, discovery_payloads
from .util import extract_latest_json_object

DEVICE_ID = "intel_gpu_top"
DEVICE_NAME = "Intel GPU Top"


class Fault(Enum):
    """A condition the loop cannot fix in place.

    Returned rather than acted on inline so the caller decides between
    restarting intel_gpu_top and exiting for the supervisor -- and so each is
    decidable in a test rather than only observable as a process exit status.
    """

    NONE = "none"
    MQTT_DOWN = "mqtt_down"
    RENDER_NODE_GONE = "render_node_disappeared"
    SAMPLE_TIMEOUT = "sample_timeout"


class Client(Protocol):
    """Anything with an awaitable ``publish``.

    A Protocol so tests can record publishes without importing aiomqtt.
    """

    async def publish(
        self,
        topic: str,
        payload: bytes | str = "",
        qos: int = 0,
        retain: bool = False,
    ) -> Any: ...


class Publisher:
    """Builds and publishes the add-on's MQTT surface."""

    def __init__(self, opts: Options, log: logging.Logger) -> None:
        self.opts = opts
        self.log = log
        self.health = MqttHealth()
        self._discovered = False
        self.device_path: str | None = None
        self.buf = ""
        self.last_sample_time = 0.0
        # Monotonic mirror: the published age stays wall-clock for HA, but a
        # restart decision must survive an NTP step.
        self.last_sample_monotonic = 0.0
        self.samples_since_start = 0
        self._last_heartbeat = 0.0
        self._last_publish_monotonic = 0.0

    # -- session lifecycle -------------------------------------------------

    def on_connected(self) -> None:
        self.health.connected = True
        self.health.last_connect_ok = time.time()
        # Republish on every session: a broker restart drops retained config.
        self._discovered = False

    def on_disconnected(self) -> None:
        """Stamp the *start* of an outage, not each failed retry.

        The reconnect loop calls this on every attempt. Restamping would reset
        the clock the disconnect watchdog measures, and since attempts are capped
        well inside the timeout, the age could never reach it -- the watchdog
        would be dead code. paho used to give us this for free by firing its
        callback once per real disconnection.
        """
        if self.health.connected or self.health.last_disconnect == 0.0:
            self.health.last_disconnect = time.time()
        self.health.connected = False

    def on_ha_birth(self) -> None:
        self.log.info("HA birth message received — will republish discovery")
        self._discovered = False

    def on_gpu_started(self, device_path: str | None) -> None:
        """Reset per-process state so the watchdogs and warm-up start clean."""
        self.device_path = device_path
        self.buf = ""
        self.samples_since_start = 0
        self.last_sample_monotonic = 0.0

    # -- watchdogs ---------------------------------------------------------

    def check_watchdogs(self, now: float, now_mono: float) -> Fault:
        """Decide whether anything is wrong enough to act on.

        Pure decision, no side effects beyond logging, so every branch is
        reachable in a test without a broker, a process, or a real clock.
        """
        o = self.opts
        if (
            not self.health.connected
            and self.health.last_disconnect > 0
            and (now - self.health.last_disconnect) > o.disconnect_timeout
        ):
            self.log.error(
                "MQTT disconnected for %.1fs (> %ss). Exiting for supervisor restart.",
                now - self.health.last_disconnect,
                o.disconnect_timeout,
            )
            return Fault.MQTT_DOWN

        # Checked before the sample timeout: a yanked GPU explains the silence,
        # and re-selecting the device is the useful response.
        if self.device_path is not None and not os.path.exists(self.device_path):
            self.log.error("GPU render node disappeared: %s", self.device_path)
            return Fault.RENDER_NODE_GONE

        if (
            self.last_sample_monotonic > 0
            and (now_mono - self.last_sample_monotonic) > o.expire_after_s
        ):
            self.log.error(
                "No intel_gpu_top samples for %.1fs",
                now_mono - self.last_sample_monotonic,
            )
            return Fault.SAMPLE_TIMEOUT

        return Fault.NONE

    # -- publishing --------------------------------------------------------

    async def publish_availability(self, mq: Client, payload: str) -> None:
        await mq.publish(self.opts.availability_topic, payload, qos=1, retain=True)

    async def publish_heartbeat(self, mq: Client, now: float) -> None:
        await mq.publish(
            self.opts.heartbeat_topic,
            json.dumps(
                {
                    "ts": now,
                    "mqtt_connected": self.health.connected,
                    "last_sample_age_s": (now - self.last_sample_time)
                    if self.last_sample_time
                    else None,
                    "device": self.device_path,
                }
            ),
            qos=0,
            retain=False,
        )

    async def maybe_heartbeat(self, mq: Client, now: float) -> None:
        if now - self._last_heartbeat >= self.opts.interval:
            self._last_heartbeat = now
            await self.publish_heartbeat(mq, now)

    async def publish_discovery(self, mq: Client, metrics: dict[str, Any]) -> None:
        o = self.opts
        self.log.info("Publishing MQTT discovery for %d sensors", len(metrics))
        await self.publish_availability(mq, "online")
        for topic, payload in discovery_payloads(
            o.discovery_prefix,
            o.base_topic,
            DEVICE_ID,
            DEVICE_NAME,
            metrics,
            o.expire_after_s,
        ).items():
            await mq.publish(topic, json.dumps(payload), qos=1, retain=True)
        self._discovered = True

    async def feed(self, mq: Client, line: str) -> bool:
        """Absorb one line of intel_gpu_top's JSON stream.

        True once a complete sample has been consumed. intel_gpu_top emits a
        pretty-printed JSON array, so a sample spans many lines and only the
        buffer knows when one is complete.
        """
        self.buf += line
        obj, self.buf = extract_latest_json_object(self.buf)
        if not obj:
            return False

        self.last_sample_time = time.time()
        self.last_sample_monotonic = time.monotonic()
        self.samples_since_start += 1
        metrics = build_metrics(obj)

        if not self._discovered and self.health.connected:
            await self.publish_discovery(mq, metrics)

        # Rate-limited to the interval. Monotonic: a wall-clock step must not
        # stall publishing or burst a backlog.
        now_mono = time.monotonic()
        if now_mono - self._last_publish_monotonic < self.opts.interval:
            return True
        self._last_publish_monotonic = now_mono

        # The first sample after a (re)start is intel_gpu_top's warm-up: its
        # counters are cumulative-since-boot rather than an interval rate, so
        # publishing it would spike every graph.
        if self.samples_since_start <= 1:
            self.log.debug("Skipping state publish for warm-up sample")
            return True

        for key, meta in metrics.items():
            value = meta["value"]
            if value is None:
                continue
            # Full precision on the wire; HA formats via
            # suggested_display_precision.
            await mq.publish(
                f"{self.opts.base_topic}/{key}/state",
                str(float(value)),
                qos=0,
                retain=False,
            )

        if self.opts.publish_raw:
            await mq.publish(
                self.opts.raw_topic, json.dumps(obj)[:200000], qos=0, retain=False
            )
        return True
