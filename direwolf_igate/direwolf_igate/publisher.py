# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""MQTT side of the add-on: discovery, states, heartbeat, watchdogs.

Publishes are async against an ``aiomqtt.Client``: the whole add-on runs on a
single event loop, so a sync send would stall the loop and stop the tee that
direwolf depends on.

Connection lifetime belongs to app.py -- this publishes into whatever session it
is handed, and a broker disruption surfaces as ``aiomqtt.MqttError`` out of the
publish so the caller can reconnect. Separate from app.py, which owns the
process, so the payload building stays reachable without a running pipe.
"""

import json
import time
from typing import Any, Protocol

from .config import DEVICE_NAME, Options
from .mqtt import (
    BINARY_SENSORS,
    SENSORS,
    MqttHealth,
    build_discovery_payloads,
    heartbeat_payload,
)
from .parser import DirewolfParser
from .util import log


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


def state_values(parser: DirewolfParser) -> dict[str, str]:
    """Current sensor states. Counters direwolf has not reported yet are
    omitted rather than published as a fabricated zero."""
    s = parser.stats
    out: dict[str, object] = {
        "packets_uploaded": s.packets_uploaded,
        "packets_downloaded": s.packets_downloaded,
        "rf_packets_received": s.rf_packets_received,
        "uploaded_rate": s.uploaded_rate,
        "downloaded_rate": s.downloaded_rate,
        "rf_rate": s.rf_rate,
        "stations_heard": s.stations_heard,
        "stations_heard_direct": s.stations_heard_direct,
        "stations_seen_total": s.stations_seen_total,
        "audio_level": s.last_audio_level,
    }
    if s.last_heard is not None:
        import datetime

        out["last_heard"] = datetime.datetime.fromtimestamp(
            s.last_heard, tz=datetime.UTC
        ).isoformat()
    return {k: str(v) for k, v in out.items() if v is not None}


def overdue(now_mono: float, since: float, last_warned: float, timeout: int) -> bool:
    """Whether a condition has held past ``timeout`` and is due a fresh warning.

    Monotonic throughout so a clock step cannot suppress or spuriously fire it.
    ``since`` of 0 means the condition has not started.
    """
    return (
        since > 0
        and (now_mono - since) > timeout
        and (now_mono - last_warned) > timeout
    )


class Publisher:
    """Builds and publishes the add-on's MQTT surface.

    ``feed_observed`` runs on the tee task and everything else on the publish
    task, both on the one event loop, so neither needs locking.
    """

    def __init__(self, opts: Options, parser: DirewolfParser) -> None:
        self.opts = opts
        self.parser = parser
        self.health = MqttHealth()
        self._discovered = False
        self._last_output_wall = 0.0
        self._last_output_monotonic = 0.0
        self._last_disconnect_warned = 0.0
        self._last_stall_warned = 0.0
        # Treated as disconnected from construction, so a broker that is never
        # reachable trips the watchdog like one that drops. Left at 0.0 it never
        # would, and only the reconnect loop's own warnings appeared.
        self.health.last_disconnect_monotonic = time.monotonic()

    # -- tee-task entry point ----------------------------------------------

    def feed_observed(self, line: str) -> None:
        """Record that direwolf produced output, then parse it."""
        self._last_output_wall = time.time()
        self._last_output_monotonic = time.monotonic()
        try:
            self.parser.feed(line)
        except Exception as e:  # noqa: BLE001 a parse bug must not kill the pipe
            log("WARNING", f"parse error: {e}", self.opts.log_level)

    # -- session lifecycle -------------------------------------------------

    def on_connected(self) -> None:
        self.health.connected = True
        self.health.last_connect_ok = time.time()
        # Republish on every session: a broker restart drops retained config.
        self._discovered = False

    def on_disconnected(self) -> None:
        self.health.connected = False
        self.health.last_disconnect = time.time()
        self.health.last_disconnect_monotonic = time.monotonic()

    def on_ha_birth(self) -> None:
        log(
            "INFO",
            "HA birth message received — will republish discovery",
            self.opts.log_level,
        )
        self._discovered = False

    # -- publishing --------------------------------------------------------

    async def publish_discovery(self, mq: Client) -> None:
        payloads = build_discovery_payloads(
            self.opts.discovery_prefix,
            self.opts.device_id,
            DEVICE_NAME,
            self.opts.base_topic,
            self.opts.availability_topic,
            self.opts.expire_after_s,
        )
        for topic, payload in payloads.items():
            await mq.publish(
                topic,
                json.dumps(payload, separators=(",", ":")),
                qos=1,
                retain=True,
            )

    async def publish_states(self, mq: Client) -> None:
        values = state_values(self.parser)
        for key, meta in SENSORS.items():
            value = values.get(key)
            if value is None:
                # Retained topics: omitting a key leaves the broker replaying
                # a dead gate's last good figure, so "None" clears it. Older HA
                # honours that only on a numeric sensor -- a timestamp sensor
                # warns every cycle instead, so skip it.
                if meta.unit is None and meta.state_class is None:
                    continue
                value = "None"
            await mq.publish(
                f"{self.opts.base_topic}/{key}/state", value, qos=0, retain=True
            )
            self.health.last_state_publish_ok = time.time()
        for key in BINARY_SENSORS:
            await mq.publish(
                f"{self.opts.base_topic}/{key}/state",
                "ON" if self.parser.stats.igate_connected else "OFF",
                qos=0,
                # Unretained: this entity carries expire_after, and a
                # redelivered retained value restarts the expiry timer.
                retain=False,
            )

    async def publish_heartbeat(self, mq: Client, now: float) -> None:
        await mq.publish(
            self.opts.heartbeat_topic,
            json.dumps(
                heartbeat_payload(
                    now=now, health=self.health, last_output=self._last_output_wall
                ),
                separators=(",", ":"),
            ),
            qos=0,
            retain=False,
        )

    async def publish_availability(self, mq: Client, payload: str) -> None:
        await mq.publish(self.opts.availability_topic, payload, qos=1, retain=True)

    # -- watchdogs ---------------------------------------------------------

    def check_watchdogs(self, now_mono: float) -> None:
        """Report stale statistics and a silent direwolf.

        Report only: the sibling add-ons exit here, but run.sh owns restarts and
        exiting would SIGPIPE direwolf.
        """
        o = self.opts
        if not self.health.connected and overdue(
            now_mono,
            self.health.last_disconnect_monotonic,
            self._last_disconnect_warned,
            o.disconnect_timeout,
        ):
            log(
                "ERROR",
                f"MQTT disconnected for "
                f"{now_mono - self.health.last_disconnect_monotonic:.1f}s "
                f"(> {o.disconnect_timeout}s). Statistics are stale; gating is "
                f"unaffected and this process will keep retrying.",
                o.log_level,
            )
            self._last_disconnect_warned = now_mono

        if overdue(
            now_mono,
            self._last_output_monotonic,
            self._last_stall_warned,
            o.stall_timeout,
        ):
            log(
                "ERROR",
                f"No output from direwolf for "
                f"{now_mono - self._last_output_monotonic:.1f}s "
                f"(> {o.stall_timeout}s); it may have stopped decoding.",
                o.log_level,
            )
            self._last_stall_warned = now_mono

    async def tick(self, mq: Client) -> None:
        """One publish cycle. Broker faults propagate so the caller reconnects."""
        if not self._discovered:
            # Immediately on (re)connect, so entities exist in HA before the
            # first state lands.
            await self.publish_discovery(mq)
            await self.publish_availability(mq, "online")
            self._discovered = True
        self.parser.sample_rates()
        await self.publish_states(mq)
        await self.publish_heartbeat(mq, time.time())
        self.check_watchdogs(time.monotonic())
