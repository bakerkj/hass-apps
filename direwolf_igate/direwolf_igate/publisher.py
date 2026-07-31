# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""MQTT side of the add-on: client lifecycle, discovery, states, watchdogs.

Separate from app.py, which owns the process, so the callbacks and watchdogs
are reachable without a running pipe.
"""

import json
import threading
import time

import paho.mqtt.client as mqtt

from .config import DEVICE_NAME, Options
from .mqtt import (
    BINARY_SENSORS,
    SENSORS,
    MqttHealth,
    build_discovery_payloads,
    connect_mqtt_with_retry,
    heartbeat_payload,
    mqtt_publish,
)
from .parser import DirewolfParser
from .util import log


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
    """Owns the MQTT client. ``feed_observed`` is called from the reader thread;
    everything else runs on the publisher thread or a paho callback."""

    def __init__(self, opts: Options, parser: DirewolfParser) -> None:
        self.opts = opts
        self.parser = parser
        self.health = MqttHealth()
        self.stop = threading.Event()
        self._discovered = False
        self._last_output_wall = 0.0
        self._last_output_monotonic = 0.0
        self._last_disconnect_warned = 0.0
        self._last_stall_warned = 0.0
        # Treated as disconnected from construction, so a broker that is never
        # reachable trips the watchdog like one that drops. Left at 0.0 it never
        # would, and only connect_mqtt_with_retry's own warnings appeared.
        self.health.last_disconnect_monotonic = time.monotonic()

        self.client = mqtt.Client(client_id=opts.client_id, clean_session=True)
        if opts.mqtt_username:
            self.client.username_pw_set(opts.mqtt_username, opts.mqtt_password)
        self.client.will_set(opts.availability_topic, "offline", qos=1, retain=True)
        self.client.reconnect_delay_set(min_delay=1, max_delay=30)
        self.client.on_connect = self._guarded_on_connect
        self.client.on_disconnect = self._guarded_on_disconnect
        self.client.on_message = self._guarded_on_message

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        """Connect and begin publishing, both on their own threads.

        connect_mqtt_with_retry never gives up, so it must not run on the
        thread that drains direwolf's output or the pipe fills and direwolf
        blocks on write.
        """
        threading.Thread(target=self._connect, daemon=True, name="mqtt-connect").start()
        threading.Thread(target=self._loop, daemon=True, name="mqtt-publish").start()

    def _connect(self) -> None:
        connect_mqtt_with_retry(
            self.client, self.opts.mqtt_host, self.opts.mqtt_port, self.opts.log_level
        )
        self.client.loop_start()

    def shutdown(self, *, farewell: bool) -> None:
        """Stop publishing, tear down the client, and with ``farewell`` send the
        retained "offline" first.

        The publishes need a live session: publish() takes paho mutexes an
        in-flight connect on the other thread may hold, and with no session
        there is nothing to say anyway. The teardown itself is unconditional.
        """
        self.stop.set()
        try:
            if farewell and self.health.connected:
                self.publish_states()
                self._publish(
                    self.opts.availability_topic, "offline", qos=1, retain=True
                )
            # disconnect() first: it needs the network loop still running to
            # flush the farewell, and sends the clean DISCONNECT that suppresses
            # the last will. loop_stop() then joins the thread.
            self.client.disconnect()
            self.client.loop_stop()
        except Exception as e:  # noqa: BLE001 shutdown must not hang or raise
            log("WARNING", f"error during shutdown publish: {e!r}", self.opts.log_level)

    # -- reader-thread entry point ----------------------------------------

    def feed_observed(self, line: str) -> None:
        """Record that direwolf produced output, then parse it."""
        self._last_output_wall = time.time()
        self._last_output_monotonic = time.monotonic()
        try:
            self.parser.feed(line)
        except Exception as e:  # noqa: BLE001 a parse bug must not kill the pipe
            log("WARNING", f"parse error: {e}", self.opts.log_level)

    # -- publishing --------------------------------------------------------

    def _publish(self, topic: str, payload: str, **kw: object) -> bool:
        return mqtt_publish(
            self.client,
            topic,
            payload,
            log_level=self.opts.log_level,
            health=self.health,
            **kw,  # type: ignore[arg-type]
        )

    def publish_discovery(self) -> None:
        payloads = build_discovery_payloads(
            self.opts.discovery_prefix,
            self.opts.device_id,
            DEVICE_NAME,
            self.opts.base_topic,
            self.opts.availability_topic,
            self.opts.expire_after_s,
        )
        for topic, payload in payloads.items():
            self._publish(
                topic,
                json.dumps(payload, separators=(",", ":")),
                qos=1,
                retain=True,
            )

    def publish_states(self) -> None:
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
            self._publish(
                f"{self.opts.base_topic}/{key}/state",
                value,
                qos=0,
                retain=True,
                mark_state=True,
            )
        for key in BINARY_SENSORS:
            self._publish(
                f"{self.opts.base_topic}/{key}/state",
                "ON" if self.parser.stats.igate_connected else "OFF",
                qos=0,
                # Unretained: this entity carries expire_after, and a
                # redelivered retained value restarts the expiry timer.
                retain=False,
            )

    def publish_heartbeat(self, now: float) -> None:
        self._publish(
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

    def tick(self) -> None:
        """One publish cycle."""
        if self.health.connected and not self._discovered:
            self.publish_discovery()
            self._discovered = True
        self.parser.sample_rates()
        self.publish_states()
        self.publish_heartbeat(time.time())
        self.check_watchdogs(time.monotonic())

    def _loop(self) -> None:
        while not self.stop.wait(self.opts.interval):
            try:
                self.tick()
            except Exception as e:  # noqa: BLE001 one bad cycle must not end them all
                log("ERROR", f"publish cycle failed: {e!r}", self.opts.log_level)

    # -- paho callbacks ----------------------------------------------------

    # paho re-raises callback exceptions out of the network loop, killing that
    # thread permanently and disabling reconnect. Nothing may escape any of them.
    def _guard(self, name: str, fn: object, *args: object) -> None:
        try:
            fn(*args)  # type: ignore[operator]
        except Exception as e:  # noqa: BLE001 must not kill paho's network thread
            log("ERROR", f"{name} failed: {e!r}", self.opts.log_level)

    def _guarded_on_connect(
        self, client: mqtt.Client, _userdata: object, _flags: object, rc: int
    ) -> None:
        self._guard("on_connect", self._on_connect, client, rc)

    def _guarded_on_disconnect(
        self, client: mqtt.Client, _userdata: object, rc: int
    ) -> None:
        self._guard("on_disconnect", self._on_disconnect, client, None, rc)

    def _guarded_on_message(
        self, client: mqtt.Client, _userdata: object, msg: mqtt.MQTTMessage
    ) -> None:
        self._guard("on_message", self._on_message, client, None, msg)

    def _on_connect(self, client: mqtt.Client, rc: int) -> None:
        o = self.opts
        if rc != 0:
            self.health.connected = False
            log("ERROR", f"MQTT connect failed rc={rc}", o.log_level)
            return
        self.health.connected = True
        self.health.last_connect_ok = time.time()
        log("INFO", f"MQTT connected to {o.mqtt_host}:{o.mqtt_port}", o.log_level)
        try:
            client.subscribe(f"{o.discovery_prefix}/status", qos=1)
        except ValueError as e:
            # Costs us the HA birth message; discovery is still republished on
            # every reconnect. Must not cost us MQTT.
            log(
                "ERROR",
                f"cannot subscribe to {o.discovery_prefix}/status: {e};"
                " HA restart will not trigger rediscovery",
                o.log_level,
            )
        self._publish(o.availability_topic, "online", qos=1, retain=True)
        # Immediately, so entities exist in HA on (re)connect.
        self.publish_discovery()
        self.publish_states()
        self._discovered = True

    def _on_disconnect(self, _client: mqtt.Client, _userdata: object, rc: int) -> None:
        self.health.connected = False
        self.health.last_disconnect = time.time()
        self.health.last_disconnect_monotonic = time.monotonic()
        log("WARNING", f"MQTT disconnected rc={rc}", self.opts.log_level)

    def _on_message(
        self, _client: mqtt.Client, _userdata: object, msg: mqtt.MQTTMessage
    ) -> None:
        if msg.payload.decode(errors="replace").strip() == "online":
            log(
                "INFO",
                "HA birth message received — will republish discovery",
                self.opts.log_level,
            )
            self._discovered = False
