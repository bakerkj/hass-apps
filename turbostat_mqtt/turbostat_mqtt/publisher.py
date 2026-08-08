# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""MQTT side of the add-on: discovery, states, heartbeat, watchdogs.

Publishes are async against an ``aiomqtt.Client``: the whole add-on runs on a
single event loop, so a sync send would stall the loop that drains turbostat.

Connection lifetime belongs to app.py -- this publishes into whatever session it
is handed, and a broker disruption surfaces as ``aiomqtt.MqttError`` out of the
publish so the caller can reconnect. Split from app.py so the decisions here --
which columns to map, when a watchdog is overdue -- are reachable without a
broker or a running turbostat.
"""

import json
import re
import time
from enum import Enum
from typing import Any, Protocol

from .config import Options
from .metadata import friendly_name, missing_expected_columns
from .mqtt import MqttHealth, build_discovery_payloads
from .parser import TurbostatParser
from .util import log, sanitize_key

DEVICE_ID = "turbostat"
DEVICE_NAME = "Turbostat"

# Columns turbostat always emits under `--enable all` that carry no HA value.
# Kept out of the unmapped-warning path so a restart is not chatty about them.
SKIP_COLS = frozenset(
    {
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
        # turbostat internal / topology columns.
        "usec",
        "Time_Of_Day_Seconds",
        "APIC",
        "X2APIC",
    }
)


class Fault(Enum):
    """A condition the loop cannot fix in place.

    Returned rather than raised so the caller decides between restarting
    turbostat and exiting for the supervisor -- and so each is testable without
    driving a process to its exit code.
    """

    NONE = "none"
    MQTT_DOWN = "mqtt_down"
    PUBLISH_STALLED = "publish_stalled"
    NO_SAMPLES_SINCE_START = "startup_no_samples"
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


def coerce(val: str) -> int | float | str:
    """turbostat emits everything as text; publish numbers as numbers.

    Falls back to the string rather than dropping the column: an unparsable
    value is still worth showing, and HA renders it as unknown.
    """
    try:
        if re.fullmatch(r"[-+]?\d+", val):
            return int(val)
        return float(val)
    except ValueError:
        return val


def map_columns(header: list[str]) -> tuple[dict[str, str], set[str]]:
    """Split turbostat's header into columns we publish and ones we retire.

    The second set is columns an older version of this add-on did publish;
    their retained discovery configs have to be cleared or HA keeps the
    entities forever.
    """
    all_cols = {col: sanitize_key(col) for col in header}
    cols_map = {
        c: k
        for c, k in all_cols.items()
        if c not in SKIP_COLS and friendly_name(c) != f"Turbostat {c}"
    }
    retired = {
        k for c, k in all_cols.items() if c not in SKIP_COLS and c not in cols_map
    }
    return cols_map, retired


class Publisher:
    """Builds and publishes the add-on's MQTT surface."""

    def __init__(self, opts: Options, parser: TurbostatParser) -> None:
        self.opts = opts
        self.parser = parser
        self.health = MqttHealth()
        self._discovered = False
        self.cols_map: dict[str, str] = {}
        self.retired_keys: set[str] = set()
        self.last_sample_time = 0.0
        # Monotonic mirror: the published age stays wall-clock for HA, but a
        # restart decision must survive an NTP step.
        self.last_sample_monotonic = 0.0
        self.first_sample_time = 0.0
        self.samples_since_start = 0
        self.turbostat_started_at = 0.0
        self._last_heartbeat = 0.0
        self._last_status_line = 0.0

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
        at 60s apart while the timeout defaults to 300s, the age could never
        reach it -- the watchdog would be dead code. paho used to give us this
        for free by firing its callback once per real disconnection.
        """
        if self.health.connected or self.health.last_disconnect == 0.0:
            self.health.last_disconnect = time.time()
        self.health.connected = False

    def on_ha_birth(self) -> None:
        log(
            "INFO",
            "HA birth message received — will republish discovery",
            self.opts.log_level,
        )
        self._discovered = False

    def on_turbostat_started(self) -> None:
        """Reset the per-process sample state so the watchdogs start clean."""
        self.parser.reset()
        self.first_sample_time = 0.0
        self.last_sample_time = 0.0
        self.last_sample_monotonic = 0.0
        self.samples_since_start = 0
        self.health.last_state_publish_ok = 0.0
        self.turbostat_started_at = time.monotonic()

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
            log(
                "ERROR",
                f"MQTT disconnected for {now - self.health.last_disconnect:.1f}s "
                f"(> {o.disconnect_timeout}s). Exiting for supervisor restart.",
                o.log_level,
            )
            return Fault.MQTT_DOWN

        if (
            self.samples_since_start == 0
            and self.turbostat_started_at > 0
            and (now_mono - self.turbostat_started_at) > o.expire_after_s
        ):
            log(
                "ERROR",
                "No turbostat samples since process start for "
                f"{now_mono - self.turbostat_started_at:.1f}s",
                o.log_level,
            )
            return Fault.NO_SAMPLES_SINCE_START

        if (
            self.samples_since_start > 0
            and self.last_sample_monotonic > 0
            and (now_mono - self.last_sample_monotonic) > o.expire_after_s
        ):
            log(
                "ERROR",
                f"No turbostat samples for {now_mono - self.last_sample_monotonic:.1f}s",
                o.log_level,
            )
            return Fault.SAMPLE_TIMEOUT

        # Only while samples are actually arriving: a publish stall means the
        # broker is taking them and HA is not seeing them, which a reconnect
        # will not fix. Without the sample guard a quiet turbostat would trip it.
        if (
            self.health.connected
            and self.last_sample_time > 0
            and (now - self.last_sample_time) <= max(o.expire_after_s, o.interval * 2)
        ):
            if (
                self.health.last_state_publish_ok > 0
                and (now - self.health.last_state_publish_ok) > o.expire_after_s
            ):
                log(
                    "ERROR",
                    "Detected MQTT state publish stall while samples are active. "
                    "Exiting for supervisor restart.",
                    o.log_level,
                )
                return Fault.PUBLISH_STALLED
            if (
                self.health.last_state_publish_ok == 0
                and self.first_sample_time > 0
                and (now - self.first_sample_time) > o.expire_after_s
            ):
                log(
                    "ERROR",
                    "No successful MQTT state publish since first sample. "
                    "Exiting for supervisor restart.",
                    o.log_level,
                )
                return Fault.PUBLISH_STALLED

        return Fault.NONE

    # -- publishing --------------------------------------------------------

    async def publish_availability(self, mq: Client, payload: str) -> None:
        await mq.publish(self.opts.availability_topic, payload, qos=1, retain=True)

    async def publish_heartbeat(self, mq: Client, now: float) -> None:
        hb = {
            "ts_ms": int(now * 1000),
            "connected": self.health.connected,
            "last_sample_age_s": round(now - self.last_sample_time, 1)
            if self.last_sample_time
            else None,
            "state_publish_age_s": round(now - self.health.last_state_publish_ok, 1)
            if self.health.last_state_publish_ok
            else None,
        }
        await mq.publish(
            self.opts.heartbeat_topic,
            json.dumps(hb, separators=(",", ":")),
            qos=0,
            retain=False,
        )

    async def maybe_heartbeat(self, mq: Client, now: float) -> None:
        if now - self._last_heartbeat >= self.opts.interval:
            self._last_heartbeat = now
            await self.publish_heartbeat(mq, now)

    async def publish_discovery(self, mq: Client) -> None:
        o = self.opts
        await self.publish_availability(mq, "online")

        # Empty retained payload deletes the config, so entities from columns we
        # no longer map do not linger in HA forever.
        for key in sorted(self.retired_keys):
            await mq.publish(
                f"{o.discovery_prefix}/sensor/{DEVICE_ID}/{key}/config",
                "",
                qos=1,
                retain=True,
            )
        if self.retired_keys:
            log(
                "INFO",
                f"Removed discovery for {len(self.retired_keys)} unmapped columns",
                o.log_level,
            )

        disc = build_discovery_payloads(
            discovery_prefix=o.discovery_prefix,
            device_id=DEVICE_ID,
            device_name=DEVICE_NAME,
            base_topic=o.base_topic,
            availability_topic=o.availability_topic,
            cols=self.cols_map,
            expire_after_s=o.expire_after_s,
        )
        for topic, cfg in disc.items():
            # Retained so an HA restart re-reads the current config on
            # subscribe. Without it HA can silently revert to whatever ancient
            # config an older add-on version left retained -- invisible until
            # the next HA restart.
            await mq.publish(
                topic, json.dumps(cfg, separators=(",", ":")), qos=1, retain=True
            )

        # Clear the per-sensor availability topics an older version published;
        # availability now rides the single device-level topic.
        for key in self.cols_map.values():
            await mq.publish(
                f"{o.base_topic}/{key}/availability", "", qos=1, retain=True
            )

        self._discovered = True
        log("INFO", f"Published discovery for {len(disc)} sensors", o.log_level)

    async def publish_sample(self, mq: Client, line: str) -> bool:
        """Parse one turbostat line and publish it. False if it was not a sample."""
        parsed = self.parser.parse_line(line)
        if parsed is None:
            return False
        header, values, raw_line = parsed
        o = self.opts
        now = time.time()

        self.samples_since_start += 1
        self.last_sample_time = now
        self.last_sample_monotonic = time.monotonic()
        if self.first_sample_time == 0.0:
            self.first_sample_time = now

        if not self.cols_map:
            self.cols_map, self.retired_keys = map_columns(header)
            missing = missing_expected_columns(header)
            if missing:
                log(
                    "WARNING",
                    f"turbostat is not emitting expected column(s): {missing}. "
                    "Likely an upstream rename or kernel change; HA entities for "
                    "these will go unavailable. Update EXPECTED_COLS and the "
                    "friendly_name() mapping in turbostat_mqtt/metadata.py if the "
                    "column moved.",
                    o.log_level,
                )

        payload = {
            self.cols_map[col]: coerce(val)
            for col, val in values.items()
            if col in self.cols_map
        }

        if not self._discovered and self.health.connected:
            await self.publish_discovery(mq)

        if o.publish_raw:
            # `_raw` mirrors the sensor payload (canonical post-alias keys,
            # parser-scaled values). `_raw_header` is the verbatim turbostat
            # header, so zipping it against `_raw_line.split()` gives consistent
            # pre-alias name -> raw-value pairs.
            await mq.publish(
                o.raw_topic,
                json.dumps(
                    {
                        "_ts_ms": int(now * 1000),
                        "_raw": {
                            self.cols_map[c]: values[c]
                            for c in values
                            if c in self.cols_map
                        },
                        "_raw_header": self.parser.original_header,
                        "_raw_line": raw_line,
                    },
                    separators=(",", ":"),
                ),
                qos=0,
                retain=False,
            )

        for key, value in payload.items():
            await mq.publish(
                f"{o.base_topic}/{key}/state", str(value), qos=0, retain=False
            )
            # After the await, so a broker fault does not stamp a publish that
            # never landed -- this clock is what the stall watchdog reads.
            self.health.last_state_publish_ok = time.time()

        if now - self._last_status_line >= 10.0:
            self._last_status_line = now
            bits = [
                f"{k}={payload[k]}"
                for k in ("pkgwatt", "corwatt", "gfxwatt", "ramwatt")
                if k in payload
            ]
            log(
                "INFO",
                " | ".join(bits) if bits else f"Published {len(payload)} keys",
                o.log_level,
            )
        return True
