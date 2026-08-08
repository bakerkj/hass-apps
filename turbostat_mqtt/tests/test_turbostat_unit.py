# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Unit tests for the orchestration that used to live inside main().

All of this was previously unreachable without a broker and a running turbostat,
so none of it was tested: option clamping, the watchdog decisions and what each
one costs, the sample loop's resilience, and the reconnect backoff.
"""

import asyncio
import time

import aiomqtt
import pytest

from turbostat_mqtt import (
    Fault,
    Publisher,
    TurbostatParser,
    coerce,
    from_mapping,
    map_columns,
)
from turbostat_mqtt import app as app_mod

# Watchdog tests do elapsed-time arithmetic against clocks that start at 0.0, so
# a real monotonic() would make them depend on the machine's uptime. Feed a
# synthetic clock large enough that any timeout under test has elapsed.
CLOCK = 1_000_000.0


# --- options -----------------------------------------------------------------


def test_bounds_match_config_json_at_both_ends() -> None:
    """Schema is interval_seconds int(1,60), expire_after_multiplier int(2,10),
    disconnect int(5,600). A hand-edited options.json arrives without Supervisor
    validation and must be clamped to the same range, upper end included."""
    fast = from_mapping({"interval_seconds": 0, "expire_after_multiplier": 0})
    assert fast.interval == 1.0
    assert fast.expire_after_s == 60

    slow = from_mapping({"interval_seconds": 9999, "expire_after_multiplier": 99})
    assert slow.interval == 60.0
    assert slow.expire_after_s == 600

    assert from_mapping({"mqtt_disconnect_timeout_seconds": 1}).disconnect_timeout == 5
    assert (
        from_mapping({"mqtt_disconnect_timeout_seconds": 9999}).disconnect_timeout
        == 600
    )


def test_derived_topics_strip_the_trailing_slash() -> None:
    o = from_mapping({"mqtt_base_topic": "ts/"})
    assert o.base_topic == "ts"
    assert o.availability_topic == "ts/availability"
    assert o.heartbeat_topic == "ts/heartbeat"
    assert o.raw_topic == "ts/raw_sample"


def test_summary_never_includes_the_password() -> None:
    # A distinctive username: a short one would match the *label* and prove
    # nothing.
    o = from_mapping({"mqtt_password": "sup3rsecret", "mqtt_username": "brian"})
    summary = o.summary()
    assert "sup3rsecret" not in summary
    assert "brian" in summary


def test_restart_grace_is_bounded_at_both_ends() -> None:
    """Half the expiry, so a flapping turbostat cannot be respawned faster than
    HA notices the gap -- but capped, so a long expiry does not leave a dead
    turbostat unattended for minutes."""
    assert from_mapping({"interval_seconds": 1}).restart_grace_seconds == 30.0
    assert from_mapping({"interval_seconds": 60}).restart_grace_seconds == 30.0


def test_an_explicit_null_falls_back_rather_than_becoming_none() -> None:
    """`or`, not a get() default: connecting to None retries forever with no
    useful error."""
    o = from_mapping({"mqtt_host": None, "client_id": None, "mqtt_base_topic": None})
    assert o.mqtt_host == "core-mosquitto"
    assert o.client_id == "turbostat-app"
    assert o.base_topic == "turbostat"


# --- column mapping ----------------------------------------------------------


def test_coerce_keeps_ints_as_ints_and_survives_junk() -> None:
    """turbostat emits text; HA wants numbers. An unparsable value is still
    worth publishing, so it falls back rather than dropping the column."""
    assert coerce("898") == 898
    assert isinstance(coerce("898"), int)
    assert coerce("45.2") == 45.2
    assert coerce("-3") == -3
    assert coerce("n/a") == "n/a"


def test_map_columns_separates_publishable_from_retired() -> None:
    """Columns we no longer map need their retained discovery cleared, or HA
    keeps the entities forever."""
    cols, retired = map_columns(["PkgWatt", "Zorblax", "IRQ", "usec"])
    assert cols["PkgWatt"] == "pkgwatt"
    # Unmapped but not skipped -> retired, so its config gets deleted.
    assert "zorblax" in retired
    # Skipped columns are neither published nor retired: they were never ours.
    assert "irq" not in retired
    assert "usec" not in retired


# --- watchdogs ---------------------------------------------------------------


def _pub(**over: object) -> Publisher:
    return Publisher(from_mapping({"interval_seconds": 10, **over}), TurbostatParser())


def test_nothing_wrong_is_the_default() -> None:
    assert _pub().check_watchdogs(CLOCK, CLOCK) is Fault.NONE


def test_mqtt_down_past_its_timeout_is_fatal() -> None:
    """Exits for a supervisor restart rather than retrying forever: a client
    that cannot reach the broker for minutes is publishing nothing."""
    pub = _pub(mqtt_disconnect_timeout_seconds=100)
    pub.health.connected = False
    pub.health.last_disconnect = CLOCK - 200

    assert pub.check_watchdogs(CLOCK, CLOCK) is Fault.MQTT_DOWN
    assert app_mod._FAULT_EXIT[Fault.MQTT_DOWN] == app_mod.EXIT_MQTT_DOWN


def test_a_disconnect_that_has_not_aged_out_is_tolerated() -> None:
    pub = _pub(mqtt_disconnect_timeout_seconds=100)
    pub.health.connected = False
    pub.health.last_disconnect = CLOCK - 10
    assert pub.check_watchdogs(CLOCK, CLOCK) is Fault.NONE


def test_a_turbostat_that_never_produced_a_sample_is_restarted() -> None:
    """Restarted, not fatal: turbostat failing to start is its problem, and
    exiting would take the working MQTT session down with it."""
    pub = _pub()
    pub.turbostat_started_at = CLOCK - (pub.opts.expire_after_s + 1)
    pub.samples_since_start = 0

    fault = pub.check_watchdogs(CLOCK, CLOCK)
    assert fault is Fault.NO_SAMPLES_SINCE_START
    assert fault in app_mod._RESTART_FAULTS
    assert fault not in app_mod._FAULT_EXIT


def test_a_turbostat_that_stopped_producing_is_restarted() -> None:
    pub = _pub()
    pub.samples_since_start = 5
    pub.last_sample_monotonic = CLOCK - (pub.opts.expire_after_s + 1)

    fault = pub.check_watchdogs(CLOCK, CLOCK)
    assert fault is Fault.SAMPLE_TIMEOUT
    assert fault in app_mod._RESTART_FAULTS


def test_a_publish_stall_while_samples_flow_is_fatal() -> None:
    """The distinguishing case: samples are arriving and the broker is taking
    them, but HA is not seeing them. A reconnect will not fix that."""
    pub = _pub()
    pub.health.connected = True
    pub.samples_since_start = 5
    pub.last_sample_time = CLOCK
    pub.last_sample_monotonic = CLOCK
    pub.health.last_state_publish_ok = CLOCK - (pub.opts.expire_after_s + 1)

    fault = pub.check_watchdogs(CLOCK, CLOCK)
    assert fault is Fault.PUBLISH_STALLED
    assert app_mod._FAULT_EXIT[fault] == app_mod.EXIT_PUBLISH_STALLED


def test_never_having_published_since_the_first_sample_is_fatal_too() -> None:
    """Distinct from the branch above: last_state_publish_ok is still 0, so an
    age comparison against it would be meaningless."""
    pub = _pub()
    pub.health.connected = True
    pub.samples_since_start = 5
    pub.last_sample_time = CLOCK
    pub.last_sample_monotonic = CLOCK
    pub.health.last_state_publish_ok = 0.0
    pub.first_sample_time = CLOCK - (pub.opts.expire_after_s + 1)

    assert pub.check_watchdogs(CLOCK, CLOCK) is Fault.PUBLISH_STALLED


def test_a_publish_stall_is_not_reported_while_samples_are_absent() -> None:
    """Without the sample guard a quiet turbostat would masquerade as a publish
    stall and exit the process for the wrong reason."""
    pub = _pub()
    pub.health.connected = True
    pub.samples_since_start = 5
    pub.last_sample_monotonic = CLOCK
    # Last sample is ancient, so the publish branch must not be consulted.
    pub.last_sample_time = CLOCK - 100_000
    pub.health.last_state_publish_ok = CLOCK - 100_000

    assert pub.check_watchdogs(CLOCK, CLOCK) is Fault.NONE


def test_restarting_turbostat_clears_the_sample_state() -> None:
    """Left stale, the fresh process inherits the old one's clocks and the
    sample-timeout watchdog fires immediately on a turbostat that is fine."""
    pub = _pub()
    pub.samples_since_start = 7
    pub.last_sample_time = 1.0
    pub.last_sample_monotonic = 1.0
    pub.first_sample_time = 1.0
    pub.health.last_state_publish_ok = 1.0

    pub.on_turbostat_started()

    assert pub.samples_since_start == 0
    assert pub.last_sample_time == 0.0
    assert pub.last_sample_monotonic == 0.0
    assert pub.first_sample_time == 0.0
    assert pub.health.last_state_publish_ok == 0.0
    assert pub.turbostat_started_at > 0


# --- publishing --------------------------------------------------------------


class _Recorder:
    """Stands in for aiomqtt.Client, recording what would go on the wire."""

    def __init__(self, error: Exception | None = None) -> None:
        self.sent: list[tuple] = []
        self.error = error

    async def publish(self, topic, payload="", qos=0, retain=False):
        if self.error is not None:
            raise self.error
        self.sent.append((topic, payload, qos, retain))


HEADER = "Busy%  Bzy_MHz  PkgWatt\n"
SAMPLE = "12.5  3200  45.2\n"


async def test_a_sample_publishes_states_and_stamps_the_health_clock() -> None:
    pub = _pub(mqtt_base_topic="ts")
    pub.health.connected = True
    mq = _Recorder()

    assert await pub.publish_sample(mq, HEADER) is False  # header is not a sample
    assert await pub.publish_sample(mq, SAMPLE) is True

    states = {t: p for t, p, _q, _r in mq.sent if t.endswith("/state")}
    assert states["ts/pkgwatt/state"] == "45.2"
    assert pub.health.last_state_publish_ok > 0
    assert pub.samples_since_start == 1


async def test_discovery_precedes_the_first_state() -> None:
    """HA discards state for an entity it has not discovered yet."""
    pub = _pub(mqtt_base_topic="ts", mqtt_discovery_prefix="disc")
    pub.health.connected = True
    mq = _Recorder()

    await pub.publish_sample(mq, HEADER)
    await pub.publish_sample(mq, SAMPLE)

    topics = [t for t, *_ in mq.sent]
    assert topics[0] == "ts/availability"
    first_config = next(i for i, t in enumerate(topics) if t.endswith("/config"))
    first_state = next(i for i, t in enumerate(topics) if t.endswith("/state"))
    assert first_config < first_state


async def test_a_rejected_publish_does_not_stamp_the_health_clock() -> None:
    """The stamp sits after the await, so a broker fault skips it -- otherwise
    a broker refusing everything keeps the stall watchdog looking healthy."""
    pub = _pub(mqtt_base_topic="ts")
    pub.health.connected = True
    pub.parser.parse_line(HEADER)

    with pytest.raises(aiomqtt.MqttError):
        await pub.publish_sample(_Recorder(aiomqtt.MqttError("refused")), SAMPLE)

    assert pub.health.last_state_publish_ok == 0.0


async def test_a_new_session_republishes_discovery() -> None:
    """A broker restart drops retained config. Seeded True first: a fresh
    Publisher starts False, so asserting False alone would pass regardless."""
    pub = _pub()
    pub._discovered = True
    pub.on_connected()
    assert pub._discovered is False
    assert pub.health.connected is True


async def test_the_birth_message_forces_rediscovery() -> None:
    pub = _pub()
    pub._discovered = True
    pub.on_ha_birth()
    assert pub._discovered is False


async def test_the_heartbeat_is_rate_limited_to_the_interval() -> None:
    pub = _pub(mqtt_base_topic="ts", interval_seconds=10)
    mq = _Recorder()

    await pub.maybe_heartbeat(mq, CLOCK)
    await pub.maybe_heartbeat(mq, CLOCK + 1)  # inside the interval
    assert len([t for t, *_ in mq.sent if t == "ts/heartbeat"]) == 1

    await pub.maybe_heartbeat(mq, CLOCK + 11)
    assert len([t for t, *_ in mq.sent if t == "ts/heartbeat"]) == 2


async def test_the_heartbeat_distinguishes_never_from_just_now() -> None:
    """``None`` means it has not happened; 0.0 would claim it just did."""
    import json

    pub = _pub(mqtt_base_topic="ts")
    mq = _Recorder()
    await pub.publish_heartbeat(mq, CLOCK)

    hb = json.loads(next(p for t, p, *_ in mq.sent if t == "ts/heartbeat"))
    assert hb["last_sample_age_s"] is None
    assert hb["state_publish_age_s"] is None
    assert hb["connected"] is False


# --- the sample loop ---------------------------------------------------------


class _FakeTurbostat:
    """Feeds canned lines, then blocks so the loop hits its bounded wait."""

    def __init__(self, lines: list[str | None], *, gap: float = 0.0) -> None:
        self.lines = list(lines)
        self.restarts: list[str] = []
        self.proc = None
        self.gap = gap  # models turbostat emitting one line per interval
        self._read_task: asyncio.Task | None = None

    async def readline(self) -> str | None:
        if self.gap:
            await asyncio.sleep(self.gap)
        if self.lines:
            return self.lines.pop(0)
        await asyncio.sleep(3600)  # never returns; the race with stop must win
        return None

    # Mirrors Turbostat.next_line so the loop exercises the real control flow.
    next_line = app_mod.Turbostat.next_line

    async def restart(self, reason: str) -> bool:
        self.restarts.append(reason)
        return True

    async def stop(self) -> None:
        pass


async def test_a_raising_sample_does_not_end_the_loop(capsys) -> None:
    """One malformed line must not exit the process: unguarded, the raise
    reaches main()'s catch-all and returns EXIT_UNEXPECTED, so a single bad
    sample costs every sensor until the supervisor restarts us."""
    pub = _pub(interval_seconds=1)
    stop = asyncio.Event()
    calls: list[int] = []

    async def boom(_mq: object, _line: str) -> bool:
        calls.append(1)
        stop.set()
        raise RuntimeError("sample exploded")

    pub.publish_sample = boom  # type: ignore[method-assign]
    rc = await app_mod._sample_loop(_Recorder(), pub, _FakeTurbostat([SAMPLE]), stop)

    assert rc == app_mod.EXIT_OK
    assert calls == [1]
    assert "sample failed" in capsys.readouterr().out


async def test_a_broker_fault_leaves_the_loop_so_the_session_reconnects() -> None:
    """MqttError must stay distinct from our own bugs: swallowed, the loop would
    spin against a dead session with every sensor frozen."""
    pub = _pub(interval_seconds=1)
    stop = asyncio.Event()

    async def boom(_mq: object, _line: str) -> bool:
        raise aiomqtt.MqttError("broker went away")

    pub.publish_sample = boom  # type: ignore[method-assign]
    with pytest.raises(aiomqtt.MqttError):
        await app_mod._sample_loop(_Recorder(), pub, _FakeTurbostat([SAMPLE]), stop)


async def test_turbostat_eof_restarts_it_rather_than_exiting() -> None:
    """EOF means turbostat died; the MQTT session is still good, so replace the
    child instead of taking the whole add-on down."""
    pub = _pub(interval_seconds=1)
    stop = asyncio.Event()
    ts = _FakeTurbostat([None])

    async def restart(reason: str) -> bool:
        ts.restarts.append(reason)
        stop.set()  # end the loop once we have seen the restart
        return True

    ts.restart = restart  # type: ignore[method-assign]
    rc = await app_mod._sample_loop(_Recorder(), pub, ts, stop)

    assert rc == app_mod.EXIT_OK
    assert ts.restarts == ["process_eof"]


async def test_a_fatal_watchdog_returns_its_exit_code() -> None:
    pub = _pub(mqtt_disconnect_timeout_seconds=5, interval_seconds=1)
    pub.health.connected = False
    pub.health.last_disconnect = 1.0  # ancient, so the timeout has elapsed

    rc = await app_mod._sample_loop(
        _Recorder(), pub, _FakeTurbostat([]), asyncio.Event()
    )
    assert rc == app_mod.EXIT_MQTT_DOWN


async def test_a_quiet_turbostat_still_lets_shutdown_through() -> None:
    """The bounded wait is what makes SIGTERM prompt: with an unbounded read the
    loop would sit in readline() until a line that never comes."""
    pub = _pub(interval_seconds=1)
    stop = asyncio.Event()
    stop.set()

    rc = await asyncio.wait_for(
        app_mod._sample_loop(_Recorder(), pub, _FakeTurbostat([]), stop), timeout=5
    )
    assert rc == app_mod.EXIT_OK


async def test_shutdown_does_not_wait_out_the_sampling_interval() -> None:
    """SIGTERM must not sit unnoticed until the next sample is due.

    turbostat emits one line per interval, so a loop that only rechecks `stop`
    between samples notices a signal after up to `interval` seconds. config.json
    ships 30s and permits 60 -- both longer than the supervisor's whole 10s stop
    grace, so the bounded MQTT teardown would never run and every restart would
    end in SIGKILL with no farewell.

    The earlier tests all used interval_seconds=1, which hid this entirely, and
    the one above sets `stop` *before* entering the loop so it never waits while
    armed. This one enters the wait first, then signals.
    """
    pub = _pub(interval_seconds=60)  # the schema maximum
    stop = asyncio.Event()
    ts = _FakeTurbostat([SAMPLE] * 10, gap=60.0)  # a line only once per interval

    async def signal_soon() -> None:
        await asyncio.sleep(0.05)  # let the loop reach the wait first
        stop.set()

    started = asyncio.get_running_loop().time()
    async with asyncio.TaskGroup() as tg:
        tg.create_task(signal_soon())
        task = tg.create_task(app_mod._sample_loop(_Recorder(), pub, ts, stop))
    elapsed = asyncio.get_running_loop().time() - started

    assert task.result() == app_mod.EXIT_OK
    # Well under the 60s interval: the loop woke on the event, not the timeout.
    assert elapsed < 5, f"shutdown waited {elapsed:.1f}s for the sampling interval"


# --- reconnect backoff -------------------------------------------------------


async def test_a_broker_that_stays_down_backs_off_to_the_cap(monkeypatch) -> None:
    """A flat retry means a multi-hour outage draws a fresh TCP connect -- and a
    fresh DNS lookup, when mqtt_host is a name -- every few seconds throughout.

    Waits are recorded rather than served, so the ramp is asserted without the
    test taking the wall-clock time the ramp describes.
    """
    pub = _pub()
    stop = asyncio.Event()
    delays: list[float] = []

    async def dead_session(*_a: object) -> int:
        raise aiomqtt.MqttError("connection refused")

    async def record(_stop: object, seconds: float) -> None:
        delays.append(seconds)
        if len(delays) >= 8:
            stop.set()

    monkeypatch.setattr(app_mod, "_session", dead_session)
    monkeypatch.setattr(app_mod, "_wait_or_stop", record)
    rc = await app_mod._reconnect_loop(pub, _FakeTurbostat([]), stop)

    assert rc == app_mod.EXIT_OK
    assert delays == [3, 6, 12, 24, 48, 60, 60, 60]  # doubling, then capped


async def test_a_session_that_connected_restarts_the_ramp(monkeypatch) -> None:
    """Backoff is for a broker that is down. One that served us and dropped the
    connection must not inherit the previous outage's delay, or every later blip
    waits the full cap."""
    pub = _pub()
    stop = asyncio.Event()
    delays: list[float] = []
    rounds = [0]

    async def session(p: Publisher, *_a: object) -> int:
        rounds[0] += 1
        if rounds[0] >= 3:  # rounds 3+ get as far as connecting
            p.on_connected()
        raise aiomqtt.MqttError("dropped")

    async def record(_stop: object, seconds: float) -> None:
        delays.append(seconds)
        if len(delays) >= 5:
            stop.set()

    monkeypatch.setattr(app_mod, "_session", session)
    monkeypatch.setattr(app_mod, "_wait_or_stop", record)
    await app_mod._reconnect_loop(pub, _FakeTurbostat([]), stop)

    assert delays == [3, 6, 3, 3, 3]


def test_an_outage_is_stamped_once_not_on_every_retry() -> None:
    """The reconnect loop calls on_disconnected() per attempt. Restamping resets
    the very clock the disconnect watchdog measures, and since attempts cap at
    60s apart while the timeout defaults to 300s, the age could never reach it
    -- EXIT_MQTT_DOWN would be unreachable."""
    pub = _pub()
    pub.on_disconnected()
    first = pub.health.last_disconnect
    assert first > 0

    time.sleep(0.01)
    pub.on_disconnected()  # a later failed retry, same outage
    assert pub.health.last_disconnect == first

    # A session that reconnected and then dropped is a new outage.
    pub.on_connected()
    pub.on_disconnected()
    assert pub.health.last_disconnect > first


async def test_the_disconnect_watchdog_still_fires_with_no_session(monkeypatch) -> None:
    """With no session the sample loop is not running, so the reconnect loop is
    the only place left that can notice the broker has been gone too long."""
    pub = _pub(mqtt_disconnect_timeout_seconds=5)
    stop = asyncio.Event()
    # An outage that started well before the timeout. Set directly, since
    # on_disconnected() deliberately will not move it once an outage is running.
    pub.health.last_disconnect = time.time() - 100

    async def dead_session(*_a: object) -> int:
        raise aiomqtt.MqttError("connection refused")

    monkeypatch.setattr(app_mod, "_session", dead_session)
    rc = await asyncio.wait_for(
        app_mod._reconnect_loop(pub, _FakeTurbostat([]), stop), timeout=10
    )

    assert rc == app_mod.EXIT_MQTT_DOWN
