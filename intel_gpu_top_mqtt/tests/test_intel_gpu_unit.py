# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Unit tests for the orchestration that used to live inside main().

All of this was previously unreachable without a broker and a running
intel_gpu_top, so none of it was tested: option precedence and clamping, the
watchdog decisions and what each one costs, the sample loop's resilience, and
the reconnect backoff.
"""

import asyncio
import contextlib
import logging
import time

import aiomqtt
import pytest
from intel_gpu_mqtt import Fault, Publisher, from_sources
from intel_gpu_mqtt import app as app_mod

LOG = logging.getLogger("test")

# Watchdog tests do elapsed-time arithmetic against clocks that start at 0.0, so
# a real monotonic() would make them depend on the machine's uptime. Feed a
# synthetic clock large enough that any timeout under test has elapsed.
CLOCK = 1_000_000.0


def _opts(**over):
    cli = {
        k: None
        for k in (
            "interval_seconds",
            "mqtt_host",
            "mqtt_port",
            "mqtt_username",
            "mqtt_password",
            "mqtt_discovery_prefix",
            "mqtt_base_topic",
            "client_id",
            "preferred_device_regex",
            "log_level",
            "publish_raw_sample",
            "expire_after_multiplier",
            "mqtt_disconnect_timeout_seconds",
            "intel_restart_grace_seconds",
        )
    }
    return from_sources(cli, {"mqtt_host": "broker", **over})


def _pub(**over) -> Publisher:
    return Publisher(_opts(**over), LOG)


class _Recorder:
    """Stands in for aiomqtt.Client, recording what would go on the wire."""

    def __init__(self, error: Exception | None = None) -> None:
        self.sent: list[tuple] = []
        self.error = error

    async def publish(self, topic, payload="", qos=0, retain=False):
        if self.error is not None:
            raise self.error
        self.sent.append((topic, payload, qos, retain))


class _FakeGpu:
    """Feeds canned lines on a cadence, then blocks."""

    def __init__(self, lines: list[str | None], *, gap: float = 0.0) -> None:
        self.lines = list(lines)
        self.restarts: list[str] = []
        self.proc = None
        self.gap = gap  # models intel_gpu_top emitting on its own schedule
        self._read_task: asyncio.Task | None = None

    async def readline(self) -> str | None:
        if self.gap:
            await asyncio.sleep(self.gap)
        if self.lines:
            return self.lines.pop(0)
        await asyncio.sleep(3600)  # never returns; the race with stop must win
        return None

    async def restart(self, reason: str) -> bool:
        self.restarts.append(reason)
        return True

    async def stop(self) -> None:
        pass

    # Mirrors GpuTop.next_line so the loop exercises the real control flow.
    next_line = app_mod.GpuTop.next_line


# --- shutdown latency --------------------------------------------------------


async def test_shutdown_does_not_wait_out_the_sampling_interval() -> None:
    """SIGTERM must not sit unnoticed until the next sample is due.

    A loop that only rechecks `stop` between samples notices a signal after up
    to `interval` seconds. config.json ships 30s and permits 60 -- both longer
    than the supervisor's whole 10s stop grace, so the bounded MQTT teardown
    would never run and every restart would end in SIGKILL with no farewell.

    Written before the rest of this file deliberately: the same defect shipped
    in turbostat because every test there used interval_seconds=1, which hid it.
    """
    pub = _pub(interval_seconds=60)  # the schema maximum
    stop = asyncio.Event()
    gpu = _FakeGpu(["{}"] * 10, gap=60.0)  # a sample only once per interval

    async def signal_soon() -> None:
        await asyncio.sleep(0.05)  # let the loop reach the wait first
        stop.set()

    started = asyncio.get_running_loop().time()
    async with asyncio.TaskGroup() as tg:
        tg.create_task(signal_soon())
        task = tg.create_task(app_mod._sample_loop(_Recorder(), pub, gpu, stop))
    elapsed = asyncio.get_running_loop().time() - started

    assert task.result() == app_mod.EXIT_OK
    assert elapsed < 5, f"shutdown waited {elapsed:.1f}s for the sampling interval"


async def test_a_quiet_gpu_still_lets_shutdown_through() -> None:
    """Nothing ever arrives: the race with stop is the only way out.

    The elapsed assertion is the point. Without it this passes even when the
    loop merely polls -- it would just take a full interval to notice -- so it
    would report healthy against the very defect it names.
    """
    pub = _pub(interval_seconds=60)
    stop = asyncio.Event()
    gpu = _FakeGpu([])

    async def signal_soon() -> None:
        await asyncio.sleep(0.05)
        stop.set()

    started = asyncio.get_running_loop().time()
    async with asyncio.TaskGroup() as tg:
        tg.create_task(signal_soon())
        task = tg.create_task(app_mod._sample_loop(_Recorder(), pub, gpu, stop))
    elapsed = asyncio.get_running_loop().time() - started

    assert task.result() == app_mod.EXIT_OK
    assert elapsed < 5, f"shutdown waited {elapsed:.1f}s with nothing to read"


# --- options -----------------------------------------------------------------


def test_cli_beats_the_options_file_which_beats_the_default() -> None:
    cli = {"interval_seconds": 7, "mqtt_base_topic": None, "mqtt_host": None}
    o = from_sources(cli, {"mqtt_base_topic": "from_file", "mqtt_host": "broker"})
    assert o.interval == 7  # CLI
    assert o.base_topic == "from_file"  # options file
    assert o.client_id == "intel-gpu-top-addon"  # built-in default


def test_a_zero_from_the_options_file_is_clamped_not_treated_as_absent() -> None:
    """`is None` at each step, not falsiness: 0 is a value a user can set, and
    reading it as absent would silently promote the default over their choice."""
    o = _opts(interval_seconds=0)
    assert o.interval == 1  # the floor, not the default of 5


def test_bounds_are_clamped_at_both_ends() -> None:
    assert _opts(expire_after_multiplier=0, interval_seconds=30).expire_after_s == 60
    assert _opts(expire_after_multiplier=99, interval_seconds=30).expire_after_s == 300
    assert _opts(mqtt_disconnect_timeout_seconds=1).disconnect_timeout == 5
    assert _opts(mqtt_disconnect_timeout_seconds=9999).disconnect_timeout == 600


def test_expire_after_has_a_floor_so_entities_do_not_flap() -> None:
    """A 1s interval x4 would expire entities faster than HA can notice."""
    assert _opts(interval_seconds=1).expire_after_s == 60


def test_summary_never_includes_the_password() -> None:
    o = _opts(mqtt_password="sup3rsecret", mqtt_username="brian")
    assert "sup3rsecret" not in o.summary()
    assert "brian" in o.summary()


def test_derived_topics_strip_the_trailing_slash() -> None:
    o = _opts(mqtt_base_topic="gpu/")
    assert o.base_topic == "gpu"
    assert o.availability_topic == "gpu/availability"
    assert o.interval_ms == o.interval * 1000


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (True, True),
        ("yes", True),
        ("1", True),
        (1, True),
        (False, False),
        ("no", False),
        ("0", False),
        ("", False),
    ],
)
def test_publish_raw_accepts_the_usual_truthy_spellings(value, expected) -> None:
    assert _opts(publish_raw_sample=value).publish_raw is expected


# --- watchdogs ---------------------------------------------------------------


def test_nothing_wrong_is_the_default() -> None:
    assert _pub().check_watchdogs(CLOCK, CLOCK) is Fault.NONE


def test_mqtt_down_past_its_timeout_is_fatal() -> None:
    pub = _pub(mqtt_disconnect_timeout_seconds=100)
    pub.health.connected = False
    pub.health.last_disconnect = CLOCK - 200

    assert pub.check_watchdogs(CLOCK, CLOCK) is Fault.MQTT_DOWN
    assert app_mod._FAULT_EXIT[Fault.MQTT_DOWN] == app_mod.EXIT_MQTT_DOWN


def test_a_vanished_render_node_restarts_rather_than_exits(tmp_path) -> None:
    """Re-selecting the device is the useful response to a yanked GPU; exiting
    would take a healthy MQTT session down with it."""
    pub = _pub()
    pub.device_path = str(tmp_path / "renderD128")  # does not exist

    fault = pub.check_watchdogs(CLOCK, CLOCK)
    assert fault is Fault.RENDER_NODE_GONE
    assert fault in app_mod._RESTART_FAULTS
    assert fault not in app_mod._FAULT_EXIT


def test_a_present_render_node_is_not_reported(tmp_path) -> None:
    node = tmp_path / "renderD128"
    node.write_text("")
    pub = _pub()
    pub.device_path = str(node)
    assert pub.check_watchdogs(CLOCK, CLOCK) is Fault.NONE


def test_samples_drying_up_restarts_the_binary() -> None:
    pub = _pub()
    pub.last_sample_monotonic = CLOCK - (pub.opts.expire_after_s + 1)

    fault = pub.check_watchdogs(CLOCK, CLOCK)
    assert fault is Fault.SAMPLE_TIMEOUT
    assert fault in app_mod._RESTART_FAULTS


def test_an_outage_is_stamped_once_not_on_every_retry() -> None:
    """The reconnect loop calls on_disconnected() per attempt. Restamping resets
    the very clock the disconnect watchdog measures, so EXIT_MQTT_DOWN would be
    unreachable."""
    pub = _pub()
    pub.on_disconnected()
    first = pub.health.last_disconnect
    assert first > 0

    time.sleep(0.01)
    pub.on_disconnected()
    assert pub.health.last_disconnect == first

    pub.on_connected()
    pub.on_disconnected()
    assert pub.health.last_disconnect > first


# --- sampling ----------------------------------------------------------------

# Shaped like real intel_gpu_top -J output: ONE continuous array, so the first
# sample opens with "[" and every later one continues with ",". Feeding repeated
# self-contained "[{...}]" blocks would not parse, because the leftover "]" from
# the previous block poisons the buffer -- worth modelling exactly, since the
# buffering is the thing under test.
_BODY = (
    '{{"rc6":{{"value":{rc6}}},'
    '"frequency":{{"requested":300.0,"actual":350.0}},'
    '"interrupts":{{"count":12.0}},'
    '"power":{{"GPU":{gpu},"Package":8.0}},'
    '"engines":{{"Render/3D":{{"busy":10.0,"sema":0.0,"wait":0.0}}}}}}\n'
)
SAMPLE = "[" + _BODY.format(rc6=75.0, gpu=1.5)
SAMPLE_NEXT = "," + _BODY.format(rc6=80.0, gpu=2.5)


async def test_the_first_sample_after_a_start_is_not_published() -> None:
    """intel_gpu_top's first sample is cumulative-since-boot rather than an
    interval rate, so publishing it would spike every graph."""
    pub = _pub(interval_seconds=1)
    pub.health.connected = True
    pub.on_gpu_started("/dev/dri/renderD128")
    mq = _Recorder()

    assert await pub.feed(mq, SAMPLE) is True
    assert pub.samples_since_start == 1
    assert not [t for t, *_ in mq.sent if t.endswith("/state")]


async def test_a_partial_sample_is_buffered_until_complete() -> None:
    """intel_gpu_top pretty-prints its JSON, so a sample spans many lines and
    only the buffer knows when one has finished."""
    pub = _pub()
    pub.health.connected = True
    mq = _Recorder()

    assert await pub.feed(mq, '[{"power":') is False
    assert await pub.feed(mq, '{"GPU":1.5}}\n]\n') is True


async def test_discovery_precedes_the_first_state() -> None:
    """HA discards state for an entity it has not discovered yet."""
    pub = _pub(interval_seconds=1)
    pub.health.connected = True
    mq = _Recorder()

    await pub.feed(mq, SAMPLE)  # warm-up: discovery only, no states
    pub._last_publish_monotonic = 0.0  # clear the rate limit for the next one
    await pub.feed(mq, SAMPLE_NEXT)

    topics = [t for t, *_ in mq.sent]
    assert topics[0].endswith("/availability")
    first_config = next(i for i, t in enumerate(topics) if t.endswith("/config"))
    first_state = next(i for i, t in enumerate(topics) if t.endswith("/state"))
    assert first_config < first_state


async def test_a_new_session_republishes_discovery() -> None:
    """A broker restart drops retained config. Seeded True first: a fresh
    Publisher starts False, so asserting False alone would pass regardless."""
    pub = _pub()
    pub._discovered = True
    pub.on_connected()
    assert pub._discovered is False


async def test_the_birth_message_forces_rediscovery() -> None:
    pub = _pub()
    pub._discovered = True
    pub.on_ha_birth()
    assert pub._discovered is False


async def test_the_heartbeat_is_rate_limited_to_the_interval() -> None:
    pub = _pub(interval_seconds=10)
    mq = _Recorder()

    await pub.maybe_heartbeat(mq, CLOCK)
    await pub.maybe_heartbeat(mq, CLOCK + 1)
    assert len(mq.sent) == 1

    await pub.maybe_heartbeat(mq, CLOCK + 11)
    assert len(mq.sent) == 2


# --- the sample loop ---------------------------------------------------------


async def test_a_raising_sample_does_not_end_the_loop(caplog) -> None:
    """One malformed sample must not exit the process: unguarded, the raise
    reaches main()'s catch-all and returns EXIT_UNEXPECTED."""
    pub = _pub(interval_seconds=1)
    stop = asyncio.Event()
    calls: list[int] = []

    async def boom(_mq, _line):
        calls.append(1)
        stop.set()
        raise RuntimeError("sample exploded")

    pub.feed = boom  # type: ignore[method-assign]
    rc = await app_mod._sample_loop(_Recorder(), pub, _FakeGpu([SAMPLE]), stop)

    assert rc == app_mod.EXIT_OK
    assert calls == [1]


async def test_a_broker_fault_leaves_the_loop_so_the_session_reconnects() -> None:
    """MqttError must stay distinct from our own bugs: swallowed, the loop would
    spin against a dead session with every sensor frozen."""
    pub = _pub(interval_seconds=1)

    async def boom(_mq, _line):
        raise aiomqtt.MqttError("broker went away")

    pub.feed = boom  # type: ignore[method-assign]
    with pytest.raises(aiomqtt.MqttError):
        await app_mod._sample_loop(
            _Recorder(), pub, _FakeGpu([SAMPLE]), asyncio.Event()
        )


async def test_eof_restarts_the_binary_rather_than_exiting() -> None:
    """EOF means intel_gpu_top died; the MQTT session is still good."""
    pub = _pub(interval_seconds=1)
    stop = asyncio.Event()
    gpu = _FakeGpu([None])

    async def restart(reason: str) -> bool:
        gpu.restarts.append(reason)
        stop.set()
        return True

    gpu.restart = restart  # type: ignore[method-assign]
    rc = await app_mod._sample_loop(_Recorder(), pub, gpu, stop)

    assert rc == app_mod.EXIT_OK
    assert gpu.restarts == ["intel_gpu_top_exited"]


async def test_a_fatal_watchdog_returns_its_exit_code() -> None:
    pub = _pub(mqtt_disconnect_timeout_seconds=5, interval_seconds=1)
    pub.health.connected = False
    pub.health.last_disconnect = 1.0  # ancient

    rc = await app_mod._sample_loop(_Recorder(), pub, _FakeGpu([]), asyncio.Event())
    assert rc == app_mod.EXIT_MQTT_DOWN


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

    async def dead_session(*_a):
        raise aiomqtt.MqttError("connection refused")

    async def record(_stop, seconds):
        delays.append(seconds)
        if len(delays) >= 8:
            stop.set()

    monkeypatch.setattr(app_mod, "_session", dead_session)
    monkeypatch.setattr(app_mod, "_wait_or_stop", record)
    assert await app_mod._reconnect_loop(pub, _FakeGpu([]), stop) == app_mod.EXIT_OK
    assert delays == [3, 6, 12, 24, 48, 60, 60, 60]


async def test_a_session_that_connected_restarts_the_ramp(monkeypatch) -> None:
    """Backoff is for a broker that is down. One that served us and dropped the
    connection must not inherit the previous outage's delay."""
    pub = _pub()
    stop = asyncio.Event()
    delays: list[float] = []
    rounds = [0]

    async def session(p, *_a):
        rounds[0] += 1
        if rounds[0] >= 3:
            p.on_connected()
        raise aiomqtt.MqttError("dropped")

    async def record(_stop, seconds):
        delays.append(seconds)
        if len(delays) >= 5:
            stop.set()

    monkeypatch.setattr(app_mod, "_session", session)
    monkeypatch.setattr(app_mod, "_wait_or_stop", record)
    await app_mod._reconnect_loop(pub, _FakeGpu([]), stop)

    assert delays == [3, 6, 3, 3, 3]


async def test_the_disconnect_watchdog_still_fires_with_no_session(monkeypatch) -> None:
    """With no session the sample loop is not running, so the reconnect loop is
    the only place left that can notice the broker has been gone too long."""
    pub = _pub(mqtt_disconnect_timeout_seconds=5)
    pub.health.last_disconnect = time.time() - 100

    async def dead_session(*_a):
        raise aiomqtt.MqttError("connection refused")

    monkeypatch.setattr(app_mod, "_session", dead_session)
    rc = await asyncio.wait_for(
        app_mod._reconnect_loop(pub, _FakeGpu([]), asyncio.Event()), timeout=10
    )
    assert rc == app_mod.EXIT_MQTT_DOWN


# --- the loop must stay free during a restart --------------------------------


class _FakeProc:
    stdout = None
    stderr = None
    returncode = None


async def test_device_enumeration_does_not_freeze_the_loop(monkeypatch) -> None:
    """`intel_gpu_top -L` is a blocking subprocess call with a 5s timeout.

    Under paho it ran while MQTT lived on its own thread, so stalling here cost
    nothing. This loop now owns MQTT I/O, the heartbeat and `stop.set()` from the
    signal handler, so blocking on it freezes all three -- and 5s on top of the
    ~9s teardown budget puts a restart-then-SIGTERM past the supervisor's grace.

    Asserted by watching whether anything else gets scheduled while the restart
    is in flight, which is the property that actually matters.
    """
    blocked_for = 0.4
    ticks: list[int] = []

    def slow_listing(_log):
        time.sleep(blocked_for)  # exactly what check_output does to the loop
        return ""

    async def fake_spawn(*_a):
        return _FakeProc()

    monkeypatch.setattr(app_mod, "list_intel_gpu_top_devices", slow_listing)
    monkeypatch.setattr(app_mod, "auto_select_device_arg", lambda *_a: (None, None))
    monkeypatch.setattr(app_mod, "start_intel_gpu_top", fake_spawn)

    async def ticker() -> None:
        while True:
            ticks.append(1)
            await asyncio.sleep(0.02)

    gpu = app_mod.GpuTop(_pub(), LOG)
    tick_task = asyncio.create_task(ticker())
    await asyncio.sleep(0)  # let the ticker start before we time anything
    ticks.clear()
    try:
        await gpu.restart("initial_start")
    finally:
        tick_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await tick_task

    # On-loop, nothing else can run for the whole 0.4s and this is ~0.
    assert len(ticks) > 5, f"event loop was frozen during restart ({len(ticks)} ticks)"
