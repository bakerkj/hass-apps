# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Unit tests for the pieces that were unreachable while they lived inside a
closure in app.py: option clamping, the watchdog decision, and the publisher's
own publish/session behaviour.
"""

import asyncio
import time

import aiomqtt
import pytest
from direwolf_igate.app import _publish_loop

from direwolf_igate import DirewolfParser, Options, Publisher, from_mapping, overdue
from direwolf_igate import app as app_mod

MYCALL = "N0CALL-10"

# Watchdog tests do elapsed-time arithmetic against a `last_warned` that starts
# at 0.0, so a real time.monotonic() makes them depend on the machine's uptime:
# they pass on a long-running box and fail on a freshly booted CI runner. Feed a
# synthetic clock large enough that any timeout under test has elapsed.
CLOCK = 1_000_000.0


# --- options -----------------------------------------------------------------


def test_bounds_match_config_json_at_both_ends() -> None:
    """Schema is interval_seconds int(5,300), expire_after_multiplier int(2,10),
    disconnect int(5,600). A value arriving without Supervisor validation -- a
    hand-edited options.json -- must be clamped to the same range, upper end
    included; an unbounded interval would stall the sensors indefinitely."""
    fast = from_mapping({"interval_seconds": 0, "expire_after_multiplier": 0})
    assert fast.interval == 5  # schema floor, not 1
    assert fast.expire_after_s == 60

    slow = from_mapping({"interval_seconds": 9999, "expire_after_multiplier": 99})
    assert slow.interval == 300  # schema ceiling
    assert slow.expire_after_s == 300 * 10

    assert from_mapping({"mqtt_disconnect_timeout_seconds": 1}).disconnect_timeout == 5
    assert (
        from_mapping({"mqtt_disconnect_timeout_seconds": 9999}).disconnect_timeout
        == 600
    )


def test_derived_topics_and_device_id() -> None:
    o = from_mapping({"mqtt_base_topic": "dw/", "client_id": "my-igate"})
    assert o.base_topic == "dw"  # trailing slash stripped
    assert o.availability_topic == "dw/availability"
    assert o.heartbeat_topic == "dw/heartbeat"
    # HA entity IDs cannot contain hyphens.
    assert o.device_id == "my_igate"


def test_stall_timeout_is_derived_from_the_disconnect_timeout() -> None:
    o = from_mapping({"mqtt_disconnect_timeout_seconds": 120})
    assert o.stall_timeout == 240


def test_summary_never_includes_the_password() -> None:
    # Distinctive values: a short username like "u" matches the *label*
    # ("mqtt_username:") and proves nothing.
    o = from_mapping({"mqtt_password": "sup3rsecret", "mqtt_username": "brian"})
    summary = o.summary(MYCALL)
    assert "sup3rsecret" not in summary
    assert "brian" in summary
    assert MYCALL in summary


# --- watchdog decision -------------------------------------------------------


def test_overdue_requires_the_condition_to_have_started() -> None:
    """A `since` of 0 means it never started; firing then would warn about a
    disconnect that never happened."""
    assert not overdue(now_mono=1000.0, since=0.0, last_warned=0.0, timeout=10)


def test_overdue_waits_for_the_timeout_then_rate_limits() -> None:
    assert not overdue(now_mono=1005.0, since=1000.0, last_warned=0.0, timeout=10)
    assert overdue(now_mono=1011.0, since=1000.0, last_warned=0.0, timeout=10)
    # Already warned recently: stay quiet rather than repeat every interval.
    assert not overdue(now_mono=1015.0, since=1000.0, last_warned=1011.0, timeout=10)
    assert overdue(now_mono=1022.0, since=1000.0, last_warned=1011.0, timeout=10)


# --- publisher ---------------------------------------------------------------


class _Recorder:
    """Stands in for ``aiomqtt.Client``, recording what would go on the wire.

    ``error`` arms it to raise instead, which is how aiomqtt reports a broker
    fault -- there is no return code to inspect.
    """

    def __init__(self, sent: list[tuple], error: Exception | None = None) -> None:
        self.sent = sent
        self.error = error

    async def publish(self, topic, payload="", qos=0, retain=False):
        if self.error is not None:
            raise self.error
        self.sent.append((topic, payload, qos, retain))


def _publisher(**over: object) -> tuple[Publisher, list[tuple], _Recorder]:
    opts: Options = from_mapping({"mqtt_enabled": True, **over})
    pub = Publisher(opts, DirewolfParser(MYCALL))
    sent: list[tuple] = []
    return pub, sent, _Recorder(sent)


async def test_states_are_retained_except_the_connectivity_sensor() -> None:
    pub, sent, mq = _publisher(mqtt_base_topic="base")
    await pub.publish_states(mq)

    by_topic = {t: (p, q, r) for t, p, q, r in sent}
    assert by_topic["base/rf_packets_received/state"][2] is True
    # expire_after lives on this one, so a retained value would restart the
    # expiry timer after an HA restart.
    assert by_topic["base/igate_connected/state"][2] is False


async def test_unknown_counters_are_published_as_unknown_not_omitted() -> None:
    """State topics are retained, so omitting a key leaves the broker replaying
    the last known value as if current. "None" reads as unknown."""
    pub, sent, mq = _publisher(mqtt_base_topic="base")
    await pub.publish_states(mq)
    by_topic = {t: p for t, p, *_ in sent}

    assert by_topic["base/packets_uploaded/state"] == "None"
    assert by_topic["base/uploaded_rate/state"] == "None"
    assert by_topic["base/rf_packets_received/state"] == "0"  # known from start


def test_a_new_session_republishes_discovery() -> None:
    """A broker restart drops retained config, so every session rediscovers.
    Seeded True first: a fresh Publisher starts False, so asserting False alone
    would pass even with the reset deleted."""
    pub, sent, _ = _publisher()
    pub._discovered = True

    pub.on_connected()

    assert pub.health.connected is True
    assert pub.health.last_connect_ok > 0
    assert pub._discovered is False
    assert sent == []  # on_connected only marks state; tick() does the sending


def test_birth_message_forces_rediscovery() -> None:
    pub, _, _ = _publisher()
    pub._discovered = True

    pub.on_ha_birth()

    assert pub._discovered is False


def test_disconnect_records_both_clocks() -> None:
    """Seeded True first: `connected` is already False on a fresh Publisher."""
    pub, _, _ = _publisher()
    pub.health.connected = True

    pub.on_disconnected()

    assert pub.health.connected is False
    assert pub.health.last_disconnect > 0
    assert pub.health.last_disconnect_monotonic > 0


async def test_the_farewell_supersedes_the_will_retained() -> None:
    """The will is retained, so the farewell must be too -- an unretained one
    would leave the broker replaying `offline` to every later subscriber."""
    pub, sent, mq = _publisher(mqtt_base_topic="base")

    await pub.publish_availability(mq, "offline")

    assert sent == [("base/availability", "offline", 1, True)]


# --- watchdog wiring (the predicate is tested above; this is the call) --------


def test_stall_watchdog_fires_at_its_own_window_not_the_disconnect_one(capsys) -> None:
    """Wired with the wrong timeout the stall warning fires at half its window;
    with `since`/`last_warned` transposed it never fires at all."""
    pub, _, _ = _publisher(mqtt_disconnect_timeout_seconds=100)
    now = CLOCK
    o = pub.opts
    assert o.stall_timeout == 200

    # Past the disconnect timeout but NOT the stall timeout: must stay quiet.
    pub._last_output_monotonic = now - (o.disconnect_timeout + 10)
    pub.check_watchdogs(now)
    assert "No output from direwolf" not in capsys.readouterr().out

    pub._last_output_monotonic = now - (o.stall_timeout + 1)
    pub.check_watchdogs(now)
    assert "No output from direwolf" in capsys.readouterr().out


def test_stall_watchdog_stays_quiet_before_any_output(capsys) -> None:
    """A zero stamp means direwolf has not spoken yet, not that it has hung."""
    pub, _, _ = _publisher()
    pub._last_output_monotonic = 0.0
    pub.check_watchdogs(CLOCK)
    assert "No output from direwolf" not in capsys.readouterr().out


def test_disconnect_watchdog_needs_both_disconnected_and_overdue(capsys) -> None:
    pub, _, _ = _publisher(mqtt_disconnect_timeout_seconds=100)
    now = CLOCK
    pub.health.last_disconnect_monotonic = now - 200

    # Overdue but connected again: nothing to warn about.
    pub.health.connected = True
    pub.check_watchdogs(now)
    assert "MQTT disconnected for" not in capsys.readouterr().out

    pub.health.connected = False
    pub.check_watchdogs(now)
    out = capsys.readouterr().out
    assert "MQTT disconnected for" in out
    # And it must not blame direwolf at the same time.
    assert "No output from direwolf" not in out


def test_watchdog_warnings_are_rate_limited(capsys) -> None:
    pub, _, _ = _publisher(mqtt_disconnect_timeout_seconds=100)
    now = CLOCK
    pub._last_output_monotonic = now - 500

    pub.check_watchdogs(now)
    assert "No output from direwolf" in capsys.readouterr().out
    pub.check_watchdogs(now + 1)  # immediately again
    assert "No output from direwolf" not in capsys.readouterr().out


# --- tick() and the reader-thread entry point --------------------------------


async def test_tick_discovers_before_publishing_states_on_a_reconnect(capsys) -> None:
    """The birth-message path clears the flag so tick() rediscovers. HA discards
    state for an entity it has not discovered, so the order matters here too --
    not only in _on_connect."""
    pub, sent, mq = _publisher(mqtt_base_topic="base", mqtt_discovery_prefix="disc")
    pub.health.connected = True
    pub._discovered = False

    await pub.tick(mq)

    topics = [t for t, *_ in sent]
    first_config = next(i for i, t in enumerate(topics) if t.endswith("/config"))
    first_state = next(i for i, t in enumerate(topics) if t.endswith("/state"))
    assert first_config < first_state
    assert pub._discovered is True


async def test_tick_does_not_rediscover_once_discovered() -> None:
    pub, sent, mq = _publisher(mqtt_base_topic="base")
    pub.health.connected = True
    pub._discovered = True

    await pub.tick(mq)

    assert not [t for t, *_ in sent if t.endswith("/config")]


async def test_tick_publishes_a_heartbeat_every_cycle() -> None:
    pub, sent, mq = _publisher(mqtt_base_topic="base")
    await pub.tick(mq)
    beats = [(t, p) for t, p, _q, r in sent if t == "base/heartbeat" and r is False]
    assert len(beats) == 1
    assert "last_output_age_s" in beats[0][1]


def test_feed_observed_stamps_both_clocks_and_parses() -> None:
    """These stamps are the only inputs to the stall watchdog and to
    last_output_age_s -- the two things that reveal a hung direwolf."""
    pub, _, _ = _publisher()
    assert pub._last_output_wall == 0.0
    assert pub._last_output_monotonic == 0.0

    pub.feed_observed("[0.3] W1XM-15>APOT30:!4221.62N/07105.36Wr test\n")

    assert abs(pub._last_output_wall - time.time()) < 60
    assert pub._last_output_monotonic > 0
    assert pub.parser.stats.rf_packets_received == 1


def test_feed_observed_survives_a_parser_failure() -> None:
    """A parse bug must not kill the pipe that carries direwolf's output."""
    pub, _, _ = _publisher()

    def boom(_line: str) -> None:
        raise RuntimeError("parser exploded")

    pub.parser.feed = boom  # type: ignore[method-assign]
    pub.feed_observed("anything\n")  # must not raise
    assert pub._last_output_monotonic > 0  # stamped before the parse


async def test_a_broker_fault_propagates_out_of_tick() -> None:
    """app.py reconnects by catching MqttError around the publish loop, so tick
    must not swallow it: a swallowed fault leaves the publisher cycling against
    a dead session, with every sensor silently frozen."""
    pub, _, mq = _publisher(mqtt_base_topic="base")
    mq.error = aiomqtt.MqttError("broker went away")

    with pytest.raises(aiomqtt.MqttError):
        await pub.tick(mq)


# --- the publish loop, which moved out of Publisher into app.py --------------


async def test_a_raising_tick_does_not_end_the_publish_loop(capsys) -> None:
    """One bad cycle must not end them all. Unguarded, the raise travels out of
    _session, past _run's MqttError-only catch, and into main()'s catch-all,
    which degrades to a plain pass-through for the rest of the process lifetime
    -- every sensor and both watchdogs gone until the container restarts."""
    pub, _, mq = _publisher(interval_seconds=5)
    stop = asyncio.Event()
    calls: list[int] = []

    async def boom(_mq: object) -> None:
        calls.append(1)
        stop.set()  # end the loop after this cycle
        raise RuntimeError("tick exploded")

    pub.tick = boom  # type: ignore[method-assign]
    await _publish_loop(mq, pub, stop)  # must return, not propagate

    assert calls == [1]
    assert "publish cycle failed" in capsys.readouterr().out


async def test_a_broker_fault_still_leaves_the_loop_to_reconnect() -> None:
    """The guard above must not swallow MqttError too: that is the signal _run
    uses to drop the session and reconnect. Caught here, the loop would spin
    against a dead broker forever with every sensor frozen."""
    pub, _, mq = _publisher(interval_seconds=5)
    stop = asyncio.Event()

    async def boom(_mq: object) -> None:
        raise aiomqtt.MqttError("broker went away")

    pub.tick = boom  # type: ignore[method-assign]
    with pytest.raises(aiomqtt.MqttError):
        await _publish_loop(mq, pub, stop)


# --- reconnect backoff -------------------------------------------------------


async def test_a_broker_that_stays_down_backs_off_to_the_cap(monkeypatch) -> None:
    """A flat retry means a multi-hour outage draws a fresh TCP connect -- and a
    fresh DNS lookup, when mqtt_host is a name -- every few seconds throughout.

    The waits are recorded rather than served, so the ramp is asserted without
    the test taking the wall-clock time the ramp describes.
    """
    pub, _, _ = _publisher()
    stop = asyncio.Event()
    delays: list[float] = []

    async def dead_session(_pub: object, _stop: object) -> None:
        raise aiomqtt.MqttError("connection refused")

    async def record(_stop: object, seconds: float) -> None:
        delays.append(seconds)
        if len(delays) >= 8:
            stop.set()

    monkeypatch.setattr(app_mod, "_session", dead_session)
    monkeypatch.setattr(app_mod, "_wait_or_stop", record)
    await app_mod._reconnect_loop(pub, stop)

    assert delays == [3, 6, 12, 24, 48, 60, 60, 60]  # doubling, then capped


async def test_a_session_that_connected_restarts_the_ramp(monkeypatch) -> None:
    """Backoff is for a broker that is down. A broker that served us and then
    dropped the connection must not inherit the previous outage's delay --
    otherwise one long outage leaves every later blip waiting the full cap."""
    pub, _, _ = _publisher()
    stop = asyncio.Event()
    delays: list[float] = []
    rounds = [0]

    async def session(p: Publisher, _stop: object) -> None:
        rounds[0] += 1
        # Rounds 3+ get as far as connecting before the broker drops them.
        if rounds[0] >= 3:
            p.on_connected()
        raise aiomqtt.MqttError("dropped")

    async def record(_stop: object, seconds: float) -> None:
        delays.append(seconds)
        if len(delays) >= 5:
            stop.set()

    monkeypatch.setattr(app_mod, "_session", session)
    monkeypatch.setattr(app_mod, "_wait_or_stop", record)
    await app_mod._reconnect_loop(pub, stop)

    # Two failures ramp 3 -> 6; the first connected session resets to 3.
    assert delays == [3, 6, 3, 3, 3]


# --- broker resolution: option, then Supervisor service, then default --------


def test_explicit_option_beats_the_supervisor_service(monkeypatch) -> None:
    monkeypatch.setenv("SVC_MQTT_HOST", "svc-broker")
    monkeypatch.setenv("SVC_MQTT_USERNAME", "svc-user")
    o = from_mapping({"mqtt_host": "my-broker", "mqtt_username": "me"})
    assert o.mqtt_host == "my-broker"
    assert o.mqtt_username == "me"


def test_supervisor_service_is_used_when_the_option_is_blank(monkeypatch) -> None:
    """The shipped defaults are blank so a user running the Mosquitto add-on
    needs no credentials of their own."""
    monkeypatch.setenv("SVC_MQTT_HOST", "core-mosquitto")
    monkeypatch.setenv("SVC_MQTT_PORT", "1884")
    monkeypatch.setenv("SVC_MQTT_USERNAME", "svc-user")
    monkeypatch.setenv("SVC_MQTT_PASSWORD", "svc-pass")
    o = from_mapping({"mqtt_host": "", "mqtt_username": "", "mqtt_password": ""})
    assert (o.mqtt_host, o.mqtt_port) == ("core-mosquitto", 1884)
    assert (o.mqtt_username, o.mqtt_password) == ("svc-user", "svc-pass")


def test_falls_back_to_the_built_in_default_with_neither(monkeypatch) -> None:
    for v in (
        "SVC_MQTT_HOST",
        "SVC_MQTT_PORT",
        "SVC_MQTT_USERNAME",
        "SVC_MQTT_PASSWORD",
    ):
        monkeypatch.delenv(v, raising=False)
    o = from_mapping({})
    assert o.mqtt_host == "core-mosquitto"
    assert o.mqtt_port == 1883
    assert o.mqtt_username == ""


async def test_state_publishes_stamp_the_health_clock() -> None:
    """publish_states() must pass mark_state; without it last_state_publish_ok
    stays 0 and the heartbeat's state_publish_age_s is permanently null -- the
    field that exists to reveal a stuck publisher."""
    pub, _, mq = _publisher(mqtt_base_topic="base")
    assert pub.health.last_state_publish_ok == 0.0

    await pub.publish_states(mq)

    assert pub.health.last_state_publish_ok > 0.0


async def test_a_rejected_publish_does_not_stamp_the_health_clock() -> None:
    """A broker refusing every publish must not keep refreshing the age. The
    stamp sits after the await, so a raise skips it."""
    pub, _, mq = _publisher(mqtt_base_topic="base")
    mq.error = aiomqtt.MqttError("refused")

    with pytest.raises(aiomqtt.MqttError):
        await pub.publish_states(mq)

    assert pub.health.last_state_publish_ok == 0.0


async def test_tick_marks_discovery_done() -> None:
    """Left unset, every tick would republish the whole discovery set."""
    pub, _, mq = _publisher()
    pub._discovered = False
    await pub.tick(mq)
    assert pub._discovered is True


def test_defaults_are_read_from_config_json_not_restated() -> None:
    """Hardcoding the values made the invariant in this test's name unfailable."""
    import json
    import pathlib

    cfg = json.loads(
        (pathlib.Path(__file__).resolve().parents[1] / "config.json").read_text()
    )
    shipped = cfg["options"]
    o = from_mapping({})

    assert o.mqtt_enabled is shipped["mqtt_enabled"]
    assert o.interval == shipped["interval_seconds"]
    assert o.log_level == shipped["log_level"]
    assert o.client_id == shipped["client_id"]
    assert o.base_topic == shipped["mqtt_base_topic"]
    assert o.discovery_prefix == shipped["mqtt_discovery_prefix"]


async def test_tick_samples_the_rf_rate_so_it_moves_without_traffic() -> None:
    """Nothing in the stream announces a quiet minute, so without a timer
    sample the RF rate stays unknown on a live gate and never falls on a deaf
    one."""
    pub, sent, mq = _publisher(mqtt_base_topic="base")
    pub.health.connected = True

    await pub.tick(mq)
    assert pub.parser.stats.rf_rate is None  # one sample so far

    pub.parser.feed("[0.3] W1XM-15>APOT30:!4221.62N/07105.36Wr")
    await asyncio.sleep(1.05)  # clear the sub-second guard
    sent.clear()
    await pub.tick(mq)

    assert pub.parser.stats.rf_rate is not None
    assert "base/rf_rate/state" in {t for t, *_ in sent}


async def test_a_rate_that_becomes_unknown_overwrites_the_retained_value() -> None:
    """Gate is doing 30/min, then restarts with a bad passcode and gates
    nothing. Merely omitting the now-unknown rate leaves the broker serving 30,
    so HA reports a dead gate as healthy."""
    pub, sent, mq = _publisher(mqtt_base_topic="base")
    pub.parser.stats.uploaded_rate = 30.0
    await pub.publish_states(mq)
    assert {t: p for t, p, *_ in sent}["base/uploaded_rate/state"] == "30.0"

    sent.clear()
    pub.parser.stats.uploaded_rate = None
    await pub.publish_states(mq)

    by_topic = {t: (p, q, r) for t, p, q, r in sent}
    payload, _qos, retain = by_topic["base/uploaded_rate/state"]
    assert payload == "None"
    # Retained, so the correction survives an HA restart too.
    assert retain is True


async def test_unknown_is_not_published_to_the_timestamp_sensor() -> None:
    """HA maps the literal "None" to unknown only where a numeric value is
    expected; older cores instead try to parse it on a timestamp sensor and
    warn every publish cycle. The state is unknown either way, so stay quiet."""
    pub, sent, mq = _publisher(mqtt_base_topic="base")
    await pub.publish_states(mq)
    by_topic = {t: p for t, p, *_ in sent}

    assert "base/last_heard/state" not in by_topic
    # But the numeric sensors, which HA does handle, still get it.
    assert by_topic["base/packets_uploaded/state"] == "None"
    assert by_topic["base/audio_level/state"] == "None"  # state_class, no unit
