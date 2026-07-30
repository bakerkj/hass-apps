# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Unit tests for the pieces that were unreachable while they lived inside a
closure in app.py: option clamping, the watchdog decision, and the publisher's
own publish/callback behaviour.
"""

import time

from direwolf_igate import DirewolfParser, Options, Publisher, from_mapping, overdue

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


def _publisher(**over: object) -> tuple[Publisher, list[tuple]]:
    opts: Options = from_mapping({"mqtt_enabled": True, **over})
    pub = Publisher(opts, DirewolfParser(MYCALL))
    sent: list[tuple] = []

    class _Client:
        rc = 0  # settable per-test so the failure branch is reachable

        def publish(self, topic, payload="", qos=0, retain=False):
            sent.append((topic, payload, qos, retain))
            outer = self

            class _I:
                rc = outer.rc

            return _I()

        def subscribe(self, *_a, **_k):
            pass

        def loop_stop(self):
            pass

        def disconnect(self):
            pass

    pub.client = _Client()  # type: ignore[assignment]
    return pub, sent


def test_states_are_retained_except_the_connectivity_sensor() -> None:
    pub, sent = _publisher(mqtt_base_topic="base")
    pub.publish_states()

    by_topic = {t: (p, q, r) for t, p, q, r in sent}
    assert by_topic["base/rf_packets_received/state"][2] is True
    # expire_after lives on this one, so a retained value would restart the
    # expiry timer after an HA restart.
    assert by_topic["base/igate_connected/state"][2] is False


def test_unknown_counters_are_published_as_unknown_not_omitted() -> None:
    """State topics are retained, so omitting a key leaves the broker replaying
    the last known value as if current. "None" reads as unknown."""
    pub, sent = _publisher(mqtt_base_topic="base")
    pub.publish_states()
    by_topic = {t: p for t, p, *_ in sent}

    assert by_topic["base/packets_uploaded/state"] == "None"
    assert by_topic["base/uploaded_rate/state"] == "None"
    assert by_topic["base/rf_packets_received/state"] == "0"  # known from start


def test_connect_publishes_availability_then_discovery_then_states() -> None:
    """Order matters: HA discards state for an entity it has not discovered."""
    pub, sent = _publisher(mqtt_base_topic="base", mqtt_discovery_prefix="disc")
    pub._on_connect(pub.client, rc=0)

    topics = [t for t, *_ in sent]
    assert topics[0] == "base/availability"
    first_config = next(i for i, t in enumerate(topics) if t.endswith("/config"))
    first_state = next(i for i, t in enumerate(topics) if t.endswith("/state"))
    assert first_config < first_state
    assert pub.health.connected is True


def test_a_failed_connect_clears_a_previously_connected_state() -> None:
    """Seeded True first: asserting False on a fresh Publisher passes even if the
    `connected = False` line is deleted, since that is the initial value."""
    pub, sent = _publisher()
    pub.health.connected = True

    pub._on_connect(pub.client, rc=5)  # 5 = not authorised

    assert pub.health.connected is False
    assert sent == []


def test_a_raising_callback_cannot_escape_into_pahos_thread() -> None:
    """paho re-raises callback exceptions out of the network loop, which kills
    that thread permanently and disables reconnect."""
    pub, _ = _publisher()

    def boom(*_a: object, **_k: object) -> None:
        raise RuntimeError("subscribe exploded")

    pub._on_connect = boom  # type: ignore[method-assign]
    pub._guarded_on_connect(pub.client, None, None, 0)  # must not raise


def test_birth_message_forces_rediscovery() -> None:
    pub, _ = _publisher()
    pub._discovered = True

    class _Msg:
        payload = b"online"

    pub._on_message(pub.client, None, _Msg())  # type: ignore[arg-type]
    assert pub._discovered is False


def test_disconnect_records_both_clocks() -> None:
    pub, _ = _publisher()
    pub._on_disconnect(pub.client, None, rc=1)

    assert pub.health.connected is False
    assert pub.health.last_disconnect > 0
    assert pub.health.last_disconnect_monotonic > 0


def test_shutdown_without_a_session_touches_nothing() -> None:
    """publish() takes paho mutexes an in-flight connect may hold, so a
    disconnected shutdown must not call into the client."""
    pub, sent = _publisher()
    pub.health.connected = False
    pub.shutdown(farewell=True)

    assert sent == []
    assert pub.stop.is_set()


def test_shutdown_with_a_session_sends_the_retained_farewell(capsys) -> None:
    pub, sent = _publisher(mqtt_base_topic="base")
    pub.health.connected = True
    pub.shutdown(farewell=True)

    farewell = [(t, p, r) for t, p, _q, r in sent if t == "base/availability"]
    assert farewell == [("base/availability", "offline", True)]
    # Cleanly, not by way of the exception handler -- which would still publish
    # the farewell first and so leave this test green while hiding a fault.
    assert "error during shutdown publish" not in capsys.readouterr().out


def test_shutdown_tears_down_the_client_even_with_no_session() -> None:
    """The publishes need a session; stopping the network loop and closing the
    socket do not, and skipping them leaks both."""
    pub, sent = _publisher()
    torn: list[str] = []
    pub.client.loop_stop = lambda: torn.append("loop_stop")  # type: ignore[method-assign]
    pub.client.disconnect = lambda: torn.append("disconnect")  # type: ignore[method-assign]
    pub.health.connected = False

    pub.shutdown(farewell=True)

    assert sent == []  # nothing published without a session
    # disconnect() before loop_stop(): the farewell needs the loop running to
    # flush, and the clean DISCONNECT suppresses the last will.
    assert torn == ["disconnect", "loop_stop"]


# --- watchdog wiring (the predicate is tested above; this is the call) --------


def test_stall_watchdog_fires_at_its_own_window_not_the_disconnect_one(capsys) -> None:
    """Wired with the wrong timeout the stall warning fires at half its window;
    with `since`/`last_warned` transposed it never fires at all."""
    pub, _ = _publisher(mqtt_disconnect_timeout_seconds=100)
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
    pub, _ = _publisher()
    pub._last_output_monotonic = 0.0
    pub.check_watchdogs(CLOCK)
    assert "No output from direwolf" not in capsys.readouterr().out


def test_disconnect_watchdog_needs_both_disconnected_and_overdue(capsys) -> None:
    pub, _ = _publisher(mqtt_disconnect_timeout_seconds=100)
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
    pub, _ = _publisher(mqtt_disconnect_timeout_seconds=100)
    now = CLOCK
    pub._last_output_monotonic = now - 500

    pub.check_watchdogs(now)
    assert "No output from direwolf" in capsys.readouterr().out
    pub.check_watchdogs(now + 1)  # immediately again
    assert "No output from direwolf" not in capsys.readouterr().out


# --- tick() and the reader-thread entry point --------------------------------


def test_tick_discovers_before_publishing_states_on_a_reconnect(capsys) -> None:
    """The birth-message path clears the flag so tick() rediscovers. HA discards
    state for an entity it has not discovered, so the order matters here too --
    not only in _on_connect."""
    pub, sent = _publisher(mqtt_base_topic="base", mqtt_discovery_prefix="disc")
    pub.health.connected = True
    pub._discovered = False

    pub.tick()

    topics = [t for t, *_ in sent]
    first_config = next(i for i, t in enumerate(topics) if t.endswith("/config"))
    first_state = next(i for i, t in enumerate(topics) if t.endswith("/state"))
    assert first_config < first_state
    assert pub._discovered is True


def test_tick_does_not_rediscover_once_discovered() -> None:
    pub, sent = _publisher(mqtt_base_topic="base")
    pub.health.connected = True
    pub._discovered = True

    pub.tick()

    assert not [t for t, *_ in sent if t.endswith("/config")]


def test_tick_publishes_a_heartbeat_every_cycle() -> None:
    pub, sent = _publisher(mqtt_base_topic="base")
    pub.tick()
    beats = [(t, p) for t, p, _q, r in sent if t == "base/heartbeat" and r is False]
    assert len(beats) == 1
    assert "last_output_age_s" in beats[0][1]


def test_feed_observed_stamps_both_clocks_and_parses() -> None:
    """These stamps are the only inputs to the stall watchdog and to
    last_output_age_s -- the two things that reveal a hung direwolf."""
    pub, _ = _publisher()
    assert pub._last_output_wall == 0.0
    assert pub._last_output_monotonic == 0.0

    pub.feed_observed("[0.3] W1XM-15>APOT30:!4221.62N/07105.36Wr test\n")

    assert abs(pub._last_output_wall - time.time()) < 60
    assert pub._last_output_monotonic > 0
    assert pub.parser.stats.rf_packets_received == 1


def test_feed_observed_survives_a_parser_failure() -> None:
    """A parse bug must not kill the pipe that carries direwolf's output."""
    pub, _ = _publisher()

    def boom(_line: str) -> None:
        raise RuntimeError("parser exploded")

    pub.parser.feed = boom  # type: ignore[method-assign]
    pub.feed_observed("anything\n")  # must not raise
    assert pub._last_output_monotonic > 0  # stamped before the parse


def test_a_raising_tick_does_not_kill_the_publisher_thread(capsys) -> None:
    """_loop must survive a bad cycle: an unguarded raise would end the thread
    for the process lifetime, silently stopping every sensor and both
    watchdogs while the tee kept running."""
    pub, _ = _publisher(interval_seconds=5)
    calls: list[int] = []

    def boom() -> None:
        calls.append(1)
        pub.stop.set()  # end the loop after this cycle
        raise RuntimeError("tick exploded")

    pub.tick = boom  # type: ignore[method-assign]
    pub._loop()  # must return, not propagate

    assert calls == [1]
    assert "publish cycle failed" in capsys.readouterr().out


# --- callbacks other than on_connect ----------------------------------------


def test_a_malformed_birth_payload_cannot_escape_into_pahos_thread() -> None:
    """A payload that is not bytes raises AttributeError on .decode(); escaping
    would kill paho's network thread and disable reconnect for good."""
    pub, _ = _publisher()

    class _Msg:
        payload = None

    pub._guarded_on_message(pub.client, None, _Msg())  # type: ignore[arg-type]


def test_a_raising_disconnect_callback_cannot_escape_either() -> None:
    pub, _ = _publisher()

    def boom(*_a: object, **_k: object) -> None:
        raise RuntimeError("disconnect exploded")

    pub._on_disconnect = boom  # type: ignore[method-assign]
    pub._guarded_on_disconnect(pub.client, None, 1)


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


def test_state_publishes_stamp_the_health_clock() -> None:
    """publish_states() must pass mark_state; without it last_state_publish_ok
    stays 0 and the heartbeat's state_publish_age_s is permanently null -- the
    field that exists to reveal a stuck publisher."""
    pub, _ = _publisher(mqtt_base_topic="base")
    assert pub.health.last_state_publish_ok == 0.0

    pub.publish_states()

    assert pub.health.last_state_publish_ok > 0.0


def test_a_rejected_publish_does_not_stamp_the_health_clock() -> None:
    """A broker refusing every publish must not keep refreshing the age."""
    pub, _ = _publisher(mqtt_base_topic="base")
    pub.client.rc = 1  # type: ignore[attr-defined]

    pub.publish_states()

    assert pub.health.last_state_publish_ok == 0.0


def test_connect_marks_discovery_done() -> None:
    """Left unset, every tick would republish the whole discovery set."""
    pub, _ = _publisher()
    pub._discovered = False
    pub._on_connect(pub.client, rc=0)
    assert pub._discovered is True


def test_start_spawns_both_threads_without_blocking() -> None:
    """start() must return immediately: the connect retry never gives up, and
    the caller goes straight on to drain direwolf's output."""
    import threading

    pub, _ = _publisher(mqtt_host="127.0.0.1", mqtt_port=1)
    before = threading.active_count()
    pub.start()
    try:
        assert threading.active_count() >= before + 2
        names = {t.name for t in threading.enumerate()}
        assert "mqtt-connect" in names
        assert "mqtt-publish" in names
    finally:
        pub.stop.set()


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


def test_tick_samples_the_rf_rate_so_it_moves_without_traffic() -> None:
    """Nothing in the stream announces a quiet minute, so without a timer
    sample the RF rate stays unknown on a live gate and never falls on a deaf
    one."""
    pub, sent = _publisher(mqtt_base_topic="base")
    pub.health.connected = True

    pub.tick()
    assert pub.parser.stats.rf_rate is None  # one sample so far

    pub.parser.feed("[0.3] W1XM-15>APOT30:!4221.62N/07105.36Wr")
    time.sleep(1.05)  # clear the sub-second guard
    sent.clear()
    pub.tick()

    assert pub.parser.stats.rf_rate is not None
    assert "base/rf_rate/state" in {t for t, *_ in sent}


def test_a_rate_that_becomes_unknown_overwrites_the_retained_value() -> None:
    """Gate is doing 30/min, then restarts with a bad passcode and gates
    nothing. Merely omitting the now-unknown rate leaves the broker serving 30,
    so HA reports a dead gate as healthy."""
    pub, sent = _publisher(mqtt_base_topic="base")
    pub.parser.stats.uploaded_rate = 30.0
    pub.publish_states()
    assert {t: p for t, p, *_ in sent}["base/uploaded_rate/state"] == "30.0"

    sent.clear()
    pub.parser.stats.uploaded_rate = None
    pub.publish_states()

    by_topic = {t: (p, q, r) for t, p, q, r in sent}
    payload, _qos, retain = by_topic["base/uploaded_rate/state"]
    assert payload == "None"
    # Retained, so the correction survives an HA restart too.
    assert retain is True


def test_unknown_is_not_published_to_the_timestamp_sensor() -> None:
    """HA maps the literal "None" to unknown only where a numeric value is
    expected; older cores instead try to parse it on a timestamp sensor and
    warn every publish cycle. The state is unknown either way, so stay quiet."""
    pub, sent = _publisher(mqtt_base_topic="base")
    pub.publish_states()
    by_topic = {t: p for t, p, *_ in sent}

    assert "base/last_heard/state" not in by_topic
    # But the numeric sensors, which HA does handle, still get it.
    assert by_topic["base/packets_uploaded/state"] == "None"
    assert by_topic["base/audio_level/state"] == "None"  # state_class, no unit
