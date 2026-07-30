# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Unit tests for the direwolf output parser and MQTT discovery payloads.

The parser is the fragile part of this add-on -- it reads a text stream that is
not a stable interface -- so the sample lines below are copied verbatim from a
live IGate rather than hand-written to match the regexes, with the
callsign and position replaced by the documentation placeholders.
"""

import datetime
import json
import pathlib
import re
import time

from direwolf_igate import (
    BINARY_SENSORS,
    MAX_SAMPLES,
    SENSORS,
    DirewolfParser,
    RateTracker,
    build_discovery_payloads,
    state_values,
)

MYCALL = "N0CALL-10"

# Verbatim from a running add-on.
OWN_IGATE_BEACON = (
    "[0L] N0CALL-10>APDW18:<IGATE,MSG_CNT=0,PKT_CNT=0,DIR_CNT=6,"
    "LOC_CNT=6,RF_CNT=39,UPL_CNT=592,DNL_CNT=396"
)
OTHER_IGATE_BEACON = (
    "[ig>tx] N1CMD-10>APWW11,TCPIP*,qAC,T2PANAMA:<IGATE,MSG_CNT=23,"
    "LOC_CNT=40,DIR_CNT=7,RF_CNT=89,UPL_CNT=1234,DNL_CNT=5678"
)
# Our own status beacon, verbatim from a live gate: direwolf logs it under
# "[ig]" when IG-bound and "[0L]" when queued for transmit.
OWN_IGATE_BEACON_IG = (
    "[ig] N0CALL-10>APDW18:<IGATE,MSG_CNT=0,PKT_CNT=0,DIR_CNT=1,"
    "LOC_CNT=1,RF_CNT=4,UPL_CNT=4,DNL_CNT=0"
)
RF_FRAME = "[0.3] W1XM-15>APOT30:!4221.62N/07105.36Wr 06.3V 53C MIT APRS"
AUDIO_LINE = "W1XM-15 audio level = 77(11/11)    |||||||__"
CONNECTED_LINE = "Now connected to IGate server noam.aprs2.net (96.53.142.57)"
LOGIN_OK_LINE = "[ig] # logresp N0CALL-10 verified, server T2TEST"
LOGIN_REJECTED_LINE = "[ig] # logresp N0CALL-10 unverified, server T2TEST"


def test_own_igate_beacon_populates_counters() -> None:
    p = DirewolfParser(MYCALL)
    p.feed(OWN_IGATE_BEACON)

    assert p.stats.packets_uploaded == 592
    assert p.stats.packets_downloaded == 396
    assert p.stats.stations_heard == 39
    assert p.stats.stations_heard_direct == 6
    # The beacon says nothing about the link: direwolf emits it whether or not
    # APRS-IS accepted the login. Link state comes from the logresp answer.
    assert p.stats.igate_connected is False


def test_other_stations_igate_beacon_is_ignored() -> None:
    """Gated-down beacons from other IGates must not be counted as ours.

    These arrive constantly from APRS-IS. Attributing one would silently
    replace every counter with a stranger's numbers.
    """
    p = DirewolfParser(MYCALL)
    p.feed(OTHER_IGATE_BEACON)

    assert p.stats.packets_uploaded is None
    assert p.stats.packets_downloaded is None
    assert p.stats.stations_heard is None


def test_own_beacon_still_wins_after_a_foreign_one() -> None:
    p = DirewolfParser(MYCALL)
    p.feed(OTHER_IGATE_BEACON)
    p.feed(OWN_IGATE_BEACON)
    p.feed(OTHER_IGATE_BEACON)

    assert p.stats.packets_uploaded == 592


def test_callsign_match_is_not_a_substring_match() -> None:
    """A longer callsign containing ours must not be mistaken for us."""
    p = DirewolfParser("W1XM")
    p.feed("[ig>tx] AW1XM-9>APRS,TCPIP*:<IGATE,UPL_CNT=999,DNL_CNT=1")

    assert p.stats.packets_uploaded is None


def test_rf_frames_counted_with_station_and_timestamp() -> None:
    p = DirewolfParser(MYCALL)
    p.feed(RF_FRAME)
    p.feed("[0.2] W1GLO-1>APOT21,WA1PLE-4,WIDE1:!4238.45N/07040.29W# 13.9V")
    p.feed(RF_FRAME)

    assert p.stats.rf_packets_received == 3
    assert p.stats.stations_seen_total == 2  # W1XM-15, W1GLO-1
    # Not merely "not None": the sensor carries device_class timestamp, so the
    # value has to be wall-clock epoch seconds. A monotonic clock satisfies
    # "not None" and renders in HA as 1970.
    assert p.stats.last_heard is not None
    assert abs(p.stats.last_heard - time.time()) < 60, (
        f"last_heard {p.stats.last_heard} is not a wall-clock timestamp"
    )


def test_own_transmissions_are_not_counted_as_rf_receptions() -> None:
    """[0L]/[0H] mark our own generated packets, not received traffic."""
    p = DirewolfParser(MYCALL)
    p.feed(OWN_IGATE_BEACON)
    p.feed("[0L] N0CALL-10>APDW18:!0000.00NR00000.00W&iGate | DireWolf")

    assert p.stats.rf_packets_received == 0


def test_audio_level_and_connection_state() -> None:
    p = DirewolfParser(MYCALL)
    assert p.stats.igate_connected is False

    p.feed(AUDIO_LINE)
    p.feed(LOGIN_OK_LINE)

    assert p.stats.last_audio_level == 77
    assert p.stats.igate_connected is True


def test_tcp_connect_alone_does_not_mean_the_igate_is_working() -> None:
    """A wrong passcode still completes the TCP handshake, so direwolf prints
    "Now connected" and then the server refuses the login. Reporting connected
    at that point would hide the exact failure this sensor exists to catch."""
    p = DirewolfParser(MYCALL)
    p.feed(CONNECTED_LINE)

    assert p.stats.igate_connected is False


def test_rejected_login_is_not_read_as_verified() -> None:
    """ "unverified" contains "verified"; a naive match reports success on the
    one line that means total failure."""
    p = DirewolfParser(MYCALL)
    p.feed(LOGIN_OK_LINE)
    assert p.stats.igate_connected is True

    p.feed(LOGIN_REJECTED_LINE)
    assert p.stats.igate_connected is False


def test_unrecognised_lines_are_harmless() -> None:
    p = DirewolfParser(MYCALL)
    for line in (
        "",
        "Dire Wolf Release 1.8.1, November 2025",
        'ERROR!!!  Unknown APRS Data Type Indicator "K"',
        "Connect to IGate server noam.aprs2.net (96.53.142.57) failed.",
    ):
        p.feed(line)

    assert p.stats.rf_packets_received == 0
    assert p.stats.packets_uploaded is None


def test_state_values_omits_unobserved_counters() -> None:
    """An unobserved counter must be absent, not published as 0."""
    p = DirewolfParser(MYCALL)
    values = state_values(p)

    assert "packets_uploaded" not in values
    assert "last_heard" not in values
    # Locally derived counters are known from the start.
    assert values["rf_packets_received"] == "0"

    p.feed(OWN_IGATE_BEACON)
    p.feed(RF_FRAME)
    values = state_values(p)

    assert values["packets_uploaded"] == "592"
    assert values["rf_packets_received"] == "1"
    assert values["last_heard"].endswith("+00:00")
    # And it must be *now*, not the epoch: parse it back and compare.
    parsed = datetime.datetime.fromisoformat(values["last_heard"])
    age = abs((datetime.datetime.now(datetime.UTC) - parsed).total_seconds())
    assert age < 60, f"last_heard rendered as {values['last_heard']}"


def test_every_published_key_has_a_discovery_entry() -> None:
    """A state topic with no discovery payload is an entity HA never creates."""
    p = DirewolfParser(MYCALL)
    p.feed(OWN_IGATE_BEACON)
    p.feed(RF_FRAME)
    p.feed(AUDIO_LINE)

    for key in state_values(p):
        assert key in SENSORS, f"{key} published with no discovery payload"


def test_discovery_payloads_are_well_formed() -> None:
    # Every argument deliberately distinct. With device_id == base_topic ==
    # discovery_prefix, swapping any two of them, or ignoring the prefix and
    # hardcoding "homeassistant/", is undetectable -- and in production they
    # genuinely differ (client_id vs mqtt_base_topic vs mqtt_discovery_prefix).
    payloads = build_discovery_payloads(
        "disc_prefix",
        "dev_id",
        "Direwolf APRS IGate",
        "base_top",
        "base_top/availability",
        120,
    )

    # Named entities, not a self-referential count: len(SENSORS) +
    # len(BINARY_SENSORS) is satisfied by 0 == 0 + 0, so emptying either
    # table -- deleting the headline igate_connected entity -- was invisible.
    assert "igate_connected" in BINARY_SENSORS
    assert {"packets_uploaded", "rf_packets_received", "last_heard"} <= set(SENSORS)
    assert len(payloads) >= 8, sorted(payloads)
    assert any("/binary_sensor/" in t for t in payloads), sorted(payloads)
    assert len(payloads) == len(SENSORS) + len(BINARY_SENSORS)

    unique_ids = set()
    for topic, payload in payloads.items():
        assert topic.startswith("disc_prefix/"), topic
        assert topic.endswith("/config")
        assert "/dev_id/" in topic, topic
        json.dumps(payload)  # must be serialisable
        assert payload["availability_topic"] == "base_top/availability"
        assert payload["state_topic"].startswith("base_top/"), payload["state_topic"]
        assert payload["unique_id"].startswith("dev_id_"), payload["unique_id"]
        unique_ids.add(payload["unique_id"])

    assert len(unique_ids) == len(payloads), "unique_id collision"


def test_counters_use_total_increasing_so_restarts_are_not_negative_deltas() -> None:
    """direwolf's counters reset when the add-on restarts; total_increasing is
    the state class that tells HA a drop is a reset, not a negative rate."""
    for key in ("packets_uploaded", "packets_downloaded", "rf_packets_received"):
        assert SENSORS[key].state_class == "total_increasing"


def test_connectivity_sensor_expires_but_rf_sensors_do_not() -> None:
    """A dead band is normal and must not mark RF sensors unavailable; a stale
    connectivity reading is actively misleading, so only that one expires."""
    payloads = build_discovery_payloads(
        "homeassistant", "dev", "Dev", "base", "base/availability", 120
    )
    for topic, payload in payloads.items():
        if "/binary_sensor/" in topic:
            assert payload["expire_after"] == 120
        else:
            assert "expire_after" not in payload


# ---------------------------------------------------------------------------
# MQTT client helpers. Exercised against the repo-wide paho stub installed by
# the root conftest, matching how turbostat_mqtt and intel_gpu_top_mqtt test
# theirs -- no broker, and no attempt to stand in for absent radio hardware.
# ---------------------------------------------------------------------------


def test_mqtt_publish_success_marks_state() -> None:
    from direwolf_igate import MqttHealth, mqtt_publish

    health = MqttHealth()
    client = _StubClient(rc=0)

    assert mqtt_publish(
        client,
        "t/x/state",
        "1",
        qos=0,
        retain=True,
        log_level="ERROR",
        health=health,
        mark_state=True,
    )
    assert health.last_state_publish_ok > 0
    assert client.published == [("t/x/state", "1", 0, True)]


def test_mqtt_publish_reports_failure_without_raising() -> None:
    """A publish failure must be survivable: this process feeds the add-on log
    and must not take direwolf down with it."""
    from direwolf_igate import MqttHealth, mqtt_publish

    health = MqttHealth()

    assert not mqtt_publish(
        _StubClient(rc=1),
        "t/x/state",
        "1",
        qos=0,
        retain=True,
        log_level="ERROR",
        health=health,
        # WITH mark_state: without it the assertion below is vacuous, since
        # the stamp is gated on mark_state (default False) and could never
        # fire regardless of rc. A broker rejecting every publish would then
        # keep stamping a fresh state_publish_age_s, so the diagnostic meant
        # to reveal a stuck publisher reports healthy through the outage.
        mark_state=True,
    )
    assert health.last_state_publish_ok == 0

    class _Boom:
        def publish(self, *_a: object, **_k: object) -> None:
            raise RuntimeError("broker exploded")

    assert not mqtt_publish(
        _Boom(),
        "t/x/state",
        "1",
        qos=0,
        retain=True,
        log_level="ERROR",
        health=health,
    )


class _StubClient:
    def __init__(self, rc: int = 0) -> None:
        self.rc = rc
        self.published: list[tuple[str, str, int, bool]] = []

    def publish(
        self, topic: str, payload: str = "", qos: int = 0, retain: bool = False
    ) -> object:
        self.published.append((topic, payload, qos, retain))

        class _Info:
            pass

        info = _Info()
        info.rc = self.rc  # type: ignore[attr-defined]
        return info


# ---------------------------------------------------------------------------
# Resilience parity with the sibling MQTT add-ons
# ---------------------------------------------------------------------------


def test_heartbeat_reports_ages_and_distinguishes_never_from_zero() -> None:
    """``None`` means the event has not happened; 0.0 would claim it just did."""
    from direwolf_igate import MqttHealth, heartbeat_payload

    health = MqttHealth()
    hb = heartbeat_payload(now=1000.0, health=health, last_output=0.0)
    assert hb["last_output_age_s"] is None
    assert hb["state_publish_age_s"] is None
    assert hb["connected"] is False

    health.connected = True
    health.last_state_publish_ok = 990.0
    hb = heartbeat_payload(now=1000.0, health=health, last_output=970.5)
    assert hb["last_output_age_s"] == 29.5
    assert hb["state_publish_age_s"] == 10.0

    # Ages are clamped at zero. `now` is captured before the states are
    # published, so a stamp can land marginally later and the subtraction go
    # negative -- observed live as "state_publish_age_s": -0.0.
    health.last_state_publish_ok = 1000.05
    hb = heartbeat_payload(now=1000.0, health=health, last_output=1000.2)
    assert hb["state_publish_age_s"] == 0.0, hb
    assert hb["last_output_age_s"] == 0.0, hb
    assert hb["connected"] is True
    assert hb["ts_ms"] == 1_000_000


def test_health_tracks_a_monotonic_mirror_for_watchdog_decisions() -> None:
    """Checks the two fields exist, default to 0.0 and are independent -- not
    that the watchdog reads the right one, which is unreachable in isolation."""
    from direwolf_igate import MqttHealth

    health = MqttHealth()
    assert health.last_disconnect == 0.0
    assert health.last_disconnect_monotonic == 0.0

    # Independent: moving wall clock must not drag the watchdog's reference.
    health.last_disconnect = 1234.0
    assert health.last_disconnect_monotonic == 0.0


# ---------------------------------------------------------------------------
# config.json. Nothing tested it, yet run.sh reads every option through
# bashio::config by name: rename one on either side and the add-on silently
# renders "null" into sdr.conf, which direwolf accepts without complaint.
# ---------------------------------------------------------------------------

ADDON_DIR = pathlib.Path(__file__).resolve().parents[1]


def _config_json() -> dict:
    return json.loads((ADDON_DIR / "config.json").read_text())


def _options_read_by_run_sh() -> set[str]:
    text = (ADDON_DIR / "run.sh").read_text()
    return set(re.findall(r"bashio::config\s+'([^']+)'", text))


def test_every_option_run_sh_reads_exists_in_the_schema() -> None:
    """A key run.sh reads but the schema does not declare returns the literal
    "null", which lands in sdr.conf as e.g. symbol="null" -- accepted by
    direwolf, wrong on the air, and invisible to every other test."""
    cfg = _config_json()
    missing = _options_read_by_run_sh() - set(cfg["schema"])
    assert not missing, f"run.sh reads options absent from schema: {sorted(missing)}"


def test_schema_and_defaults_agree() -> None:
    """Every declared option needs a default, and every default a declaration."""
    cfg = _config_json()
    schema, options = set(cfg["schema"]), set(cfg["options"])
    assert not schema - options, f"declared but no default: {sorted(schema - options)}"
    assert not options - schema, f"default but undeclared: {sorted(options - schema)}"


def test_position_options_are_typed_as_bounded_floats() -> None:
    """latitude/longitude are the only values interpolated into sdr.conf
    without passing through run.sh's character-stripping loop -- the Supervisor
    validating them as bounded floats is what makes that safe. Retyping either
    to str reopens it, and nothing else would notice."""
    schema = _config_json()["schema"]
    assert schema["latitude"] == "float(-90.0,90.0)", schema["latitude"]
    assert schema["longitude"] == "float(-180.0,180.0)", schema["longitude"]


def test_passcode_is_declared_as_a_secret() -> None:
    """`password` keeps it out of the Supervisor UI and the add-on's config
    display; plain `str` would show the APRS-IS shared secret in clear."""
    assert _config_json()["schema"]["iglogin_passcode"] == "password"


def test_station_snapshot_is_immutable_and_swapped_not_mutated() -> None:
    """The publisher thread reads `stations_seen` while the reader thread is
    adding to it. Correctness rests on the snapshot being a *new* immutable
    object each time rather than a container mutated in place -- otherwise a
    reader could observe a half-updated set, and we would be back to relying on
    set.add/len being atomic under the GIL.
    """
    p = DirewolfParser(MYCALL)
    p.feed(RF_FRAME)
    first = p.stats.stations_seen

    assert isinstance(first, frozenset)
    assert first == {"W1XM-15"}

    p.feed("[0.2] W1GLO-1>APOT21,WIDE1:!4238.45N/07040.29W# 13.9V")
    second = p.stats.stations_seen

    # A held reference must not have changed underneath the reader.
    assert first == {"W1XM-15"}, "snapshot mutated in place under a reader"
    assert second == {"W1XM-15", "W1GLO-1"}
    assert first is not second
    assert p.stats.stations_seen_total == 2


def test_repeat_sightings_do_not_rebuild_the_snapshot() -> None:
    """Only a genuinely new station costs a rebuild; the hot path stays cheap
    for the common case of the same stations beaconing repeatedly."""
    p = DirewolfParser(MYCALL)
    p.feed(RF_FRAME)
    snapshot = p.stats.stations_seen

    p.feed(RF_FRAME)
    p.feed(RF_FRAME)

    assert p.stats.stations_seen is snapshot, "rebuilt for an already-seen station"
    assert p.stats.rf_packets_received == 3  # packets still counted
    assert p.stats.stations_seen_total == 1


def test_mycall_is_matched_case_insensitively() -> None:
    """If normalisation regressed, our own <IGATE> beacon would stop matching
    and every counter would sit at None. run.sh uppercases too; this pins the
    parser's own contract."""
    p = DirewolfParser("n0call-10")
    p.feed(OWN_IGATE_BEACON)  # the beacon itself is upper case

    assert p.stats.packets_uploaded == 592


def test_only_the_connectivity_sensor_is_unretained() -> None:
    """expire_after and retain interact badly: a retained value is redelivered
    on an HA restart and restarts the expiry timer, so a dead IGate would read
    connected for another window. The connectivity state is therefore published
    unretained, while the numeric sensors stay retained so their history
    survives a restart.

    Asserted against the discovery payloads rather than the publish calls:
    expire_after must appear on exactly the entity whose state is unretained.
    """
    payloads = build_discovery_payloads(
        "disc", "dev", "Dev", "base", "base/availability", 120
    )
    expiring = {t for t, p in payloads.items() if "expire_after" in p}

    assert expiring, "no entity carries expire_after"
    assert all("/binary_sensor/" in t for t in expiring), sorted(expiring)
    # And every expiring entity must be a BINARY_SENSORS key, i.e. one of the
    # keys publish_all() sends with retain=False.
    for topic in expiring:
        assert topic.rsplit("/", 2)[-2] in BINARY_SENSORS, topic


def test_a_status_beacon_cannot_override_a_rejected_login() -> None:
    """direwolf emits its own <IGATE> beacon regardless of whether APRS-IS
    accepted the login, so it must not be read as proof of a working link --
    otherwise a refused passcode reads as connected within one beacon interval,
    which is the exact case the rejection handling exists to catch."""
    p = DirewolfParser(MYCALL)
    p.feed(LOGIN_REJECTED_LINE)
    assert p.stats.igate_connected is False

    p.feed(OWN_IGATE_BEACON)

    assert p.stats.igate_connected is False, "status beacon overrode the rejection"
    # The counters still land -- they are an independent signal.
    assert p.stats.packets_uploaded == 592


def test_another_igates_beacon_heard_on_rf_counts_as_rf_traffic() -> None:
    """Neighbouring IGates' status beacons are routine over-the-air traffic.
    Excluding them undercounts RF packets and leaves last_heard stale on a quiet
    band where they are the only decodes."""
    p = DirewolfParser(MYCALL)
    p.feed(
        "[0.3] W1AW-10>APDW18:<IGATE,MSG_CNT=0,PKT_CNT=0,DIR_CNT=4,"
        "LOC_CNT=4,RF_CNT=9,UPL_CNT=1,DNL_CNT=2"
    )

    assert p.stats.rf_packets_received == 1
    assert p.stats.stations_seen_total == 1
    assert p.stats.last_heard is not None
    # But it must not be mistaken for our own accounting.
    assert p.stats.packets_uploaded is None
    assert p.stats.igate_connected is False


# -- rates ------------------------------------------------------------------


def test_rate_needs_two_samples_before_it_reports() -> None:
    """One sample is a total, not a rate. Reporting 0 would look like a gate
    that is up and gating nothing, which is the opposite of the truth."""
    r = RateTracker()
    assert r.per_minute is None

    r.feed(1000.0, 100)
    assert r.per_minute is None

    r.feed(1060.0, 160)
    assert r.per_minute == 60.0  # 60 packets in 60 s


def test_rate_averages_over_the_whole_retained_window() -> None:
    """Oldest-to-newest, not the last pair: a one-cycle lull must not read as
    a dead gate on a band that is merely bursty."""
    r = RateTracker(window_s=600.0)
    r.feed(0.0, 0)
    r.feed(60.0, 30)
    r.feed(120.0, 30)  # nothing arrived this cycle

    assert r.per_minute == 15.0  # 30 packets over 120 s, not 0


def test_rate_drops_samples_that_have_left_the_window() -> None:
    """Otherwise it is a lifetime average that converges and stops moving."""
    r = RateTracker(window_s=100.0)
    r.feed(0.0, 0)
    r.feed(50.0, 5000)  # a burst, long ago
    r.feed(200.0, 5010)

    # The interval containing the burst has aged out: 10 packets over the
    # remaining 150 s, not the 1503/min a lifetime average would report.
    assert r.per_minute == 4.0


def test_rate_keeps_two_samples_even_when_the_feed_is_slower_than_the_window() -> None:
    """The APRS-IS counters only move on the beacon schedule, which a user can
    set longer than the window. Trimming to one sample would report None
    forever rather than a slower average."""
    r = RateTracker(window_s=600.0)
    r.feed(0.0, 0)
    r.feed(1800.0, 900)
    r.feed(3600.0, 1800)

    assert r.per_minute == 30.0  # 900 packets over the last 1800 s


def test_rate_treats_a_counter_going_backwards_as_a_restart() -> None:
    """direwolf's counters restart from zero with the process. Carrying the old
    baseline forward would publish a large negative rate."""
    r = RateTracker()
    r.feed(0.0, 5000)
    r.feed(60.0, 5060)
    assert r.per_minute == 60.0

    r.feed(120.0, 0)  # direwolf restarted
    assert r.per_minute is None, "a reset must read as unknown, never negative"

    r.feed(180.0, 30)
    assert r.per_minute == 30.0  # rebaselined on the new counter


def test_rate_ignores_a_span_too_short_to_extrapolate() -> None:
    """Two samples in the same instant would turn one packet into thousands
    per minute."""
    r = RateTracker()
    r.feed(1000.0, 100)
    r.feed(1000.05, 101)

    assert r.per_minute is None


def test_igate_beacons_produce_aprs_is_rates(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """The rate comes from consecutive status beacons, so it is unknown until
    the second one -- one beacon interval after startup."""
    clock = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    p = DirewolfParser(MYCALL)
    p.feed(OWN_IGATE_BEACON)  # UPL_CNT=592, DNL_CNT=396

    p.sample_rates()
    assert p.stats.packets_uploaded == 592
    assert p.stats.uploaded_rate is None
    assert p.stats.downloaded_rate is None

    clock[0] += 600.0
    p.feed(
        "[0L] N0CALL-10>APDW18:<IGATE,MSG_CNT=0,PKT_CNT=0,DIR_CNT=6,"
        "LOC_CNT=6,RF_CNT=39,UPL_CNT=892,DNL_CNT=456"
    )

    p.sample_rates()
    assert p.stats.uploaded_rate == 30.0  # 300 packets in 600 s
    assert p.stats.downloaded_rate == 6.0  # 60 packets in 600 s


def test_rf_rate_falls_towards_zero_on_a_dead_band(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """The RF counter is sampled on a timer rather than on traffic, so silence
    registers. Sampling only when a line arrives would freeze the rate at its
    last busy value and hide a failed dongle."""
    clock = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    p = DirewolfParser(MYCALL)
    p.sample_rates()
    assert p.stats.rf_rate is None

    for _ in range(30):
        p.feed(RF_FRAME)
    clock[0] += 60.0
    p.sample_rates()
    assert p.stats.rf_rate == 30.0

    clock[0] += 60.0  # band goes quiet, no lines at all
    p.sample_rates()
    assert p.stats.rf_rate == 15.0
    clock[0] += 480.0
    p.sample_rates()
    assert p.stats.rf_rate is not None and p.stats.rf_rate < 5.0


def test_rates_are_measurements_not_totals() -> None:
    """total_increasing on a rate would make HA sum it into a meaningless
    lifetime figure and treat every dip as a counter reset."""
    for key in ("uploaded_rate", "downloaded_rate", "rf_rate"):
        assert SENSORS[key].state_class == "measurement"
        assert SENSORS[key].unit == "packets/min"


def test_rates_are_published_only_once_known() -> None:
    """A rate is absent until it has two samples; publishing 0 in the meantime
    would show a working gate as idle for the first beacon interval."""
    p = DirewolfParser(MYCALL)
    p.feed(OWN_IGATE_BEACON)
    values = state_values(p)

    assert "uploaded_rate" not in values
    assert "rf_rate" not in values


# -- provenance of the status beacon ----------------------------------------

# APRS third-party format nests a whole packet after "}". Anyone on APRS-IS can
# send one naming any callsign, so this arrives with a stranger's source but
# ours in the position the own-beacon regex looks at.
THIRD_PARTY_INJECTION = (
    "[ig] W1AW>APRS,TCPIP*,qAC,T2TEST:}N0CALL-10>APDW18,TCPIP*:"
    "<IGATE,MSG_CNT=0,DIR_CNT=9,LOC_CNT=9,RF_CNT=9,UPL_CNT=900000,DNL_CNT=900000"
)
# A second station misconfigured with the same callsign -- routine on APRS.
DUPLICATE_CALLSIGN_BEACON = (
    "[ig>tx] N0CALL-10>APWW11,TCPIP*,qAC,T2TEST:<IGATE,MSG_CNT=0,"
    "DIR_CNT=8,LOC_CNT=8,RF_CNT=8,UPL_CNT=900000,DNL_CNT=900000"
)


def test_status_beacon_is_accepted_only_when_locally_generated() -> None:
    """A callsign is not a credential: ours in a packet from APRS-IS means
    someone else typed it. The marker is the shape of a locally generated
    packet, not the log prefix, which varies by how the beacon was sent."""
    for line in (THIRD_PARTY_INJECTION, DUPLICATE_CALLSIGN_BEACON):
        p = DirewolfParser(MYCALL)
        p.feed(line)

        assert p.stats.packets_uploaded is None, line
        assert p.stats.packets_downloaded is None, line
        assert p.stats.stations_heard is None, line

    # And both forms of the genuine article are accepted, so the guard is not
    # just rejecting everything.
    p = DirewolfParser(MYCALL)
    p.feed(OWN_IGATE_BEACON)
    assert p.stats.packets_uploaded == 592

    p = DirewolfParser(MYCALL)
    p.feed(OWN_IGATE_BEACON_IG)
    assert p.stats.packets_uploaded == 4, "an IG-bound beacon must still count"


def test_injected_beacon_cannot_strand_the_rate_at_a_fabricated_value(
    monkeypatch,  # type: ignore[no-untyped-def]
) -> None:
    """A foreign counter spikes the rate; our next real beacon is lower, which
    resets the tracker and blanks it -- so the spike stands and the true rate
    is never published at all."""
    clock = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    p = DirewolfParser(MYCALL)
    p.feed(OWN_IGATE_BEACON)  # UPL_CNT=592
    p.feed(THIRD_PARTY_INJECTION)  # UPL_CNT=900000, ignored

    clock[0] += 600.0
    p.feed(
        "[0L] N0CALL-10>APDW18:<IGATE,MSG_CNT=0,PKT_CNT=0,DIR_CNT=6,"
        "LOC_CNT=6,RF_CNT=39,UPL_CNT=892,DNL_CNT=456"
    )
    p.sample_rates()

    assert p.stats.uploaded_rate == 30.0  # the true rate, published


def test_aprs_is_rate_goes_unknown_once_beacons_stop(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """These counters only move when a status beacon arrives, so without an
    ageing rule a gate whose APRS-IS link died keeps reporting the throughput
    it had while it was working -- the exact failure the sensor exists to
    show."""
    clock = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    p = DirewolfParser(MYCALL)
    p.feed(OWN_IGATE_BEACON)  # UPL_CNT=592
    clock[0] += 900.0
    p.feed(
        "[0L] N0CALL-10>APDW18:<IGATE,MSG_CNT=0,PKT_CNT=0,DIR_CNT=6,"
        "LOC_CNT=6,RF_CNT=39,UPL_CNT=892,DNL_CNT=456"
    )
    p.sample_rates()
    assert p.stats.uploaded_rate == 20.0  # 300 packets in 900 s

    clock[0] += 900.0  # one beacon missed: could still be jitter
    p.sample_rates()
    assert p.stats.uploaded_rate == 20.0

    clock[0] += 1000.0  # past two intervals: the link is gone, say so
    p.sample_rates()
    assert p.stats.uploaded_rate is None
    assert p.stats.downloaded_rate is None


def test_every_entity_is_primary() -> None:
    """diagnostic keeps an entity out of auto-generated dashboards, off Assist,
    and inside a device's collapsed section -- it only ever hides. Guessing
    wrong costs the user something they have to notice is missing before they
    can go looking for it, so nothing here is filed that way."""
    for key, meta in list(SENSORS.items()) + list(BINARY_SENSORS.items()):
        assert meta.entity_category is None, key

    payloads = build_discovery_payloads(
        "disc", "dev", "Direwolf APRS IGate", "base", "base/availability", 120
    )
    # Absent, not present-and-null: HA treats an explicit null as a value.
    for topic, payload in payloads.items():
        assert "entity_category" not in payload, topic


def test_rate_sample_carries_its_own_cadence() -> None:
    """value_at ages a rate against the interval it was measured over, so a
    15-minute beacon and a 30-second poll need no shared configuration."""
    r = RateTracker()
    r.feed(0.0, 0)
    r.feed(900.0, 300)

    assert r.latest is not None
    assert r.latest.cadence == 900.0
    assert r.value_at(900.0) == 20.0
    assert r.value_at(900.0 + 1799) == 20.0
    assert r.value_at(900.0 + 1801) is None


def test_same_timestamp_flood_cannot_grow_without_bound() -> None:
    """Samples sharing a timestamp are never old enough to trim, so only the
    hard cap stops a flood of them."""
    r = RateTracker()
    for i in range(MAX_SAMPLES * 3):
        r.feed(1000.0, i)

    assert len(r._samples) <= MAX_SAMPLES


def test_readme_documents_every_entity() -> None:
    """The entity table is hand-edited, so a rename that lands in the code and
    not the table leaves the docs describing entities that do not exist."""
    readme = (pathlib.Path(__file__).parent.parent / "README.md").read_text()

    for key, meta in list(SENSORS.items()) + list(BINARY_SENSORS.items()):
        assert meta.name in readme, f"{key} ({meta.name!r}) is not in README.md"


def test_our_own_beacon_counts_whatever_path_it_carries() -> None:
    """Our own beacon arrives bare, with the APRS-IS client path TCPIP*, or
    with a via path, depending on how direwolf sent it. Requiring a bare tocall
    drops the last two and the counters silently stop."""
    for path in ("", ",TCPIP*", ",WIDE1-1", ",WIDE1-1,WIDE2-1", ",TCPIP*,LOCAL"):
        for tag in ("[ig]", "[0L]", "[0H]", "[1L]"):
            p = DirewolfParser(MYCALL)
            p.feed(f"{tag} {MYCALL}>APDW18{path}:<IGATE,UPL_CNT=592,DNL_CNT=396")

            assert p.stats.packets_uploaded == 592, f"{tag} {path!r} was dropped"


def test_a_q_construct_marks_a_packet_that_came_back_from_aprs_is() -> None:
    """Servers stamp qAC/qAR/qAS on relay, so a packet carrying one has been
    through APRS-IS and is not ours no matter whose callsign it bears."""
    for q in ("qAC", "qAR", "qAS", "qAo"):
        p = DirewolfParser(MYCALL)
        p.feed(f"[ig>tx] {MYCALL}>APWW11,TCPIP*,{q},T2TEST:<IGATE,UPL_CNT=999999")

        assert p.stats.packets_uploaded is None, f"{q} was accepted as our own"


def test_beacon_symbol_check_accepts_real_aprs_symbol_codes() -> None:
    """Most APRS symbols are a table character plus punctuation, so an
    alphanumeric whitelist refuses the documented form of the option and the
    add-on will not start. Evaluates run.sh's own condition, not a copy."""
    import subprocess

    text = (ADDON_DIR / "run.sh").read_text()
    m = re.search(r'\[\[ "\$\{BEACON_SYMBOL\}" =~ (\S+) \]\]', text)
    assert m, "could not find the beacon_symbol check in run.sh"
    pattern = m.group(1)

    legal = ["igate", "I&", "/#", "R&", "/_", "/>", "\\#", "/-"]
    illegal = ["not a symbol", "", "a" * 21, "igate every=1"]

    for value in legal + illegal:
        proc = subprocess.run(
            ["bash", "-c", f'[[ "$1" =~ {pattern} ]]', "_", value],
            capture_output=True,
            check=False,
        )
        accepted = proc.returncode == 0
        if value in legal:
            assert accepted, f"legal symbol {value!r} would block startup"
        else:
            assert not accepted, f"{value!r} must be rejected"


def test_mqtt_enabled_reads_a_string_the_way_it_is_written() -> None:
    """Supervisor delivers a real bool, but a hand-edited options.json can hold
    a string -- and bool("false") is True, which would start publishing on a
    config that plainly says not to."""
    from direwolf_igate import enabled, from_mapping

    for falsey in (False, "false", "False", "no", "off", "0", "", None):
        assert enabled({"mqtt_enabled": falsey}) is False, repr(falsey)
        assert from_mapping({"mqtt_enabled": falsey}).mqtt_enabled is False

    for truthy in (True, "true", "True", "yes", "on", "1"):
        assert enabled({"mqtt_enabled": truthy}) is True, repr(truthy)
        assert from_mapping({"mqtt_enabled": truthy}).mqtt_enabled is True

    assert enabled({}) is False
