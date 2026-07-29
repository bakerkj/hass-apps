# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for the HALPI2 Power add-on."""

import argparse
import json
from typing import Any
from unittest import mock

import pytest

from halpi2_power import app, config, halpid_client, sensors
from halpi2_power import mqtt as hmqtt

# Captured verbatim from halpid 5.1.1 on real hardware. The upstream README
# documents different field names (dcin_voltage, power_state, ...) which the
# daemon does not actually emit -- these are the real ones.
VALUES_SAMPLE: dict[str, Any] = {
    "5v_output_enabled": True,
    "I_in": 0.24633178114891052,
    "T_mcu": 324.5357971191406,
    "T_pcb": 311.7718200683594,
    "V_cap": 10.35076904296875,
    "V_in": 13.8134765625,
    "daemon_version": "5.1.1",
    "device_id": "0011223344556677",
    "firmware_version": "3.3.1",
    "hardware_version": "N/A",
    "num_leds": 5,
    "state": "OperationalCoOp",
    "watchdog_elapsed": 0.0,
    "watchdog_enabled": True,
    "watchdog_timeout": 10.0,
}


class FakeClient:
    """Records publishes instead of talking to a broker."""

    def __init__(self) -> None:
        self.published: list[tuple[str, str, int, bool]] = []

    def publish(
        self, topic: str, payload: Any = "", qos: int = 0, retain: bool = False
    ) -> None:
        self.published.append((topic, payload, qos, retain))

    def topics(self) -> list[str]:
        return [t for t, _, _, _ in self.published]


# --------------------------------------------------------------------------
# render_state
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (12.5, "12.5"),
        (12.0, "12"),
        (0.8, "0.8"),
        (0.123456, "0.1235"),  # rounded to 4dp
        ("OperationalCoOp", "OperationalCoOp"),
        (0, "0"),
        (-0.0, "0"),  # a tiny negative reading must not publish "-0"
    ],
)
def test_render_state(value: Any, expected: str) -> None:
    assert app.render_state(value) == expected


# --------------------------------------------------------------------------
# discovery payloads
# --------------------------------------------------------------------------


def test_publish_all_discovery_covers_every_sensor() -> None:
    client = FakeClient()
    hmqtt.publish_all_discovery(
        client,  # type: ignore[arg-type]
        "homeassistant",
        "dev123",
        "halpi2_power",
        VALUES_SAMPLE,
        40,
    )

    # one config per numeric sensor, plus power_state, plus the derived countdowns
    assert len(client.published) == (
        len(sensors.SENSOR_DEFS) + 1 + len(sensors.DERIVED_DEFS)
    )
    for key in sensors.SENSOR_DEFS:
        assert f"homeassistant/sensor/dev123/{key}/config" in client.topics()
    assert "homeassistant/sensor/dev123/power_state/config" in client.topics()

    # discovery must be retained, or HA loses the entities on restart
    assert all(retain for _, _, _, retain in client.published)


def test_discovery_payload_shape() -> None:
    client = FakeClient()
    hmqtt.publish_discovery(
        client,  # type: ignore[arg-type]
        "homeassistant",
        "dev123",
        "halpi2_power",
        "dcin_voltage",
        sensors.SENSOR_DEFS["dcin_voltage"],
        VALUES_SAMPLE,
        40,
    )
    _topic, raw, _qos, _retain = client.published[0]
    payload = json.loads(raw)

    assert payload["unique_id"] == "dev123_dcin_voltage"
    # object_id (a real discovery key) suggests the entity_id; default_entity_id
    # is not a discovery key and must not be emitted.
    assert payload["object_id"] == "halpi2_dcin_voltage"
    assert "default_entity_id" not in payload
    assert payload["device_class"] == "voltage"
    assert payload["unit_of_measurement"] == "V"
    assert payload["state_topic"] == "halpi2_power/dcin_voltage/state"
    assert payload["availability_topic"] == "halpi2_power/availability"
    assert payload["expire_after"] == 40
    # firmware/hardware surface as device metadata, not entities
    assert payload["device"]["sw_version"] == "3.3.1"
    assert payload["device"]["hw_version"] == "N/A"
    assert payload["device"]["identifiers"] == ["dev123"]


def test_power_state_is_a_plain_text_sensor() -> None:
    client = FakeClient()
    hmqtt.publish_discovery(
        client,  # type: ignore[arg-type]
        "homeassistant",
        "dev123",
        "halpi2_power",
        sensors.POWER_STATE_KEY,
        sensors.POWER_STATE_DEF,
        VALUES_SAMPLE,
        40,
    )
    payload = json.loads(client.published[0][1])
    # Deliberately NOT device_class=enum: halpid emits a wide set of state
    # strings (operating, power-outage, startup/shutdown), and a fixed `options`
    # list would mark any unlisted state invalid -- blanking the entity exactly
    # during an outage. A plain text sensor accepts whatever the daemon reports.
    assert "device_class" not in payload
    assert "options" not in payload
    assert "state_class" not in payload
    assert payload["icon"] == "mdi:power-plug"
    assert payload["unique_id"] == "dev123_power_state"
    assert payload["state_topic"] == "halpi2_power/power_state/state"


def test_ha_birth_message_flags_rediscovery() -> None:
    # When Home Assistant restarts it re-publishes "online" on <prefix>/status
    # and drops our retained discovery configs. build_client's on_message must
    # flag a republish so the entities return without an add-on restart.
    health = hmqtt.MqttHealth()
    client = app.build_client(
        {"username": None, "password": None},
        "cid",
        "halpi2_power",
        "homeassistant",
        health,
    )
    client.on_message(
        client, None, mock.Mock(topic="homeassistant/status", payload=b"online")
    )
    assert health.rediscover is True

    # An "offline" birth, or traffic on some other topic, must NOT trigger it.
    health.rediscover = False
    client.on_message(
        client, None, mock.Mock(topic="homeassistant/status", payload=b"offline")
    )
    client.on_message(
        client, None, mock.Mock(topic="halpi2_power/other", payload=b"online")
    )
    assert health.rediscover is False


def test_expire_after_has_a_floor() -> None:
    client = FakeClient()
    hmqtt.publish_discovery(
        client,  # type: ignore[arg-type]
        "homeassistant",
        "dev123",
        "halpi2_power",
        "dcin_voltage",
        sensors.SENSOR_DEFS["dcin_voltage"],
        VALUES_SAMPLE,
        1,
    )
    assert json.loads(client.published[0][1])["expire_after"] == 5


def test_clear_discovery_publishes_empty_retained() -> None:
    client = FakeClient()
    hmqtt.clear_discovery(client, "homeassistant", "dev123", "dcin_voltage")  # type: ignore[arg-type]
    topic, payload, _qos, retain = client.published[0]
    assert topic == "homeassistant/sensor/dev123/dcin_voltage/config"
    assert payload == ""
    assert retain is True


# --------------------------------------------------------------------------
# halpid client
# --------------------------------------------------------------------------


def test_get_values_rejects_non_object() -> None:
    with (
        mock.patch.object(halpid_client, "_request", return_value=[1, 2, 3]),
        pytest.raises(halpid_client.HalpidError, match="expected object"),
    ):
        halpid_client.get_values("/nonexistent.sock")


def test_get_values_returns_payload() -> None:
    with mock.patch.object(halpid_client, "_request", return_value=VALUES_SAMPLE):
        assert halpid_client.get_values("/nonexistent.sock") == VALUES_SAMPLE


def test_request_wraps_socket_errors() -> None:
    # A missing socket must surface as HalpidError, not a bare OSError, so the
    # publisher's retry path catches it.
    with pytest.raises(halpid_client.HalpidError):
        halpid_client.get_values("/definitely/not/a/socket.sock", timeout=0.1)


# --------------------------------------------------------------------------
# options + MQTT resolution
# --------------------------------------------------------------------------


def test_load_options_file_accepts_flat_object(tmp_path: Any) -> None:
    p = tmp_path / "options.json"
    p.write_text(json.dumps({"interval_seconds": 15, "log_level": "DEBUG"}))
    ap = argparse.ArgumentParser()
    opts = config.load_options_file(str(p), ap)
    assert opts["interval_seconds"] == 15


def test_load_options_file_accepts_nested_object(tmp_path: Any) -> None:
    p = tmp_path / "options.json"
    p.write_text(json.dumps({"options": {"interval_seconds": 22}}))
    ap = argparse.ArgumentParser()
    assert config.load_options_file(str(p), ap)["interval_seconds"] == 22


def test_load_options_file_flat_mode_is_not_mistaken_for_nested(tmp_path: Any) -> None:
    # `mode` is a first-class option, so a flat payload carrying it must be read
    # as flat even if a spurious "options" key is present -- not unwrapped.
    p = tmp_path / "options.json"
    p.write_text(json.dumps({"mode": "solo", "options": {"mode": "coop"}}))
    ap = argparse.ArgumentParser()
    assert config.load_options_file(str(p), ap)["mode"] == "solo"


def test_redact_hides_mqtt_password() -> None:
    out = config.redact_options_for_log(
        {"mqtt_password": "hunter2", "mqtt_username": "bob"}
    )
    assert out["mqtt_password"] == "***"
    assert out["mqtt_username"] == "bob"


def test_redact_leaves_empty_password_alone() -> None:
    out = config.redact_options_for_log({"mqtt_password": ""})
    assert out["mqtt_password"] == ""


def test_redact_hides_secrets_by_name_marker() -> None:
    # A secret added later (not in the explicit allowlist) is still redacted by
    # its name, so it is not logged in the clear.
    out = config.redact_options_for_log(
        {"mqtt_token": "abc123", "tls_secret": "s3cr3t", "interval_seconds": 10}
    )
    assert out["mqtt_token"] == "***"
    assert out["tls_secret"] == "***"
    assert out["interval_seconds"] == 10


def test_resolve_mqtt_prefers_explicit_options() -> None:
    service = {
        "host": "core-mosquitto",
        "port": 1883,
        "username": "svc",
        "password": "svcpw",
    }
    with mock.patch.object(config, "_supervisor_mqtt_service", return_value=service):
        cfg = config.resolve_mqtt_config({"mqtt_host": "broker.lan", "mqtt_port": 8883})
    assert cfg["host"] == "broker.lan"
    assert cfg["port"] == 8883
    # Supervisor credentials must NOT leak to a different broker.
    assert cfg["username"] is None


def test_resolve_mqtt_uses_supervisor_when_unset() -> None:
    service = {
        "host": "core-mosquitto",
        "port": 1883,
        "username": "svc",
        "password": "svcpw",
    }
    with mock.patch.object(config, "_supervisor_mqtt_service", return_value=service):
        cfg = config.resolve_mqtt_config({})
    assert cfg["host"] == "core-mosquitto"
    assert cfg["username"] == "svc"
    assert cfg["password"] == "svcpw"


def test_resolve_mqtt_raises_without_any_broker() -> None:
    with (
        mock.patch.object(config, "_supervisor_mqtt_service", return_value=None),
        pytest.raises(RuntimeError, match="No MQTT broker configured"),
    ):
        config.resolve_mqtt_config({})


# --------------------------------------------------------------------------
# sensor definitions
# --------------------------------------------------------------------------


def test_power_outage_detected_from_controller_state() -> None:
    # halpid reports the suffixed on-cap state values below (not a bare prefix);
    # in_power_outage must still detect them via its prefix match.
    assert app.in_power_outage({"state": "BlackoutShutdown", "V_in": 12.0}, 9.0) is True
    assert app.in_power_outage({"state": "BlackoutCoOp", "V_in": 12.0}, 9.0) is True


def test_power_outage_detected_from_low_voltage_before_state_flips() -> None:
    # The countdown should appear as soon as the voltage collapses, without
    # waiting for the controller to transition state.
    assert app.in_power_outage({"state": "OperationalCoOp", "V_in": 8.2}, 9.0) is True


def test_not_in_power_outage_when_nominal() -> None:
    assert app.in_power_outage({"state": "OperationalCoOp", "V_in": 13.7}, 9.0) is False


def test_power_outage_threshold_is_exclusive() -> None:
    # Exactly at the limit is not yet a power outage.
    assert app.in_power_outage({"state": "OperationalCoOp", "V_in": 9.0}, 9.0) is False
    assert app.in_power_outage({"state": "OperationalCoOp", "V_in": 8.999}, 9.0) is True


def test_power_outage_ignores_missing_voltage() -> None:
    assert app.in_power_outage({"state": "OperationalCoOp"}, 9.0) is False


def test_derived_sensors_are_published_in_discovery() -> None:
    client = FakeClient()
    hmqtt.publish_all_discovery(
        client,  # type: ignore[arg-type]
        "homeassistant",
        "dev123",
        "halpi2_power",
        VALUES_SAMPLE,
        40,
    )
    topics = client.topics()
    assert "homeassistant/sensor/dev123/power_outage_elapsed/config" in topics
    assert "homeassistant/sensor/dev123/shutdown_in/config" in topics
    # numeric + power_state + the two derived
    assert len(client.published) == len(sensors.SENSOR_DEFS) + 1 + len(
        sensors.DERIVED_DEFS
    )


def test_poll_interval_is_short_enough_to_catch_a_power_outage() -> None:
    """The read cadence must be well under the shutdown deadline.

    halpid shuts down after power_outage_time_limit (5 s by default). If halpid is
    read less often than that, a power outage can begin and end between two reads
    and never be observed at all. This is why reading is decoupled from
    publishing rather than simply lowering the publish interval.
    """
    default_power_outage_time_limit = 5.0
    assert app.POLL_INTERVAL_S <= default_power_outage_time_limit / 2, (
        "read cadence must be at most half the shutdown deadline"
    )


def test_trace_writes_baseline_then_discharge(tmp_path: Any) -> None:
    # The baseline row carries pre-cut V_in/I_in (load power); discharge rows do
    # not, so effective capacitance is only computable if the baseline is present.
    trace = tmp_path / "trace.csv"
    with mock.patch.object(app, "TRACE_PATH", str(trace)):
        app.append_trace(
            0.0,
            {"V_in": 13.4, "V_cap": 10.35, "I_in": 0.34, "state": "OperationalSolo"},
            phase="baseline",
        )
        app.append_trace(
            2.0,
            {"V_in": 0.02, "V_cap": 9.1, "I_in": 0.0, "state": "BlackoutShutdown"},
            phase="discharge",
        )
    lines = trace.read_text().splitlines()
    assert lines[0] == "iso_time,phase,elapsed_s,V_in,V_cap,I_in,state"
    assert ",baseline,0.0,13.4," in lines[1]
    assert ",discharge,2.0,0.02," in lines[2]


def test_numeric_sensors_all_declare_units_and_classes() -> None:
    for key, definition in sensors.SENSOR_DEFS.items():
        assert definition["unit"], f"{key} missing unit"
        assert definition["device_class"], f"{key} missing device_class"
        assert definition["state_class"] == "measurement", f"{key} wrong state_class"


def test_every_sensor_source_exists_in_the_real_payload() -> None:
    # Entity keys are ours, but each `source` must name a field halpid really
    # returns -- this is what silently broke when trusting the upstream README.
    for key, definition in sensors.SENSOR_DEFS.items():
        src = definition["source"]
        assert src in VALUES_SAMPLE, f"{key}: source {src!r} absent from /values"
    assert sensors.POWER_STATE_DEF["source"] in VALUES_SAMPLE


def test_temperatures_are_converted_from_kelvin() -> None:
    # halpid reports kelvin; HA expects celsius.
    mcu = sensors.extract(VALUES_SAMPLE, sensors.SENSOR_DEFS["mcu_temperature"])
    assert mcu is not None
    assert 50.0 < mcu < 53.0, f"expected ~51.4 C, got {mcu}"

    pcb = sensors.extract(VALUES_SAMPLE, sensors.SENSOR_DEFS["pcb_temperature"])
    assert pcb is not None
    assert 37.0 < pcb < 40.0, f"expected ~38.6 C, got {pcb}"


def test_extract_passes_through_untransformed_values() -> None:
    v = sensors.extract(VALUES_SAMPLE, sensors.SENSOR_DEFS["dcin_voltage"])
    assert v == VALUES_SAMPLE["V_in"]


def test_extract_returns_none_for_missing_source() -> None:
    assert sensors.extract({}, sensors.SENSOR_DEFS["dcin_voltage"]) is None


def test_power_state_reads_the_state_field() -> None:
    # Co-op vs Solo is the single most important signal this add-on surfaces.
    assert sensors.extract(VALUES_SAMPLE, sensors.POWER_STATE_DEF) == "OperationalCoOp"
    assert sensors.extract({"state": "OperationalSolo"}, sensors.POWER_STATE_DEF) == (
        "OperationalSolo"
    )
