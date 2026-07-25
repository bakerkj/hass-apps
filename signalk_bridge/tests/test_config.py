# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for option loading, secret redaction, and MQTT broker resolution."""

import argparse
import json
from pathlib import Path

import pytest

from signalk_bridge import config


def _ap() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(prog="test")


def test_load_options_flat(tmp_path: Path) -> None:
    p = tmp_path / "o.json"
    p.write_text(json.dumps({"signalk_url": "http://x", "mqtt_host": "h"}))
    opts = config.load_options_file(str(p), _ap())
    assert opts["signalk_url"] == "http://x"


def test_load_options_unwraps_nested(tmp_path: Path) -> None:
    # Some callers pass {"options": {...}}; unwrap when no top-level option keys.
    p = tmp_path / "o.json"
    p.write_text(json.dumps({"options": {"mqtt_host": "h"}}))
    assert config.load_options_file(str(p), _ap())["mqtt_host"] == "h"


def test_load_options_prefers_flat_over_nested(tmp_path: Path) -> None:
    p = tmp_path / "o.json"
    p.write_text(json.dumps({"mqtt_host": "top", "options": {"mqtt_host": "nested"}}))
    assert config.load_options_file(str(p), _ap())["mqtt_host"] == "top"


def test_load_options_bad_json_errors(tmp_path: Path) -> None:
    p = tmp_path / "o.json"
    p.write_text("{not json")
    with pytest.raises(SystemExit):
        config.load_options_file(str(p), _ap())


def test_load_options_missing_file_errors(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        config.load_options_file(str(tmp_path / "nope.json"), _ap())


def test_redaction_masks_secrets() -> None:
    red = config.redact_options_for_log(
        {"mqtt_password": "p", "signalk_token": "t", "mqtt_host": "h"}
    )
    assert red["mqtt_password"] == "***"
    assert red["signalk_token"] == "***"
    assert red["mqtt_host"] == "h"


def test_redaction_leaves_empty_secret_untouched() -> None:
    # An empty secret has nothing to hide; masking it would imply one is set.
    assert config.redact_options_for_log({"mqtt_password": ""})["mqtt_password"] == ""


def test_resolve_mqtt_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "_supervisor_mqtt_service", lambda *a, **k: None)
    cfg = config.resolve_mqtt_config(
        {
            "mqtt_host": "broker",
            "mqtt_port": 1884,
            "mqtt_username": "u",
            "mqtt_password": "p",
        }
    )
    assert cfg == {"host": "broker", "port": 1884, "username": "u", "password": "p"}


def test_resolve_mqtt_defaults_port_and_anonymous(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "_supervisor_mqtt_service", lambda *a, **k: None)
    cfg = config.resolve_mqtt_config({"mqtt_host": "broker"})
    assert cfg["port"] == 1883
    assert cfg["username"] is None
    assert cfg["password"] is None


def test_resolve_mqtt_uses_supervisor_when_no_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        config,
        "_supervisor_mqtt_service",
        lambda *a, **k: {
            "host": "core-mosquitto",
            "port": 1883,
            "username": "svc",
            "password": "pw",
        },
    )
    cfg = config.resolve_mqtt_config({})
    assert cfg["host"] == "core-mosquitto"
    assert cfg["username"] == "svc"
    assert cfg["password"] == "pw"


def test_resolve_mqtt_explicit_host_does_not_borrow_supervisor_creds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An external broker must not inherit Supervisor credentials that belong to
    # a different broker.
    monkeypatch.setattr(
        config,
        "_supervisor_mqtt_service",
        lambda *a, **k: {"host": "core-mosquitto", "username": "svc", "password": "pw"},
    )
    cfg = config.resolve_mqtt_config({"mqtt_host": "external"})
    assert cfg["host"] == "external"
    assert cfg["username"] is None


def test_resolve_mqtt_no_broker_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "_supervisor_mqtt_service", lambda *a, **k: None)
    with pytest.raises(RuntimeError):
        config.resolve_mqtt_config({})
