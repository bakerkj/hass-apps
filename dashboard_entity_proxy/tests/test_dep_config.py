# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

import json

import pytest
from dashboard_entity_proxy.const import DEFAULT_HA_URL

from dashboard_entity_proxy import config


def _write(tmp_path, content):
    path = tmp_path / "options.json"
    path.write_text(content)
    return str(path)


def test_defaults(tmp_path):
    cfg = config.load(_write(tmp_path, "{}"))
    assert cfg.homeassistant_url == DEFAULT_HA_URL
    assert cfg.transparent is True
    assert cfg.state_update_interval == 0.0
    assert cfg.passthrough_all is False
    assert cfg.log_level == "INFO"


def test_overrides(tmp_path):
    cfg = config.load(
        _write(
            tmp_path,
            json.dumps(
                {
                    "homeassistant_url": "https://ha.example:8123",
                    "transparent": False,
                    "state_update_interval": "2s",
                    "extra_entities": ["sensor.x"],
                    "include_entity_globs": ["light.*"],
                    "exclude_entity_globs": ["*_battery"],
                    "passthrough_all": True,
                    "log_level": "debug",
                }
            ),
        )
    )
    assert cfg.homeassistant_url == "https://ha.example:8123"
    assert cfg.transparent is False
    assert cfg.state_update_interval == 2.0
    assert cfg.extra_entities == ["sensor.x"]
    assert cfg.include_entity_globs == ["light.*"]
    assert cfg.exclude_entity_globs == ["*_battery"]
    assert cfg.passthrough_all is True
    assert cfg.log_level == "DEBUG"


@pytest.mark.parametrize(
    "opts",
    [
        {"homeassistant_url": "ftp://ha:21"},
        {"homeassistant_url": "http://"},
        {"log_level": "VERBOSE"},
        {"state_update_interval": "nope"},
        {"state_update_interval": "0s"},
        {"extra_entities": ["not_an_entity"]},
        {"extra_entities": ["Sensor.Upper"]},
        {"extra_entities": ["sensor."]},
        {"extra_entities": [".kitchen"]},
        {"include_entity_globs": [""]},
        {"include_entity_globs": ["light .*"]},
        {"exclude_entity_globs": ["foo/bar"]},
        {"exclude_entity_globs": ["light\t*"]},
    ],
)
def test_invalid(tmp_path, opts):
    with pytest.raises(ValueError):
        config.load(_write(tmp_path, json.dumps(opts)))


@pytest.mark.parametrize(
    "opts",
    [
        {"extra_entities": ["sensor.kitchen_temp", "binary_sensor.front_door"]},
        {"include_entity_globs": ["light.*", "*_battery", "sensor.[kl]*"]},
        {"exclude_entity_globs": ["sensor.*_diagnostic"]},
    ],
)
def test_valid_lists(tmp_path, opts):
    # Smoke test that legitimate entity ids and glob shapes load cleanly.
    config.load(_write(tmp_path, json.dumps(opts)))


@pytest.mark.parametrize(
    "interval,seconds",
    [
        ("2s", 2.0),
        ("500ms", 0.5),
        ("1h", 3600.0),
        ("1.5h", 5400.0),
        ("90s", 90.0),
        # humanfriendly treats a bare number as seconds.
        ("5", 5.0),
    ],
)
def test_state_update_interval_shapes(tmp_path, interval, seconds):
    cfg = config.load(_write(tmp_path, json.dumps({"state_update_interval": interval})))
    assert cfg.state_update_interval == pytest.approx(seconds)


def test_missing_file(tmp_path):
    with pytest.raises(OSError):
        config.load(str(tmp_path / "missing.json"))


@pytest.mark.parametrize(
    "customization_file",
    [
        "/homeassistant/foo.yaml",
        "/homeassistant/sub/dir/foo.yaml",
        "/share/foo.yaml",
    ],
)
def test_customization_file_allowed_prefixes(tmp_path, customization_file):
    """Paths under the read-only Supervisor mounts (/homeassistant, /share)
    load without complaint. The validator does not stat the file itself —
    the YAML is opened later in app startup — so a non-existent name under
    an allowed prefix still passes config validation.
    """
    cfg = config.load(
        _write(tmp_path, json.dumps({"customization_file": customization_file}))
    )
    assert cfg.customization_file == customization_file


def test_customization_file_empty_string_accepted(tmp_path):
    """An empty ``customization_file`` is the "no customization configured"
    path — must remain a no-op (no path check, no error).
    """
    cfg = config.load(_write(tmp_path, json.dumps({"customization_file": ""})))
    assert cfg.customization_file == ""


@pytest.mark.parametrize(
    "customization_file",
    [
        "/etc/passwd",
        "/data/options.json",
        "/ssl/privkey.pem",
        # ``..`` traversal must be normalised away before the prefix check;
        # this resolves to ``/etc/passwd`` and is rejected.
        "/homeassistant/../etc/passwd",
        # The deprecated ``/config`` mount path is no longer in the
        # allowed-prefix list after the switch to ``homeassistant_config``.
        "/config/foo.yaml",
        # Relative paths — cwd at validation time is arbitrary, so anything
        # outside the allowed roots must be rejected on principle.
        "etc/passwd",
    ],
)
def test_customization_file_rejected_outside_allowed_prefixes(
    tmp_path, customization_file
):
    with pytest.raises(ValueError, match="customization_file"):
        config.load(
            _write(tmp_path, json.dumps({"customization_file": customization_file}))
        )


def test_customization_file_oserror_becomes_friendly_value_error(tmp_path, monkeypatch):
    """Path.resolve can raise OSError (symlink loop, EACCES on a path
    component). Without the guard the user sees a raw traceback in the
    Supervisor log; with it, a ValueError that names the option.
    """
    from pathlib import Path

    def _boom(self):
        raise OSError("symlink loop")

    monkeypatch.setattr(Path, "resolve", _boom)
    with pytest.raises(ValueError, match="customization_file"):
        config.load(
            _write(
                tmp_path, json.dumps({"customization_file": "/homeassistant/x.yaml"})
            )
        )
