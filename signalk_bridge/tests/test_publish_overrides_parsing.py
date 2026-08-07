# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for ``publish_path_overrides`` option parsing.

The option is a list of ``{path, interval_seconds}`` objects (HA supervisor's
schema DSL doesn't support wildcard-key dicts). This test file locks in the
shape so a regression to the original ``{"*": "float"}`` form -- which the
Supervisor rejects at addon start with a schema-validation error -- is
caught before it ships.
"""

import json
from pathlib import Path

from signalk_bridge.app import _parse_publish_overrides


def test_parse_list_of_objects() -> None:
    raw = [
        {"path": "navigation.position", "interval_seconds": 0.5},
        {"path": "environment.wind.*", "interval_seconds": 1.0},
    ]
    assert _parse_publish_overrides(raw) == {
        "navigation.position": 0.5,
        "environment.wind.*": 1.0,
    }


def test_parse_empty_list_yields_no_overrides() -> None:
    assert _parse_publish_overrides([]) == {}


def test_parse_none_yields_no_overrides() -> None:
    assert _parse_publish_overrides(None) == {}


def test_parse_rejects_dict_form_from_the_broken_pre_release_schema() -> None:
    """The addon shipped with a `{"*": "float"}` schema for one release that
    Supervisor never accepted. If someone hand-migrated their options to
    that dict shape based on the old README, we should quietly skip it
    (not crash) so the addon starts on defaults."""
    raw = {"navigation.position": 0.5}
    assert _parse_publish_overrides(raw) == {}


def test_parse_skips_entries_with_bad_path_or_interval() -> None:
    raw = [
        {"path": "good.path", "interval_seconds": 2.0},
        {"path": "", "interval_seconds": 1.0},  # empty path
        {"path": None, "interval_seconds": 1.0},  # non-str path
        {"path": "no.interval"},  # missing interval
        {"path": "bad.interval", "interval_seconds": "nope"},  # non-numeric
        "not-a-dict",  # bogus entry
        {},  # empty obj
    ]
    assert _parse_publish_overrides(raw) == {"good.path": 2.0}


def test_parse_accepts_int_intervals() -> None:
    raw = [{"path": "electrical.batteries.*.voltage", "interval_seconds": 5}]
    assert _parse_publish_overrides(raw) == {"electrical.batteries.*.voltage": 5.0}


def test_defaults_in_config_json_parse_cleanly() -> None:
    """Whatever we ship as the addon's default options must round-trip through
    the parser -- otherwise a fresh install starts with a broken limiter."""
    cfg = json.loads(
        (Path(__file__).resolve().parents[1] / "config.json").read_text(
            encoding="utf-8"
        )
    )
    parsed = _parse_publish_overrides(cfg["options"]["publish_path_overrides"])
    assert "navigation.position" in parsed
    assert parsed["navigation.position"] == 0.5
    # Every default entry should have made it through -- no silent drops.
    assert len(parsed) == len(cfg["options"]["publish_path_overrides"])
