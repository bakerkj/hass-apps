# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for DSC / MOB / distress notification classification.

Safety-critical notifications must reach HA as ``device_class: safety`` (which
the mobile app and dashboards escalate distinctly) rather than the generic
``problem`` class shared with routine engine / battery alarms. Verified against
the full resolve_special pipeline so a regression in the app.py wiring is
caught in addition to the paths.py table.
"""

from typing import Any

from signalk_bridge.app import resolve_special
from signalk_bridge.paths import notification_device_class


def _tree(path: str, message: str = "test") -> dict[str, Any]:
    """Build a minimal SK tree with one notification leaf at ``path``."""
    parts = path.split(".")
    node: dict[str, Any] = {}
    cur = node
    for p in parts[:-1]:
        cur[p] = {}
        cur = cur[p]
    cur[parts[-1]] = {
        "value": {"state": "alarm", "message": message, "method": ["visual"]},
        "$source": "test",
        "timestamp": "2026-08-05T04:00:00Z",
    }
    return node


def test_dsc_call_gets_safety_device_class() -> None:
    assert (
        notification_device_class("notifications.communications.dsc.abc123") == "safety"
    )
    assert notification_device_class("notifications.dsc.abc123") == "safety"


def test_mob_gets_safety_device_class() -> None:
    assert notification_device_class("notifications.mob") == "safety"
    assert notification_device_class("notifications.mob.detected") == "safety"


def test_distress_gets_safety_device_class() -> None:
    assert (
        notification_device_class("notifications.communications.distress.received")
        == "safety"
    )


def test_routine_alarm_stays_problem_class() -> None:
    assert (
        notification_device_class("notifications.propulsion.port.overTemperature")
        == "problem"
    )
    assert (
        notification_device_class("notifications.instrument.PilotOffCourse")
        == "problem"
    )


def test_dsc_notification_reaches_publish_pipeline_as_safety() -> None:
    ents = resolve_special(_tree("notifications.communications.dsc.MDA000123"))
    key = "notifications_communications_dsc_mda000123"
    assert key in ents
    e = ents[key]
    assert e["component"] == "binary_sensor"
    assert e["device_class"] == "safety"
    assert e["state"] == "ON"


def test_mob_notification_reaches_publish_pipeline_as_safety() -> None:
    ents = resolve_special(_tree("notifications.mob", message="MOB detected"))
    e = ents["notifications_mob"]
    assert e["device_class"] == "safety"


def test_routine_notification_reaches_publish_pipeline_as_problem() -> None:
    ents = resolve_special(
        _tree("notifications.propulsion.port.checkEngine", message="check engine")
    )
    e = ents["notifications_propulsion_port_checkengine"]
    assert e["device_class"] == "problem"
