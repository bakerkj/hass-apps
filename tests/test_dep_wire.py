# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

import json

from dashboard_entity_proxy import wire


def test_parse_single_object():
    assert wire.parse_messages('{"id":1,"type":"event"}') == [
        {"id": 1, "type": "event"}
    ]


def test_parse_array():
    assert wire.parse_messages('[{"id":1},{"id":2}]') == [{"id": 1}, {"id": 2}]


def test_parse_whitespace_led_array():
    assert wire.parse_messages('  \n[{"a":1}]') == [{"a": 1}]


def test_parse_empty_array():
    assert wire.parse_messages("[]") == []


def test_parse_non_json_returns_none():
    assert wire.parse_messages("not json") is None


def test_parse_scalar_returns_empty():
    assert wire.parse_messages("42") == []
    assert wire.parse_messages('"x"') == []


def test_array_filters_non_dicts():
    assert wire.parse_messages('[{"a":1},42,{"b":2}]') == [{"a": 1}, {"b": 2}]


def test_array_filtering_emits_debug_log(caplog):
    import logging

    with caplog.at_level(logging.DEBUG, logger="dashboard_entity_proxy.wire"):
        wire.parse_messages('[{"a":1},42,{"b":2},"x"]')
    msgs = [r.getMessage() for r in caplog.records]
    assert any("dropped 2" in m and "array of 4" in m for m in msgs), msgs


def test_unexpected_top_level_shape_emits_debug_log(caplog):
    import logging

    with caplog.at_level(logging.DEBUG, logger="dashboard_entity_proxy.wire"):
        wire.parse_messages("42")
    msgs = [r.getMessage() for r in caplog.records]
    assert any("top-level JSON shape is int" in m for m in msgs), msgs


def test_clean_array_emits_no_debug_log(caplog):
    import logging

    with caplog.at_level(logging.DEBUG, logger="dashboard_entity_proxy.wire"):
        wire.parse_messages('[{"a":1},{"b":2}]')
    assert not caplog.records


def test_event_message_round_trips():
    raw = wire.event_message(7, {"c": {"x.y": {"+": {"s": "on"}}}})
    assert json.loads(raw) == {
        "id": 7,
        "type": "event",
        "event": {"c": {"x.y": {"+": {"s": "on"}}}},
    }


def test_result_ok_shape():
    assert json.loads(wire.result_ok(9)) == {
        "id": 9,
        "type": "result",
        "success": True,
        "result": None,
    }


def test_command_with_extra_fields():
    raw = wire.command(5, "lovelace/config", url_path="my-dashboard", force=False)
    assert json.loads(raw) == {
        "id": 5,
        "type": "lovelace/config",
        "url_path": "my-dashboard",
        "force": False,
    }
