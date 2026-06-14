# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

from typing import Any

from _session_factory import build_session_for_test
from dashboard_entity_proxy.inflight_types import (
    ClientConfig,
    ClientReq,
    ClientUnsubscribe,
    ConfigFetch,
    Mirror,
)
from dashboard_entity_proxy.proxy import TunnelConnection
from dashboard_entity_proxy.session import SessionRegistry, filter_event


def test_filter_event_passthrough_when_scope_none():
    ev: dict[str, Any] = {"c": {"x.y": {}}}
    assert filter_event(ev, None) is ev


def test_filter_event_keeps_only_in_scope():
    ev = {
        "a": {"light.a": {"s": "on"}, "light.b": {"s": "on"}},
        "c": {"sensor.x": {}},
        "r": ["light.b", "switch.z"],
    }
    out = filter_event(ev, {"light.a", "light.b"})
    assert out is not None
    assert set(out.get("a", {})) == {"light.a", "light.b"}
    assert "c" not in out
    assert out["r"] == ["light.b"]


def test_filter_event_nothing_relevant_returns_none():
    assert filter_event({"c": {"a.b": {}}}, {"c.d"}) is None


def test_tunnel_connection_status_shape():
    c = TunnelConnection("192.0.2.5", "/api/websocket", passthrough=True)
    c.msgs_client_to_ha = 3
    c.msgs_ha_to_client = 17
    s = c.status()
    assert s["kind"] == "passthrough"
    assert s["remote_addr"] == "192.0.2.5"
    assert s["target_path"] == "/api/websocket"
    assert s["messages_client_to_ha"] == 3
    assert s["messages_ha_to_client"] == 17
    assert "connected_at" in s

    t = TunnelConnection("192.0.2.6", "/raw/ws?x=1", passthrough=False)
    assert t.status()["kind"] == "tunnel"


def test_session_registry_accepts_heterogeneous_connections():
    # ``retention_seconds=0`` disables disconnect-retention so the legacy
    # "removed means gone" semantics still apply for this test.
    reg = SessionRegistry(retention_seconds=0)
    a = TunnelConnection("192.0.2.1", "/api/websocket", passthrough=True)
    b = TunnelConnection("192.0.2.2", "/x", passthrough=False)
    reg.add(a)
    reg.add(b)
    snap = reg.snapshot()
    assert {s["remote_addr"] for s in snap} == {"192.0.2.1", "192.0.2.2"}
    reg.remove(a)
    assert {s["remote_addr"] for s in reg.snapshot()} == {"192.0.2.2"}


def test_session_registry_retains_recent_disconnects():
    """A removed connection lingers as a snapshot with ``disconnected_at``
    set, so the status UI can show "this tab was here a moment ago" rather
    than blinking it away. Beyond ``retention_seconds`` it's pruned.
    """
    reg = SessionRegistry(retention_seconds=60)
    a = TunnelConnection("192.0.2.1", "/api/websocket", passthrough=True)
    reg.add(a)
    reg.remove(a)
    snap = reg.snapshot()
    assert len(snap) == 1
    assert snap[0]["remote_addr"] == "192.0.2.1"
    assert "disconnected_at" in snap[0]


def test_session_registry_prunes_expired_disconnects():
    reg = SessionRegistry(retention_seconds=0.01)
    a = TunnelConnection("192.0.2.1", "/api/websocket", passthrough=True)
    reg.add(a)
    reg.remove(a)
    # Force the recent entry's timestamp into the past.
    reg._recent = [(t - 1.0, dt, s) for t, dt, s in reg._recent]
    assert reg.snapshot() == []


def test_session_registry_remove_is_idempotent():
    """``remove`` called twice should not double-record the disconnect."""
    reg = SessionRegistry(retention_seconds=60)
    a = TunnelConnection("192.0.2.1", "/api/websocket", passthrough=True)
    reg.add(a)
    reg.remove(a)
    reg.remove(a)
    assert len(reg.snapshot()) == 1


def test_session_registry_sorts_by_raw_datetime_not_iso_string():
    """``snapshot()`` orders by the raw ``_connected_at`` ``datetime``, not
    the ISO string in the snapshot dict. A test double that emits a
    timezone-naive (or otherwise lexically-misordered) ISO string while
    carrying the correct raw ``datetime`` must still sort correctly.
    """
    from datetime import datetime, timedelta, timezone

    class _DoubleFmtConn:
        def __init__(self, remote: str, dt, iso_override: str | None = None) -> None:
            self._remote = remote
            self._connected_at = dt
            self._iso = iso_override or dt.isoformat()

        def status(self):
            return {
                "kind": "intercept",
                "remote_addr": self._remote,
                "connected_at": self._iso,
            }

    base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    # ``earlier`` has the smaller raw datetime but its ISO string (tz-naive,
    # offset-stripped) sorts AFTER the tz-aware ISO of ``later`` lexically.
    earlier = _DoubleFmtConn(
        "192.0.2.1",
        base,
        iso_override=base.replace(tzinfo=None).isoformat(),
    )
    later = _DoubleFmtConn("192.0.2.2", base + timedelta(seconds=5))

    reg = SessionRegistry(retention_seconds=60)
    reg.add(later)
    reg.add(earlier)
    out = reg.snapshot()
    assert [s["remote_addr"] for s in out] == ["192.0.2.1", "192.0.2.2"]


def test_pump_ha_drops_non_json_text_frame_without_forwarding(caplog):
    """A non-JSON text frame from HA is a protocol violation; the pump must
    log at DEBUG and drop it (NOT forward to the client). Forwarding such
    frames would just confuse the client side of the proxy.
    """
    import asyncio
    import logging

    from aiohttp import WSMsgType

    class _FakeMsg:
        def __init__(self, mtype, data):
            self.type = mtype
            self.data = data

    class _FakeWsHa:
        def __init__(self, messages):
            self._messages = list(messages)

        def __aiter__(self):
            return self

        async def __anext__(self):
            if not self._messages:
                raise StopAsyncIteration
            return self._messages.pop(0)

    sent_to_client: list[str] = []
    sent_bytes: list[bytes] = []
    handled: list[dict] = []

    s = build_session_for_test(log=logging.getLogger("test_dep_session_unit.pump_ha"))
    s.ws_ha = _FakeWsHa(  # type: ignore[assignment]
        [
            _FakeMsg(WSMsgType.TEXT, "not-json-at-all"),
            _FakeMsg(WSMsgType.TEXT, '{"id":1,"type":"result","success":true}'),
        ]
    )
    s.send_client = lambda data: sent_to_client.append(data)  # type: ignore[method-assign,assignment]
    s.send_client_bytes = lambda data: sent_bytes.append(data)  # type: ignore[method-assign]
    s._handle_ha = lambda m: handled.append(m)  # type: ignore[method-assign,assignment]
    s._cleanup = lambda: None  # type: ignore[method-assign]

    with caplog.at_level(logging.DEBUG, logger="test_dep_session_unit.pump_ha"):
        asyncio.run(s._pump_ha())

    # The non-JSON frame must NOT reach the client.
    assert "not-json-at-all" not in sent_to_client
    assert sent_to_client == []
    # The JSON frame must have been dispatched to ``_handle_ha`` as a dict.
    assert handled == [{"id": 1, "type": "result", "success": True}]
    # And the drop must have left a DEBUG-level diagnostic trail.
    assert any(
        "non-JSON text frame from HA" in rec.message and rec.levelno == logging.DEBUG
        for rec in caplog.records
    )


def test_session_registry_sort_handles_mixed_live_and_recent_by_datetime():
    """Live + recently-disconnected entries are merged into a single sorted
    list, ordered by the raw ``datetime`` regardless of which bucket they
    came from.
    """
    from datetime import datetime, timedelta, timezone

    class _Conn:
        def __init__(self, remote: str, dt) -> None:
            self._remote = remote
            self._connected_at = dt

        def status(self):
            return {
                "kind": "intercept",
                "remote_addr": self._remote,
                "connected_at": self._connected_at.isoformat(),
            }

    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    older = _Conn("192.0.2.1", base)
    middle = _Conn("192.0.2.2", base + timedelta(seconds=1))
    newer = _Conn("192.0.2.3", base + timedelta(seconds=2))

    reg = SessionRegistry(retention_seconds=60)
    reg.add(older)
    reg.add(middle)
    reg.add(newer)
    # Disconnect the middle one so it moves to the recents bucket.
    reg.remove(middle)
    out = reg.snapshot()
    assert [s["remote_addr"] for s in out] == [
        "192.0.2.1",
        "192.0.2.2",
        "192.0.2.3",
    ]


def _make_session_for_id_translation():
    """Real Session built via the test factory. Tests reaching for the
    InflightTable's internals go through ``s._inflight_table.table`` etc.
    A capture-or-discard ``send_client`` stub keeps the translator from
    actually queueing frames; tests that want to see them re-assign
    ``s.send_client`` after this returns.
    """
    s = build_session_for_test()
    s.send_client = lambda _text: None  # type: ignore[method-assign,assignment]
    return s


def test_id_translation_rewrites_client_id_and_records_inflight():
    s = _make_session_for_id_translation()
    msg: dict[str, Any] = {
        "id": 7,
        "type": "call_service",
        "domain": "light",
        "service": "toggle",
    }
    s._translate_client_id_for_ha(msg)
    new_id = msg["id"]
    # The allocator hands out monotonic positive ints starting at 1.
    assert new_id >= 1
    assert s._inflight_table.table[new_id] == ClientReq(7)


def test_id_translation_is_monotonic_across_calls():
    s = _make_session_for_id_translation()
    msg1: dict[str, Any] = {"id": 5, "type": "ping"}
    msg2: dict[str, Any] = {"id": 6, "type": "ping"}
    s._translate_client_id_for_ha(msg1)
    s._translate_client_id_for_ha(msg2)
    assert msg2["id"] > msg1["id"]


def test_id_translation_rewrites_unsubscribe_subscription_field():
    s = _make_session_for_id_translation()
    # Client first subscribes (id=5) — that allocates an HA-side id and
    # records ClientReq(5) in the inflight table.
    sub_msg: dict[str, Any] = {
        "id": 5,
        "type": "subscribe_events",
        "event_type": "state_changed",
    }
    s._translate_client_id_for_ha(sub_msg)
    ha_sub_id = sub_msg["id"]
    # The reverse index must be populated so unsubscribe is O(1).
    assert s._inflight_table.client_to_ha[5] == ha_sub_id
    # Later the client unsubscribes referring to id=5; the proxy must
    # rewrite that reference to the HA-space id it forwarded with.
    unsub: dict[str, Any] = {"id": 6, "type": "unsubscribe_events", "subscription": 5}
    s._translate_client_id_for_ha(unsub)
    assert unsub["subscription"] == ha_sub_id
    assert unsub["id"] > ha_sub_id  # strictly increasing
    # Cleanup of the subscription's inflight is deferred until HA acks the
    # unsubscribe — so the subscribe entry's reverse-index slot still
    # resolves to ``ha_sub_id`` here. (See ``client_unsubscribe`` dispatch
    # for the cleanup path on result.)
    assert s._inflight_table.client_to_ha[5] == ha_sub_id
    # The unsubscribe command's own inflight entry is tagged so the
    # dispatcher knows which subscription to retire on result.
    assert s._inflight_table.table[unsub["id"]] == ClientUnsubscribe(6, ha_sub_id)


def test_translate_marks_whitelisted_subscription_known():
    """``subscribe_events`` is in ``HA_SUBSCRIPTION_COMMANDS``; the
    allocator id should be registered as a known subscription so the
    grace-period sweep never considers it.
    """
    s = _make_session_for_id_translation()
    msg = {"id": 7, "type": "subscribe_events", "event_type": "state_changed"}
    s._translate_client_id_for_ha(msg)
    assert msg["id"] in s._inflight_table.known_subscriptions


def test_translate_namespaced_whitelist_match():
    """Namespaced subscription names are looked up by exact match
    (no prefix heuristic), so a name HA actually uses
    (``frontend/subscribe_user_data``) is classified correctly.
    """
    s = _make_session_for_id_translation()
    msg = {"id": 8, "type": "frontend/subscribe_user_data", "key": "x"}
    s._translate_client_id_for_ha(msg)
    assert msg["id"] in s._inflight_table.known_subscriptions


def test_translate_unknown_command_not_marked_subscription():
    """Commands not in the whitelist start unclassified; the dispatch
    path decides their fate from the response shape.
    """
    s = _make_session_for_id_translation()
    msg = {"id": 9, "type": "get_states"}
    s._translate_client_id_for_ha(msg)
    assert msg["id"] not in s._inflight_table.known_subscriptions
    assert msg["id"] not in s._inflight_table.pop_at


def _make_session_for_sweep():
    """Real Session built via the test factory. Tests drive the
    eligibility logic against ``s._inflight_table`` directly without
    spinning up the periodic task or asyncio loop.
    """
    return build_session_for_test()


def test_sweep_drops_entries_whose_deadline_is_in_the_past():
    s = _make_session_for_sweep()
    s._inflight_table.table[10] = ClientReq(1)
    s._inflight_table.table[11] = ClientReq(2)
    s._inflight_table.pop_at[10] = 100.0  # expired
    s._inflight_table.pop_at[11] = 200.0  # still pending

    reclaimed = s._inflight_table.sweep(now=150.0)

    assert reclaimed == 1
    assert 10 not in s._inflight_table.table
    assert 10 not in s._inflight_table.pop_at
    assert 11 in s._inflight_table.table
    assert 11 in s._inflight_table.pop_at


def test_sweep_leaves_entries_without_a_deadline_alone():
    """Known subscriptions (whitelisted or event-promoted) have no
    pop_at entry, so the sweep must ignore them entirely — even when
    the table is otherwise empty.
    """
    s = _make_session_for_sweep()
    s._inflight_table.table[42] = ClientReq(7)  # no pop_at — known sub

    s._inflight_table.sweep(now=99_999.0)

    assert 42 in s._inflight_table.table  # kept


def test_sweep_with_no_expirations_is_a_no_op():
    s = _make_session_for_sweep()
    s._inflight_table.table[10] = ClientReq(1)
    s._inflight_table.pop_at[10] = 1000.0

    assert s._inflight_table.sweep(now=500.0) == 0
    assert 10 in s._inflight_table.table
    assert 10 in s._inflight_table.pop_at


def test_sweep_at_deadline_boundary_is_inclusive():
    """now == deadline should pop — otherwise a deadline of
    exactly-now would skate by indefinitely on a perfectly-timed
    sweep.
    """
    s = _make_session_for_sweep()
    s._inflight_table.table[10] = ClientReq(1)
    s._inflight_table.pop_at[10] = 500.0

    assert s._inflight_table.sweep(now=500.0) == 1
    assert 10 not in s._inflight_table.table


# ---- ClientReq dispatch-time classification --------------------------------
# These tests exercise ``Session._dispatch_inflight``'s ClientReq branch,
# which classifies an inbound HA frame for an unknown id: an ``event``
# promotes the entry to a known subscription (cancelling any pending
# sweep), while a ``result`` schedules a grace-period sweep. Once an id
# is known, classification is bypassed on later frames.


def test_dispatch_client_req_event_promotes_to_known_and_cancels_sweep():
    """An ``event`` frame for an unclassified ``ClientReq`` entry must
    promote the id to ``known_subscriptions`` and clear any pending
    sweep deadline — otherwise the sweep would later reclaim a live
    subscription.
    """
    s = _make_session_for_id_translation()
    s._inflight_table.insert(42, ClientReq(client_id=7))
    s._inflight_table.schedule_pop(42, deadline=999.0)

    s._dispatch_inflight(42, ClientReq(client_id=7), {"id": 42, "type": "event"})

    assert s._inflight_table.is_known(42)
    assert 42 not in s._inflight_table.pop_at


def test_dispatch_client_req_unknown_result_schedules_grace_sweep():
    """A ``result`` frame for an unclassified ``ClientReq`` entry must
    schedule a sweep at ``now + INFLIGHT_GRACE_SECONDS`` — that's the
    window the grace-sweep uses to forward late HA results.
    """
    import time

    from dashboard_entity_proxy.const import INFLIGHT_GRACE_SECONDS

    s = _make_session_for_id_translation()
    s._inflight_table.insert(42, ClientReq(client_id=7))
    before = time.monotonic()
    s._dispatch_inflight(42, ClientReq(client_id=7), {"id": 42, "type": "result"})
    after = time.monotonic()

    deadline = s._inflight_table.pop_at[42]
    assert before + INFLIGHT_GRACE_SECONDS <= deadline <= after + INFLIGHT_GRACE_SECONDS


def test_dispatch_client_req_known_entry_skips_classification():
    """Once an id is in ``known_subscriptions``, further frames just
    forward — no second classification pass that could reset pop_at.
    """
    s = _make_session_for_id_translation()
    s._inflight_table.insert(42, ClientReq(client_id=7))
    s._inflight_table.mark_known(42)

    s._dispatch_inflight(
        42, ClientReq(client_id=7), {"id": 42, "type": "event", "event": {}}
    )

    assert 42 not in s._inflight_table.pop_at


def test_unsubscribe_defers_classification_cleanup_until_ack():
    """``unsubscribe_events`` translation keeps the subscription's inflight
    entry and classification in place until HA acks the unsubscribe; an
    eager pop would silently drop any straggler events HA emits between
    the unsubscribe being forwarded and the ack landing.
    """
    s = _make_session_for_id_translation()
    sub: dict[str, Any] = {
        "id": 10,
        "type": "subscribe_events",
        "event_type": "state_changed",
    }
    s._translate_client_id_for_ha(sub)
    ha_sub = sub["id"]
    assert ha_sub in s._inflight_table.known_subscriptions

    unsub: dict[str, Any] = {"id": 11, "type": "unsubscribe_events", "subscription": 10}
    s._translate_client_id_for_ha(unsub)

    # Cleanup is deferred — the subscription's state survives translation.
    assert ha_sub in s._inflight_table.table
    assert ha_sub in s._inflight_table.known_subscriptions
    # The unsubscribe command itself is tagged so dispatch can retire the
    # subscription when the result frame arrives.
    assert s._inflight_table.table[unsub["id"]] == ClientUnsubscribe(11, ha_sub)


def test_handle_ha_drops_event_for_unknown_id_after_sweep():
    """If an HA-side message arrives for an integer id that has no
    inflight entry — because the grace-period sweep already reclaimed
    it — the proxy must DROP the message rather than forward it to the
    client. Forwarding would surface the proxy's allocator id to the
    client as "event for unknown subscription".
    """
    import logging

    s = _make_session_for_id_translation()
    s._to_client = type("Q", (), {"put_nowait": lambda self, *_: None})()
    sent: list[object] = []
    s.send_client = lambda data: sent.append(data)
    s._log = logging.getLogger("test")

    s._handle_ha({"id": 999, "type": "event", "event": {}})

    assert sent == [], "event for swept inflight id should be dropped"


def test_id_translation_leaves_non_int_id_alone():
    s = _make_session_for_id_translation()
    msg = {"type": "auth", "access_token": "x"}  # auth has no id
    s._translate_client_id_for_ha(msg)
    assert "id" not in msg
    assert s._inflight_table.table == {}


# ---- _scope_from_config branches -------------------------------------------


def _make_session_for_scope_from_config():
    """Real Session for tests that exercise ``scope_from_config`` against
    default scope filters.
    """
    return build_session_for_test()


def test_scope_from_config_fallback_on_unsuccessful_response():
    s = _make_session_for_scope_from_config()
    out = s._scope.scope_from_config({"success": False, "result": {"views": []}})
    assert out.all is True


def test_scope_from_config_fallback_when_result_is_not_a_dict():
    s = _make_session_for_scope_from_config()
    assert s._scope.scope_from_config({"success": True, "result": None}).all is True
    assert s._scope.scope_from_config({"success": True, "result": "string"}).all is True


def test_scope_from_config_fallback_for_strategy_dashboard():
    # ``strategy: {type: original-states}`` is a generated dashboard with no
    # explicit YAML cards; the walker can't extract anything from it.
    s = _make_session_for_scope_from_config()
    out = s._scope.scope_from_config(
        {
            "success": True,
            "result": {"strategy": {"type": "original-states"}},
        }
    )
    assert out.all is True


def test_scope_from_config_fallback_for_parseable_but_empty_dashboard():
    """A dashboard that yields zero entities from the walker (no cards
    reference anything, no patterns) widens to all rather than serving
    a blank snapshot."""
    s = _make_session_for_scope_from_config()
    out = s._scope.scope_from_config({"success": True, "result": {"views": [{}]}})
    assert out.all is True


def test_scope_from_config_returns_extracted_entities():
    s = _make_session_for_scope_from_config()
    out = s._scope.scope_from_config(
        {
            "success": True,
            "result": {
                "views": [
                    {"cards": [{"entity": "light.kitchen"}, {"entity": "sensor.temp"}]}
                ]
            },
        }
    )
    assert out.all is False
    # ``apply_filters`` also folds in the frontend baseline (zone.home),
    # so the result is a superset of the cards' direct references.
    assert {"light.kitchen", "sensor.temp"}.issubset(set(out.ids))


# ---- throttle buffer ------------------------------------------------------


def _make_session_for_throttle():
    """Real Session for the throttle-buffer drain tests; the scope starts
    out as None (all entities) so the drain's in-scope filter passes
    everything through unless a test narrows it.
    """
    return build_session_for_test()


def test_record_throttle_added_and_changed_become_dirty():
    s = _make_session_for_throttle()
    s._throttle_buffer.record(
        {
            "a": {"light.a": {"s": "on"}},
            "c": {"sensor.x": {"+": {"s": "21"}}},
        }
    )
    assert s._throttle_buffer.dirty == {"light.a", "sensor.x"}
    assert s._throttle_buffer.removed == set()


def test_record_throttle_removal_clears_dirty():
    """An entity that appeared earlier in the window and then was removed
    should ONLY emit a removal at flush time, not a stale add+remove.
    """
    s = _make_session_for_throttle()
    s._throttle_buffer.record({"a": {"light.a": {"s": "on"}}})
    s._throttle_buffer.record({"r": ["light.a"]})
    assert s._throttle_buffer.dirty == set()
    assert s._throttle_buffer.removed == {"light.a"}


def test_record_throttle_add_after_remove_clears_removed():
    """The mirror image: an entity removed then re-added within the
    window should net to a dirty (add) only.
    """
    s = _make_session_for_throttle()
    s._throttle_buffer.record({"r": ["light.a"]})
    s._throttle_buffer.record({"a": {"light.a": {"s": "on"}}})
    assert s._throttle_buffer.dirty == {"light.a"}
    assert s._throttle_buffer.removed == set()


def test_drain_throttle_filters_adds_but_not_removes_by_scope_set():
    """Adds are filtered against the current scope (don't add entities the
    client shouldn't see). Removes are NOT — if scope tightened mid-window,
    an entity that was previously in scope and is now not is exactly the
    entity the client needs the removal for. Filtering removes through the
    new scope would silently drop the removal and leave the entity stuck in
    the client's state until reconnect.
    """
    s = _make_session_for_throttle()
    s._throttle_buffer.dirty.update({"light.a", "light.b", "sensor.x"})
    s._throttle_buffer.removed.update({"light.c", "sensor.y"})
    s._scope.set = {"light.a", "light.c"}  # scope excludes light.b, sensor.*
    adds, removes = s._throttle_buffer.drain(
        lambda eid: s._scope.set is None or eid in s._scope.set
    )
    assert adds == ["light.a"]
    # Both removes survive the drain — including ``sensor.y`` which is no
    # longer in the (post-tightening) scope set.
    assert removes == ["light.c", "sensor.y"]
    # Buffers cleared even for ids that were filtered out, so the next
    # window starts fresh.
    assert s._throttle_buffer.dirty == set()
    assert s._throttle_buffer.removed == set()


def test_drain_throttle_with_scope_set_none_passes_all_through():
    s = _make_session_for_throttle()
    s._throttle_buffer.dirty.update({"light.a", "light.b"})
    s._throttle_buffer.removed.update({"sensor.x"})
    s._scope.set = None  # "all entities" — no filtering
    adds, removes = s._throttle_buffer.drain(
        lambda eid: s._scope.set is None or eid in s._scope.set
    )
    assert adds == ["light.a", "light.b"]
    assert removes == ["sensor.x"]


# ---- inflight cap ----------------------------------------------------------


def _wire_capture_send_client(s) -> list[str]:
    """Replace ``send_client`` with a list-capturing stub so the cap test
    can inspect the protocol-error frame without spinning up an asyncio
    writer.
    """
    sent: list[str] = []
    s.send_client = lambda data: sent.append(data)
    return sent


def test_handle_client_rejects_new_command_when_cap_is_hit(caplog):
    """At the cap, a brand-new client-kind command must be rejected by
    ``_handle_client`` before any navigation side-effect runs. The
    inflight table is unchanged, no allocator id is consumed, and the
    client gets a protocol-error result.
    """
    import logging

    from dashboard_entity_proxy.const import INFLIGHT_MAX

    s = _make_session_for_id_translation()
    sent = _wire_capture_send_client(s)
    for i in range(INFLIGHT_MAX):
        s._inflight_table.table[10_000_000 + i] = ClientReq(i)
    assert len(s._inflight_table.table) == INFLIGHT_MAX
    s.send_ha = lambda _t: None

    msg = {"id": 42, "type": "call_service", "domain": "light", "service": "toggle"}
    with caplog.at_level(logging.WARNING, logger=s._log.name):
        s._handle_client(msg)

    assert len(s._inflight_table.table) == INFLIGHT_MAX
    assert s._inflight_table.cap_hits == 1
    assert len(sent) == 1
    import json

    err = json.loads(sent[0])
    assert err["id"] == 42
    assert err["success"] is False
    assert err["error"]["code"] == "inflight_cap"
    assert any("inflight cap reached" in r.message for r in caplog.records)


def test_handle_client_cap_warning_logs_only_once(caplog):
    """Successive cap-hit rejections increment the counter every time
    but log the warning at most once per session, so operators don't
    see thousands of identical lines under attack.
    """
    import logging

    from dashboard_entity_proxy.const import INFLIGHT_MAX

    s = _make_session_for_id_translation()
    _wire_capture_send_client(s)
    for i in range(INFLIGHT_MAX):
        s._inflight_table.table[20_000_000 + i] = ClientReq(i)
    s.send_ha = lambda _t: None

    with caplog.at_level(logging.WARNING, logger=s._log.name):
        s._handle_client({"id": 50, "type": "x"})
        s._handle_client({"id": 51, "type": "x"})
        s._handle_client({"id": 52, "type": "x"})

    assert s._inflight_table.cap_hits == 3
    warnings = [r for r in caplog.records if "inflight cap reached" in r.message]
    assert len(warnings) == 1


def test_translate_under_cap_is_unaffected():
    """Below the cap, the translator's behavior is unchanged: id is
    rewritten, inflight entry is recorded, the function returns True so
    the caller forwards to HA.
    """
    from dashboard_entity_proxy.const import INFLIGHT_MAX

    s = _make_session_for_id_translation()
    _wire_capture_send_client(s)
    # One entry shy of the cap.
    for i in range(INFLIGHT_MAX - 1):
        s._inflight_table.table[30_000_000 + i] = ClientReq(i)

    msg = {"id": 7, "type": "call_service"}
    ok = s._translate_client_id_for_ha(msg)

    assert ok is True
    assert msg["id"] != 7  # translated
    assert s._inflight_table.cap_hits == 0
    assert len(s._inflight_table.table) == INFLIGHT_MAX


def test_handle_client_unsubscribe_at_cap_is_allowed():
    """``unsubscribe_events`` is exempt from the cap check: its ack
    retires both itself and the referenced subscription, so the net
    effect is to shrink the table. Blocking it at cap would leave a
    misbehaving session unable to recover.
    """
    from dashboard_entity_proxy.const import INFLIGHT_MAX

    s = _make_session_for_id_translation()
    sent = _wire_capture_send_client(s)
    # Pre-fill to the cap; include a subscription whose client id is 5
    # so the unsubscribe path can rewrite the subscription field.
    s._inflight_table.insert(40_000_001, ClientReq(5))
    for i in range(INFLIGHT_MAX - 1):
        s._inflight_table.table[40_000_002 + i] = ClientReq(100 + i)
    assert len(s._inflight_table.table) == INFLIGHT_MAX
    sent_to_ha: list[str] = []
    s.send_ha = lambda t: sent_to_ha.append(t)

    s._handle_client({"id": 99, "type": "unsubscribe_events", "subscription": 5})

    # The unsubscribe was admitted (not rejected as inflight_cap) and
    # forwarded to HA after rewriting ``subscription``.
    assert s._inflight_table.cap_hits == 0
    assert not any("inflight_cap" in s for s in sent)
    assert len(sent_to_ha) == 1
    import json

    forwarded = json.loads(sent_to_ha[0])
    assert forwarded["type"] == "unsubscribe_events"
    assert forwarded["subscription"] == 40_000_001


def test_on_scope_ready_lifts_phase_to_ready_from_both_pre_ready_states():
    """``_on_scope_ready`` lifts ``Phase.CONNECTING`` and ``Phase.STARTUP``
    to ``READY`` — the CONNECTING case covers the rare path where the
    scope-ready watchdog fires before HA's auth handshake completes.
    Without the CONNECTING handling, the status snapshot reports an
    impossible state (phase=connecting, scope_ready=true).
    """
    from dashboard_entity_proxy.phase import Phase

    # CONNECTING → READY (the recently-added path).
    s = build_session_for_test()
    s._phase = Phase.CONNECTING
    s._on_scope_ready()
    assert s._phase is Phase.READY

    # STARTUP → READY (the normal path).
    s = build_session_for_test()
    s._phase = Phase.STARTUP
    s._on_scope_ready()
    assert s._phase is Phase.READY


def test_on_scope_ready_does_not_lift_phase_when_already_closing():
    """``_on_scope_ready`` must not regress the phase to READY after
    cleanup has begun (``_cleanup`` sets ``CLOSING``). A late scope
    transition during teardown is harmless to ignore.
    """
    from dashboard_entity_proxy.phase import Phase

    s = build_session_for_test()
    s._phase = Phase.CLOSING
    s._on_scope_ready()
    assert s._phase is Phase.CLOSING


def test_subscribe_entities_ignores_client_supplied_entity_ids_filter():
    """SECURITY: a client ``subscribe_entities`` request that carries a
    custom ``entity_ids`` filter must be IGNORED. Honouring the filter
    would let the frontend escape the proxy's scope by passing its own
    entity list. The proxy decides scope from the dashboard config and
    customization, not from the client's subscribe message.
    """
    s = build_session_for_test()
    s._mirror_ready = True
    s._scope.ready = True
    s._scope.set = {"sensor.in_scope"}
    s._scope.ids = ["sensor.in_scope"]
    s._store.apply({"a": {"sensor.in_scope": {"s": "1"}, "light.out": {"s": "on"}}})
    sent = _wire_capture_send_client(s)

    # Client tries to subscribe with a filter that would expand scope.
    s._handle_client(
        {
            "id": 42,
            "type": "subscribe_entities",
            "entity_ids": ["light.out", "light.also_out_of_scope"],
        }
    )

    # The proxy ignores the filter and serves the proxy-decided scope.
    # We expect: ack + initial snapshot event. The snapshot must only
    # contain in-scope ids.
    import json

    payloads = [json.loads(s) for s in sent]
    # Find the event frame (the snapshot).
    events = [p for p in payloads if p.get("type") == "event"]
    assert len(events) == 1
    snap = events[0]["event"]["a"]
    assert set(snap) == {"sensor.in_scope"}
    # The light.out the client asked for must NOT appear.
    assert "light.out" not in snap


def test_handle_client_unsubscribe_unknown_subscription_is_rejected_locally():
    """A client ``unsubscribe_events`` whose ``subscription`` field
    references an id the proxy never allocated must NOT be forwarded
    to HA. Forwarding would send an untranslated client-space id into
    HA's id space, where it could collide with a proxy-owned
    subscription (e.g., the mirror at id 1) and silently retire the
    wrong stream. The proxy returns ``not_found`` to the client and
    leaves the inflight table unchanged.
    """
    s = _make_session_for_id_translation()
    sent = _wire_capture_send_client(s)
    sent_to_ha: list[str] = []
    s.send_ha = lambda t: sent_to_ha.append(t)
    inflight_before = len(s._inflight_table.table)

    # Sub id 7777 was never allocated by the proxy.
    s._handle_client({"id": 99, "type": "unsubscribe_events", "subscription": 7777})

    # Nothing forwarded to HA.
    assert sent_to_ha == []
    # Client got a not_found error.
    assert len(sent) == 1
    import json

    err = json.loads(sent[0])
    assert err["id"] == 99
    assert err["success"] is False
    assert err["error"]["code"] == "not_found"
    # The just-inserted ClientReq was popped, so the table is unchanged.
    assert len(s._inflight_table.table) == inflight_before


def test_session_status_exposes_inflight_fields():
    """The status snapshot consumed by the Ingress UI must include the
    inflight count and cap-hit counter so operators can spot a runaway
    client without attaching a debugger.
    """
    s = build_session_for_test()
    s._scope.ready = True
    s._inflight_table.table.update({1: Mirror(), 2: ClientReq(10), 3: ClientReq(11)})
    s._inflight_table.cap_hits = 5

    snap = s.status()
    assert snap["inflight_count"] == 3
    assert snap["inflight_cap_hits"] == 5


# ---- A1: deferred unsubscribe cleanup on ack -------------------------------


def test_client_unsubscribe_success_ack_cleans_subscription():
    """When HA replies ``success: true`` to a client unsubscribe, the
    dispatcher must retire the referenced subscription's inflight entry
    AND classification state (now, not at translation time), and forward
    the result to the client rewritten back to the client's id.
    """
    from dashboard_entity_proxy.session import Session

    s = _make_session_for_id_translation()
    sent: list[str] = []
    s.send_client = lambda data: sent.append(data)

    # Subscribe first so a subscription inflight + classification exists.
    sub: dict[str, Any] = {
        "id": 10,
        "type": "subscribe_events",
        "event_type": "state_changed",
    }
    s._translate_client_id_for_ha(sub)
    ha_sub = sub["id"]
    assert ha_sub in s._inflight_table.known_subscriptions

    # Then unsubscribe.
    unsub: dict[str, Any] = {"id": 11, "type": "unsubscribe_events", "subscription": 10}
    s._translate_client_id_for_ha(unsub)
    cmd_id = unsub["id"]
    assert s._inflight_table.table[cmd_id] == ClientUnsubscribe(11, ha_sub)

    # Simulate HA's success ack landing.
    Session._handle_ha(
        s,
        {"id": cmd_id, "type": "result", "success": True, "result": None},
    )

    # Subscription state is retired now.
    assert ha_sub not in s._inflight_table.table
    assert ha_sub not in s._inflight_table.known_subscriptions
    assert ha_sub not in s._inflight_table.pop_at
    # The unsubscribe command itself is also popped.
    assert cmd_id not in s._inflight_table.table
    # Client got the success result rewritten back to id=11.
    import json

    payload = json.loads(sent[-1])
    assert payload["id"] == 11
    assert payload["success"] is True


def test_client_unsubscribe_failure_ack_logs_and_still_cleans(caplog):
    """A ``success: false`` ack on the client's unsubscribe (HA already
    dropped the subscription, race) must still retire the proxy's state
    — the client believes the subscription is gone, so leaving the
    inflight entry routing future events would be wrong. The error code
    is logged at WARNING so upstream churn is visible.
    """
    import logging

    from dashboard_entity_proxy.session import Session

    s = _make_session_for_id_translation()
    sent: list[str] = []
    s.send_client = lambda data: sent.append(data)

    sub = {"id": 20, "type": "subscribe_events", "event_type": "state_changed"}
    s._translate_client_id_for_ha(sub)
    ha_sub = sub["id"]

    unsub = {"id": 21, "type": "unsubscribe_events", "subscription": 20}
    s._translate_client_id_for_ha(unsub)
    cmd_id = unsub["id"]

    with caplog.at_level(logging.WARNING, logger=s._log.name):
        Session._handle_ha(
            s,
            {
                "id": cmd_id,
                "type": "result",
                "success": False,
                "error": {"code": "not_found", "message": "Subscription not found."},
            },
        )

    # State retired regardless of failure.
    assert ha_sub not in s._inflight_table.table
    assert ha_sub not in s._inflight_table.known_subscriptions
    assert cmd_id not in s._inflight_table.table
    # Warning surfaced the code so operators see the upstream race.
    assert any(
        "client unsubscribe failed" in r.message and "not_found" in r.message
        for r in caplog.records
    )


# ---- A2: watchdog-widening suppresses late-resolve remove burst ------------


def _make_session_for_apply_scope():
    """Real Session seeded with a few mirror ids so the normal-diff
    branch in ``ScopeResolver.apply`` can iterate over them. One fake
    subscription is registered so the diff path actually emits.
    """
    s = build_session_for_test()
    s._store.apply(
        {
            "a": {
                "light.a": {"s": "on"},
                "light.b": {"s": "off"},
                "sensor.x": {"s": "1"},
            }
        }
    )
    s._subs.add_live(99)
    return s


def test_apply_scope_suppresses_remove_burst_after_watchdog_widening():
    """When the watchdog widened scope to all entities, a subsequent narrow
    ``_apply_scope`` must NOT emit a multi-thousand-id remove frame. The
    client keeps the wider view; the next navigation diffs normally.
    """
    from dashboard_entity_proxy.session import ScopeSet

    s = _make_session_for_apply_scope()
    # Simulate the watchdog firing: scope widened to all entities,
    # snapshot already served to the client.
    s._scope.widened_by_watchdog = True
    sent_adds: list[list[str]] = []
    sent_removes: list[list[str]] = []
    s._scope._emit_add = lambda ids: sent_adds.append(list(ids))
    s._scope._emit_remove = lambda ids: sent_removes.append(list(ids))

    # Late narrow resolve arrives: only light.a is in scope.
    s._scope.apply(ScopeSet(ids=["light.a"]))

    # No removes — that's the whole point.
    assert sent_removes == []
    # No adds either — the client already has light.a from the wide snapshot.
    assert sent_adds == []
    # Flag cleared so subsequent navigations diff normally.
    assert s._scope.widened_by_watchdog is False
    # New scope is recorded for future diffs. apply() re-applies the user's
    # filters and baseline, so the FRONTEND_BASELINE ids appear alongside
    # the resolver's chosen id.
    assert s._scope.set is not None
    assert "light.a" in s._scope.set


def test_apply_scope_intervening_all_entities_preserves_watchdog_flag():
    """The watchdog-widened flag must NOT be consumed by an intervening
    all-entities ``apply()`` (e.g., a heuristic widen, or the registry
    fallback that returns ``ScopeSet(all=True)``). If it were, the
    next narrow apply would emit the full remove-burst the flag was
    designed to suppress. Regression test for the premature-consumption
    fix.
    """
    from dashboard_entity_proxy.session import ScopeSet

    s = _make_session_for_apply_scope()
    s._scope.widened_by_watchdog = True
    sent_removes: list[list[str]] = []
    s._scope._emit_remove = lambda ids: sent_removes.append(list(ids))
    s._scope._emit_add = lambda _ids: None

    # Intervening all-entities apply: scope stays widened, flag should
    # NOT clear because no narrowing actually happened.
    s._scope.apply(ScopeSet(all=True))
    assert s._scope.widened_by_watchdog is True

    # Now the narrow apply lands. The flag protects the diff path so
    # no removes fire.
    s._scope.apply(ScopeSet(ids=["light.a"]))
    assert sent_removes == []
    # And NOW the flag clears, since the narrow apply consumed it.
    assert s._scope.widened_by_watchdog is False


def test_apply_scope_diffs_normally_when_flag_not_set():
    """Sanity: without the watchdog-widening flag, ``_apply_scope`` runs
    the normal diff and emits removes for ids dropped from scope.
    """
    from dashboard_entity_proxy.session import ScopeSet

    s = _make_session_for_apply_scope()
    # Pretend the previous scope was {light.a, light.b}.
    s._scope.set = {"light.a", "light.b"}
    sent_adds: list[list[str]] = []
    sent_removes: list[list[str]] = []
    s._scope._emit_add = lambda ids: sent_adds.append(list(ids))
    s._scope._emit_remove = lambda ids: sent_removes.append(list(ids))

    # Narrow to {light.a, sensor.x}: light.b drops, sensor.x added.
    s._scope.apply(ScopeSet(ids=["light.a", "sensor.x"]))

    assert sent_removes == [["light.b"]]
    assert sent_adds == [["sensor.x"]]


def test_late_ha_result_after_sweep_is_forwarded_not_dropped():
    """When the inflight sweep reclaims a ``ClientReq(N)`` entry whose
    HA response hadn't arrived yet, a late ``result`` frame still needs
    to reach the client — otherwise the client hangs on its original id.
    The proxy remembers swept entries in a bounded ring and uses that
    to route the late ack.
    """
    import logging

    from dashboard_entity_proxy.session import Session

    s = _make_session_for_sweep()
    # Standard handle_ha needs these too.
    s._log = logging.getLogger("test_dep_session_unit.late_ack")
    sent: list[str] = []
    s.send_client = lambda text: sent.append(text)

    # Set up a swept client entry: ha_id 42 was the proxy's allocation
    # for client id 7. Sweep reclaims it.
    s._inflight_table.table[42] = ClientReq(7)
    s._inflight_table.pop_at[42] = 100.0  # expired
    s._inflight_table.client_to_ha[7] = 42
    s._inflight_table.sweep(now=150.0)

    # Inflight is empty, but the swept ring remembers the mapping.
    assert s._inflight_table.table == {}
    assert s._inflight_table.swept_to_client_id == {42: 7}

    # Late HA result arrives.
    Session._handle_ha(
        s,
        {
            "id": 42,
            "type": "result",
            "success": True,
            "result": {"value": "late"},
        },
    )

    # The client received a frame with its original id 7, not the proxy
    # allocator id 42.
    assert len(sent) == 1
    import json

    parsed = json.loads(sent[0])
    assert parsed["id"] == 7
    assert parsed["result"] == {"value": "late"}
    # The recall entry is consumed (one late ack per swept id).
    assert 42 not in s._inflight_table.swept_to_client_id


def test_swept_recall_ring_is_bounded():
    """The swept-id recall ring must not grow unbounded — sweep events
    on a busy session would otherwise accumulate one entry per swept
    one-shot for the life of the connection.
    """
    from dashboard_entity_proxy.const import INFLIGHT_SWEPT_RECALL_MAX

    s = _make_session_for_sweep()
    for i in range(INFLIGHT_SWEPT_RECALL_MAX + 50):
        s._inflight_table.record_swept_client_id(ha_id=10_000 + i, client_id=i)
    assert len(s._inflight_table.swept_to_client_id) == INFLIGHT_SWEPT_RECALL_MAX
    # Oldest entries evicted; newest retained.
    assert 10_000 not in s._inflight_table.swept_to_client_id
    assert (
        10_000 + INFLIGHT_SWEPT_RECALL_MAX + 49
    ) in s._inflight_table.swept_to_client_id


def test_lovelace_updated_out_of_order_responses_apply_latest_only():
    """Two ``lovelace_updated`` events in quick succession can leave two
    ``_inject_config_fetch`` calls inflight. Out-of-order responses must
    not let the older fetch overwrite the cache with stale data.
    """
    from dashboard_entity_proxy.session import ScopeSet

    s = build_session_for_test()

    resolved: list[ScopeSet | None] = []

    def _capture(url, mid, entry):
        # Use the real latest-fetch guard so the test verifies that
        # gate lives in one place (inside cache_and_reapply) rather
        # than ignoring it via the stub.
        if entry is None or not s._dashboard_cache.is_latest_fetch(url, mid):
            return
        resolved.append(entry)
        s._dashboard_cache.scopes[url] = entry

    s._scope.cache_and_reapply = _capture  # type: ignore[method-assign]
    s._scope.scope_from_config = lambda msg: ScopeSet(  # type: ignore[method-assign]
        ids=msg["result"]["views"][0]["badges"]
    )

    # Two inflight config fetches for the same dashboard, ids 11 and 17.
    s._inflight_table.table[11] = ConfigFetch("my-dashboard")
    s._inflight_table.table[17] = ConfigFetch("my-dashboard")
    s._dashboard_cache.latest_fetch_id["my-dashboard"] = 17

    # Older response arrives first — must be dropped.
    s._dispatch_inflight(
        11,
        ConfigFetch("my-dashboard"),
        {
            "id": 11,
            "type": "result",
            "success": True,
            "result": {"views": [{"badges": ["sensor.old"]}]},
        },
    )
    assert resolved == [], "stale fetch result must not be applied"

    # Newer response arrives — applied.
    s._dispatch_inflight(
        17,
        ConfigFetch("my-dashboard"),
        {
            "id": 17,
            "type": "result",
            "success": True,
            "result": {"views": [{"badges": ["sensor.new"]}]},
        },
    )
    assert resolved == [ScopeSet(ids=["sensor.new"])]
    assert s._dashboard_cache.scopes["my-dashboard"] == ScopeSet(ids=["sensor.new"])


async def test_scope_watchdog_no_op_if_done_set_during_timeout(monkeypatch):
    """If the session closes during the same event-loop tick the watchdog
    times out on, the watchdog must NOT invoke its widen action —
    emitting frames to a closing peer would error inside the writer.

    Constructs the watchdog directly with fake callbacks; the test
    doesn't need a Session at all.
    """
    import asyncio
    import logging

    from dashboard_entity_proxy.scope_watchdog import ScopeWatchdog

    done = asyncio.Event()
    done.set()
    widen_calls = 0

    def _widen() -> None:
        nonlocal widen_calls
        widen_calls += 1

    async def _immediate_timeout(coro, timeout):
        coro.close()
        raise asyncio.TimeoutError

    monkeypatch.setattr(asyncio, "wait_for", _immediate_timeout)

    watchdog = ScopeWatchdog(
        done=done,
        timeout=1.0,
        log=logging.getLogger("test_dep_session_unit.scope_watchdog"),
        is_scope_ready=lambda: False,
        widen_to_all=_widen,
    )
    await watchdog.run()

    assert widen_calls == 0


async def test_scope_watchdog_widens_when_not_ready_and_done_unset(monkeypatch):
    """Inverse of the closing-session case: timeout fires, ``done`` is
    still unset, ``is_scope_ready`` returns False — widen is invoked.
    """
    import asyncio
    import logging

    from dashboard_entity_proxy.scope_watchdog import ScopeWatchdog

    done = asyncio.Event()
    widen_calls = 0

    def _widen() -> None:
        nonlocal widen_calls
        widen_calls += 1

    async def _immediate_timeout(coro, timeout):
        coro.close()
        raise asyncio.TimeoutError

    monkeypatch.setattr(asyncio, "wait_for", _immediate_timeout)

    watchdog = ScopeWatchdog(
        done=done,
        timeout=1.0,
        log=logging.getLogger("test_dep_session_unit.scope_watchdog"),
        is_scope_ready=lambda: False,
        widen_to_all=_widen,
    )
    await watchdog.run()

    assert widen_calls == 1


def test_apply_scope_always_includes_calendar_and_todo_domains():
    """``ALWAYS_IN_SCOPE_DOMAINS`` entities (``calendar.*``, ``todo.*``)
    must land in every resolved scope so HA's system panels for those
    domains render on first navigation without a reload.
    """
    from dashboard_entity_proxy.session import ScopeSet

    s = _make_session_for_apply_scope()
    # Seed mirror with a calendar and a todo on top of the existing ids.
    s._store.apply(
        {
            "a": {
                "calendar.work": {"s": "off"},
                "todo.shopping": {"s": "ok"},
            }
        }
    )
    sent_adds: list[list[str]] = []
    sent_removes: list[list[str]] = []
    s._scope._emit_add = lambda ids: sent_adds.append(list(ids))
    s._scope._emit_remove = lambda ids: sent_removes.append(list(ids))

    # Narrow scope explicitly asks for only ``light.a``; the
    # always-in-scope domains should still appear in the resolved set.
    s._scope.set = set()
    s._scope.apply(ScopeSet(ids=["light.a"]))

    assert s._scope.set is not None
    assert "calendar.work" in s._scope.set
    assert "todo.shopping" in s._scope.set
    assert "light.a" in s._scope.set


def test_fold_in_pattern_matches_picks_up_new_calendar_entity():
    """A brand-new ``calendar.*`` entity arriving in the mirror after the
    scope was resolved must fold into the live scope so it appears in
    ``hass.states`` immediately — without waiting for a navigation or
    reload to refresh the scope.
    """
    s = _make_session_for_apply_scope()
    s._scope.set = {"light.a"}
    s._scope.ids = ["light.a"]
    s._scope.entry = None  # no pattern rules — purely narrow scope

    # Simulate a mirror event that introduces a fresh calendar entity.
    s._scope.fold_in_pattern_matches({"a": {"calendar.newly_added": {"s": "off"}}})

    assert "calendar.newly_added" in s._scope.set
    assert "calendar.newly_added" in s._scope.ids


# ---- A3: cleanup drain timeout ---------------------------------------------


def test_cleanup_drain_timeout_constant_is_short_positive():
    """``CLEANUP_DRAIN_TIMEOUT`` must be small enough to bound teardown
    latency under a stuck peer but non-zero so writers get a real chance
    to flush their current send.
    """
    from dashboard_entity_proxy.const import CLEANUP_DRAIN_TIMEOUT

    assert 0.0 < CLEANUP_DRAIN_TIMEOUT <= 2.0


# ---- A7: permission-denied lovelace/config does not widen ------------------


def test_scope_from_config_unauthorized_does_not_widen(caplog):
    """``success: false`` with a permission-denial code (e.g.
    ``unauthorized``) must NOT widen scope — widening on denial would
    invert the proxy's "tighter than HA" contract. The function returns
    None so the caller (``_resolve_scope``) leaves the cache untouched.
    """
    import logging

    s = _make_session_for_scope_from_config()
    with caplog.at_level(logging.WARNING, logger=s._log.name):
        out = s._scope.scope_from_config(
            {
                "success": False,
                "error": {"code": "unauthorized", "message": "denied"},
            }
        )
    assert out is None
    assert any(
        "lovelace/config denied" in r.message and "unauthorized" in r.message
        for r in caplog.records
    )


def test_scope_from_config_forbidden_does_not_widen():
    """Same as unauthorized: ``forbidden`` is a permission denial."""
    s = _make_session_for_scope_from_config()
    out = s._scope.scope_from_config(
        {"success": False, "error": {"code": "forbidden", "message": "x"}}
    )
    assert out is None


def test_scope_from_config_not_found_widens_with_warning(caplog):
    """``not_found`` is a "dashboard doesn't exist" condition — the
    previous behaviour (widen to all entities, log a warning) is
    preserved.
    """
    import logging

    s = _make_session_for_scope_from_config()
    with caplog.at_level(logging.WARNING, logger=s._log.name):
        out = s._scope.scope_from_config(
            {
                "success": False,
                "error": {"code": "not_found", "message": "no such dashboard"},
            }
        )
    assert out is not None and out.all is True
    assert any("serving all entities" in r.message for r in caplog.records)


def test_resolve_scope_does_not_overwrite_cache_on_denial():
    """``cache_and_reapply`` called with ``entry=None`` (permission denial)
    must leave the existing cached scope intact.
    """
    from dashboard_entity_proxy.session import ScopeSet

    s = build_session_for_test()
    s._dashboard_cache.set("lovelace", ScopeSet(ids=["light.a"]))
    # Stub the resolver's resolve_current_view so the test can detect
    # whether the denial branch (incorrectly) re-resolves.
    called: list[bool] = []
    s._scope.resolve_current_view = lambda: called.append(True)  # type: ignore[method-assign]

    s._scope.cache_and_reapply("lovelace", 1, None)

    assert s._dashboard_cache.scopes["lovelace"].ids == ["light.a"]
    assert called == []


# ---- A8: lovelace_updated invalidates the dashboard cache ------------------


def _make_session_for_lovelace_updated():
    """Real Session plus a helper that drives
    ``DashboardCache.handle_lovelace_updated`` with an inject-fetch
    capturing list. Tests assert on ``s._inject_calls`` to verify
    whether the current-view refetch path fired.
    """
    s = build_session_for_test()
    inject_calls: list[str] = []
    s._inject_calls = inject_calls  # type: ignore[attr-defined]

    def _do(msg):
        s._dashboard_cache.handle_lovelace_updated(
            msg,
            log=s._log,
            current_view=s._nav.current_view,
            inject_config_fetch=inject_calls.append,
        )

    s._do_lovelace_updated = _do  # type: ignore[attr-defined]
    return s


def test_lovelace_updated_drops_cached_entry():
    """A ``lovelace_updated`` event for a cached url_path must drop the
    cache entry so the next navigation re-fetches.
    """
    from dashboard_entity_proxy.session import ScopeSet

    s = _make_session_for_lovelace_updated()
    s._dashboard_cache.scopes["my-dash"] = ScopeSet(ids=["light.a"])
    s._dashboard_cache.scopes["other"] = ScopeSet(ids=["light.b"])

    s._do_lovelace_updated(
        {
            "type": "event",
            "event": {
                "event_type": "lovelace_updated",
                "data": {"url_path": "my-dash"},
            },
        }
    )

    assert "my-dash" not in s._dashboard_cache.scopes
    # Unrelated entries untouched.
    assert "other" in s._dashboard_cache.scopes


def test_lovelace_updated_for_default_dashboard_keys_on_empty_string():
    """HA emits ``url_path: None`` for the default lovelace dashboard;
    the proxy caches that under the empty-string key, so a None event
    must invalidate the empty-string entry.
    """
    from dashboard_entity_proxy.session import ScopeSet

    s = _make_session_for_lovelace_updated()
    s._dashboard_cache.scopes[""] = ScopeSet(ids=["light.a"])

    s._do_lovelace_updated(
        {
            "type": "event",
            "event": {
                "event_type": "lovelace_updated",
                "data": {"url_path": None},
            },
        }
    )

    assert "" not in s._dashboard_cache.scopes


def test_lovelace_updated_for_current_view_triggers_refetch():
    """If the user is currently viewing the dashboard that was edited,
    the handler should issue an immediate config fetch so scope
    re-resolves now rather than only on the next navigation.
    """
    from dashboard_entity_proxy.navigation import View, ViewKind
    from dashboard_entity_proxy.session import ScopeSet

    s = _make_session_for_lovelace_updated()
    s._nav.current_view = View(ViewKind.DASHBOARD, "my-dash")
    s._dashboard_cache.scopes["my-dash"] = ScopeSet(ids=["light.a"])

    s._do_lovelace_updated(
        {
            "type": "event",
            "event": {
                "event_type": "lovelace_updated",
                "data": {"url_path": "my-dash"},
            },
        }
    )

    assert s._inject_calls == ["my-dash"]


def test_lovelace_updated_for_other_view_does_not_refetch():
    """If the edited dashboard isn't the current view, the cache drop
    is enough — no eager refetch (the next navigation will fetch).
    """
    from dashboard_entity_proxy.navigation import View, ViewKind
    from dashboard_entity_proxy.session import ScopeSet

    s = _make_session_for_lovelace_updated()
    s._nav.current_view = View(ViewKind.DASHBOARD, "other")
    s._dashboard_cache.scopes["my-dash"] = ScopeSet(ids=["light.a"])

    s._do_lovelace_updated(
        {
            "type": "event",
            "event": {
                "event_type": "lovelace_updated",
                "data": {"url_path": "my-dash"},
            },
        }
    )

    assert s._inject_calls == []


def test_lovelace_updated_subscription_failure_logs_not_fatal(caplog):
    """A ``success: false`` on the subscription itself is logged but
    NOT treated as fatal — missing the auto-invalidation hook is an
    annoyance, not a correctness break.
    """
    import logging

    s = _make_session_for_lovelace_updated()
    with caplog.at_level(logging.WARNING, logger=s._log.name):
        s._do_lovelace_updated(
            {"type": "result", "success": False, "error": {"code": "x"}}
        )
    assert any(
        "lovelace_updated subscription failed" in r.message for r in caplog.records
    )


# ---- A9: O(1) registry drops via dict-keyed raw caches ---------------------


def _make_session_for_registry_drops():
    """Real Session for the registry drop / merge tests. The
    ``ScopeResolver.resolve_current_view`` hook is stubbed to a no-op so
    individual tests can replace it to observe rebuild-triggered
    re-resolves (RegistryStore was wired to call through ``s._scope``
    by Session's ``__init__``).
    """
    s = build_session_for_test()
    s._scope.resolve_current_view = lambda: None  # type: ignore[method-assign]
    return s


def test_result_index_keys_by_field_and_skips_invalid_rows():
    """``result_index`` folds rows into a dict keyed by ``row[key]`` and
    silently skips rows whose key is missing or not a string — matches
    the registry builder's tolerance for malformed rows.
    """
    from dashboard_entity_proxy._msg_utils import result_index

    msg = {
        "success": True,
        "result": [
            {"entity_id": "light.a", "x": 1},
            {"entity_id": "light.b", "x": 2},
            {"x": 3},  # no entity_id — skipped
            {"entity_id": 42, "x": 4},  # non-string id — skipped
        ],
    }
    idx = result_index(msg, "entity_id")
    assert set(idx) == {"light.a", "light.b"}
    assert idx["light.a"]["x"] == 1


def test_rebuild_index_consumes_dict_values():
    """The index rebuild must work against the new dict-keyed raw
    stores: it passes ``.values()`` into ``entity_index.build`` so lookups
    return the same results as the old list-keyed code.
    """
    from dashboard_entity_proxy import entity_index

    s = _make_session_for_registry_drops()
    s._registry_store.have_entities = True
    s._registry_store.have_devices = True
    s._registry_store.raw_entities = {
        "light.kitchen": {
            "entity_id": "light.kitchen",
            "device_id": "dev1",
            "platform": "hue",
            "area_id": "kitchen",
        },
    }
    s._registry_store.raw_devices = {"dev1": {"id": "dev1", "area_id": "kitchen"}}

    s._registry_store.rebuild_index_if_ready()

    assert isinstance(s._registry_store.index, entity_index.EntityIndex)
    assert s._registry_store.index.device("dev1") == ["light.kitchen"]
    assert s._registry_store.index.integration("hue") == ["light.kitchen"]
    assert s._registry_store.index.area("kitchen") == ["light.kitchen"]


def test_incremental_remove_tombstones_inflight_entity_get():
    """Race: a create event triggers a per-entity get; before the get
    response lands, a remove event arrives for the same entity. The
    remove-flush must tombstone the in-flight get so its response does
    NOT resurrect the just-removed row.
    """
    s = _make_session_for_registry_drops()
    s._registry_store.mode = "incremental"
    s._registry_store.burst_threshold = 50
    s._registry_store.pending_entity_gets = {"light.x"}  # get for light.x is in flight
    # Simulate the remove-flush: pending now carries a remove for light.x.
    s._registry_store.pending_entity_events = {"light.x": "remove"}
    s._registry_store.flush_pending_entities()
    # Tombstone recorded: the eid is gone from the pending-gets set.
    assert "light.x" not in s._registry_store.pending_entity_gets
    # Now the late get response arrives. With the fix, the merge is
    # skipped and _raw_entities stays empty.
    s._registry_store.handle_entity_get(
        "light.x", {"success": True, "result": {"entity_id": "light.x"}}
    )
    assert "light.x" not in s._registry_store.raw_entities


def test_incremental_handle_entity_get_normal_path_merges_and_rebuilds():
    """Sanity: when no remove tombstone is pending, ``_handle_entity_get``
    merges the row and (when the pending-gets set drains) triggers an
    index rebuild. Companion to the tombstone test above so a future
    change to the tombstone logic can't silently break the happy path.
    """
    s = _make_session_for_registry_drops()
    s._registry_store.have_entities = True
    s._registry_store.have_devices = True
    s._registry_store.raw_devices = {}
    s._registry_store.pending_entity_gets = {"light.y"}
    rebuilt: list[bool] = []
    s._scope.resolve_current_view = lambda: rebuilt.append(True)
    s._registry_store.handle_entity_get(
        "light.y", {"success": True, "result": {"entity_id": "light.y"}}
    )
    assert "light.y" in s._registry_store.raw_entities
    assert s._registry_store.pending_entity_gets == set()
    assert rebuilt == [True]  # rebuild fired since pending-gets drained


# ---- B5: id_reuse rejection logging + counter -----------------------------


def _make_session_for_check_client_id():
    """Real Session for ``_check_client_id``; pre-sets the last-seen
    client id to 5 (so a test sending id=3 trips the rejection path) and
    captures the protocol-error frame the rejector emits.
    """
    s = build_session_for_test()
    s._last_client_id = 5
    sent: list[str] = []
    s._sent = sent  # type: ignore[attr-defined]
    s.send_client = lambda data: sent.append(data)  # type: ignore[method-assign,assignment]
    return s


def test_check_client_id_first_reuse_logs_info_and_counts(caplog):
    """First id-reuse rejection per session logs at INFO with the offending
    id; the counter ticks up; the protocol-error frame is still sent.
    """
    import logging

    s = _make_session_for_check_client_id()
    with caplog.at_level(logging.INFO, logger=s._log.name):
        ok = s._check_client_id({"id": 3, "type": "x"})
    assert ok is False
    assert s._id_reuse_rejections == 1
    assert s._id_reuse_logged is True
    # The INFO message names the offending id so an operator can spot the
    # misbehaving client.
    assert any(
        rec.levelno == logging.INFO
        and "id=3" in rec.message
        and "client id reuse" in rec.message
        for rec in caplog.records
    )
    # The client got exactly one error frame.
    assert len(s._sent) == 1


def test_check_client_id_repeat_rejection_logs_debug_only(caplog):
    """Subsequent rejections log at DEBUG only — operators get one INFO
    line per session, not one per offending message.
    """
    import logging

    s = _make_session_for_check_client_id()
    # Prime the once-flag so the first INFO is already spent.
    s._id_reuse_logged = True
    s._id_reuse_rejections = 1

    with caplog.at_level(logging.DEBUG, logger=s._log.name):
        s._check_client_id({"id": 3, "type": "x"})
        s._check_client_id({"id": 4, "type": "x"})

    assert s._id_reuse_rejections == 3
    # Zero INFO records about id-reuse — only DEBUG.
    info_lines = [
        r
        for r in caplog.records
        if r.levelno == logging.INFO and "client id reuse" in r.message
    ]
    assert info_lines == []
    debug_lines = [
        r
        for r in caplog.records
        if r.levelno == logging.DEBUG and "client id reuse" in r.message
    ]
    assert len(debug_lines) == 2


def test_check_client_id_increasing_id_passes_without_logging(caplog):
    """A properly-increasing client id passes through silently."""
    import logging

    s = _make_session_for_check_client_id()
    with caplog.at_level(logging.DEBUG, logger=s._log.name):
        ok = s._check_client_id({"id": 9, "type": "x"})
    assert ok is True
    assert s._id_reuse_rejections == 0
    assert s._sent == []
    assert s._last_client_id == 9


# ---- B7: split inflight insert vs retag -----------------------------------


def _make_session_for_inflight_mutators():
    """Real Session for testing ``InflightTable.insert`` / ``retag`` and
    their effects on the parallel reverse index.
    """
    return build_session_for_test()


def test_inflight_insert_records_new_entry_and_reverse_index():
    s = _make_session_for_inflight_mutators()
    s._inflight_table.insert(100, ClientReq(7))
    assert s._inflight_table.table[100] == ClientReq(7)
    assert s._inflight_table.client_to_ha[7] == 100


def test_inflight_insert_asserts_on_duplicate_id():
    """Inserting an id that already exists is a programmer error — the
    parallel sets (``known_subscriptions`` / ``pop_at``) only
    track inserts, so a silent overwrite would corrupt them.
    """
    import pytest

    s = _make_session_for_inflight_mutators()
    s._inflight_table.insert(100, ClientReq(7))
    with pytest.raises(ValueError):
        s._inflight_table.insert(100, ClientReq(8))


def test_inflight_retag_overwrites_existing_entry():
    """``client → client_config`` is the canonical retag transition."""
    s = _make_session_for_inflight_mutators()
    s._inflight_table.insert(100, ClientReq(7))
    s._inflight_table.retag(100, ClientConfig(7, "lovelace"))
    assert s._inflight_table.table[100] == ClientConfig(7, "lovelace")
    # Reverse index still resolves under the same original client id.
    assert s._inflight_table.client_to_ha[7] == 100


def test_inflight_retag_to_client_unsubscribe_drops_reverse_index():
    """A retag to a non-client kind drops the reverse-index slot — the
    unsubscribe command's id space is the unsubscribe's own client id, not
    the subscription's.
    """
    s = _make_session_for_inflight_mutators()
    s._inflight_table.insert(100, ClientReq(7))
    s._inflight_table.retag(100, ClientUnsubscribe(7, 50))
    assert s._inflight_table.table[100] == ClientUnsubscribe(7, 50)
    # ``client_unsubscribe`` is not in the (``client``, ``client_config``) set
    # that the reverse index tracks; the previous slot was popped and no new
    # one was added.
    assert 7 not in s._inflight_table.client_to_ha


def test_inflight_retag_asserts_on_missing_id():
    """Retag of an id we never inserted is a programmer error."""
    import pytest

    s = _make_session_for_inflight_mutators()
    with pytest.raises(ValueError):
        s._inflight_table.retag(100, ClientConfig(7, "lovelace"))


# ---- B10: throttle loop is cancellable on shutdown ------------------------


def test_periodic_task_exits_promptly_when_done_set():
    """``PeriodicTask`` (used for the throttle / refetch / sweep loops)
    must race ``done.wait()`` against the interval so a session
    disconnecting partway through a window exits within milliseconds.
    With a 5-second interval and an immediate ``done.set()``, the loop
    should return well under the interval.
    """
    import asyncio
    import time

    from dashboard_entity_proxy._periodic import PeriodicTask

    async def run() -> None:
        done = asyncio.Event()
        task = PeriodicTask(done=done, interval=5.0, tick=lambda: None)

        async def fire_done() -> None:
            await asyncio.sleep(0.01)
            done.set()

        start = time.monotonic()
        await asyncio.gather(task.run(), fire_done())
        elapsed = time.monotonic() - start
        # Generously under the interval — proves we did not block on
        # ``asyncio.sleep(5.0)``.
        assert elapsed < 1.0

    asyncio.run(run())


# ---- B11: heuristic widen resets on next lovelace/config ------------------


def _make_session_for_heuristic_reset():
    """Real Session, pre-set so that a heuristic widen has just happened
    (view = ALL). The lovelace/config branch of ``_handle_client`` is
    what we're testing; id-translation and the navigate side-effect get
    stubbed so the test only observes the navigate invocation.
    """
    from dashboard_entity_proxy.navigation import View, ViewKind

    s = build_session_for_test()
    s._nav.current_view = View(ViewKind.ALL)
    navigated: list[tuple[View, str | None]] = []
    s._navigated = navigated  # type: ignore[attr-defined]

    def fake_navigate(view, allow_inject, path=None):
        navigated.append((view, path))
        s._nav.current_view = view

    s._nav.navigate = fake_navigate  # type: ignore[method-assign]
    # Bypass id rewriting and HA writes — the test only cares about the
    # widen-reset and the navigate call.
    s._translate_client_id_for_ha = lambda _m: True  # type: ignore[method-assign,assignment]
    s.send_ha = lambda _t: None  # type: ignore[method-assign,assignment]
    return s


def test_handle_client_lovelace_config_renavigates_to_dashboard():
    """A client ``lovelace/config`` request must re-navigate to the
    dashboard view, so a cached client coming out of a heuristic widen
    doesn't stay on ViewKind.ALL forever.
    """
    from dashboard_entity_proxy.navigation import ViewKind

    s = _make_session_for_heuristic_reset()
    s._handle_client({"id": 1, "type": "lovelace/config", "url_path": "my-dash"})
    assert len(s._navigated) == 1
    view, path = s._navigated[0]
    assert view.kind is ViewKind.DASHBOARD
    assert view.key == "my-dash"
    assert path == "/my-dash"


# ---- B12: status() default summary + ?detail=full -------------------------


def _make_session_for_status_shape():
    """Real Session, pre-set to a ready dashboard view so ``status()``
    returns a populated snapshot for shape assertions.
    """
    from dashboard_entity_proxy.navigation import View, ViewKind
    from dashboard_entity_proxy.phase import Phase

    s = build_session_for_test()
    s._phase = Phase.READY
    s._nav.current_view = View(ViewKind.DASHBOARD, "lovelace")
    s._nav.current_path = "/lovelace"
    s._scope.ready = True
    return s


def test_status_default_summary_omits_full_scope_entities():
    """The default ``status()`` shape carries ``scope_count`` and
    ``scope_sample`` but NOT the full ``scope_entities`` list — that's the
    whole point of the change (avoid ballooning the per-poll JSON on large
    installs).
    """
    s = _make_session_for_status_shape()
    ids = [f"light.{i}" for i in range(120)]
    s._scope.ids = ids
    s._scope.set = set(ids)

    snap = s.status()
    assert snap["scope_count"] == 120
    assert "scope_entities" not in snap
    # Sample is capped at 50 ids.
    assert len(snap["scope_sample"]) == 50
    assert snap["scope_sample"] == ids[:50]


def test_status_detail_full_includes_scope_entities():
    """``detail='full'`` switches in the full entity list. ``scope_count``
    and ``scope_sample`` are still present so a UI that consumes one shape
    can also handle the other.
    """
    s = _make_session_for_status_shape()
    ids = ["light.a", "light.b", "light.c"]
    s._scope.ids = ids
    s._scope.set = set(ids)

    snap = s.status(detail="full")
    assert snap["scope_count"] == 3
    assert snap["scope_entities"] == ids
    assert snap["scope_sample"] == ids


def test_status_scope_all_omits_sample():
    """When scope is "all entities" there is no list to sample from; the
    summary carries ``scope_count == 0`` and no ``scope_sample`` key.
    """
    s = _make_session_for_status_shape()
    snap = s.status()
    assert snap["scope_all"] is True
    assert snap["scope_count"] == 0
    assert "scope_sample" not in snap
    assert "scope_entities" not in snap


def test_status_includes_queue_high_water_and_opened_command_counts():
    """``status()`` surfaces ``queue_high_water`` (peak observed outbound
    queue depth for the session) and ``opened_command_counts`` (histogram
    of client command types this session has opened). These let the
    status UI surface what's driving a session's load.
    """
    s = _make_session_for_status_shape()
    # Drive a few translations through the id allocator so the histogram
    # has interesting content.
    for mtype in ["render_template", "render_template", "subscribe_events"]:
        s._translate_client_id_for_ha({"id": 1, "type": mtype})

    snap = s.status()
    assert snap["queue_high_water"] == 0  # nothing enqueued yet
    assert snap["opened_command_counts"] == {
        "render_template": 2,
        "subscribe_events": 1,
    }


def test_queue_high_water_tracks_outbound_to_client_peak():
    """The ``_client_queue_high_water`` field tracks the max depth ever
    observed for the outbound-to-client queue across the session, so
    a burst that drains before the next status snapshot is still visible.
    """
    s = build_session_for_test()
    # Bypass real WS plumbing by stubbing the bytes-counter so tests don't
    # need a real connection.
    for _ in range(5):
        s.send_client("ping")
    assert s._client_queue_high_water == 5
    # Drain everything; the high-water mark must NOT regress.
    while not s._to_client.empty():
        s._to_client.get_nowait()
    s.send_client("after-drain")
    assert s._client_queue_high_water == 5


def test_session_registry_snapshot_forwards_detail_to_session():
    """``SessionRegistry.snapshot(detail='full')`` must forward the kwarg
    to each live session's ``status()`` — otherwise the UI's full-detail
    request silently downgrades to the summary shape.
    """
    from dashboard_entity_proxy.session import SessionRegistry

    captured: list[str] = []

    class _Conn:
        _connected_at = None

        def status(self, *, detail: str = "summary") -> dict:
            captured.append(detail)
            return {"kind": "intercept", "remote_addr": "x", "connected_at": "z"}

    from datetime import datetime, timezone

    c = _Conn()
    c._connected_at = datetime.now(timezone.utc)
    reg = SessionRegistry(retention_seconds=0)
    reg.add(c)

    reg.snapshot(detail="full")
    assert captured == ["full"]
    reg.snapshot()
    assert captured == ["full", "summary"]


def test_session_registry_snapshot_falls_back_for_status_without_detail():
    """``TunnelConnection.status()`` does not accept ``detail``; the
    registry must call it bare instead of erroring.
    """
    from dashboard_entity_proxy.session import SessionRegistry

    class _LegacyConn:
        _connected_at = None

        def status(self) -> dict:
            return {"kind": "tunnel", "remote_addr": "y", "connected_at": "z"}

    from datetime import datetime, timezone

    c = _LegacyConn()
    c._connected_at = datetime.now(timezone.utc)
    reg = SessionRegistry(retention_seconds=0)
    reg.add(c)

    snap = reg.snapshot(detail="full")
    assert snap[0]["kind"] == "tunnel"


# ---- /energy: scope from HA's energy/get_prefs ---------------------------


def test_extract_energy_entities_pulls_ids_from_nested_prefs():
    """``extract_energy_entities`` walks the prefs tree and collects
    every entity-id-shaped string regardless of nesting depth or field
    name — resilient to HA adding new energy-source types.
    """
    from dashboard_entity_proxy.dashboard import extract_energy_entities

    prefs = {
        "energy_sources": [
            {
                "type": "grid",
                "flow_from": [
                    {
                        "stat_energy_from": "sensor.grid_consumption",
                        "stat_cost": "sensor.grid_cost",
                    }
                ],
                "flow_to": [{"stat_energy_to": "sensor.grid_return"}],
                "cost_adjustment_day": 0,  # numeric — not an entity
            },
            {
                "type": "solar",
                "stat_energy_from": "sensor.solar_production",
                "config_entry_solar_forecast": ["abcdef123456"],
            },
        ],
        "device_consumption": [
            {"stat_consumption": "sensor.washer"},
            {"stat_consumption": "sensor.dryer"},
        ],
    }
    got = extract_energy_entities(prefs)
    assert got == {
        "sensor.grid_consumption",
        "sensor.grid_cost",
        "sensor.grid_return",
        "sensor.solar_production",
        "sensor.washer",
        "sensor.dryer",
    }


def test_extract_energy_entities_ignores_non_entity_strings():
    """Config-entry ids, opaque tokens, numeric values, and other non-
    entity strings must not be picked up as entities.
    """
    from dashboard_entity_proxy.dashboard import extract_energy_entities

    assert extract_energy_entities({"x": "abcdef0123456789"}) == set()
    assert extract_energy_entities({"x": 42}) == set()
    assert extract_energy_entities({}) == set()


def test_energy_scope_returns_all_when_prefs_empty():
    """Before ``energy/get_prefs`` lands (or when it returned nothing),
    ``_energy_scope`` falls back to ALL so the panel still works.
    """
    s = build_session_for_test()
    assert s._scope.energy_entities == set()  # invariant: default-empty

    scope = s._scope._energy_scope()
    assert scope.all is True


def test_domain_scope_handles_multi_domain_key():
    """``_domain_scope`` accepts a comma-joined domain list (used by
    ``/map`` and ``/media-browser``) and produces an ExtractResult whose
    ``include_domains`` covers every listed domain.
    """
    s = build_session_for_test()

    scope = s._scope._domain_scope("device_tracker,person,zone,geo_location")
    assert scope.extract is not None
    assert set(scope.extract.include_domains) == {
        "device_tracker",
        "person",
        "zone",
        "geo_location",
    }


def test_domain_scope_honors_exclude_entity_globs():
    """``exclude_entity_globs`` must filter out entities on domain panels
    too — the user opted them out at session level and expects them gone
    from /calendar, /todo, /map, /media-browser just like dashboards.
    Regression for the bug where ``_domain_scope`` passed an empty list to
    ``apply_filters`` and ``apply()`` later re-expanded patterns without
    re-applying the filter.
    """
    from dashboard_entity_proxy.options import Options

    s = build_session_for_test(
        opts=Options(exclude_globs=["calendar.old_*"]),
    )
    # Seed the mirror with two calendar entities, one matching the exclude.
    s._scope._store._entities.update(
        {"calendar.old_holidays": {}, "calendar.new_holidays": {}}
    )

    # _domain_scope filters the initial set.
    scope = s._scope._domain_scope("calendar")
    assert "calendar.new_holidays" in scope.ids
    assert "calendar.old_holidays" not in scope.ids

    # apply() preserves the filter even after expand_patterns runs.
    s._scope.apply(scope)
    assert s._scope.set is not None
    assert "calendar.new_holidays" in s._scope.set
    assert "calendar.old_holidays" not in s._scope.set


def test_fold_in_pattern_matches_honors_exclude_entity_globs():
    """Late-arriving entities that match a card's include pattern (or
    ALWAYS_IN_SCOPE_DOMAINS) still must not sneak past the user's
    exclude_entity_globs. Regression for the bypass in
    ``fold_in_pattern_matches``.
    """
    from dashboard_entity_proxy.options import Options

    s = build_session_for_test(
        opts=Options(exclude_globs=["calendar.old_*"]),
    )
    # Establish a live calendar scope so fold_in has something to fold into.
    scope = s._scope._domain_scope("calendar")
    s._scope.apply(scope)

    # Now simulate two new calendar entities arriving.
    s._scope.fold_in_pattern_matches(
        {"a": {"calendar.new_event": {}, "calendar.old_event": {}}}
    )

    assert s._scope.set is not None
    assert "calendar.new_event" in s._scope.set
    assert "calendar.old_event" not in s._scope.set


def test_energy_scope_uses_resolved_entities_when_present():
    """When ``energy/get_prefs`` resolved a non-empty set,
    ``_energy_scope`` returns a narrow ``ScopeSet`` covering those
    entities.
    """
    s = build_session_for_test()
    s._scope.energy_entities = {"sensor.grid", "sensor.solar"}

    scope = s._scope._energy_scope()
    assert scope.all is False
    assert "sensor.grid" in scope.ids
    assert "sensor.solar" in scope.ids
