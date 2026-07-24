# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Subscription event routing under a real browser.

These tests cover the bug class that was hardest to surface with the
in-process fake HA: a client subscribes via ``subscribe_events`` or a
namespaced ``*/subscribe_*`` command, the proxy forwards the request,
HA replies with a ``result`` and starts streaming events with the same
id, and the proxy MUST keep routing every subsequent event back to the
client (translating the id back to the client's original). Earlier
versions popped the inflight entry on the result frame, breaking every
subscription after the first message.
"""

import pytest
from _wait import wait_for_session_ready

E2E = pytest.mark.e2e


@E2E
def test_subscribe_events_state_changed_routes_through_proxy(browser, ha):
    """Client subscribes to ``state_changed`` via the proxy. A real
    state change in HA must produce an event delivered back through
    the proxy with the client's original subscription id.

    Covers the ``client`` dispatch path's never-pop guarantee: the
    result frame arrives, the inflight entry stays registered, and
    subsequent events keep their id translated back to the client.
    """
    chrome = browser("/lovelace")
    # Wait for the frontend to complete auth + initial subscribe_entities.
    wait_for_session_ready(chrome)

    # Drive the subscription from the browser, trigger a state change
    # from the same browser via REST, and collect events.
    result = chrome.execute_async_script(
        """
        const done = arguments[arguments.length - 1];
        const token = arguments[0];
        const ha = document.querySelector('home-assistant');
        if (!ha || !ha.hass || !ha.hass.connection) {
            done({error: 'no hass connection'});
            return;
        }
        const conn = ha.hass.connection;
        const received = [];
        conn.subscribeEvents(
            (ev) => received.push({
                event_type: ev.event_type,
                entity_id: ev.data && ev.data.entity_id,
            }),
            'state_changed',
        ).then((unsub) => {
            // Fire a state change via REST so the proxy sees the
            // subsequent event coming back through the WS.
            fetch('/api/services/input_boolean/toggle', {
                method: 'POST',
                headers: {
                    'Authorization': 'Bearer ' + token,
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({entity_id: 'input_boolean.int_test_a'}),
            }).then(() => {
                setTimeout(() => {
                    unsub();
                    done({
                        received: received.length,
                        matching: received.filter(
                            (e) => e.event_type === 'state_changed'
                                && e.entity_id === 'input_boolean.int_test_a',
                        ).length,
                        sample: received.slice(0, 5),
                    });
                }, 1500);
            });
        }).catch((err) => done({error: String(err)}));
        """,
        ha["access_token"],
    )

    assert "error" not in result, f"subscription failed: {result}"
    assert result["received"] >= 1, (
        f"no events arrived through proxy after state change: {result}"
    )
    assert result["matching"] >= 1, (
        f"state_changed event for input_boolean.int_test_a not delivered: {result}"
    )


@E2E
def test_browser_logs_no_unknown_subscription_warning(browser, ha):
    """Regression test: the proxy used to pop client subscription
    inflight entries on the result frame, causing subsequent events
    to arrive with un-translated allocator ids. The HA frontend logs
    ``Received event for unknown subscription <N>. Unsubscribing.`` in
    that case. After the fix, no such message should appear.
    """
    chrome = browser("/lovelace")
    wait_for_session_ready(chrome, timeout=8)
    # Also exercise subscribe_events explicitly so any routing bug
    # surfaces in console output rather than just being latent.
    chrome.execute_async_script(
        """
        const done = arguments[arguments.length - 1];
        const ha = document.querySelector('home-assistant');
        const conn = ha && ha.hass && ha.hass.connection;
        if (!conn) { done(false); return; }
        conn.subscribeEvents(() => {}, 'state_changed')
            .then(() => setTimeout(() => done(true), 1500))
            .catch(() => done(false));
        """
    )
    logs = chrome.get_log("browser")
    offending = [
        entry
        for entry in logs
        if "unknown subscription" in (entry.get("message") or "").lower()
    ]
    assert not offending, (
        f"frontend reported {len(offending)} unknown-subscription warnings — "
        f"the subscription-routing fix has regressed. samples: "
        f"{[entry.get('message', '')[:200] for entry in offending[:3]]}"
    )
