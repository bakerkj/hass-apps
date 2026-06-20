# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Namespaced subscription routing.

These tests exercise commands whose name does NOT start with
``subscribe_`` but which open a streaming subscription server-side:
``weather/subscribe_forecast``, ``render_template``, etc.
``HA_SUBSCRIPTION_COMMANDS`` must include each one so the proxy keeps
the inflight entry past the result frame and subsequent events route
back to the client.
"""

import pytest

from _wait import wait_for_session_ready

E2E = pytest.mark.e2e


@E2E
def test_weather_subscribe_forecast_routes_through_proxy(browser, ha):
    """``weather/subscribe_forecast`` is a ``namespaced/subscribe_*``
    streaming command. The whitelist match classifies it as a
    subscription, so the proxy keeps the inflight entry past the
    result frame.

    The test accepts EITHER a successful event stream OR HA's
    ``forecast_not_supported`` error result as proof of routing: in
    both cases the response (event or error) had to travel back
    through the proxy with the id translated to the client's
    original. A retention failure would manifest as the promise
    hanging (no callback, no error) and the test timing out in
    ``execute_async_script``.
    """
    chrome = browser("/lovelace")
    wait_for_session_ready(chrome)

    result = chrome.execute_async_script(
        """
        const done = arguments[arguments.length - 1];
        const ha = document.querySelector('home-assistant');
        const conn = ha && ha.hass && ha.hass.connection;
        if (!conn) { done({error: 'no hass.connection'}); return; }
        const received = [];
        conn.subscribeMessage(
            (ev) => received.push({has_forecast: Array.isArray(ev.forecast)}),
            {type: 'weather/subscribe_forecast',
             entity_id: 'weather.int_test_weather',
             forecast_type: 'daily'},
        ).then((unsub) => {
            setTimeout(() => {
                unsub();
                done({path: 'event_stream', received: received.length});
            }, 1500);
        }).catch((err) => done({
            path: 'error_result',
            error_code: err && err.code,
            error_message: err && (err.message || JSON.stringify(err)),
        }));
        """,
    )

    # Either path proves the result frame travelled through the proxy
    # with the client id intact. A retention bug would hang.
    assert result.get("path") in ("event_stream", "error_result"), (
        f"weather/subscribe_forecast did not route through the proxy "
        f"at all (the promise never resolved or rejected): {result}"
    )
    if result["path"] == "event_stream":
        assert result["received"] >= 1, (
            f"forecast subscription succeeded but no events arrived: {result}"
        )


@E2E
def test_render_template_subscription_routes_through_proxy(browser, ha):
    """``render_template`` is the canonical "no subscribe in the name"
    streaming subscription. It re-renders on each state change of the
    referenced entities. Whitelist match keeps the inflight entry; the
    callback must receive the rendered string.
    """
    chrome = browser("/lovelace")
    wait_for_session_ready(chrome)

    result = chrome.execute_async_script(
        """
        const done = arguments[arguments.length - 1];
        const ha = document.querySelector('home-assistant');
        const conn = ha && ha.hass && ha.hass.connection;
        if (!conn) { done({error: 'no hass.connection'}); return; }
        const received = [];
        conn.subscribeMessage(
            (ev) => received.push({result: ev.result, listeners: ev.listeners}),
            {type: 'render_template',
             template: "{{ states('input_boolean.int_test_a') }}"},
        ).then((unsub) => {
            setTimeout(() => {
                unsub();
                done({received: received.length, sample: received.slice(0, 3)});
            }, 1500);
        }).catch((err) => done({error: String(err)}));
        """,
    )

    assert "error" not in result, f"render_template failed: {result}"
    assert result["received"] >= 1, f"no template render events received: {result}"
    # The first render should report the state of int_test_a (off
    # by default after HA boot).
    sample = result["sample"]
    rendered_values = [
        e.get("result") for e in sample if "result" in e and e["result"] is not None
    ]
    assert any(v in ("on", "off") for v in rendered_values), (
        f"render results don't look like an input_boolean state: {sample}"
    )
