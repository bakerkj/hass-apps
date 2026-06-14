# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Multi-dashboard navigation: scope narrows/widens correctly as the
client moves between dashboards in the same WebSocket session.

Covers the ``client_config`` dispatch path's re-resolution behaviour:
the proxy must observe each ``lovelace/config`` response, cache the
extracted scope, and apply the cache entry whose ``url_path`` matches
the current view.
"""

from __future__ import annotations

import pytest

from _wait import wait_until

INTEGRATION = pytest.mark.integration


def _create_dashboard(ha_ws, url_path: str, entity_ids: list[str]) -> None:
    r = ha_ws(
        {
            "type": "lovelace/dashboards/create",
            "url_path": url_path,
            "title": f"Test {url_path}",
            "icon": "mdi:test-tube",
            "require_admin": False,
            "show_in_sidebar": False,
            "mode": "storage",
        }
    )
    assert r.get("success"), f"create {url_path} failed: {r}"
    r = ha_ws(
        {
            "type": "lovelace/config/save",
            "url_path": url_path,
            "config": {
                "title": f"Test {url_path}",
                "views": [
                    {
                        "title": "Main",
                        "cards": [
                            {"type": "entity", "entity": eid} for eid in entity_ids
                        ],
                    }
                ],
            },
        }
    )
    assert r.get("success"), f"save {url_path} failed: {r}"


def _scope_probe(chrome) -> dict[str, bool]:
    """Snapshot of which int_test entities are currently in
    ``hass.states`` — the cheapest assertion surface for scope changes.
    """
    return chrome.execute_script(
        """
        const ha = document.querySelector('home-assistant');
        const has = (e) => e in (ha?.hass?.states || {});
        return {
            a: has('input_boolean.int_test_a'),
            b: has('input_boolean.int_test_b'),
            c: has('input_boolean.int_test_c'),
            d: has('input_boolean.int_test_d'),
        };
        """
    )


@INTEGRATION
def test_navigation_narrows_then_renarrows(browser, ha_ws):
    """Two dashboards with disjoint entity sets. After navigating
    A→B, scope should reflect B's entities (and have shed A's).
    """
    _create_dashboard(ha_ws, "int-nav-a", ["input_boolean.int_test_a"])
    _create_dashboard(ha_ws, "int-nav-b", ["input_boolean.int_test_b"])

    chrome = browser("/int-nav-a/0")
    wait_until(
        lambda: _scope_probe(chrome),
        lambda s: s["a"] and not s["b"],
        timeout=7,
    )

    # Navigate to B without dropping the WS connection. ``location.href``
    # change drives HA's frontend through its router, which sends a new
    # ``lovelace/config`` for url_path=int-nav-b — the proxy must pick
    # that up and renarrow scope.
    chrome.execute_script("location.href = '/int-nav-b/0';")
    wait_until(
        lambda: _scope_probe(chrome),
        lambda s: s["b"] and not s["a"],
        timeout=7,
    )
