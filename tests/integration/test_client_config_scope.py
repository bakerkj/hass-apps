# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Regression test for the bootstrap-scope-resolution bug.

When the HA frontend loads a non-default dashboard URL, it sends its
own ``lovelace/config`` request for that ``url_path``. The proxy must
intercept that response to learn what entities the dashboard
references and narrow scope accordingly — without that, the dashboard
stays widened to whatever the proxy's eager startup fetch resolved
(typically the default dashboard).

The original redesign dropped that interception by accident; this test
uses a real HA + real Chrome + real proxy to ensure the
``client_config`` dispatch path keeps doing its job.
"""

from __future__ import annotations

import time

import pytest

INTEGRATION = pytest.mark.integration


def _create_test_dashboard(ha_ws, url_path: str, entities: list[str]) -> None:
    """Create a Lovelace dashboard with the given entities listed as
    ``entity:`` refs. The dashboard is registered and its config saved
    so the HA frontend will serve it on navigation.
    """
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
    assert r.get("success"), f"create dashboard failed: {r}"
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
                            {"type": "entity", "entity": eid} for eid in entities
                        ],
                    }
                ],
            },
        }
    )
    assert r.get("success"), f"config save failed: {r}"


@INTEGRATION
def test_client_lovelace_config_narrows_scope(browser, ha_ws):
    """Dashboard with explicit ``entity: input_boolean.int_test_a`` only.
    Browser through the proxy must end up with that entity in
    ``hass.states`` and other ``input_boolean.int_test_*`` excluded.
    """
    dashboard_url = "int-client-config"
    _create_test_dashboard(ha_ws, dashboard_url, ["input_boolean.int_test_a"])

    chrome = browser(f"/{dashboard_url}/0")
    # Let frontend run subscribe_entities + lovelace/config + render.
    time.sleep(8)

    in_scope = chrome.execute_script(
        """
        const ha = document.querySelector('home-assistant');
        if (!ha || !ha.hass) return {error: 'no home-assistant element'};
        const probe = (eid) => eid in (ha.hass.states || {});
        return {
            in_a: probe('input_boolean.int_test_a'),
            in_b: probe('input_boolean.int_test_b'),
            in_c: probe('input_boolean.int_test_c'),
            in_d: probe('input_boolean.int_test_d'),
            total_states: Object.keys(ha.hass.states || {}).length,
            baseline_sun: probe('sun.sun'),
            baseline_zone: probe('zone.home'),
        };
        """
    )

    assert in_scope.get("in_a"), (
        f"input_boolean.int_test_a (explicit entity ref) should be in scope: {in_scope}"
    )
    assert not in_scope.get("in_b"), (
        f"input_boolean.int_test_b should NOT be in scope: {in_scope}"
    )
    assert not in_scope.get("in_c"), (
        f"input_boolean.int_test_c should NOT be in scope: {in_scope}"
    )
    assert not in_scope.get("in_d"), (
        f"input_boolean.int_test_d should NOT be in scope: {in_scope}"
    )
    assert in_scope.get("baseline_sun"), (
        f"sun.sun (FRONTEND_BASELINE) should be in scope: {in_scope}"
    )
    assert in_scope.get("baseline_zone"), (
        f"zone.home (FRONTEND_BASELINE) should be in scope: {in_scope}"
    )
    # Sanity: scope is genuinely narrow, not widen-to-all.
    assert in_scope["total_states"] < 50, (
        f"scope appears widened to all entities ({in_scope['total_states']}): {in_scope}"
    )
