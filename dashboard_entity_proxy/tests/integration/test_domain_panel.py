# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Domain-panel scope check: when the browser navigates from a narrowly-
scoped dashboard to ``/calendar``, the corresponding domain entities
should appear in ``hass.states`` without a page reload.
"""

import time

import pytest

from _wait import wait_until

E2E = pytest.mark.e2e


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


def _inject_state(ha: dict[str, str], entity_id: str, state: str) -> None:
    """POST /api/states/<entity_id> to put an entity into HA's state machine.
    The entity has no integration backing, but it appears in hass.states
    just like any other entity — sufficient to exercise the proxy's
    domain-scope mirror logic.
    """
    import json
    import urllib.request

    headers = {
        "Authorization": f"Bearer {ha['access_token']}",
        "Content-Type": "application/json",
    }
    req = urllib.request.Request(
        f"{ha['url']}/api/states/{entity_id}",
        data=json.dumps(
            {"state": state, "attributes": {"friendly_name": entity_id}}
        ).encode(),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        assert r.status in (200, 201), f"set state failed: {r.status}"


def _list_calendar_entities(chrome) -> list[str]:
    return chrome.execute_script(
        """
        const ha = document.querySelector('home-assistant');
        const st = ha?.hass?.states || {};
        return Object.keys(st).filter(k => k.startsWith('calendar.'));
        """
    )


@E2E
def test_navigate_to_calendar_loads_calendar_entities(browser, ha, ha_ws, proxy_url):
    """From a narrowly-scoped dashboard, navigating to /calendar should
    populate hass.states with calendar.* entities without a reload.
    """
    # Inject a calendar.* entity into HA's state machine. No real
    # integration backing — we just need the entity to exist so the
    # proxy mirror has something to filter on.
    _inject_state(ha, "calendar.int_test_cal", "off")
    time.sleep(2)

    # Create a non-calendar dashboard.
    _create_dashboard(ha_ws, "int-no-calendar", ["input_boolean.int_test_a"])

    # Open the narrow dashboard. Wait for the proxy to settle scope so
    # ALWAYS_IN_SCOPE_DOMAINS adds calendar.int_test_cal to hass.states.
    chrome = browser("/int-no-calendar/0")
    cal_before = wait_until(
        lambda: _list_calendar_entities(chrome),
        lambda cal: "calendar.int_test_cal" in cal,
        timeout=7,
    )
    print("\n=== before /calendar navigate ===")
    print(f"calendar.* in hass.states: {cal_before}")

    # Simulate browser_mod's path update without changing the URL — the
    # integration-test HA doesn't ship browser_mod, but the proxy reads
    # ``browser_mod/update`` off the WS by type regardless of whether HA
    # handles it. Sending only the message keeps the same WS session
    # alive, which is exactly what we want to test (navigate-without-
    # reload).
    chrome.execute_script(
        """
        const ha = document.querySelector('home-assistant');
        ha.hass.connection.sendMessage({
            type: 'browser_mod/update',
            browserID: 'int_test_browser',
            data: { browser: { path: '/calendar', visibility: 'visible' } }
        });
        """
    )
    cal_after = wait_until(
        lambda: _list_calendar_entities(chrome),
        lambda cal: bool(cal),
        timeout=7,
    )
    print("\n=== after /calendar navigate ===")
    print(f"calendar.* in hass.states: {cal_after}")


@E2E
def test_calendar_entities_in_scope_without_navigation(browser, ha, ha_ws):
    """A narrowly-scoped dashboard with no /calendar navigation and no
    browser_mod/update should still have calendar.* entities in
    hass.states, courtesy of ``ALWAYS_IN_SCOPE_DOMAINS``. This is the
    render-race fix: if calendar entities are already in state when the
    user navigates to /calendar, the panel renders correctly on the
    first try regardless of when the proxy's add-event lands.
    """
    _inject_state(ha, "calendar.int_test_cal2", "off")
    time.sleep(2)
    _create_dashboard(ha_ws, "int-no-cal-2", ["input_boolean.int_test_b"])

    chrome = browser("/int-no-cal-2/0")
    cal = wait_until(
        lambda: _list_calendar_entities(chrome),
        lambda cal: "calendar.int_test_cal2" in cal,
        timeout=7,
    )
    print("\n=== narrow dashboard, no /calendar nav ===")
    print(f"calendar.* in hass.states: {cal}")
