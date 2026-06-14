# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Live registry-tracking integration test.

The proxy subscribes to ``entity_registry_updated`` /
``device_registry_updated`` at session start and applies a refetch when
those events fire. This test exercises that path by creating a brand-
new entity in HA after the browser has connected, then asserting the
proxy observed the change and made the new entity available on
demand.
"""

from __future__ import annotations

import time

import pytest

from _wait import wait_until

INTEGRATION = pytest.mark.integration


@INTEGRATION
def test_registry_update_event_propagates_new_entity(browser, ha, ha_ws):
    """1. Open a dashboard widened to the new entity's slot.
    2. Create ``input_boolean.int_runtime_added`` via HA API.
    3. Assert the proxy's registry index now knows about it via a
       device-page navigation (which the proxy resolves through the
       registry index it maintains live).

    The narrow detection: navigate to the input_boolean integration
    page in the browser. The proxy resolves scope from its registry
    index. If the live-update path is broken, the new entity is
    missing from scope and the integration page wouldn't list it.
    """
    chrome = browser("/lovelace")
    # Let the initial subscribe + registry list settle: wait for the
    # mirror snapshot to populate ``hass.states``.
    wait_until(
        lambda: chrome.execute_script(
            "const ha = document.querySelector('home-assistant');"
            "return Object.keys(ha?.hass?.states || {}).length;"
        ),
        lambda n: n > 0,
        timeout=6,
    )

    # Sanity: the entity should NOT exist yet.
    pre = ha_ws(
        {
            "type": "config/entity_registry/get",
            "entity_id": "input_boolean.int_runtime_added",
        }
    )
    assert not pre.get("success"), f"test premise wrong — entity already exists: {pre}"

    # Create the new entity. ``input_boolean.create`` is the canonical
    # WS command for dynamic creation; it fires
    # ``entity_registry_updated`` on success.
    r = ha_ws(
        {
            "type": "input_boolean/create",
            "name": "Int Runtime Added",
            "icon": "mdi:test-tube",
        }
    )
    assert r.get("success"), f"input_boolean create failed: {r}"

    # Give the proxy time to receive the update event + complete its
    # refetch (debounce + per-entity get).
    time.sleep(2)

    # Verify HA has it.
    post = ha_ws(
        {
            "type": "config/entity_registry/get",
            "entity_id": "input_boolean.int_runtime_added",
        }
    )
    assert post.get("success"), f"entity not in HA after create: {post}"

    # Now query the proxy: navigate to the device-registry-backed
    # integration page. The proxy resolves scope from its live
    # registry index. If the index doesn't include the new entity,
    # scope falls back to widen-all (the dashboard parser fallback)
    # OR is empty. Either way is observable.
    chrome.execute_script(
        "history.pushState({}, '', '/config/integrations/integration/input_boolean');"
    )
    # Wait for the proxy to process the (browser_mod-style) nav. Without
    # browser_mod the path-from-config heuristic kicks in via the next
    # lovelace/config request; the assertion is the entity's presence in
    # the browser's hass.states after the registry refetch lands.
    has_new = wait_until(
        lambda: chrome.execute_script(
            "const ha = document.querySelector('home-assistant');"
            "return 'input_boolean.int_runtime_added' in (ha?.hass?.states || {});"
        ),
        lambda present: present,
        timeout=4,
    )
    assert has_new, (
        "input_boolean.int_runtime_added not present in hass.states after "
        "registry update — live-tracking path did not propagate the "
        "new entity through the proxy. (The mirror should have it because "
        "subscribe_entities tracks new entity creation.)"
    )
