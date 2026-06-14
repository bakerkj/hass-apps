# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Customization-file integration test.

Regression coverage for the iCloud3 / weather-chart-card bug class:
custom HACS cards that depend on entities the YAML doesn't list (the
card's renderer reads a hardcoded id or a config default at runtime).
Without ``implicit_entities`` declared in customization, those
entities are filtered out and the card breaks. With customization
loaded, they're present in scope and the card renders.

This test uses a dedicated proxy instance (via ``proxy_factory``) so
the customization can be wired in without disturbing the
session-shared ``proxy_url``.
"""

from __future__ import annotations

import time

import pytest

from dashboard_entity_proxy.customization import Customization

INTEGRATION = pytest.mark.integration


def _create_dashboard(ha_ws, url_path: str, cards: list[dict]) -> None:
    """Provision a Lovelace dashboard with the given raw card configs."""
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
                "views": [{"title": "Main", "cards": cards}],
            },
        }
    )
    assert r.get("success"), f"config save failed: {r}"


@INTEGRATION
def test_implicit_entities_from_customization_reach_browser(
    proxy_factory, browser, ha, ha_ws
):
    """Card type ``custom:fake-card`` is configured with NO entity
    references in its YAML. Without customization the walker has no
    way to know the card needs ``input_boolean.int_test_c``. With
    customization registering that as an implicit entity for the
    type, it lands in scope and reaches the browser.

    Two proxies are run side by side: one without customization, one
    with. Same dashboard URL drives both. The browser's state should
    differ exactly by the implicit entity.
    """
    dashboard_url = "int-custom-card"
    # A card declaring no entity refs at all — its renderer would
    # subscribe to ``input_boolean.int_test_c`` at runtime.
    _create_dashboard(
        ha_ws,
        dashboard_url,
        [
            {"type": "custom:fake-card", "name": "no explicit entity"},
            # An anchor entity so scope is narrow rather than widen-to-all.
            {"type": "entity", "entity": "input_boolean.int_test_a"},
        ],
    )

    # 1) Without customization: implicit entity should NOT be in scope.
    plain_proxy = proxy_factory()
    chrome = browser(f"{plain_proxy}/{dashboard_url}/0")
    time.sleep(8)
    plain_state = chrome.execute_script(
        "const ha = document.querySelector('home-assistant');"
        "return {has_c: 'input_boolean.int_test_c' in (ha?.hass?.states || {}),"
        " has_a: 'input_boolean.int_test_a' in (ha?.hass?.states || {})};"
    )
    assert plain_state["has_a"], (
        f"explicit entity missing without customization: {plain_state}"
    )
    assert not plain_state["has_c"], (
        f"implicit entity in scope WITHOUT customization — test premise wrong: "
        f"{plain_state}"
    )

    # 2) With customization registering the implicit entity.
    cust = Customization(
        implicit_entities={"custom:fake-card": ("input_boolean.int_test_c",)}
    )
    custom_proxy = proxy_factory(customization=cust)
    chrome = browser(f"{custom_proxy}/{dashboard_url}/0")
    time.sleep(8)
    custom_state = chrome.execute_script(
        "const ha = document.querySelector('home-assistant');"
        "return {has_c: 'input_boolean.int_test_c' in (ha?.hass?.states || {}),"
        " has_a: 'input_boolean.int_test_a' in (ha?.hass?.states || {})};"
    )
    assert custom_state["has_a"], (
        f"explicit entity missing with customization: {custom_state}"
    )
    assert custom_state["has_c"], (
        f"implicit entity NOT in scope with customization — the "
        f"customization-file loading path has regressed: {custom_state}"
    )
