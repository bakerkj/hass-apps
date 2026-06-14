# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

import pytest

from dashboard_entity_proxy.navigation import (
    ViewKind,
    classify_path,
    is_settings_signal,
    path_from_browser_mod,
    url_path_from_config_request,
)


@pytest.mark.parametrize(
    "path,kind,key",
    [
        ("/lovelace/0", ViewKind.DASHBOARD, ""),
        ("/lovelace-my-dashboard/home", ViewKind.DASHBOARD, "lovelace-my-dashboard"),
        ("/lovelace", ViewKind.DASHBOARD, ""),
        ("/config/devices/device/abc123", ViewKind.DEVICE, "abc123"),
        ("/config/integrations/integration/hue", ViewKind.INTEGRATION, "hue"),
        ("/config/areas/area/kitchen", ViewKind.AREA, "kitchen"),
        ("/config/helpers", ViewKind.HELPERS, ""),
        ("/config/devices/dashboard", ViewKind.ALL, ""),
        ("/config/integrations/dashboard", ViewKind.ALL, ""),
        ("/config", ViewKind.ALL, ""),
        ("/developer-tools/state", ViewKind.ALL, ""),
        ("/history", ViewKind.ALL, ""),
        ("/calendar", ViewKind.DOMAIN, "calendar"),
        ("/calendar/some_id", ViewKind.DOMAIN, "calendar"),
        ("/todo", ViewKind.DOMAIN, "todo"),
        ("/shopping-list", ViewKind.DOMAIN, "todo"),
        ("/media-browser", ViewKind.DOMAIN, "media_player"),
        ("/map", ViewKind.DOMAIN, "device_tracker,person,zone,geo_location"),
        ("/energy", ViewKind.ENERGY, ""),
        ("/profile/general", ViewKind.ALL, ""),
        (
            "/lovelace-my-dashboard/view?edit=1",
            ViewKind.DASHBOARD,
            "lovelace-my-dashboard",
        ),
    ],
)
def test_classify_path(path, kind, key):
    view = classify_path(path)
    assert view.kind is kind
    assert view.key == key


@pytest.mark.parametrize(
    "msg_type,want",
    [
        ("config_entries/get", True),
        ("config/config_entries/list", True),
        ("config_entries/subscribe", False),  # called on every page load
        ("config_entries/subscribe_remove", False),
        ("config/entity_registry/list", False),  # called on every page load
        ("subscribe_entities", False),
        ("lovelace/config", False),
        ("get_states", False),
        ("config/entity_registry/list_for_display", False),  # app-wide collection
    ],
)
def test_is_settings_signal(msg_type, want):
    assert is_settings_signal(msg_type) is want


def test_path_from_browser_mod():
    # Real browser_mod wire format: state is nested under "browser".
    assert (
        path_from_browser_mod(
            {
                "type": "browser_mod/update",
                "browserID": "abc",
                "data": {
                    "browser": {
                        "path": "/calendar",
                        "visibility": "visible",
                        "userAgent": "Mozilla/5.0",
                    },
                },
            }
        )
        == "/calendar"
    )
    # Flat data.path fallback for forks/legacy senders.
    assert (
        path_from_browser_mod(
            {
                "type": "browser_mod/update",
                "data": {
                    "path": "/lovelace-my-dashboard/home",
                    "visibility": "visible",
                },
            }
        )
        == "/lovelace-my-dashboard/home"
    )
    # Nested form wins when both are present.
    assert (
        path_from_browser_mod(
            {
                "type": "browser_mod/update",
                "data": {
                    "browser": {"path": "/calendar"},
                    "path": "/old-shape",
                },
            }
        )
        == "/calendar"
    )
    assert path_from_browser_mod({"type": "browser_mod/update", "data": {}}) is None
    assert (
        path_from_browser_mod({"type": "browser_mod/update", "data": {"browser": {}}})
        is None
    )
    assert path_from_browser_mod({"type": "browser_mod/update"}) is None


def test_url_path_from_config_request():
    assert url_path_from_config_request({"url_path": "my-dashboard"}) == "my-dashboard"
    assert url_path_from_config_request({"url_path": None}) == ""
    assert url_path_from_config_request({}) == ""
