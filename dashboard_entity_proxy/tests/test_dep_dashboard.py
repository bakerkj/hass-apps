# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

import fnmatch
import re
from typing import Any

import pytest
from dashboard_entity_proxy.customization import CardEntityKeys, Customization
from dashboard_entity_proxy.dashboard import (
    apply_filters,
    expand_patterns,
    extract_scope,
    is_strategy,
    matches_extract,
)


def extract_entities(
    config: dict[str, Any], customization: Customization | None = None
) -> list[str]:
    """Test helper: sorted, unique entity ids referenced anywhere in a
    dashboard config. Was a public production helper; moved here when its
    only callers turned out to be these tests.
    """
    return list(extract_scope(config, customization).ids)


def test_extract_entities_full():
    cfg = {
        "views": [
            {
                "path": "home",
                "badges": ["sensor.front_door"],
                "cards": [
                    {"type": "entity", "entity": "light.kitchen"},
                    {
                        "type": "entities",
                        "entities": [
                            "switch.fan",
                            {"entity": "sensor.temp"},
                            {"entity": "not_an_entity"},
                        ],
                    },
                    {
                        "type": "picture-elements",
                        "elements": [
                            {
                                "type": "image",
                                "camera_image": "camera.porch",
                                "tap_action": {
                                    "action": "call-service",
                                    "service": "light.toggle",
                                    "target": {"entity_id": "light.porch"},
                                },
                            },
                        ],
                    },
                ],
            },
            {
                "path": "second",
                "sections": [
                    {
                        "type": "grid",
                        "cards": [{"type": "entity", "entity": "climate.hallway"}],
                    },
                ],
            },
        ]
    }
    assert extract_entities(cfg) == [
        "camera.porch",
        "climate.hallway",
        "light.kitchen",
        "light.porch",
        "sensor.front_door",
        "sensor.temp",
        "switch.fan",
    ]


def test_extract_entities_no_views():
    assert extract_entities({}) == []


def test_extract_skips_invalid_entity_strings():
    # Strings without a domain.name shape (no dot) should never be added.
    cfg = {
        "views": [
            {
                "cards": [
                    {"type": "entity", "entity": "not_an_entity"},
                    {"type": "entity", "entity": "Light.Kitchen"},  # mixed case
                    {"type": "entity", "entity": "light.with-hyphen"},  # bad chars
                    {"type": "entity", "entity": "light.kitchen"},
                ]
            }
        ]
    }
    assert extract_entities(cfg) == ["light.kitchen"]


def test_apply_filters_merge_dedupe_drop_empty():
    # zone.home and sun.sun are always carried in unless excluded — see
    # FRONTEND_BASELINE.
    assert apply_filters(["a.1", "b.2"], ["b.2", "c.3", ""], [], []) == [
        "a.1",
        "b.2",
        "c.3",
        "sun.sun",
        "zone.home",
    ]


def test_apply_filters_include():
    # The baseline bypasses include globs (the frontend needs it regardless).
    assert apply_filters(["light.a", "sensor.b"], [], ["light.*"], []) == [
        "light.a",
        "sun.sun",
        "zone.home",
    ]


def test_apply_filters_exclude():
    assert apply_filters(["light.a", "sensor.x_battery"], [], [], ["*_battery"]) == [
        "light.a",
        "sun.sun",
        "zone.home",
    ]


def test_apply_filters_include_then_exclude():
    assert apply_filters(["light.a", "light.b_x"], [], ["light.*"], ["*_x"]) == [
        "light.a",
        "sun.sun",
        "zone.home",
    ]


def test_apply_filters_empty_result_still_carries_baseline():
    assert apply_filters(["light.a"], [], ["sensor.*"], []) == ["sun.sun", "zone.home"]


def test_apply_filters_baseline_excluded_when_user_asks():
    # Explicit excludes covering every baseline entity trump the frontend
    # safety net.
    assert apply_filters(["light.a"], [], [], ["zone.*", "sun.*"]) == ["light.a"]


def test_apply_filters_customization_baseline_extends_frontend_baseline():
    cust = Customization(baseline_entities=("zone.work", "sensor.always_on"))
    assert apply_filters(["light.a"], [], [], [], cust) == [
        "light.a",
        "sensor.always_on",
        "sun.sun",
        "zone.home",
        "zone.work",
    ]


def test_apply_filters_customization_baseline_still_respects_exclude():
    cust = Customization(baseline_entities=("zone.work",))
    # The customization-supplied baseline goes through the same exclude check
    # as the built-in baseline. ``zone.*`` here suppresses both zone.home and
    # zone.work; sun.sun (also built-in baseline) is untouched.
    assert apply_filters(["light.a"], [], [], ["zone.*"], cust) == [
        "light.a",
        "sun.sun",
    ]


def test_is_strategy():
    assert is_strategy({"strategy": {"type": "original-states"}})
    assert is_strategy({"views": [{"strategy": {"type": "area"}}]})
    assert not is_strategy({"views": [{"cards": [{"entity": "light.x"}]}]})
    assert not is_strategy({})


# --- pattern-aware extraction --------------------------------------------


def test_extract_scope_auto_entities_globs_and_domains():
    cfg = {
        "views": [
            {
                "cards": [
                    {
                        "type": "custom:auto-entities",
                        "filter": {
                            "include": [
                                {"entity_id": "sensor.cpu_*", "state": "on"},
                                {"domain": "light"},
                                {"entity_id": "switch.fixed_one"},  # static
                            ],
                            "exclude": [
                                {"entity_id": "sensor.cpu_low"},
                                {"state": "unavailable"},  # ignored, dynamic
                            ],
                        },
                        "card": {"type": "entities"},
                    }
                ]
            }
        ]
    }
    extract = extract_scope(cfg)
    assert extract.include_globs == ("sensor.cpu_*",)
    assert extract.include_domains == ("light",)
    assert extract.ids == ("switch.fixed_one",)
    # Exclude entries — both glob and literal — go to exclude_globs so
    # they can never accidentally land in the static ids set.
    assert extract.exclude_globs == ("sensor.cpu_low",)
    # state-only excludes are ignored — purely dynamic.


def test_extract_scope_flex_table_card_regex():
    cfg = {
        "views": [
            {
                "cards": [
                    {
                        "type": "custom:flex-table-card",
                        "entities": {
                            "include": "sensor.cpu_capacity_[0-9]+_summary",
                            "exclude": "sensor.cpu_capacity_99_summary",
                        },
                    }
                ]
            }
        ]
    }
    extract = extract_scope(cfg)
    assert extract.include_regexes == ("sensor.cpu_capacity_[0-9]+_summary",)
    assert extract.exclude_regexes == ("sensor.cpu_capacity_99_summary",)
    assert extract.ids == ()


def test_expand_patterns_with_mirror_glob():
    cfg = {
        "views": [
            {
                "cards": [
                    {
                        "type": "custom:auto-entities",
                        "filter": {
                            "include": [{"entity_id": "sensor.wifi_*"}],
                            "exclude": [{"entity_id": "sensor.wifi_router"}],
                        },
                    }
                ]
            }
        ]
    }
    extract = extract_scope(cfg)
    mirror = [
        "sensor.wifi_kitchen",
        "sensor.wifi_router",  # excluded
        "sensor.wifi_office",
        "light.kitchen",  # no include match
        "sensor.unrelated",
    ]
    result = expand_patterns(extract, mirror)
    assert result == {"sensor.wifi_kitchen", "sensor.wifi_office"}


def test_expand_patterns_with_mirror_regex():
    cfg = {
        "views": [
            {
                "cards": [
                    {
                        "type": "custom:flex-table-card",
                        "entities": {
                            "include": "sensor.cpu_capacity_[0-9]+_summary",
                        },
                    }
                ]
            }
        ]
    }
    extract = extract_scope(cfg)
    mirror = [
        "sensor.cpu_capacity_0_summary",
        "sensor.cpu_capacity_15_summary",
        "sensor.cpu_capacity_abc_summary",  # regex won't match (not digits)
        "sensor.unrelated",
    ]
    result = expand_patterns(extract, mirror)
    assert result == {
        "sensor.cpu_capacity_0_summary",
        "sensor.cpu_capacity_15_summary",
    }


def test_expand_patterns_with_domain_filter():
    cfg = {
        "views": [
            {
                "cards": [
                    {
                        "type": "custom:auto-entities",
                        "filter": {"include": [{"domain": "light"}]},
                    }
                ]
            }
        ]
    }
    extract = extract_scope(cfg)
    mirror = ["light.a", "light.b", "switch.c", "sensor.d"]
    assert expand_patterns(extract, mirror) == {"light.a", "light.b"}


def test_expand_patterns_no_includes_returns_static_only():
    cfg = {"views": [{"cards": [{"type": "entity", "entity": "light.x"}]}]}
    extract = extract_scope(cfg)
    # No patterns at all; should just return the static ids.
    assert expand_patterns(extract, ["any.thing"]) == {"light.x"}


def test_matches_extract_per_entity():
    cfg = {
        "views": [
            {
                "cards": [
                    {
                        "type": "custom:auto-entities",
                        "filter": {"include": [{"entity_id": "sensor.wifi_*"}]},
                    }
                ]
            }
        ]
    }
    extract = extract_scope(cfg)
    assert matches_extract(extract, "sensor.wifi_kitchen") is True
    assert matches_extract(extract, "switch.kitchen") is False


def test_extract_scope_safe_against_bad_regex():
    cfg = {
        "views": [
            {
                "cards": [
                    {
                        "type": "custom:flex-table-card",
                        "entities": {"include": "[unclosed"},
                    }
                ]
            }
        ]
    }
    # Bad regex shouldn't blow up the parser — it gets captured but is
    # silently skipped during expansion.
    extract = extract_scope(cfg)
    assert extract.include_regexes == ("[unclosed",)
    assert expand_patterns(extract, ["sensor.x"]) == set()


def test_glob_matches_with_auto_entities_dot_semantics():
    # auto-entities converts globs to regex with `*` -> `.*` and leaves the
    # literal `.` as the regex any-char metacharacter. So
    # ``sensor.*.wifi_signal`` matches ``sensor.foo_wifi_signal`` because
    # the literal `.` matches the `_`. We mirror that behavior.
    cfg = {
        "views": [
            {
                "cards": [
                    {
                        "type": "custom:auto-entities",
                        "filter": {"include": [{"entity_id": "sensor.*.wifi_signal"}]},
                    }
                ]
            }
        ]
    }
    extract = extract_scope(cfg)
    mirror = [
        "sensor.kitchen_wifi_signal",
        "sensor.basement_light_wifi_signal",
        "sensor.kitchen",  # no wifi_signal suffix
        "switch.toggle_wifi_signal",  # wrong domain
    ]
    expanded = expand_patterns(extract, mirror)
    assert expanded == {
        "sensor.kitchen_wifi_signal",
        "sensor.basement_light_wifi_signal",
    }


def test_extract_scope_empty_method():
    assert extract_scope({}).empty()
    cfg = {"views": [{"cards": [{"entity": "light.x"}]}]}
    assert not extract_scope(cfg).empty()
    cfg_dynamic_only = {
        "views": [
            {
                "cards": [
                    {
                        "type": "custom:auto-entities",
                        "filter": {"include": [{"entity_id": "sensor.x_*"}]},
                    }
                ]
            }
        ]
    }
    # Has a pattern but no static ids — not empty, scope can expand.
    assert not extract_scope(cfg_dynamic_only).empty()


def test_extract_light_entity_card_effects_list():
    """``custom:light-entity-card`` ``effects_list:`` accepts either an
    ``input_select`` entity id (string) or a literal list of effect names.
    Only the entity-shaped string lands in scope.
    """
    cfg = {
        "views": [
            {
                "cards": [
                    {
                        "type": "custom:light-entity-card",
                        "entity": "light.kitchen",
                        "effects_list": "input_select.custom_effects",
                    },
                    {
                        "type": "custom:light-entity-card",
                        "entity": "light.bedroom",
                        "effects_list": ["fade", "strobe", "rainbow"],
                    },
                ]
            }
        ]
    }
    assert extract_entities(cfg) == [
        "input_select.custom_effects",
        "light.bedroom",
        "light.kitchen",
    ]


def test_extract_button_card_triggers_update():
    """``custom:button-card`` ``triggers_update:`` is a single entity id,
    a list of entity ids, or the literal sentinel ``"all"`` (rejected by
    ``_looks_like_entity``).
    """
    cfg = {
        "views": [
            {
                "cards": [
                    {
                        "type": "custom:button-card",
                        "entity": "media_player.living_room",
                        "triggers_update": "sensor.youtube_watching",
                    },
                    {
                        "type": "custom:button-card",
                        "entity": "light.kitchen",
                        "triggers_update": [
                            "sensor.last_changed_1",
                            "input_boolean.refresh_flag",
                        ],
                    },
                    {
                        "type": "custom:button-card",
                        "entity": "switch.fan",
                        "triggers_update": "all",  # sentinel, must be rejected
                    },
                ]
            }
        ]
    }
    assert extract_entities(cfg) == [
        "input_boolean.refresh_flag",
        "light.kitchen",
        "media_player.living_room",
        "sensor.last_changed_1",
        "sensor.youtube_watching",
        "switch.fan",
    ]


def test_extract_scheduler_card_customize_map():
    """``custom:scheduler-card`` ``customize:`` block — keys are entity ids
    or entity-id globs, values are per-entity metadata.
    """
    cfg = {
        "views": [
            {
                "cards": [
                    {
                        "type": "custom:scheduler-card",
                        "include": ["light.kitchen"],
                        "customize": {
                            "light.bedroom": {"name": "Bedroom", "icon": "mdi:bed"},
                            "switch.fan_*": {"name": "Fans"},  # glob
                            "not-an-entity-id": {"name": "metadata only"},
                        },
                    }
                ]
            }
        ]
    }
    r = extract_scope(cfg)
    assert "light.bedroom" in r.ids
    assert "light.kitchen" in r.ids  # from the include list
    assert "switch.fan_*" in r.include_globs


def test_extract_auto_entities_group_rule():
    """``custom:auto-entities`` ``filter.include:`` entries can carry a
    ``group: group.x`` rule. The static analyzer can't expand group members,
    but the group entity itself must be in scope or the card sees
    ``unavailable``.
    """
    cfg = {
        "views": [
            {
                "cards": [
                    {
                        "type": "custom:auto-entities",
                        "filter": {
                            "include": [
                                {"group": "group.my_doors", "state": "open"},
                                {"group": "group.my_windows"},
                                {"group": "not-an-entity-shape"},
                                {"domain": "binary_sensor"},
                            ]
                        },
                    }
                ]
            }
        ]
    }
    r = extract_scope(cfg)
    assert "group.my_doors" in r.ids
    assert "group.my_windows" in r.ids
    assert "binary_sensor" in r.include_domains


def test_card_specific_keys_dont_leak_to_other_cards():
    """The whole point of ``_BUILTIN_CARD_ENTITY_KEYS`` over a global
    keyset addition: ``battery: sensor.foo`` on an UNRELATED card type
    must NOT be picked up. Only inside the registered power-flow card
    types does ``battery`` resolve.
    """
    cfg = {
        "views": [
            {
                "cards": [
                    {
                        "type": "custom:some-other-card",
                        "battery": "sensor.would_have_leaked",
                        "grid": "sensor.also_leaked",
                        "home": "sensor.also_leaked_too",
                    }
                ]
            }
        ]
    }
    assert extract_entities(cfg) == []


def test_builtin_card_frame_merges_with_customization():
    """A user's ``Customization.card_entity_keys`` entry for a card type
    that ALSO has a builtin frame entry produces the additive union, not
    a replacement: the user's keys join the builtin's slot names.
    """
    from dashboard_entity_proxy.customization import CardEntityKeys, Customization

    cust = Customization(
        card_entity_keys={
            "custom:power-flow-card-plus": CardEntityKeys(
                singles=frozenset({"my_custom_slot"}),
            )
        }
    )
    cfg = {
        "views": [
            {
                "cards": [
                    {
                        "type": "custom:power-flow-card-plus",
                        "entities": {
                            # builtin slot
                            "grid": "sensor.grid",
                            # user-extended slot
                            "my_custom_slot": "sensor.custom",
                        },
                    }
                ]
            }
        ]
    }
    assert extract_entities(cfg, cust) == ["sensor.custom", "sensor.grid"]


def test_extract_flex_table_card_still_takes_dict_shape():
    """Regression: ``entities: {include: <regex>, exclude: <regex>}`` keeps
    its dedicated handling and does not fall through to the generic
    recursion that the non-flex shape now uses.
    """
    cfg = {
        "views": [
            {
                "cards": [
                    {
                        "type": "custom:flex-table-card",
                        "entities": {
                            "include": "^sensor\\.foo_.*$",
                            "exclude": "^sensor\\.foo_bar$",
                        },
                    }
                ]
            }
        ]
    }
    r = extract_scope(cfg)
    assert r.include_regexes == ("^sensor\\.foo_.*$",)
    assert r.exclude_regexes == ("^sensor\\.foo_bar$",)


def test_extract_suffix_entity_keys():
    """Generic ``*_entity`` keys (``base_entity``, ``camera_entity``,
    ``azimuth_entity``, ...) carry single entity references on cards like
    ``custom:example-status-card`` and ``custom:example-radar-card`` that
    do not name their keys with one of the explicit single-entity names.
    """
    cfg = {
        "views": [
            {
                "cards": [
                    {
                        "type": "custom:example-status-card",
                        "base_entity": "sensor.example_base",
                        "camera_entity": "camera.example_view",
                    },
                    {
                        "type": "custom:example-radar-card",
                        "azimuth_entity": "sensor.example_azimuth",
                        "counter_entity": "sensor.example_counter",
                        "distance_entity": "sensor.example_distance",
                    },
                ]
            }
        ]
    }
    assert extract_entities(cfg) == [
        "camera.example_view",
        "sensor.example_azimuth",
        "sensor.example_base",
        "sensor.example_counter",
        "sensor.example_distance",
    ]


def test_extract_suffix_entity_key_rejects_non_entity_values():
    """A ``*_entity`` key whose value isn't entity-id-shaped (a flag, a
    config sub-tree, an empty string) does not pollute the result.
    """
    cfg = {
        "views": [
            {
                "cards": [
                    {
                        "type": "custom:lunar-phase-card",
                        "entity": "",  # explicitly empty, should not add
                    },
                    {
                        "type": "custom:fictional-card",
                        "hidden_entity": True,  # flag-shaped, not an id
                        "count_entity": 7,
                        "name_entity": "Not An Entity Id",  # text label
                    },
                ]
            }
        ]
    }
    assert extract_entities(cfg) == []


def test_extract_scheduler_card_bare_include_list():
    """``custom:scheduler-card`` ships ``include:`` and ``exclude:`` as flat
    lists of entity-id strings at the card root — neither the auto-entities
    nor the flex-table-card shape. Entity-shaped strings land as static
    ids; entity-id globs land as include / exclude patterns.
    """
    cfg = {
        "views": [
            {
                "cards": [
                    {
                        "type": "custom:scheduler-card",
                        "include": [
                            "climate.first_floor",
                            "climate.second_floor",
                            "switch.bedroom_*",
                        ],
                        "exclude": [
                            "climate.basement",
                        ],
                    }
                ]
            }
        ]
    }
    r = extract_scope(cfg)
    assert tuple(r.ids) == ("climate.first_floor", "climate.second_floor")
    assert "switch.bedroom_*" in r.include_globs
    assert "climate.basement" in r.exclude_globs


_EXAMPLE_LOG_CUSTOMIZATION = Customization(
    implicit_entities={
        "custom:example-event-log-card": ("sensor.example_event_log",),
    }
)


def test_extract_implicit_card_entity_from_customization():
    """A card type listed in ``Customization.implicit_entities`` seeds its
    entity ids into the scope whenever the card appears, even though the
    YAML body declares no entity references. With no customization the same
    card body contributes nothing.
    """
    cfg = {
        "views": [
            {
                "cards": [
                    {"type": "custom:example-event-log-card"},
                ]
            }
        ]
    }
    assert extract_entities(cfg) == []
    assert extract_entities(cfg, _EXAMPLE_LOG_CUSTOMIZATION) == [
        "sensor.example_event_log"
    ]


def test_extract_card_entity_keys_singles_and_lists():
    """A card-specific ``entity_keys`` override teaches the walker that
    extra dict keys for that card type hold entity references. The override
    only applies inside that card subtree.
    """
    cust = Customization(
        card_entity_keys={
            "custom:my-card": CardEntityKeys(
                singles=frozenset({"primary"}),
                lists=frozenset({"secondary"}),
            )
        }
    )
    cfg = {
        "views": [
            {
                "cards": [
                    {
                        "type": "custom:my-card",
                        "primary": "sensor.a",
                        "secondary": [
                            "switch.b",
                            {"entity": "light.c"},
                        ],
                    },
                    # Same key names on a different card type — no override
                    # registered, so neither field contributes to scope.
                    {
                        "type": "custom:other-card",
                        "primary": "sensor.unrelated",
                        "secondary": ["sensor.also_unrelated"],
                    },
                ]
            }
        ]
    }
    assert extract_entities(cfg, cust) == ["light.c", "sensor.a", "switch.b"]


def test_extract_card_entity_keys_glob_pattern():
    """A glob in ``entity_keys.singles`` covers numbered/dynamic slot keys
    without enumerating every one — the walker matches them via the
    compiled pattern and pulls their string values into scope.
    """
    cust = Customization(
        card_entity_keys={
            "custom:slot-card": CardEntityKeys(
                single_patterns=(re.compile(fnmatch.translate("slot_*")),),
            )
        }
    )
    cfg = {
        "views": [
            {
                "cards": [
                    {
                        "type": "custom:slot-card",
                        "slot_alpha": "sensor.a",
                        "slot_beta": "sensor.b",
                        "slot_with_dict": {"entity": "light.c"},
                        # Not matched: name doesn't start with ``slot_``.
                        "alpha": "sensor.unrelated",
                    }
                ]
            }
        ]
    }
    assert extract_entities(cfg, cust) == [
        "light.c",
        "sensor.a",
        "sensor.b",
    ]


def test_extract_card_entity_keys_do_not_inherit_to_nested_card():
    """A nested card with its own ``type:`` resets the entity-key frame —
    the parent's overrides do not bleed into the child.
    """
    cust = Customization(
        card_entity_keys={
            "custom:outer": CardEntityKeys(singles=frozenset({"primary"}))
        }
    )
    cfg = {
        "views": [
            {
                "cards": [
                    {
                        "type": "custom:outer",
                        "primary": "sensor.outer_primary",
                        "cards": [
                            {
                                # No customization for this type → ``primary``
                                # is just data, not an entity reference.
                                "type": "tile",
                                "primary": "sensor.tile_primary",
                                "entity": "sensor.tile_entity",
                            }
                        ],
                    }
                ]
            }
        ]
    }
    assert extract_entities(cfg, cust) == [
        "sensor.outer_primary",
        "sensor.tile_entity",
    ]


def test_extract_implicit_card_entity_combines_with_other_extraction():
    """The implicit-entity seed unions with static ids extracted from sibling
    cards on the same view; it neither overwrites them nor blocks them.
    """
    cfg = {
        "views": [
            {
                "cards": [
                    {"type": "custom:example-event-log-card"},
                    {"type": "entity", "entity": "sensor.phone_battery"},
                ]
            }
        ]
    }
    assert extract_entities(cfg, _EXAMPLE_LOG_CUSTOMIZATION) == [
        "sensor.example_event_log",
        "sensor.phone_battery",
    ]


# --- Built-in card / shape audit -----------------------------------------
#
# These exercise every entity-reference shape documented for HA's built-in
# Lovelace cards. Generic recursion already handles most shapes; the cases
# below lock in the trickier ones and the action-key gating boundary.


def test_extract_sections_view_layout():
    # The ``sections`` view type carries a ``sections:`` list, each with
    # its own ``cards:`` list. Generic recursion threads through both.
    cfg = {
        "views": [
            {
                "type": "sections",
                "sections": [
                    {
                        "type": "grid",
                        "cards": [
                            {"type": "tile", "entity": "light.s1"},
                            {"type": "entity", "entity": "sensor.s1"},
                        ],
                    },
                    {
                        "type": "grid",
                        "cards": [{"type": "tile", "entity": "switch.s2"}],
                    },
                ],
                "badges": [{"type": "entity", "entity": "sensor.outdoor"}],
            }
        ]
    }
    assert extract_entities(cfg) == [
        "light.s1",
        "sensor.outdoor",
        "sensor.s1",
        "switch.s2",
    ]


# --- Per-card lock-down: each built-in card type, primary entity shape ---
#
# Most of these reduce to entity:/entities:/tap_action: and are already
# covered indirectly by the generic-recursion tests above. The point of
# this block is one explicit assertion per card so a regression on a
# specific card type fails the test named after that card.


# Per-card lock-down for trivially-shaped built-in cards: one row per
# card type so a regression on a specific card type fails with the
# card's name in the test id. The pytest.param id= preserves what the
# old per-card test functions did.
@pytest.mark.parametrize(
    ("card", "expected"),
    [
        pytest.param(
            {
                "type": "alarm-panel",
                "entity": "alarm_control_panel.house",
                "states": ["arm_away", "arm_home"],
            },
            ["alarm_control_panel.house"],
            id="alarm-panel",
        ),
        pytest.param(
            {
                "type": "button",
                "entity": "switch.x",
                "tap_action": {"action": "toggle"},
            },
            ["switch.x"],
            id="button",
        ),
        pytest.param(
            {
                "type": "calendar",
                "entities": ["calendar.personal", "calendar.family"],
            },
            ["calendar.family", "calendar.personal"],
            id="calendar",
        ),
        pytest.param(
            {
                "type": "gauge",
                "entity": "sensor.power",
                "attribute": "current",
                "min": 0,
                "max": 100,
            },
            ["sensor.power"],
            id="gauge",
        ),
        pytest.param(
            {
                "type": "humidifier",
                "entity": "humidifier.bedroom",
                "features": [{"type": "humidifier-modes"}],
            },
            ["humidifier.bedroom"],
            id="humidifier",
        ),
        pytest.param(
            {
                "type": "light",
                "entity": "light.bedroom",
                "hold_action": {
                    "action": "more-info",
                    "entity": "light.bedroom",
                },
            },
            ["light.bedroom"],
            id="light",
        ),
        pytest.param(
            {
                "type": "map",
                "entities": [
                    {"entity": "device_tracker.phone_a"},
                    {"entity": "person.alice"},
                ],
            },
            ["device_tracker.phone_a", "person.alice"],
            id="map",
        ),
        pytest.param(
            {
                "type": "markdown",
                "content": "{{ states('sensor.a') }}",
                "entity_id": "sensor.a",
            },
            ["sensor.a"],
            id="markdown",
        ),
        pytest.param(
            {"type": "media-control", "entity": "media_player.living_room"},
            ["media_player.living_room"],
            id="media-control",
        ),
        pytest.param(
            {
                "type": "picture-entity",
                "entity": "camera.front_door",
                "camera_image": "camera.front_door",
                "camera_view": "live",
                "tap_action": {"action": "more-info"},
            },
            ["camera.front_door"],
            id="picture-entity",
        ),
        pytest.param(
            {"type": "plant-status", "entity": "plant.basil"},
            ["plant.basil"],
            id="plant-status",
        ),
        pytest.param(
            {"type": "sensor", "entity": "sensor.outdoor_temp", "graph": "line"},
            ["sensor.outdoor_temp"],
            id="sensor",
        ),
        pytest.param(
            {
                "type": "statistic",
                "entity": "sensor.power_consumption",
                "period": {"calendar": {"period": "day"}},
                "stat_type": "mean",
            },
            ["sensor.power_consumption"],
            id="statistic",
        ),
        pytest.param(
            {
                "type": "thermostat",
                "entity": "climate.living_room",
                "features": [
                    {"type": "climate-hvac-modes"},
                    {"type": "target-temperature"},
                ],
            },
            ["climate.living_room"],
            id="thermostat",
        ),
        pytest.param(
            {
                "type": "tile",
                "entity": "cover.garage",
                "features": [{"type": "cover-open-close"}],
            },
            ["cover.garage"],
            id="tile",
        ),
        pytest.param(
            {"type": "todo-list", "entity": "todo.groceries"},
            ["todo.groceries"],
            id="todo-list",
        ),
        pytest.param(
            {
                "type": "weather-forecast",
                "entity": "weather.home",
                "tap_action": {"action": "more-info"},
            },
            ["weather.home"],
            id="weather-forecast",
        ),
    ],
)
def test_extract_builtin_card_primary_entity_shape(card, expected):
    cfg = {"views": [{"cards": [card]}]}
    assert extract_entities(cfg) == expected


@pytest.mark.parametrize(
    ("card", "expected"),
    [
        pytest.param(
            {
                "type": "button",
                "tap_action": {
                    "action": "call-service",
                    "service": "x.y",
                    "data": {"entity_id": ["light.a", "light.b"]},
                },
            },
            ["light.a", "light.b"],
            id="action-list-target",
        ),
        pytest.param(
            {
                "type": "button",
                "entity": "light.x",
                "tap_action": {
                    "action": "navigate",
                    "target": {"entity_id": "light.ignored"},
                },
            },
            ["light.x"],
            id="ignores-non-call-service-action",
        ),
        pytest.param(
            {
                "type": "button",
                "entity": "light.card_primary",
                "tap_action": {
                    "action": "more-info",
                    "entity": "sensor.wind_direction_downsampled",
                },
            },
            ["light.card_primary", "sensor.wind_direction_downsampled"],
            id="more-info-action-entity",
        ),
        pytest.param(
            {
                "type": "button",
                "entity": "light.card_primary",
                "tap_action": {
                    "action": "toggle",
                    "entity": "switch.override_target",
                },
            },
            ["light.card_primary", "switch.override_target"],
            id="toggle-action-entity",
        ),
        pytest.param(
            {
                "type": "button",
                "entity": "light.x",
                "tap_action": {
                    "action": "more-info",
                    "entity": "not_an_entity_id",
                },
            },
            ["light.x"],
            id="more-info-action-rejects-non-entity-value",
        ),
    ],
)
def test_extract_card_action_shapes(card, expected):
    cfg = {"views": [{"cards": [card]}]}
    assert extract_entities(cfg) == expected


@pytest.mark.parametrize(
    ("card", "expected"),
    [
        pytest.param(
            {
                "type": "custom:mushroom-chips-card",
                "chips": [
                    {"type": "entity", "entity": "light.kitchen"},
                    {"type": "entity", "entity": "sensor.temp"},
                    {"type": "menu"},
                ],
            },
            ["light.kitchen", "sensor.temp"],
            id="mushroom-chips",
        ),
        pytest.param(
            {
                "type": "custom:apexcharts-card",
                "series": [
                    {"entity": "sensor.power"},
                    {"entity": "sensor.solar"},
                ],
            },
            ["sensor.power", "sensor.solar"],
            id="apexcharts-series",
        ),
        pytest.param(
            {
                "type": "entities",
                "entities": [
                    {
                        "type": "custom:multiple-entity-row",
                        "entity": "climate.living_room",
                        "entities": [
                            {"entity": "sensor.living_room_humidity"},
                            {"entity": "sensor.living_room_temp"},
                        ],
                    }
                ],
            },
            [
                "climate.living_room",
                "sensor.living_room_humidity",
                "sensor.living_room_temp",
            ],
            id="multiple-entity-row-nested-entities",
        ),
        pytest.param(
            {
                "type": "custom:mushroom-template-card",
                "entity": ["light.a", "light.b"],
                "primary": "{{ states('light.a') }}",
            },
            ["light.a", "light.b"],
            id="mushroom-template-card-entity-list",
        ),
        # A nested ``filter: {...}`` dict on a card whose type is NOT
        # ``custom:auto-entities`` is opaque config — without the type guard
        # the walker would harvest ``sensor.unrelated_*`` as a scope glob.
        pytest.param(
            {
                "type": "custom:some-other-card",
                "entity": "sensor.real_entity",
                "filter": {
                    "include": [{"entity_id": "sensor.unrelated_*"}],
                },
            },
            ["sensor.real_entity"],
            id="filter-key-only-fires-on-auto-entities-card",
        ),
        # auto-entities filter rules are dynamic; only the static entity
        # from the nested card is harvested.
        pytest.param(
            {
                "type": "custom:auto-entities",
                "filter": {
                    "include": [
                        {"entity_id": "sensor.battery_*"},
                        {"domain": "binary_sensor"},
                    ]
                },
                "card": {
                    "type": "entities",
                    "entities": ["sensor.uptime"],
                },
            },
            ["sensor.uptime"],
            id="auto-entities-child-card-only",
        ),
        pytest.param(
            {
                "type": "picture-elements",
                "image": "/local/floor.png",
                "elements": [
                    {
                        "type": "state-icon",
                        "entity": "light.lr",
                        "tap_action": {
                            "action": "call-service",
                            "service": "light.toggle",
                            "target": {"entity_id": "light.lr"},
                        },
                    },
                    {
                        "type": "conditional",
                        "conditions": [{"entity": "binary_sensor.dark", "state": "on"}],
                        "elements": [{"type": "icon", "entity": "light.night"}],
                    },
                ],
            },
            ["binary_sensor.dark", "light.lr", "light.night"],
            id="picture-elements-deeply-nested",
        ),
        pytest.param(
            {
                "type": "custom:history-explorer-card",
                "entities": [
                    "sensor.outdoor_temp",
                    {"entity": "sensor.indoor_temp"},
                ],
            },
            ["sensor.indoor_temp", "sensor.outdoor_temp"],
            id="history-explorer-card",
        ),
        pytest.param(
            {
                "type": "custom:layout-card",
                "layout_type": "grid",
                "cards": [
                    {"type": "entity", "entity": "light.a"},
                    {
                        "type": "custom:stack-in-card",
                        "cards": [
                            {"type": "tile", "entity": "switch.b"},
                        ],
                    },
                ],
            },
            ["light.a", "switch.b"],
            id="layout-card-recursion",
        ),
        pytest.param(
            {
                "type": "custom:power-flow-card-plus",
                "entities": {
                    "battery": {"entity": "sensor.powerwall_battery_now"},
                    "grid": {"entity": "sensor.powerwall_site_now"},
                    "home": {"entity": "sensor.powerwall_load_now"},
                    "individual": [
                        {"entity": "sensor.gas_flow"},
                        {"entity": "sensor.water_flow"},
                    ],
                    "solar": {"entity": "sensor.powerwall_solar_now"},
                },
            },
            [
                "sensor.gas_flow",
                "sensor.powerwall_battery_now",
                "sensor.powerwall_load_now",
                "sensor.powerwall_site_now",
                "sensor.powerwall_solar_now",
                "sensor.water_flow",
            ],
            id="power-flow-card-plus-entities-dict",
        ),
        pytest.param(
            {
                "type": "custom:power-flow-card",
                "entities": {
                    "battery": "sensor.battery_in_out",
                    "battery_charge": "sensor.battery_percent",
                    "grid": "sensor.grid_in_out",
                    "solar": "sensor.solar_out",
                    "home": "sensor.home_consumption",
                },
            },
            [
                "sensor.battery_in_out",
                "sensor.battery_percent",
                "sensor.grid_in_out",
                "sensor.home_consumption",
                "sensor.solar_out",
            ],
            id="power-flow-card-short-entities-dict",
        ),
        pytest.param(
            {
                "type": "custom:power-flow-card-plus",
                "entities": {
                    "grid": {
                        "entity": {
                            "consumption": "sensor.grid_consumption",
                            "production": "sensor.grid_production",
                        },
                    },
                    "battery": {
                        "entity": {
                            "consumption": "sensor.battery_consumption",
                            "production": "sensor.battery_production",
                        },
                        "state_of_charge": "sensor.battery_state_of_charge",
                    },
                },
            },
            [
                "sensor.battery_consumption",
                "sensor.battery_production",
                "sensor.battery_state_of_charge",
                "sensor.grid_consumption",
                "sensor.grid_production",
            ],
            id="power-flow-card-plus-split-entity-dict",
        ),
        pytest.param(
            {
                "type": "custom:clock-weather-card",
                "entity": "weather.home",
                "temperature_sensor": "sensor.outdoor_temp",
                "humidity_sensor": "sensor.outdoor_humidity",
                "aqi_sensor": "sensor.air_quality",
            },
            [
                "sensor.air_quality",
                "sensor.outdoor_humidity",
                "sensor.outdoor_temp",
                "weather.home",
            ],
            id="star-sensor-suffix",
        ),
        pytest.param(
            {
                "type": "custom:lumina-energy-card",
                "sensor_pv1": "sensor.solar_production",
                "sensor_bat1_soc": "sensor.battery_soc",
                "sensor_grid_power": "sensor.grid_power",
                "sensor_count": 5,
                "sensor_label": "PV1",
            },
            [
                "sensor.battery_soc",
                "sensor.grid_power",
                "sensor.solar_production",
            ],
            id="sensor-underscore-prefix",
        ),
        pytest.param(
            {
                "type": "custom:windrose-card",
                "wind_direction_entity": {
                    "entity": "sensor.wind_direction",
                    "use_statistics": True,
                },
                "windspeed_entities": [
                    {"entity": "sensor.wind_speed"},
                ],
            },
            ["sensor.wind_direction", "sensor.wind_speed"],
            id="nested-suffix-entity-dict",
        ),
        pytest.param(
            {
                "type": "custom:advanced-camera-card",
                "cameras": [
                    {"camera_entity": "camera.front_west"},
                    {"camera_entity": "camera.front_east"},
                    {"camera_entity": "camera.back_yard"},
                ],
            },
            [
                "camera.back_yard",
                "camera.front_east",
                "camera.front_west",
            ],
            id="advanced-camera-card-nested-camera-entity",
        ),
    ],
)
def test_extract_custom_community_card_shapes(card, expected):
    cfg = {"views": [{"cards": [card]}]}
    assert extract_entities(cfg) == expected


@pytest.mark.parametrize(
    ("card", "expected"),
    [
        pytest.param(
            {
                "type": "entity",
                "entity": "light.shown",
                "visibility": [
                    {
                        "condition": "state",
                        "entity": "sun.sun",
                        "state": "above_horizon",
                    },
                    {
                        "condition": "numeric_state",
                        "entity": "sensor.brightness",
                        "above": 50,
                    },
                ],
            },
            ["light.shown", "sensor.brightness", "sun.sun"],
            id="card-visibility-conditions",
        ),
        pytest.param(
            {
                "type": "conditional",
                "conditions": [
                    {
                        "condition": "or",
                        "conditions": [
                            {
                                "condition": "state",
                                "entity": "binary_sensor.motion_a",
                                "state": "on",
                            },
                            {
                                "condition": "and",
                                "conditions": [
                                    {
                                        "condition": "state",
                                        "entity": "binary_sensor.motion_b",
                                        "state": "on",
                                    },
                                    {
                                        "condition": "numeric_state",
                                        "entity": "sensor.lux",
                                        "below": 10,
                                    },
                                ],
                            },
                        ],
                    }
                ],
                "card": {"type": "entity", "entity": "light.hallway"},
            },
            [
                "binary_sensor.motion_a",
                "binary_sensor.motion_b",
                "light.hallway",
                "sensor.lux",
            ],
            id="conditional-card-nested-logic-conditions",
        ),
        pytest.param(
            {
                "type": "heading",
                "heading": "My Heading",
                "badges": [
                    {"type": "entity", "entity": "sensor.t1"},
                    {"type": "entity", "entity": "sensor.t2"},
                    {"type": "button", "icon": "mdi:home"},
                ],
                "tap_action": {
                    "action": "more-info",
                    "entity": "sensor.heading_target",
                },
            },
            ["sensor.heading_target", "sensor.t1", "sensor.t2"],
            id="heading-card-with-modern-badges",
        ),
        pytest.param(
            {
                "type": "picture-elements",
                "image": "/local/floor.png",
                "elements": [
                    {
                        "type": "service-button",
                        "title": "Toggle",
                        "service": "light.toggle",
                        "service_data": {"entity_id": "light.bedroom"},
                        "style": {"top": "10%", "left": "20%"},
                    }
                ],
            },
            ["light.bedroom"],
            id="picture-elements-service-button",
        ),
        pytest.param(
            {
                "type": "picture-elements",
                "image": "/local/floor.png",
                "elements": [
                    {
                        "type": "conditional",
                        "entity": "sun.sun",
                        "state": "above_horizon",
                        "elements": [
                            {"type": "state-icon", "entity": "light.day_lamp"}
                        ],
                    }
                ],
            },
            ["light.day_lamp", "sun.sun"],
            id="picture-elements-conditional-legacy-form",
        ),
        pytest.param(
            {
                "type": "logbook",
                "title": "Recent events",
                "target": {"entity_id": ["light.kitchen", "switch.fan"]},
            },
            ["light.kitchen", "switch.fan"],
            id="logbook-card-target-block",
        ),
        # Long-term statistic_ids carry a ``domain:thing`` shape that isn't
        # a subscribable entity_id; they must not appear in scope.
        pytest.param(
            {
                "type": "statistics-graph",
                "entities": [
                    "sensor.outdoor_temp",
                    "recorder:total_energy",
                ],
            },
            ["sensor.outdoor_temp"],
            id="statistic-id-with-colon-not-subscribable",
        ),
        # ``geo_location_sources:`` lists integration source names, not
        # entity ids.
        pytest.param(
            {
                "type": "map",
                "entities": ["device_tracker.mine"],
                "geo_location_sources": ["nws_alerts", "usgs_earthquakes"],
            },
            ["device_tracker.mine"],
            id="map-card-geo-location-sources-not-entities",
        ),
        pytest.param(
            {
                "type": "tile",
                "entity": "light.x",
                "icon_tap_action": {
                    "action": "navigate",
                    "target": {"entity_id": "light.ignored"},
                },
            },
            ["light.x"],
            id="tile-icon-tap-action-navigate-ignores-target",
        ),
        pytest.param(
            {
                "type": "tile",
                "entity": "light.x",
                "icon_tap_action": {
                    "action": "call-service",
                    "service": "light.toggle",
                    "target": {"entity_id": "light.icon_target"},
                },
            },
            ["light.icon_target", "light.x"],
            id="tile-icon-tap-action-call-service-extracts-target",
        ),
        pytest.param(
            {
                "type": "tile",
                "entity": "light.x",
                "icon_tap_action": {
                    "action": "more-info",
                    "entity": "sensor.icon_target",
                },
            },
            ["light.x", "sensor.icon_target"],
            id="tile-icon-tap-action-more-info-extracts-entity",
        ),
        pytest.param(
            {
                "type": "markdown",
                "content": "{{ states('sensor.a') }}",
                "entity_id": ["sensor.a", "sensor.b"],
            },
            ["sensor.a", "sensor.b"],
            id="markdown-entity-id-list",
        ),
        pytest.param(
            {
                "type": "entities",
                "entities": [
                    {
                        "type": "call-service",
                        "name": "Run script",
                        "service": "script.do",
                        "service_data": {"entity_id": "light.target"},
                    }
                ],
            },
            ["light.target"],
            id="entities-card-call-service-row",
        ),
        pytest.param(
            {
                "type": "entities",
                "entities": [
                    {
                        "type": "conditional",
                        "conditions": [
                            {
                                "condition": "state",
                                "entity": "binary_sensor.night",
                                "state": "on",
                            }
                        ],
                        "row": {"entity": "light.bedroom"},
                    }
                ],
            },
            ["binary_sensor.night", "light.bedroom"],
            id="entities-card-conditional-row",
        ),
        pytest.param(
            {
                "type": "entities",
                "entities": [
                    {
                        "type": "buttons",
                        "entities": [
                            {"entity": "light.a"},
                            {"entity": "switch.b"},
                        ],
                    }
                ],
            },
            ["light.a", "switch.b"],
            id="entities-card-buttons-row",
        ),
        pytest.param(
            {
                "type": "picture",
                "image_entity": "camera.front_door",
                "tap_action": {
                    "action": "more-info",
                    "entity": "camera.front_door",
                },
            },
            ["camera.front_door"],
            id="picture-card-image-entity",
        ),
        pytest.param(
            {
                "type": "area",
                "area": "kitchen",
                "features": [
                    {
                        "type": "area-controls",
                        "controls": [
                            {"entity_id": "light.kitchen_main"},
                            {"entity_id": "switch.kitchen_fan"},
                        ],
                    }
                ],
            },
            ["light.kitchen_main", "switch.kitchen_fan"],
            id="area-card-feature-area-controls-entity-id-list",
        ),
        # Legacy ``action: call-service`` with a ``data:`` block carrying
        # the entity_id (the modern ``target:`` shape didn't exist in
        # 2020-era configs).
        pytest.param(
            {
                "type": "button",
                "tap_action": {
                    "action": "call-service",
                    "service": "light.toggle",
                    "data": {"entity_id": "light.legacy_target"},
                },
            },
            ["light.legacy_target"],
            id="legacy-call-service-action-data-field",
        ),
        pytest.param(
            {
                "type": "entity",
                "entity": "sensor.main",
                "footer": {
                    "type": "graph",
                    "entity": "sensor.history",
                },
            },
            ["sensor.history", "sensor.main"],
            id="entity-card-footer-graph-entities",
        ),
        pytest.param(
            {
                "type": "entity-filter",
                "entities": ["sensor.a", "sensor.b", "sensor.c"],
                "state_filter": ["on", "open"],
                "card": {"type": "glance"},
            },
            ["sensor.a", "sensor.b", "sensor.c"],
            id="entity-filter-card-wraps-inner-card",
        ),
        pytest.param(
            {
                "type": "glance",
                "entities": [
                    {
                        "entity": "light.a",
                        "tap_action": {"action": "more-info"},
                    },
                    {
                        "entity": "switch.b",
                        "tap_action": {"action": "toggle"},
                    },
                ],
            },
            ["light.a", "switch.b"],
            id="glance-card-with-dict-items-and-actions",
        ),
        pytest.param(
            {
                "type": "grid",
                "columns": 2,
                "cards": [
                    {"type": "entity", "entity": "sensor.a"},
                    {"type": "entity", "entity": "sensor.b"},
                ],
            },
            ["sensor.a", "sensor.b"],
            id="grid-card-container-harvests-children",
        ),
        pytest.param(
            {
                "type": "history-graph",
                "entities": [
                    "sensor.outdoor_temp",
                    {"entity": "sensor.indoor_temp"},
                ],
            },
            ["sensor.indoor_temp", "sensor.outdoor_temp"],
            id="history-graph-card-mixed-strings-and-dicts",
        ),
        pytest.param(
            {
                "type": "horizontal-stack",
                "cards": [
                    {"type": "entity", "entity": "sensor.a"},
                    {"type": "entity", "entity": "sensor.b"},
                ],
            },
            ["sensor.a", "sensor.b"],
            id="horizontal-stack-container-harvests-children",
        ),
        pytest.param(
            {
                "type": "picture-glance",
                "entity": "light.background",
                "image_entity": "camera.front_door",
                "camera_image": "camera.front_door",
                "entities": [
                    "light.a",
                    {"entity": "switch.b"},
                ],
            },
            [
                "camera.front_door",
                "light.a",
                "light.background",
                "switch.b",
            ],
            id="picture-glance-card-combined-shapes",
        ),
        pytest.param(
            {
                "type": "statistics-graph",
                "entities": ["sensor.indoor_temp", "sensor.outdoor_temp"],
                "stat_types": ["mean", "min", "max"],
            },
            ["sensor.indoor_temp", "sensor.outdoor_temp"],
            id="statistics-graph-card-with-real-entities",
        ),
        pytest.param(
            {
                "type": "vertical-stack",
                "cards": [
                    {"type": "entity", "entity": "sensor.a"},
                    {"type": "entity", "entity": "sensor.b"},
                ],
            },
            ["sensor.a", "sensor.b"],
            id="vertical-stack-container-harvests-children",
        ),
    ],
)
def test_extract_card_visibility_conditional_container_shapes(card, expected):
    cfg = {"views": [{"cards": [card]}]}
    assert extract_entities(cfg) == expected


@pytest.mark.parametrize(
    ("card", "expected"),
    [
        pytest.param(
            {
                "type": "conditional",
                "conditions": [
                    {
                        "condition": "not",
                        "conditions": [
                            {
                                "condition": "state",
                                "entity": "binary_sensor.guests_home",
                                "state": "on",
                            }
                        ],
                    }
                ],
                "card": {"type": "entity", "entity": "light.everyday"},
            },
            ["binary_sensor.guests_home", "light.everyday"],
            id="condition-not-with-nested-state-condition",
        ),
        # screen / user / time conditions don't reference entities; the
        # wrapped card's own entity is the only one in scope.
        pytest.param(
            {
                "type": "conditional",
                "conditions": [
                    {
                        "condition": "screen",
                        "media_query": "(min-width: 800px)",
                    },
                    {"condition": "user", "users": ["abc123def"]},
                    {"condition": "time", "after": "08:00:00"},
                ],
                "card": {"type": "entity", "entity": "light.day"},
            },
            ["light.day"],
            id="condition-screen-user-time-carry-no-entity",
        ),
    ],
)
def test_extract_conditional_leaf_type_shapes(card, expected):
    cfg = {"views": [{"cards": [card]}]}
    assert extract_entities(cfg) == expected


@pytest.mark.parametrize(
    ("card", "expected"),
    [
        pytest.param(
            {
                "type": "picture-elements",
                "image": "/local/floor.png",
                "elements": [
                    {
                        "type": "state-badge",
                        "entity": "sensor.bedroom_temp",
                        "style": {"top": "10%", "left": "20%"},
                    }
                ],
            },
            ["sensor.bedroom_temp"],
            id="picture-elements-state-badge-element",
        ),
        pytest.param(
            {
                "type": "picture-elements",
                "image": "/local/floor.png",
                "elements": [
                    {
                        "type": "state-label",
                        "entity": "sensor.outdoor_temp",
                        "attribute": "unit_of_measurement",
                    }
                ],
            },
            ["sensor.outdoor_temp"],
            id="picture-elements-state-label-element",
        ),
        pytest.param(
            {
                "type": "picture-elements",
                "image": "/local/floor.png",
                "elements": [
                    {
                        "type": "icon",
                        "entity": "light.kitchen",
                        "icon": "mdi:lightbulb",
                        "tap_action": {"action": "toggle"},
                    }
                ],
            },
            ["light.kitchen"],
            id="picture-elements-icon-element-with-tap-action",
        ),
        pytest.param(
            {
                "type": "picture-elements",
                "image": "/local/floor.png",
                "elements": [
                    {
                        "type": "image",
                        "entity": "binary_sensor.window",
                        "camera_image": "camera.front_door",
                        "state_image": {
                            "on": "/local/open.png",
                            "off": "/local/closed.png",
                        },
                    }
                ],
            },
            ["binary_sensor.window", "camera.front_door"],
            id="picture-elements-image-element-with-camera-and-state-image",
        ),
        pytest.param(
            {
                "type": "picture-elements",
                "image": "/local/floor.png",
                "elements": [
                    {
                        "type": "action-button",
                        "title": "Toggle",
                        "tap_action": {
                            "action": "perform-action",
                            "perform_action": "light.toggle",
                            "target": {"entity_id": "light.bedroom"},
                        },
                    }
                ],
            },
            ["light.bedroom"],
            id="picture-elements-action-button-element",
        ),
    ],
)
def test_extract_picture_elements_element_shapes(card, expected):
    cfg = {"views": [{"cards": [card]}]}
    assert extract_entities(cfg) == expected


@pytest.mark.parametrize(
    ("card", "expected"),
    [
        pytest.param(
            {
                "type": "entities",
                "entities": [
                    {
                        "type": "attribute",
                        "entity": "sun.sun",
                        "attribute": "next_rising",
                        "name": "Next sunrise",
                    }
                ],
            },
            ["sun.sun"],
            id="entities-card-attribute-row",
        ),
        pytest.param(
            {
                "type": "entities",
                "entities": [
                    {
                        "type": "button",
                        "entity": "light.kitchen",
                        "name": "Run",
                        "tap_action": {"action": "toggle"},
                    }
                ],
            },
            ["light.kitchen"],
            id="entities-card-button-row",
        ),
        pytest.param(
            {
                "type": "entities",
                "entities": [
                    {
                        "type": "input-select",
                        "entity": "input_select.theme",
                    }
                ],
            },
            ["input_select.theme"],
            id="entities-card-input-select-row",
        ),
    ],
)
def test_extract_entities_card_row_shapes(card, expected):
    cfg = {"views": [{"cards": [card]}]}
    assert extract_entities(cfg) == expected


# Cards whose YAML carries no entity references — either by design
# (energy-* cards fetch their data from the API, iframe/shopping-list
# carry only static content) or because the test fixture didn't add any.
@pytest.mark.parametrize(
    "card",
    [
        pytest.param({"type": "clock", "clock_size": "large"}, id="clock"),
        pytest.param(
            {
                "type": "iframe",
                "url": "https://example.com",
                "aspect_ratio": "50%",
            },
            id="iframe",
        ),
        pytest.param({"type": "energy-usage-graph"}, id="energy-usage-graph"),
        pytest.param(
            {"type": "shopping-list", "title": "Groceries"},
            id="shopping-list",
        ),
        pytest.param(
            {
                "type": "shortcut",
                "title": "Lights off",
                "tap_action": {"action": "navigate", "navigation_path": "/x"},
            },
            id="shortcut",
        ),
        # A bare ``include:`` whose items are arbitrary strings (a tag list,
        # a section label list) must not be misread as entity ids.
        pytest.param(
            {
                "type": "custom:fictional-tagged-card",
                "include": ["LivingRoom", "kitchen", "garage"],
            },
            id="scheduler-bare-include-ignores-non-entity-strings",
        ),
        # image_tap_action: navigate carries navigation_path, not an entity
        # reference; a spurious target.entity_id under it must not leak.
        pytest.param(
            {
                "type": "area",
                "area": "kitchen",
                "image_tap_action": {
                    "action": "navigate",
                    "target": {"entity_id": "light.ignored"},
                },
            },
            id="area-card-image-tap-action-navigate-ignores-target",
        ),
    ],
)
def test_extract_card_has_no_entity(card):
    cfg = {"views": [{"cards": [card]}]}
    assert extract_entities(cfg) == []


# --- Action types -------------------------------------------------------


# Action subtrees that DO contribute an entity reference — covers the
# three shapes the extractor recognises (perform-action / call-service
# under target / data / service_data). One id per shape.
@pytest.mark.parametrize(
    "tap_action",
    [
        pytest.param(
            {
                "action": "perform-action",
                "perform_action": "light.turn_on",
                "target": {"entity_id": "light.kitchen"},
                "data": {"brightness": 200},
            },
            id="perform-action-target",
        ),
        pytest.param(
            {
                "action": "perform-action",
                "perform_action": "light.turn_on",
                "data": {"entity_id": "light.kitchen"},
            },
            id="perform-action-data-legacy",
        ),
        pytest.param(
            {
                "action": "call-service",
                "service": "light.toggle",
                "service_data": {"entity_id": "light.kitchen"},
            },
            id="call-service-service-data",
        ),
    ],
)
def test_extract_service_action_finds_entity_id(tap_action):
    cfg = {"views": [{"cards": [{"type": "button", "tap_action": tap_action}]}]}
    assert extract_entities(cfg) == ["light.kitchen"]


# Action types that don't themselves carry entity references — only the
# host card's own ``entity:`` should land in scope, the action subtree
# is structural. One row per action type to keep per-action regressions
# named.
@pytest.mark.parametrize(
    "tap_action",
    [
        pytest.param(
            {"action": "navigate", "navigation_path": "/lovelace/0"},
            id="navigate",
        ),
        pytest.param(
            {"action": "url", "url_path": "https://example.com"},
            id="url",
        ),
        pytest.param(
            {"action": "assist", "pipeline_id": "last_used"},
            id="assist",
        ),
        pytest.param(
            {
                "action": "fire-dom-event",
                "browser_mod": {"service": "browser_mod.popup"},
            },
            id="fire-dom-event",
        ),
        pytest.param({"action": "none"}, id="none"),
    ],
)
def test_extract_action_does_not_leak_entities(tap_action):
    cfg = {
        "views": [
            {
                "cards": [
                    {
                        "type": "button",
                        "entity": "light.x",
                        "tap_action": tap_action,
                    }
                ]
            }
        ]
    }
    assert extract_entities(cfg) == ["light.x"]


# --- Badge types -------------------------------------------------------


def test_extract_view_badges_legacy_string_form():
    cfg = {
        "views": [
            {
                "badges": ["sensor.outdoor_temp", "binary_sensor.front_door"],
                "cards": [],
            }
        ]
    }
    assert extract_entities(cfg) == [
        "binary_sensor.front_door",
        "sensor.outdoor_temp",
    ]


def test_extract_view_badges_modern_entity_dict_form():
    cfg = {
        "views": [
            {
                "badges": [
                    {"type": "entity", "entity": "sensor.outdoor_temp"},
                    {"type": "entity", "entity": "binary_sensor.front_door"},
                ],
                "cards": [],
            }
        ]
    }
    assert extract_entities(cfg) == [
        "binary_sensor.front_door",
        "sensor.outdoor_temp",
    ]


def test_extract_badge_type_entity_filter():
    cfg = {
        "views": [
            {
                "badges": [
                    {
                        "type": "entity-filter",
                        "entities": ["sensor.a", "sensor.b", "sensor.c"],
                        "state_filter": ["unavailable"],
                    }
                ],
                "cards": [],
            }
        ]
    }
    assert extract_entities(cfg) == ["sensor.a", "sensor.b", "sensor.c"]


def test_extract_badge_type_shortcut_has_no_entity():
    cfg = {
        "views": [
            {
                "badges": [
                    {
                        "type": "shortcut",
                        "icon": "mdi:home",
                        "tap_action": {
                            "action": "navigate",
                            "navigation_path": "/lovelace/0",
                        },
                    }
                ],
                "cards": [],
            }
        ]
    }
    assert extract_entities(cfg) == []


# --- Entities-card row types -------------------------------------------


# entities-card rows whose body carries no entity reference. The
# walker must recurse into them but find nothing.
@pytest.mark.parametrize(
    "rows",
    [
        pytest.param(
            [{"type": "cast", "view": "0", "dashboard": "lovelace"}], id="cast"
        ),
        pytest.param(
            [
                {"type": "divider"},
                {"type": "section", "label": "Section"},
                {"type": "weblink", "url": "https://example.com"},
            ],
            id="divider-section-weblink",
        ),
        pytest.param([{"type": "text", "name": "Note", "text": "Hello"}], id="text"),
    ],
)
def test_extract_entities_card_row_no_entity(rows):
    cfg = {"views": [{"cards": [{"type": "entities", "entities": rows}]}]}
    assert extract_entities(cfg) == []


# --- View types --------------------------------------------------------


def test_extract_panel_view_with_single_card():
    cfg = {
        "views": [
            {
                "panel": True,
                "cards": [{"type": "media-control", "entity": "media_player.lr"}],
            }
        ]
    }
    assert extract_entities(cfg) == ["media_player.lr"]
