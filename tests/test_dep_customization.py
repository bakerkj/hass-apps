# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Unit tests for the optional customization YAML loader."""

from __future__ import annotations

from pathlib import Path

import pytest

from dashboard_entity_proxy.customization import (
    CardEntityKeys,
    Customization,
    load,
)


def _write(tmp_path: Path, body: str) -> str:
    p = tmp_path / "dep.yaml"
    p.write_text(body, encoding="utf-8")
    return str(p)


def test_missing_file_raises(tmp_path: Path) -> None:
    """An explicitly-named path that doesn't exist is a configuration error
    — load() refuses to silently treat it as "no customization" because
    that hides typos in the configured path.
    """
    with pytest.raises(FileNotFoundError):
        load(str(tmp_path / "does-not-exist.yaml"))


def test_empty_file_returns_empty_customization(tmp_path: Path) -> None:
    path = _write(tmp_path, "")
    assert load(path) == Customization()


def test_full_schema_round_trip(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        version: 1
        baseline:
          entities:
            - zone.work
            - sensor.always_on
        cards:
          "custom:example-event-log-card":
            implicit_entities:
              - sensor.example_event_log
          "custom:other-card":
            implicit_entities:
              - sensor.a
              - sensor.b
            entity_keys:
              singles: [primary]
              lists: [secondary]
        helpers:
          extra_platforms:
            - custom_template_helper
        """,
    )
    cust = load(path)
    assert cust.baseline_entities == ("zone.work", "sensor.always_on")
    assert cust.implicit_entities == {
        "custom:example-event-log-card": ("sensor.example_event_log",),
        "custom:other-card": ("sensor.a", "sensor.b"),
    }
    assert cust.card_entity_keys == {
        "custom:other-card": CardEntityKeys(
            singles=frozenset({"primary"}), lists=frozenset({"secondary"})
        )
    }
    assert cust.extra_helper_platforms == ("custom_template_helper",)


def test_entity_keys_overlap_is_error(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        version: 1
        cards:
          "custom:foo":
            entity_keys:
              singles: [name]
              lists: [name]
        """,
    )
    with pytest.raises(ValueError, match="validation failed"):
        load(path)


def test_entity_keys_glob_in_singles_matches_literal_in_lists(
    tmp_path: Path,
) -> None:
    """Cross-side collision: a glob in ``singles`` matches a literal in
    ``lists``. The walker would silently pick the singles branch, so the
    loader rejects the ambiguity at parse time.
    """
    path = _write(
        tmp_path,
        """
        version: 1
        cards:
          "custom:foo":
            entity_keys:
              singles: ["entity_*"]
              lists: [entity_1]
        """,
    )
    with pytest.raises(ValueError, match="validation failed"):
        load(path)


def test_entity_keys_glob_in_lists_matches_literal_in_singles(
    tmp_path: Path,
) -> None:
    """Mirror of the previous case: glob in ``lists`` matches literal in
    ``singles``.
    """
    path = _write(
        tmp_path,
        """
        version: 1
        cards:
          "custom:foo":
            entity_keys:
              singles: [entity_1]
              lists: ["entity_*"]
        """,
    )
    with pytest.raises(ValueError, match="validation failed"):
        load(path)


def test_entity_keys_unknown_sub_key_is_error(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        version: 1
        cards:
          "custom:foo":
            entity_keys:
              regexes: [foo]
        """,
    )
    with pytest.raises(ValueError, match="validation failed"):
        load(path)


def test_entity_keys_empty_lists_drop_entry(tmp_path: Path) -> None:
    """Declaring ``entity_keys: {singles: [], lists: []}`` registers nothing."""
    path = _write(
        tmp_path,
        """
        version: 1
        cards:
          "custom:foo":
            entity_keys:
              singles: []
              lists: []
        """,
    )
    cust = load(path)
    assert cust.card_entity_keys == {}


def test_only_version_is_valid(tmp_path: Path) -> None:
    """A file with just ``version: 1`` is a valid (empty) customization."""
    path = _write(tmp_path, "version: 1\n")
    assert load(path) == Customization()


def test_missing_version_is_error(tmp_path: Path) -> None:
    path = _write(tmp_path, "baseline:\n  entities: [zone.work]\n")
    with pytest.raises(ValueError, match="validation failed"):
        load(path)


def test_unsupported_version_is_error(tmp_path: Path) -> None:
    path = _write(tmp_path, "version: 99\n")
    with pytest.raises(ValueError, match="validation failed"):
        load(path)


def test_unknown_top_level_key_is_error(tmp_path: Path) -> None:
    path = _write(tmp_path, "version: 1\nwidgets:\n  - foo\n")
    with pytest.raises(ValueError, match="validation failed"):
        load(path)


def test_unknown_card_key_is_error(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        version: 1
        cards:
          "custom:foo":
            widgets: [bar]
        """,
    )
    with pytest.raises(ValueError, match="validation failed"):
        load(path)


def test_non_string_entity_id_is_error(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        version: 1
        baseline:
          entities: [42]
        """,
    )
    with pytest.raises(ValueError, match="validation failed"):
        load(path)


def test_top_level_must_be_mapping(tmp_path: Path) -> None:
    path = _write(tmp_path, "- just-a-list\n")
    with pytest.raises(ValueError, match="top-level must be a mapping"):
        load(path)


def test_empty_card_implicit_entities_is_dropped(tmp_path: Path) -> None:
    """A card entry with an empty (or absent) ``implicit_entities`` list is
    not registered — there's nothing to seed.
    """
    path = _write(
        tmp_path,
        """
        version: 1
        cards:
          "custom:foo":
            implicit_entities: []
        """,
    )
    cust = load(path)
    assert cust.implicit_entities == {}


def test_entity_keys_glob_compiles_to_pattern(tmp_path: Path) -> None:
    """Entries containing ``*``, ``?``, or ``[`` are stored as compiled
    patterns; plain names stay in the literal frozenset alongside.
    """
    path = _write(
        tmp_path,
        """
        version: 1
        cards:
          "custom:foo":
            entity_keys:
              singles: [primary, slot_*, "entity_?"]
              lists: ["[abc]_list", trailing]
        """,
    )
    cust = load(path)
    frame = cust.card_entity_keys["custom:foo"]
    assert frame.singles == frozenset({"primary"})
    assert frame.lists == frozenset({"trailing"})
    assert frame.matches_single("primary")
    assert frame.matches_single("slot_alpha")
    assert frame.matches_single("slot_42")
    assert frame.matches_single("entity_1")
    assert not frame.matches_single("entity_long")  # ``?`` is one char
    assert not frame.matches_single("slot")  # glob requires the underscore
    assert frame.matches_list("a_list")
    assert frame.matches_list("trailing")
    assert not frame.matches_list("d_list")  # ``[abc]`` excludes ``d``


def test_entity_keys_glob_is_anchored(tmp_path: Path) -> None:
    """``entity_*`` matches keys that START with ``entity_``, not keys
    that merely contain it — fnmatch.translate's implicit ``\\Z`` and
    matching from the start protect against accidental substring matches.
    """
    path = _write(
        tmp_path,
        """
        version: 1
        cards:
          "custom:foo":
            entity_keys:
              singles: ["entity_*"]
        """,
    )
    frame = load(path).card_entity_keys["custom:foo"]
    assert frame.matches_single("entity_42")
    assert frame.matches_single("entity_42_suffix")  # tail wildcard absorbs it
    # Anchoring at the head: substring match is rejected.
    assert not frame.matches_single("not_entity_42")
    assert not frame.matches_single("xentity_42")
