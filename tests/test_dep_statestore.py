# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

from dashboard_entity_proxy.statestore import StateStore


def test_apply_add_and_get():
    s = StateStore()
    s.apply(
        {
            "a": {
                "light.kitchen": {
                    "s": "on",
                    "a": {"brightness": 255},
                    "c": "c1",
                    "lc": 100.0,
                    "lu": 100.0,
                }
            }
        }
    )
    assert len(s) == 1
    es = s.get("light.kitchen")
    assert es is not None
    assert es.state == "on"
    assert es.attributes["brightness"] == 255
    assert es.last_changed == 100.0 and es.last_updated == 100.0


def test_add_without_lu_defaults_to_lc():
    s = StateStore()
    s.apply({"a": {"x.y": {"s": "on", "lc": 50.0}}})
    es = s.get("x.y")
    assert es is not None and es.last_updated == 50.0


def test_change_updates_state_and_merges_attributes():
    s = StateStore()
    s.apply(
        {"a": {"x.y": {"s": "on", "a": {"b": 1, "c": "red"}, "lc": 1.0, "lu": 1.0}}}
    )
    s.apply(
        {
            "c": {
                "x.y": {
                    "+": {"lu": 2.0, "a": {"b": 2, "d": "z"}},
                    "-": {"a": ["c"]},
                }
            }
        }
    )
    es = s.get("x.y")
    assert es is not None
    assert es.attributes == {"b": 2, "d": "z"}
    assert es.state == "on"  # unchanged
    assert es.last_changed == 1.0 and es.last_updated == 2.0


def test_change_for_unknown_entity_defensive():
    s = StateStore()
    s.apply({"c": {"ghost.x": {"+": {"s": "on", "lc": 5.0, "lu": 5.0}}}})
    es = s.get("ghost.x")
    assert es is not None and es.state == "on"


def test_remove():
    s = StateStore()
    s.apply({"a": {"a.1": {"s": "x", "lc": 1.0}, "b.2": {"s": "y", "lc": 1.0}}})
    s.apply({"r": ["a.1"]})
    assert s.get("a.1") is None
    assert s.get("b.2") is not None


def test_snapshot_omits_lu_when_equal_lc():
    s = StateStore()
    s.apply(
        {
            "a": {
                "eq.e": {"s": "x", "lc": 5.0, "lu": 5.0},
                "ne.e": {"s": "x", "lc": 5.0, "lu": 9.0},
            }
        }
    )
    snap = s.snapshot(None)
    assert "lu" not in snap["eq.e"]
    assert snap["ne.e"]["lu"] == 9.0


def test_snapshot_subset_skips_missing():
    s = StateStore()
    s.apply({"a": {"a.1": {"s": "x", "lc": 1.0}}})
    snap = s.snapshot(["a.1", "missing.x"])
    assert "a.1" in snap and "missing.x" not in snap


def test_snapshot_all_includes_everything():
    s = StateStore()
    s.apply({"a": {"a.1": {"s": "x", "lc": 1.0}, "b.2": {"s": "y", "lc": 1.0}}})
    assert set(s.snapshot(None)) == {"a.1", "b.2"}
