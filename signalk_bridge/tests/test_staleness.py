# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for adaptive per-path staleness detection."""

from datetime import UTC, datetime, timedelta

from signalk_bridge.app import _map_tree
from signalk_bridge.staleness import StalenessTracker, parse_ts

BASE = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def at(seconds: float) -> datetime:
    return BASE + timedelta(seconds=seconds)


def _tracker(**kw) -> StalenessTracker:
    kw.setdefault("floor_s", 40.0)
    kw.setdefault("cap_s", 600.0)
    return StalenessTracker(**kw)


# --- timestamp parsing ------------------------------------------------------


def test_parse_ts_handles_zulu_and_offset() -> None:
    assert parse_ts("2026-01-01T12:00:00Z") == BASE
    assert parse_ts("2026-01-01T12:00:00+00:00") == BASE


def test_parse_ts_rejects_junk() -> None:
    assert parse_ts(None) is None
    assert parse_ts("not-a-date") is None
    assert parse_ts(12345) is None


def test_parse_ts_rejects_naive_timestamp() -> None:
    # A tz-less timestamp parses to a naive datetime, which would crash every
    # compare/subtract in the poll loop -> whole-bridge outage. Reject it.
    assert parse_ts("2026-01-01T12:00:00") is None
    assert parse_ts("2026-01-01T12:00:00.500") is None


# --- learning + thresholds --------------------------------------------------


def test_disabled_when_cap_not_positive() -> None:
    t = _tracker(cap_s=0.0)
    t.observe({"p": BASE})
    assert t.stale_paths({"p": BASE}, at(100_000)) == set()


def test_never_learned_path_falls_back_to_cap() -> None:
    t = _tracker(cap_s=100.0)
    t.observe({"p": BASE})  # seen once, no cadence learned yet
    assert t.stale_paths({"p": BASE}, at(99)) == set()
    assert t.stale_paths({"p": BASE}, at(101)) == {"p"}


def test_fast_path_expires_at_floor() -> None:
    # 2s cadence -> 3*2=6s < floor, so the floor (40s) governs; a single missed
    # poll must not flap it.
    t = _tracker(floor_s=40.0, cap_s=600.0)
    for s in (0, 2, 4):
        t.observe({"p": at(s)})
    assert t.stale_paths({"p": at(4)}, at(4 + 39)) == set()
    assert t.stale_paths({"p": at(4)}, at(4 + 41)) == {"p"}


def test_learned_threshold_is_clamped_to_cap() -> None:
    # The cap is the maximum detection latency for ANY path: a large learned
    # interval (or an anomalous gap) can't push the threshold past it.
    t = _tracker(floor_s=40.0, cap_s=100.0)
    for s in (0, 300, 600):
        t.observe({"p": at(s)})
    # interval=300 -> max(3*300,40)=900, clamped down to the 100s cap.
    assert t.stale_paths({"p": at(600)}, at(600 + 99)) == set()
    assert t.stale_paths({"p": at(600)}, at(600 + 101)) == {"p"}


def test_rolling_max_absorbs_a_single_slow_poll() -> None:
    # One 15s jitter gap among 2s gaps lifts the threshold so the healthy
    # sensor doesn't flap.
    t = _tracker(floor_s=5.0, cap_s=600.0)
    for s in (0, 2, 17, 19, 21):  # gaps 2,15,2,2 -> max 15
        t.observe({"p": at(s)})
    # threshold = max(3*15, 5) = 45
    assert t.stale_paths({"p": at(21)}, at(21 + 44)) == set()
    assert t.stale_paths({"p": at(21)}, at(21 + 46)) == {"p"}


def test_notifications_are_exempt() -> None:
    t = _tracker(cap_s=100.0)
    t.observe({"notifications.instrument.foo": BASE})
    assert t.stale_paths({"notifications.instrument.foo": BASE}, at(100_000)) == set()


def test_live_path_never_goes_stale() -> None:
    t = _tracker(floor_s=40.0, cap_s=600.0)
    # Refresh every 2s and always query at the latest timestamp.
    for s in range(0, 60, 2):
        t.observe({"p": at(s)})
        assert t.stale_paths({"p": at(s)}, at(s)) == set()


# --- freshness-bounded persistence ------------------------------------------


def test_persisted_cadence_catches_device_dead_after_brief_restart(tmp_path) -> None:
    t = _tracker(data_dir=str(tmp_path), learning_max_age_s=1800.0)
    for s in (0, 2, 4):
        t.observe({"p": at(s)})
    t.save(at(10))

    # Fresh process after a 90s restart: loads the snapshot (< 1800s old).
    t2 = _tracker(data_dir=str(tmp_path), learning_max_age_s=1800.0)
    t2.load(at(100))
    # A device that died during the downtime returns with a frozen old
    # timestamp; with its cadence known, it's flagged on the first poll instead
    # of waiting out the cap.
    t2.observe({"p": at(0)})
    assert t2.stale_paths({"p": at(0)}, at(100)) == {"p"}  # age 100 > 3*2 floored to 40


def test_persistence_snapshot_ignored_when_too_old(tmp_path) -> None:
    t = _tracker(data_dir=str(tmp_path), learning_max_age_s=1800.0)
    for s in (0, 2, 4):
        t.observe({"p": at(s)})
    t.save(at(10))

    t2 = _tracker(cap_s=600.0, data_dir=str(tmp_path), learning_max_age_s=1800.0)
    t2.load(at(3000))  # 2990s later, past the freshness window -> cold start
    t2.observe({"p": at(0)})
    # No seed -> the never-learned path uses the cap, not the (unknown) cadence.
    assert t2.stale_paths({"p": at(0)}, at(500)) == set()  # 500 < cap 600
    assert t2.stale_paths({"p": at(0)}, at(700)) == {"p"}


def test_persistence_disabled_writes_no_file(tmp_path) -> None:
    t = _tracker(data_dir=str(tmp_path), learning_max_age_s=0.0)
    for s in (0, 2, 4):
        t.observe({"p": at(s)})
    t.save(at(10))
    assert not (tmp_path / "signalk_staleness.json").exists()


def test_load_missing_or_corrupt_file_is_safe(tmp_path) -> None:
    t = _tracker(data_dir=str(tmp_path), learning_max_age_s=1800.0)
    t.load(BASE)  # no file
    (tmp_path / "signalk_staleness.json").write_text("{ not json")
    t.load(BASE)  # corrupt
    # Still functional, no seed.
    t.observe({"p": BASE})
    assert t.stale_paths({"p": BASE}, at(500)) == set()
    assert t.stale_paths({"p": BASE}, at(700)) == {"p"}


# --- fixes from the adversarial review --------------------------------------


def _leaf(value: object, ts: str) -> dict:
    return {"value": value, "timestamp": ts}


def _iso(seconds: float) -> str:
    return at(seconds).isoformat()


def test_recovery_after_stale() -> None:
    t = _tracker(floor_s=40.0, cap_s=600.0)
    for s in (0, 2, 4):
        t.observe({"p": at(s)})
    assert t.stale_paths({"p": at(4)}, at(4 + 100)) == {"p"}  # frozen -> stale
    t.observe({"p": at(200)})  # fresh data returns
    assert t.stale_paths({"p": at(200)}, at(200)) == set()  # recovered


def test_unobserved_seed_is_not_restamped_and_ages_out(tmp_path) -> None:
    t = _tracker(data_dir=str(tmp_path), learning_max_age_s=1800.0)
    for s in (0, 2, 4):
        t.observe({"p": at(s)})
    t.save(at(10))  # saved_at = at(10)

    t2 = _tracker(data_dir=str(tmp_path), learning_max_age_s=1800.0)
    t2.load(at(100))  # within window -> seeds "p"
    t2.save(at(5000))  # nothing observed this session -> must not re-stamp

    import json

    data = json.loads((tmp_path / "signalk_staleness.json").read_text())
    assert data["saved_at"] == at(10).isoformat()  # original stamp, not refreshed

    # Loading past the window from the ORIGINAL stamp cold-starts (no seed).
    t3 = _tracker(cap_s=600.0, data_dir=str(tmp_path), learning_max_age_s=1800.0)
    t3.load(at(5000))
    t3.observe({"p": at(0)})
    assert t3.stale_paths({"p": at(0)}, at(500)) == set()  # never-learned -> cap


def test_future_saved_at_is_rejected(tmp_path) -> None:
    t = _tracker(data_dir=str(tmp_path), learning_max_age_s=1800.0)
    for s in (0, 2, 4):
        t.observe({"p": at(s)})
    t.save(at(1000))  # snapshot stamped "in the future" vs the loader's clock

    t2 = _tracker(cap_s=600.0, data_dir=str(tmp_path), learning_max_age_s=1800.0)
    t2.load(at(0))  # now < saved_at -> negative age -> reject, no seed
    t2.observe({"p": at(0)})
    assert t2.stale_paths({"p": at(0)}, at(500)) == set()  # cap, not a seeded cadence
    assert t2.stale_paths({"p": at(0)}, at(700)) == {"p"}


def test_map_tree_withholds_dead_device_but_keeps_position() -> None:
    # Integration of the app-loop glue: a dead sounder's entity is withheld so
    # HA expire_after clears it; navigation.position (a device_tracker with no
    # expire_after) is exempt so it isn't frozen-hidden on the map.
    tracker = StalenessTracker(floor_s=40.0, cap_s=600.0)

    def tree(ts: str) -> dict:
        return {
            "environment": {"depth": {"belowKeel": _leaf(10.0, ts)}},
            "navigation": {"position": _leaf({"latitude": 1.0, "longitude": 2.0}, ts)},
        }

    for s in (0, 2, 4):  # warm up both paths to a ~2s cadence
        _map_tree(tracker, tree(_iso(s)), {}, (), False, at(s))
    ents = _map_tree(tracker, tree(_iso(4)), {}, (), False, at(204))
    assert "environment_depth_belowkeel" not in ents  # dead sounder withheld
    assert ents["navigation_position"]["component"] == "device_tracker"  # exempt


def test_map_tree_noop_disabled_and_recovers() -> None:
    tree_now = {"environment": {"depth": {"belowKeel": _leaf(10.0, _iso(0))}}}
    # tracker=None: nothing withheld, entities still produced from one flatten.
    assert "environment_depth_belowkeel" in _map_tree(
        None, tree_now, {}, (), False, at(0)
    )

    tracker = StalenessTracker(floor_s=40.0, cap_s=600.0)

    def tree(ts: str) -> dict:
        return {"environment": {"depth": {"belowKeel": _leaf(10.0, ts)}}}

    for s in (0, 2, 4):
        _map_tree(tracker, tree(_iso(s)), {}, (), False, at(s))
    dead = _map_tree(tracker, tree(_iso(4)), {}, (), False, at(204))
    assert "environment_depth_belowkeel" not in dead  # withheld
    back = _map_tree(tracker, tree(_iso(300)), {}, (), False, at(300))
    assert "environment_depth_belowkeel" in back  # fresh data -> recovered


def test_map_tree_catch_all_excludes_stale_paths() -> None:
    # publish_unmapped must build the catch-all from the SAME staleness-filtered
    # flat: a stale UNMAPPED leaf must not be republished (that would reset
    # expire_after and defeat staleness).
    tracker = StalenessTracker(floor_s=40.0, cap_s=600.0)
    key = "electrical_venus_totalpanelpower"  # unmapped -> catch-all

    def tree(ts: str) -> dict:
        return {"electrical": {"venus": {"totalPanelPower": _leaf(111.0, ts)}}}

    for s in (0, 2, 4):  # learn a ~2s cadence
        _map_tree(tracker, tree(_iso(s)), {}, (), False, at(s), True)
    stale_run = _map_tree(tracker, tree(_iso(4)), {}, (), False, at(204), True)
    assert key not in stale_run  # stale -> withheld from the catch-all too
    fresh_run = _map_tree(tracker, tree(_iso(300)), {}, (), False, at(300), True)
    assert key in fresh_run  # fresh again -> back as a diagnostic sensor


def test_map_tree_catch_all_skips_deduped_mapped_paths() -> None:
    # A value the mapper deduped (SoC reported at two mapped paths) must NOT
    # reappear as an (other) diagnostic via the catch-all.
    tree = {
        "electrical": {
            "batteries": {
                "house": {
                    "stateOfCharge": _leaf(0.87, _iso(0)),
                    "capacity": {"stateOfCharge": _leaf(0.87, _iso(0))},
                }
            }
        }
    }
    ents = _map_tree(None, tree, {}, (), False, at(0), True)
    assert [
        k for k, e in ents.items() if e.get("entity_category") == "diagnostic"
    ] == []
