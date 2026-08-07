# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for the per-path publish rate limiter."""

from signalk_bridge.ratelimit import PublishRateLimiter, build_overrides


def _limiter(default: float = 1.0, overrides=None) -> PublishRateLimiter:
    return PublishRateLimiter(
        default_interval=default,
        overrides=build_overrides(overrides or {}),
    )


def test_first_offer_publishes_immediately() -> None:
    lim = _limiter()
    assert lim.offer("propulsion.0.revolutions", 42, now=0.0) == 42


def test_second_offer_inside_window_is_held() -> None:
    lim = _limiter(default=1.0)
    lim.offer("propulsion.0.revolutions", 42, now=0.0)
    assert lim.offer("propulsion.0.revolutions", 43, now=0.5) is None
    assert lim.offer("propulsion.0.revolutions", 44, now=0.9) is None


def test_due_flushes_latest_value_at_window_open() -> None:
    lim = _limiter(default=1.0)
    lim.offer("propulsion.0.revolutions", 42, now=0.0)
    lim.offer("propulsion.0.revolutions", 43, now=0.5)
    lim.offer("propulsion.0.revolutions", 44, now=0.9)
    # Only the LATEST held value surfaces, not each held delta.
    assert lim.due(now=1.0) == {"propulsion.0.revolutions": 44}
    # And it does not re-fire on a later tick with no fresh offer.
    assert lim.due(now=2.5) == {}


def test_due_does_not_flush_before_window_opens() -> None:
    lim = _limiter(default=1.0)
    lim.offer("navigation.speedOverGround", 5.0, now=0.0)
    lim.offer("navigation.speedOverGround", 5.1, now=0.4)
    assert lim.due(now=0.9) == {}
    assert lim.due(now=1.0) == {"navigation.speedOverGround": 5.1}


def test_per_path_override_beats_default() -> None:
    lim = _limiter(default=5.0, overrides={"navigation.position": 0.5})
    assert lim.offer("navigation.position", "fix", now=0.0) == "fix"
    # 0.5s cap, so at 0.6s the next offer is fresh.
    assert lim.offer("navigation.position", "fix2", now=0.6) == "fix2"
    # Same time on a non-overridden path uses the 5.0s default.
    assert lim.offer("environment.wind.speedTrue", 3.2, now=0.0) == 3.2
    assert lim.offer("environment.wind.speedTrue", 3.3, now=0.6) is None


def test_more_specific_glob_wins_over_generic() -> None:
    lim = _limiter(
        default=5.0,
        overrides={
            "electrical.batteries.*.voltage": 2.0,
            "electrical.*": 10.0,
        },
    )
    # The narrower pattern wins even though both match.
    assert lim.interval_for("electrical.batteries.house.voltage") == 2.0
    # The generic pattern still applies where the specific one doesn't match.
    assert lim.interval_for("electrical.solar.chargers.0.power") == 10.0


def test_paths_are_independent() -> None:
    lim = _limiter(default=1.0)
    lim.offer("a.b", 1, now=0.0)
    # A separate path is unaffected by a's window.
    assert lim.offer("c.d", 9, now=0.1) == 9


def test_forget_drops_slot_state() -> None:
    lim = _limiter(default=1.0)
    lim.offer("vessels.urn:mrn:imo:mmsi:12345.navigation.position", "fix", now=0.0)
    lim.offer("vessels.urn:mrn:imo:mmsi:12345.navigation.position", "fix2", now=0.3)
    lim.forget("vessels.urn:mrn:imo:mmsi:12345.navigation.position")
    # Forgotten path publishes immediately -- as if never seen.
    assert (
        lim.offer("vessels.urn:mrn:imo:mmsi:12345.navigation.position", "fix3", now=0.4)
        == "fix3"
    )
    # And no stale pending value resurfaces on a later tick -- the fresh
    # offer above went through, so the slot is idle.
    assert lim.due(now=10.0) == {}


def test_build_overrides_ignores_garbage() -> None:
    out = build_overrides({"good.path": 1.0, 42: 2.0, "bad": None})  # type: ignore[dict-item]
    assert ("good.path", 1.0) in out
    # Non-string keys and None intervals are dropped rather than crashing.
    assert all(isinstance(p, str) and i is not None for p, i in out)


def test_build_overrides_returns_empty_for_none() -> None:
    assert build_overrides(None) == []
    assert build_overrides({}) == []
