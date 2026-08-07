# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Per-path MQTT publish rate limiter.

Signal K deltas arrive at the source's own cadence (sub-second for GPS, wind,
RPM), which is more MQTT churn than any downstream consumer wants. This module
caps how often a given Signal K path may republish while never dropping the
latest observed value: an update inside the cap window is remembered and
emitted as soon as the window opens.

Not a debouncer -- it's a leaky bucket with size one. The last value in wins.
Order-preserving across paths is unnecessary because each path is independent.
"""

import fnmatch
import math
from dataclasses import dataclass, field
from typing import Any


@dataclass
class _Slot:
    """One path's rate-limit state.

    ``last_publish_at`` is monotonic seconds; ``-inf`` means "never
    published, next update goes through immediately" -- can't be 0.0
    because a caller passing ``now=0.0`` (fine for tests) would then be
    inside the cap window instead of past it. ``pending`` carries the
    most recent value observed inside the cap window, or is absent when
    the slot is idle.
    """

    last_publish_at: float = -math.inf
    pending: Any = None
    has_pending: bool = False


@dataclass
class PublishRateLimiter:
    """Cap per-path publish rate; publish the latest value at window open.

    ``default_interval`` applies to any path with no override. ``overrides``
    maps an fnmatch-style path pattern to an interval; the first pattern
    that matches wins, so put more specific patterns first.
    """

    default_interval: float
    overrides: list[tuple[str, float]] = field(default_factory=list)
    _slots: dict[str, _Slot] = field(default_factory=dict)

    def interval_for(self, path: str) -> float:
        for pattern, interval in self.overrides:
            if fnmatch.fnmatchcase(path, pattern):
                return interval
        return self.default_interval

    def offer(self, path: str, value: Any, now: float) -> Any | None:
        """Present a fresh value for ``path``. Returns the value to publish
        now, or None if the cap window is still open.

        The caller is expected to publish the returned value verbatim. A
        ``None`` return does not mean "no value" -- ``None`` is not a legal
        Signal K state -- it means "hold; the pending buffer has recorded
        it and :meth:`due` will surface it once the window opens."
        """
        slot = self._slots.setdefault(path, _Slot())
        window = self.interval_for(path)
        if now - slot.last_publish_at >= window:
            slot.last_publish_at = now
            slot.pending = None
            slot.has_pending = False
            return value
        slot.pending = value
        slot.has_pending = True
        return None

    def due(self, now: float) -> dict[str, Any]:
        """Paths whose cap window has now opened while a value was pending.

        Called on the publish tick to flush anything buffered by :meth:`offer`
        during the last cap window.
        """
        out: dict[str, Any] = {}
        for path, slot in self._slots.items():
            if not slot.has_pending:
                continue
            window = self.interval_for(path)
            if now - slot.last_publish_at < window:
                continue
            out[path] = slot.pending
            slot.last_publish_at = now
            slot.pending = None
            slot.has_pending = False
        return out

    def forget(self, path: str) -> None:
        """Drop all state for ``path``. Used when an entity is unregistered
        (e.g., an AIS target expired) so its slot doesn't accumulate."""
        self._slots.pop(path, None)


def build_overrides(spec: dict[str, float] | None) -> list[tuple[str, float]]:
    """Compile a config-shaped ``{pattern: interval}`` map into a stable
    ordered list. Longer (more specific) patterns are tried first so that
    ``navigation.position`` beats ``navigation.*`` on a matching key.
    """
    if not spec:
        return []
    return sorted(
        (
            (p, float(i))
            for p, i in spec.items()
            if isinstance(p, str) and i is not None
        ),
        key=lambda pair: (-len(pair[0]), pair[0]),
    )
