# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Adaptive, per-path staleness tracking.

Signal K keeps a value after its source stops broadcasting -- it only freezes
the ``timestamp``. So "device went away" shows up as a leaf whose timestamp
stops advancing while its value lingers. This tracker learns each path's normal
refresh cadence (from the gaps between its timestamps) and flags a path stale
once its value is much older than that cadence. The caller drops stale paths
from the publish set, and Home Assistant's ``expire_after`` then marks the
entity unavailable -- recovering automatically when fresh data returns.

The key property is that ``timestamp`` tracks *broadcast* cadence, not value
*change*: N2K devices rebroadcast on a fixed schedule even when the number is
static, so a slow-changing value (engine hours, tank level) still refreshes
often and is never wrongly expired. Only a source that actually stopped goes
stale.
"""

import datetime
import json
import logging
import os
from collections import deque
from typing import Any

log = logging.getLogger(__name__)

# Expire a path at this multiple of its learned refresh interval.
_K = 3.0
# Recent inter-update gaps kept per path; the learned interval is their max, so
# normal jitter (a slow poll, a momentarily busy bus) doesn't flap a healthy
# sensor. Old anomalies age out after this many refreshes.
_WINDOW = 20
# Refreshes needed before the learned cadence is trusted.
_WARMUP = 2
# Event-driven paths whose timestamp legitimately stops advancing while normal
# (a quiescent alarm must stay OFF, not flip to unavailable).
_EXEMPT_PREFIXES = ("notifications.",)

_SNAPSHOT_FILE = "signalk_staleness.json"


def parse_ts(ts: Any) -> datetime.datetime | None:
    """Parse a Signal K ISO-8601 timestamp into a tz-aware datetime, or None.

    A timezone-less string parses to a *naive* datetime, which would raise
    ``TypeError`` the moment it is compared or subtracted against our aware
    ``now`` -- inside the poll loop that throws every cycle and takes the whole
    bridge offline, and at startup it crash-loops ``load()``. So a naive (or
    unparsable) result is rejected rather than returned.
    """
    if not isinstance(ts, str):
        return None
    try:
        parsed = datetime.datetime.fromisoformat(ts)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


class _PathState:
    __slots__ = ("gaps", "last")

    def __init__(self, last: datetime.datetime) -> None:
        self.last = last
        self.gaps: deque[float] = deque(maxlen=_WINDOW)

    def interval(self) -> float | None:
        """Learned refresh interval (max recent gap), or None until warmed up."""
        return max(self.gaps) if len(self.gaps) >= _WARMUP else None


class StalenessTracker:
    """Learns per-path refresh cadence and reports which paths are stale.

    Held in memory across poll cycles. A freshness-bounded snapshot of the
    learned intervals is optionally persisted so a *brief* restart can judge a
    path against its known cadence immediately (catching a device that died
    during the downtime on the first poll instead of waiting out ``cap_s``); a
    long outage self-invalidates the snapshot and the tracker cold-starts.
    """

    def __init__(
        self,
        *,
        floor_s: float,
        cap_s: float,
        data_dir: str = "/data",
        learning_max_age_s: float = 0.0,
    ) -> None:
        self._floor = float(floor_s)
        self._cap = float(cap_s)
        self._data_dir = data_dir
        self._learning_max_age = float(learning_max_age_s)
        self._paths: dict[str, _PathState] = {}
        # Intervals seeded from a recent on-disk snapshot; used for paths that
        # have not refreshed since startup so they aren't stuck on the cap.
        self._seed: dict[str, float] = {}

    def observe(self, ts_map: dict[str, datetime.datetime]) -> None:
        """Record this cycle's timestamps, learning each path's cadence."""
        for path, ts in ts_map.items():
            st = self._paths.get(path)
            if st is None:
                self._paths[path] = _PathState(ts)
            elif ts > st.last:
                st.gaps.append((ts - st.last).total_seconds())
                st.last = ts

    def _threshold(self, path: str) -> float:
        st = self._paths.get(path)
        interval = st.interval() if st is not None else None
        if interval is None:
            interval = self._seed.get(path)
        if interval is None:
            # Never learned (e.g. a device already dead at startup that never
            # refreshes): the absolute cap is the only backstop.
            return self._cap
        # Bound to [floor, cap]. The cap is the maximum detection latency the
        # operator accepts, so no path -- learned or not -- exceeds it, and an
        # anomalous gap can't inflate a path's latency without limit. The cap
        # must therefore be set above the slowest device's broadcast interval.
        return min(max(_K * interval, self._floor), self._cap)

    def stale_paths(
        self, ts_map: dict[str, datetime.datetime], now: datetime.datetime
    ) -> set[str]:
        """Paths whose value is older than their allowed staleness threshold."""
        if self._cap <= 0:
            return set()
        stale: set[str] = set()
        for path, ts in ts_map.items():
            if path.startswith(_EXEMPT_PREFIXES):
                continue
            if (now - ts).total_seconds() > self._threshold(path):
                stale.add(path)
        return stale

    # --- freshness-bounded persistence -------------------------------------

    def _file(self) -> str:
        return os.path.join(self._data_dir, _SNAPSHOT_FILE)

    def load(self, now: datetime.datetime) -> None:
        """Seed learned intervals from a snapshot iff it is recent enough."""
        if self._learning_max_age <= 0:
            return
        try:
            with open(self._file(), encoding="utf-8") as f:
                data = json.load(f)
        except OSError, json.JSONDecodeError:
            return
        saved = parse_ts(data.get("saved_at")) if isinstance(data, dict) else None
        if saved is None:
            return
        age = (now - saved).total_seconds()
        # Reject too-old (a long outage must cold-start) AND future-dated
        # snapshots: an RTC-less Pi can boot with its clock behind the saved_at,
        # making age negative -- which must not sneak a stale snapshot past the
        # freshness window.
        if not 0 <= age <= self._learning_max_age:
            return  # cold start
        intervals = data.get("intervals")
        if isinstance(intervals, dict):
            self._seed = {
                str(p): float(v)
                for p, v in intervals.items()
                if isinstance(v, (int, float)) and not isinstance(v, bool) and v > 0
            }
            log.info(
                "Loaded %d learned cadence(s) from a %.0fs-old snapshot",
                len(self._seed),
                (now - saved).total_seconds(),
            )

    def save(self, now: datetime.datetime) -> None:
        """Atomically persist learned intervals with a freshness stamp."""
        if self._learning_max_age <= 0:
            return
        # Persist ONLY paths actually re-learned this session, NOT the seeds
        # loaded from disk. Re-emitting an unobserved seed with a fresh saved_at
        # would let a dead device's cadence live forever across brief restarts,
        # defeating the freshness window (a long outage must not keep learning).
        intervals: dict[str, float] = {}
        for path, st in self._paths.items():
            iv = st.interval()
            if iv is not None:
                intervals[path] = iv
        if not intervals:
            return
        payload = {"saved_at": now.isoformat(), "intervals": intervals}
        # Per-process temp so two instances sharing a data_dir can't interleave
        # into one another's half-written file before the atomic replace.
        tmp = f"{self._file()}.{os.getpid()}.tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f)
            os.replace(tmp, self._file())
        except OSError as exc:
            log.debug("Could not persist staleness snapshot: %s", exc)
