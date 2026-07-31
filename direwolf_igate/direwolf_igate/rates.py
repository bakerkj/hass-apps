# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Packets per minute derived from a cumulative counter."""

from collections import deque
from typing import NamedTuple

# A floor, not a ceiling -- see the trim in feed(). Long enough that a bursty
# band averages out and a beacon-driven counter has a predecessor to subtract.
RATE_WINDOW_S = 600.0

# How long a rate keeps describing "now" after its source goes quiet.
STALE_AFTER_CADENCES = 2.0

# Backstop: same-timestamp samples are never old enough to trim.
MAX_SAMPLES = 1024


class RateSample(NamedTuple):
    """An answer stamped with when and over what it was measured.

    Immutable, so a reader on another thread never sees a half-updated deque.
    """

    per_minute: float
    at: float
    cadence: float


class RateTracker:
    """Rate of a cumulative counter over a sliding window.

    The rise between the oldest sample still inside the window and the newest.
    The caller decides when to feed, so a beacon-driven counter and one polled
    every cycle share this code; a feed slower than the window falls back to
    the last two samples rather than to no answer.

    A counter that goes backwards was reset: history is dropped and the rate
    reads unknown, rather than going negative or spiking.
    """

    def __init__(self, window_s: float = RATE_WINDOW_S) -> None:
        self.window_s = window_s
        self._samples: deque[tuple[float, float]] = deque(maxlen=MAX_SAMPLES)
        self.latest: RateSample | None = None

    def feed(self, now_mono: float, value: float) -> None:
        if self._samples and value < self._samples[-1][1]:
            self._samples.clear()
        self._samples.append((now_mono, value))
        # Drop the head only while the next sample is old enough to start the
        # window. That alone keeps two, since with two left samples[1] is the
        # one just appended and its age is 0.
        while (
            len(self._samples) > 2 and now_mono - self._samples[1][0] >= self.window_s
        ):
            self._samples.popleft()
        self.latest = self._measure()

    def value_at(self, now_mono: float) -> float | None:
        """The rate as of ``now``, or ``None`` once it is too old to mean now.

        Only feeding refreshes the answer, so a source going quiet would
        otherwise leave its last busy figure standing forever.
        """
        s = self.latest
        if s is None or now_mono - s.at > s.cadence * STALE_AFTER_CADENCES:
            return None
        return s.per_minute

    def _measure(self) -> RateSample | None:
        if len(self._samples) < 2:
            return None
        (t0, v0) = self._samples[0]
        (t1, v1) = self._samples[-1]
        elapsed = t1 - t0
        # A sub-second span is a duplicate line or a clock artefact;
        # extrapolated to a minute it turns one packet into thousands.
        if elapsed < 1.0:
            return None
        return RateSample(
            per_minute=round((v1 - v0) / elapsed * 60.0, 2),
            at=t1,
            cadence=t1 - self._samples[-2][0],
        )

    @property
    def per_minute(self) -> float | None:
        """The last measured rate, ignoring staleness. Publishing uses
        value_at; this is for callers that do their own ageing."""
        return self.latest.per_minute if self.latest else None
