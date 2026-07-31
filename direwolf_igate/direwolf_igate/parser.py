# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Derives IGate statistics from direwolf's output stream.

Cumulative counters come from the ``<IGATE,...>`` status beacon -- an APRS
status packet, so a stable format, but only emitted on the IBEACON schedule
(``beacon_every``). Liveness (RF packet count, last-heard, audio level, link
state) comes from decode lines, where being current matters more than exact.

Unrecognised lines are ignored; callers tee the stream independently.
"""

import re
import time
from dataclasses import dataclass, field

from .rates import RateTracker

# "<IGATE,MSG_CNT=0,PKT_CNT=0,DIR_CNT=6,LOC_CNT=6,RF_CNT=39,UPL_CNT=592,DNL_CNT=396"
_IGATE_FIELD_RE = re.compile(r"([A-Z]+_CNT)=(\d+)")

# "[0.3] W1XM-15>APOT30:!4221.62N/07105.36Wr 06.3V 53C MIT APRS"
# The channel.decoder prefix marks an RF frame; our own packets use [0L]/[0H].
_RF_FRAME_RE = re.compile(r"^\[(\d+)\.(\d+)\]\s+([A-Z0-9\-]+)>")

# "[ig]", "[0L]", "[0H]" -- our own beacon appears under all of them.
_LOG_PREFIX = r"^\[[^\]]*\]\s+"

# qAC/qAR/qAS/qAo: stamped on by an APRS-IS server when it relays, so a packet
# carrying one did not originate here.
_Q_CONSTRUCT_RE = re.compile(r"(?:^|,)q[A-Za-z]{2}(?:,|$)")

# "W1XM-15 audio level = 77(11/11)    |||||||__"
_AUDIO_LEVEL_RE = re.compile(r"audio level = (\d+)")

# The server's answer to the login, not "Now connected" (which prints on TCP
# connect, before a wrong passcode is refused). "unverified" contains
# "verified", so test the negative first.
_LOGIN_REJECTED_RE = re.compile(r"logresp\s+\S+\s+unverified", re.IGNORECASE)
_LOGIN_OK_RE = re.compile(r"logresp\s+\S+\s+verified", re.IGNORECASE)
_DISCONNECTED_RE = re.compile(
    r"Connection to IGate server .* lost|Disconnected from IGate server"
)


@dataclass
class IGateStats:
    """Current view of the IGate. ``None`` means "not yet observed"."""

    # From the <IGATE> status beacon -- direwolf's own cumulative accounting.
    packets_uploaded: int | None = None
    packets_downloaded: int | None = None
    # Station counts over direwolf's rolling window, not packet totals.
    stations_heard: int | None = None
    stations_heard_direct: int | None = None

    # Derived here, in real time, from decode lines.
    rf_packets_received: int = 0
    last_heard: float | None = None
    last_audio_level: int | None = None
    igate_connected: bool = False

    # Packets per minute over a sliding window. Unlike the totals these survive
    # a restart of the add-on meaningfully: they say what the gate is doing now
    # rather than how much it happened to do since it last came up. ``None``
    # until two samples exist.
    uploaded_rate: float | None = None
    downloaded_rate: float | None = None
    rf_rate: float | None = None

    # The set belongs to the reading thread; the publisher reads only
    # ``stations_seen``, an immutable snapshot swapped in one attribute store.
    _callsigns: set[str] = field(default_factory=set, repr=False)
    stations_seen: frozenset[str] = field(default_factory=frozenset)

    @property
    def stations_seen_total(self) -> int:
        return len(self.stations_seen)

    def note_station(self, call: str) -> None:
        """Record a station. Call only from the direwolf-reading thread."""
        if call in self._callsigns:
            return
        self._callsigns.add(call)
        self.stations_seen = frozenset(self._callsigns)


class DirewolfParser:
    """Feed it direwolf's output one line at a time; read ``stats``.

    Other stations' ``<IGATE>`` beacons arrive constantly from APRS-IS, so a
    status beacon counts only when its source callsign is ``mycall``.
    """

    def __init__(self, mycall: str) -> None:
        self.mycall = mycall.upper()
        self.stats = IGateStats()
        # Anchored, because a callsign is not a credential: third-party format
        # ("}SRC>PATH:payload") lets any station nest ours mid-line. The path is
        # captured rather than forbidden -- our own beacon may carry TCPIP* or a
        # via path -- and screened for a q-construct below.
        self._own_igate_re = re.compile(
            _LOG_PREFIX + rf"{re.escape(self.mycall)}>([A-Z0-9\-]+(?:,[^:]*)?):<IGATE,"
        )
        # One thread per deque: feed() owns the APRS-IS trackers, sample_rates()
        # the RF one. Only RateTracker.latest crosses threads, immutably.
        self._upl_rate = RateTracker()
        self._dnl_rate = RateTracker()
        self._rf_rate = RateTracker()

    def sample_rates(self, now_mono: float | None = None) -> None:
        """Refresh all three rates. Call on a timer, from one thread.

        Rates need a clock, not a stream: nothing in the output marks quiet
        time passing, so a rate driven only by ``feed`` would hold its last
        busy value exactly when the gate has stopped working.
        """
        now = time.monotonic() if now_mono is None else now_mono
        self._rf_rate.feed(now, self.stats.rf_packets_received)
        s = self.stats
        s.rf_rate = self._rf_rate.value_at(now)
        s.uploaded_rate = self._upl_rate.value_at(now)
        s.downloaded_rate = self._dnl_rate.value_at(now)

    def feed(self, line: str) -> None:
        line = line.rstrip("\n")

        m = _RF_FRAME_RE.match(line)
        if m and "<IGATE," in line:
            # Another IGate heard over the air: ordinary RF traffic for the
            # liveness stats, never our own accounting.
            self._count_rf_frame(m)
            return

        if "<IGATE," in line:
            m = self._own_igate_re.match(line)
            if m and not _Q_CONSTRUCT_RE.search(m.group(1)):
                self._apply_igate_status(line)
            return

        if m:
            self._count_rf_frame(m)
            return

        m = _AUDIO_LEVEL_RE.search(line)
        if m:
            self.stats.last_audio_level = int(m.group(1))
            return

        if _LOGIN_REJECTED_RE.search(line):
            self.stats.igate_connected = False
            return

        if _LOGIN_OK_RE.search(line):
            self.stats.igate_connected = True
            return

        if _DISCONNECTED_RE.search(line):
            self.stats.igate_connected = False

    def _count_rf_frame(self, m: re.Match[str]) -> None:
        self.stats.rf_packets_received += 1
        self.stats.last_heard = time.time()
        self.stats.note_station(m.group(3))

    def _apply_igate_status(self, line: str) -> None:
        fields = {k: int(v) for k, v in _IGATE_FIELD_RE.findall(line)}
        s = self.stats
        now = time.monotonic()
        # Feeds the trackers only; sample_rates publishes, so one writer.
        if "UPL_CNT" in fields:
            s.packets_uploaded = fields["UPL_CNT"]
            self._upl_rate.feed(now, s.packets_uploaded)
        if "DNL_CNT" in fields:
            s.packets_downloaded = fields["DNL_CNT"]
            self._dnl_rate.feed(now, s.packets_downloaded)
        if "RF_CNT" in fields:
            s.stations_heard = fields["RF_CNT"]
        if "DIR_CNT" in fields:
            s.stations_heard_direct = fields["DIR_CNT"]
        # Deliberately does not touch igate_connected: direwolf emits this
        # beacon whether or not APRS-IS accepted the login.
