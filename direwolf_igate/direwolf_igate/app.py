# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Process plumbing: tee direwolf's stream to the add-on log, drive the publisher.

This process sits in a pipe between direwolf and the log, so it must never exit:
that would SIGPIPE direwolf and stop the IGate over a metrics fault. Every line
is teed before anything else runs, and unexpected failures degrade to a plain
pass-through. The MQTT side lives in publisher.py.
"""

import argparse
import os
import signal
import sys
from types import FrameType

from . import __version__
from . import config as config_mod
from .parser import DirewolfParser
from .publisher import Publisher
from .util import log


def main() -> int:
    """Never die of our own accord: any failure degrades to a pass-through."""
    _reconfigure_stdio()
    try:
        return _run()
    except Exception as e:  # noqa: BLE001 last line of defence; see module docstring
        print(
            f"[WARNING] statistics publisher failed ({e!r}); "
            f"continuing as a plain pass-through",
            flush=True,
        )
        try:
            tee_only()
        except Exception:  # noqa: BLE001,S110 stdout is gone; nothing left to try
            pass
        return 0


def _reconfigure_stdio() -> None:
    """Decode with surrogateescape, explicitly rather than by locale accident.

    APRS payloads are 8-bit: a bearing report gated down from APRS-IS carries a
    Latin-1 degree sign. Strict decoding would raise out of the read loop and
    kill the gateway. stdout must match so the bytes can be written back.
    """
    for stream in (sys.stdin, sys.stdout):
        try:
            stream.reconfigure(errors="surrogateescape")  # type: ignore[union-attr]
        except Exception:  # noqa: BLE001,S110 not a TextIO (tests); keep defaults
            pass


def _run() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--options", required=True)
    ap.add_argument(
        "--mycall",
        required=True,
        help="Own callsign; used to reject other stations' <IGATE> beacons.",
    )
    args = ap.parse_args()

    # A bad options file degrades to a pass-through; raising here would SIGPIPE
    # direwolf over a metrics config.
    try:
        raw = config_mod.read(args.options)
    except (OSError, ValueError) as e:
        print(f"[WARNING] cannot read {args.options}: {e}; statistics disabled")
        tee_only()
        return 0

    if not config_mod.enabled(raw):
        tee_only()
        return 0

    try:
        opts = config_mod.from_mapping(raw)
    except (TypeError, ValueError) as e:
        print(f"[WARNING] bad MQTT option ({e}); statistics disabled")
        tee_only()
        return 0

    log("INFO", f"Direwolf IGate MQTT v{__version__} starting", opts.log_level)
    log("INFO", opts.summary(args.mycall), opts.log_level)

    pub = Publisher(opts, DirewolfParser(args.mycall))
    pub.start()

    def shutdown(signum: int, _frame: FrameType | None) -> None:
        """Publish the farewell, then re-raise for the conventional exit status.

        The read loop never consults the stop event, so the ``finally`` below is
        unreachable on a signal and the retained "offline" has to go out here.
        """
        try:
            pub.shutdown(farewell=True)
        finally:
            signal.signal(signum, signal.SIG_DFL)
            os.kill(os.getpid(), signum)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    try:
        for line in sys.stdin:
            # Tee first: the log must not depend on anything below succeeding.
            sys.stdout.write(line)
            sys.stdout.flush()
            pub.feed_observed(line)
    finally:
        # EOF path; the signal handler publishes its own farewell.
        pub.shutdown(farewell=True)

    return 0


def tee_only() -> None:
    for line in sys.stdin:
        sys.stdout.write(line)
        sys.stdout.flush()
