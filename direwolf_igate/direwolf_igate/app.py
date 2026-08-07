# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Process plumbing: tee direwolf's stream to the add-on log, drive the publisher.

This process sits in a pipe between direwolf and the log, so it must never exit:
that would SIGPIPE direwolf and stop the IGate over a metrics fault. Every line
is teed before anything else runs, and unexpected failures degrade to a plain
pass-through. The MQTT surface lives in publisher.py.

One asyncio loop, aiohttp-style: aiomqtt for the broker, connect_read_pipe for
direwolf's output, asyncio.Event for shutdown. There are no threads and no
``signal.signal``, so the tee cannot stall on a broker fault and a signal cannot
be lost -- ``add_signal_handler`` rides ``set_wakeup_fd``, which wakes the
selector whenever the signal lands rather than needing the main thread to be
between bytecodes.
"""

import argparse
import asyncio
import contextlib
import signal
import sys

import aiomqtt

from . import __version__
from . import config as config_mod
from .parser import DirewolfParser
from .publisher import Publisher
from .util import log

EXIT_SIGNALS = (signal.SIGTERM, signal.SIGINT)

# Broker reconnect backoff. The tee runs regardless, so this only delays stats.
_RECONNECT_SECONDS = 3


def main() -> int:
    """Never die of our own accord: any failure degrades to a pass-through."""
    _reconfigure_stdio()
    try:
        return asyncio.run(_run())
    except Exception as e:  # noqa: BLE001 last line of defence; see module docstring
        print(
            f"[WARNING] statistics publisher failed ({e!r}); "
            f"continuing as a plain pass-through",
            flush=True,
        )
        # Closing the loop restores SIGTERM to SIG_DFL but SIGINT only to
        # default_int_handler, which is still a Python-level handler and so
        # still loseable in the blocking read below.
        _restore_default_signal_handlers()
        try:
            tee_only()
        except Exception:  # noqa: BLE001,S110 stdout is gone; nothing left to try
            pass
        return 0


def _restore_default_signal_handlers() -> None:
    for signum in EXIT_SIGNALS:
        try:
            signal.signal(signum, signal.SIG_DFL)
        except OSError, ValueError:
            pass


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


async def _wait_or_stop(stop: asyncio.Event, seconds: float) -> None:
    """Sleep, but return early once shutdown is requested."""
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)


async def _tee(pub: Publisher, stop: asyncio.Event) -> None:
    """Drain direwolf forever. Never gated on MQTT: if this stops, the pipe
    fills at 64 KiB and direwolf blocks on write, taking the gateway off air."""
    reader = asyncio.StreamReader()
    await asyncio.get_running_loop().connect_read_pipe(
        lambda: asyncio.StreamReaderProtocol(reader), sys.stdin.buffer
    )
    while line := await reader.readline():
        # Tee first and byte-exact: the log must not depend on anything below
        # succeeding, and an 8-bit APRS payload must round-trip untouched.
        sys.stdout.buffer.write(line)
        sys.stdout.buffer.flush()
        pub.feed_observed(line.decode("utf-8", "surrogateescape"))
    stop.set()  # EOF: direwolf is gone, so we are done


async def _watch_birth(mq: aiomqtt.Client, pub: Publisher) -> None:
    """Republish discovery when HA announces it has restarted."""
    async for message in mq.messages:
        if (
            message.payload
            and bytes(message.payload).decode(errors="replace").strip() == "online"
        ):
            pub.on_ha_birth()


async def _publish_loop(
    mq: aiomqtt.Client, pub: Publisher, stop: asyncio.Event
) -> None:
    """Publish until shutdown or a broker fault, which the caller reconnects."""
    while not stop.is_set():
        await pub.tick(mq)
        await _wait_or_stop(stop, pub.opts.interval)


async def _session(pub: Publisher, stop: asyncio.Event) -> None:
    """One connected session: subscribe, publish, and surface faults."""
    o = pub.opts
    async with aiomqtt.Client(
        hostname=o.mqtt_host,
        port=o.mqtt_port,
        username=o.mqtt_username or None,
        password=o.mqtt_password or None,
        identifier=o.client_id,
        will=aiomqtt.Will(
            topic=o.availability_topic, payload=b"offline", qos=1, retain=True
        ),
        keepalive=60,
    ) as mq:
        log("INFO", f"MQTT connected to {o.mqtt_host}:{o.mqtt_port}", o.log_level)
        pub.on_connected()
        await mq.subscribe(f"{o.discovery_prefix}/status", qos=1)
        birth = asyncio.create_task(_watch_birth(mq, pub), name="birth")
        try:
            await _publish_loop(mq, pub, stop)
        finally:
            birth.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await birth
            # Supersede the will while the session is still up.
            with contextlib.suppress(aiomqtt.MqttError):
                await pub.publish_availability(mq, "offline")


async def _run() -> int:
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
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in EXIT_SIGNALS:
        loop.add_signal_handler(signum, stop.set)

    tee = asyncio.create_task(_tee(pub, stop), name="tee")
    try:
        while not stop.is_set():
            try:
                await _session(pub, stop)
            except aiomqtt.MqttError as e:
                pub.on_disconnected()
                log(
                    "WARNING",
                    f"MQTT: {e}; reconnecting in {_RECONNECT_SECONDS}s",
                    opts.log_level,
                )
                pub.check_watchdogs(asyncio.get_running_loop().time())
                await _wait_or_stop(stop, _RECONNECT_SECONDS)
    finally:
        tee.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await tee
        if not tee.cancelled() and (tee_error := tee.exception()) is not None:
            # main()'s pass-through fallback covers a broken tee.
            raise tee_error

    return 0


def tee_only() -> None:
    for line in sys.stdin:
        sys.stdout.write(line)
        sys.stdout.flush()
