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

# Broker reconnect backoff, doubling to a cap. The tee runs regardless, so this
# only delays stats. Capped rather than flat: a broker down for hours would
# otherwise draw a fresh TCP connect -- and a fresh DNS lookup, when mqtt_host is
# a name -- every few seconds for the whole outage. Matches what paho did for us
# before (reconnect_delay_set(1, 30) plus a 5s->60s ramp on the initial connect).
_RECONNECT_MIN_SECONDS = 3
_RECONNECT_MAX_SECONDS = 60

# Ceiling on any single aiomqtt operation. aiomqtt defaults to 10s, which is the
# whole budget the supervisor gives us to stop: a qos=1 publish awaiting a PUBACK
# and then the disconnect in __aexit__ can each burn the full 10s, so a broker
# holding the TCP connection open without acking would push us past SIGKILL --
# the exact escalation this add-on's shutdown path exists to avoid.
_MQTT_TIMEOUT_SECONDS = 5

# The farewell is best-effort and rides an even shorter leash. If the broker is
# not acking we are being killed regardless, and the retained last will already
# says "offline" -- which is precisely the case it exists for.
_FAREWELL_TIMEOUT_SECONDS = 2


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
        try:
            await pub.tick(mq)
        except aiomqtt.MqttError:
            raise  # the session is gone; _run reconnects
        except Exception as e:  # noqa: BLE001 one bad cycle must not end them all
            # Anything else is our own bug -- a payload that will not serialise,
            # a parser state we mishandle. Skip the cycle and take the next one:
            # letting it out reaches main()'s catch-all, which degrades to a
            # plain pass-through for the rest of the process lifetime, so one
            # bad cycle would cost every sensor and both watchdogs until the
            # container restarts.
            log("ERROR", f"publish cycle failed: {e!r}", pub.opts.log_level)
        await _wait_or_stop(stop, pub.opts.interval)


async def _flush_and_say_goodbye(mq: aiomqtt.Client, pub: Publisher) -> None:
    """Last states, then the retained farewell that supersedes the will."""
    await pub.publish_states(mq)
    await pub.publish_availability(mq, "offline")


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
        timeout=_MQTT_TIMEOUT_SECONDS,
    ) as mq:
        log("INFO", f"MQTT connected to {o.mqtt_host}:{o.mqtt_port}", o.log_level)
        pub.on_connected()
        try:
            await mq.subscribe(f"{o.discovery_prefix}/status", qos=1)
        except (ValueError, aiomqtt.MqttError) as e:
            # ValueError is a malformed prefix (a config typo); MqttError is the
            # broker refusing -- an ACL that grants publish on our base topic but
            # not subscribe elsewhere is a normal lockdown for a stats-only
            # client. Neither may end the session: letting them out means _run
            # reconnects and dies here again every time, so the publisher never
            # reaches its first publish and no stats ever appear. Costs us the HA
            # birth message; discovery is still republished on every reconnect.
            # A genuinely dead session raises again on the first publish below,
            # which is the path that reconnects.
            log(
                "ERROR",
                f"cannot subscribe to {o.discovery_prefix}/status: {e};"
                " HA restart will not trigger rediscovery",
                o.log_level,
            )
        birth = asyncio.create_task(_watch_birth(mq, pub), name="birth")
        try:
            await _publish_loop(mq, pub, stop)
        finally:
            birth.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await birth
            # Flush current values, then supersede the will, while the session
            # is still up -- otherwise the retained states HA keeps while we are
            # away are a whole interval stale.
            # Bounded as one budget: an unresponsive broker must not spend our
            # whole stop allowance here.
            try:
                await asyncio.wait_for(
                    _flush_and_say_goodbye(mq, pub),
                    timeout=_FAREWELL_TIMEOUT_SECONDS,
                )
            except Exception as e:  # noqa: BLE001 shutdown must not raise
                log("WARNING", f"error during shutdown publish: {e!r}", o.log_level)


async def _reconnect_loop(pub: Publisher, stop: asyncio.Event) -> None:
    """Hold a session up, reconnecting with a capped backoff when it drops."""
    delay = _RECONNECT_MIN_SECONDS
    while not stop.is_set():
        connected_at = pub.health.last_connect_ok
        try:
            await _session(pub, stop)
        except aiomqtt.MqttError as e:
            pub.on_disconnected()
            # A session that got as far as connecting starts the ramp over:
            # backoff is for a broker that is down, not one that dropped a
            # client it was happy to serve a moment ago.
            if pub.health.last_connect_ok != connected_at:
                delay = _RECONNECT_MIN_SECONDS
            log("WARNING", f"MQTT: {e}; reconnecting in {delay}s", pub.opts.log_level)
            pub.check_watchdogs(asyncio.get_running_loop().time())
            await _wait_or_stop(stop, delay)
            delay = min(delay * 2, _RECONNECT_MAX_SECONDS)


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
        await _reconnect_loop(pub, stop)
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
