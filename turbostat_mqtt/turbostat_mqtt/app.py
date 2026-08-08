# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Process plumbing: run turbostat, drive the publisher, own the exit codes.

One asyncio loop: aiomqtt for the broker, create_subprocess_exec for turbostat,
asyncio.Event for shutdown. There are no threads and no ``signal.signal``, so a
signal cannot be lost -- ``add_signal_handler`` rides ``set_wakeup_fd``, which
wakes the selector whenever the signal lands rather than needing the main thread
to be between bytecodes. The MQTT surface lives in publisher.py.

Exit codes are the interface to run.sh, which restarts us on any non-zero:
11 MQTT down past its timeout, 12 publishes stalled while samples flowed,
14 an unexpected fault. 0 means we were asked to stop.
"""

import argparse
import asyncio
import contextlib
import signal
import time

import aiomqtt

from . import __version__
from . import config as config_mod
from .parser import TurbostatParser, start_turbostat
from .publisher import Fault, Publisher
from .util import log

EXIT_SIGNALS = (signal.SIGTERM, signal.SIGINT)

EXIT_OK = 0
EXIT_MQTT_DOWN = 11
EXIT_PUBLISH_STALLED = 12
EXIT_UNEXPECTED = 14

# Broker reconnect backoff, doubling to a cap. Capped rather than flat: a broker
# down for hours would otherwise draw a fresh TCP connect -- and a fresh DNS
# lookup, when mqtt_host is a name -- every few seconds for the whole outage.
_RECONNECT_MIN_SECONDS = 3
_RECONNECT_MAX_SECONDS = 60

# How long to wait for turbostat to die on terminate() before killing it.
_TERM_TIMEOUT_SECONDS = 3

# Ceiling on any single aiomqtt operation. aiomqtt defaults to 10s, which is the
# whole budget the supervisor gives us to stop: a qos=1 publish awaiting a PUBACK
# and then the disconnect in __aexit__ can each burn the full 10s, so a broker
# that holds the TCP connection open without acking would push us past SIGKILL.
_MQTT_TIMEOUT_SECONDS = 5

# The farewell is best-effort and rides an even shorter leash. If the broker is
# not acking we are being killed regardless, and the retained last will already
# says "offline" -- which is exactly the case it exists for.
_FAREWELL_TIMEOUT_SECONDS = 2

# Faults the loop fixes in place by restarting turbostat, rather than exiting.
_RESTART_FAULTS = {Fault.NO_SAMPLES_SINCE_START, Fault.SAMPLE_TIMEOUT}

_FAULT_EXIT = {
    Fault.MQTT_DOWN: EXIT_MQTT_DOWN,
    Fault.PUBLISH_STALLED: EXIT_PUBLISH_STALLED,
}


class Turbostat:
    """The turbostat child process and the one task draining it.

    Restarts are debounced: a flapping turbostat must not be respawned faster
    than HA notices the gap it leaves.
    """

    def __init__(self, pub: Publisher) -> None:
        self.pub = pub
        self.proc: asyncio.subprocess.Process | None = None
        self._last_restart_attempt = 0.0

    async def restart(self, reason: str) -> bool:
        """Replace the running turbostat. False if the debounce declined."""
        o = self.pub.opts
        # Monotonic: a pure duration must not be defeated by a wall-clock step.
        now = asyncio.get_running_loop().time()
        if (
            reason != "initial_start"
            and (now - self._last_restart_attempt) < o.restart_grace_seconds
        ):
            log(
                "WARNING",
                f"Skipping turbostat restart (grace period) reason={reason}",
                o.log_level,
            )
            return False
        self._last_restart_attempt = now

        await self.stop()
        self.pub.on_turbostat_started()
        self.proc = await start_turbostat(o.interval)
        log(
            "INFO",
            f"Started turbostat: interval={o.interval}s reason={reason}",
            o.log_level,
        )
        return True

    async def stop(self) -> None:
        proc = self.proc
        self.proc = None
        if proc is None or proc.returncode is not None:
            return
        with contextlib.suppress(ProcessLookupError):
            proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=_TERM_TIMEOUT_SECONDS)
        except TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            await proc.wait()

    async def readline(self) -> str | None:
        """Next line, or None at EOF. Never blocks anything but this task."""
        proc = self.proc
        if proc is None or proc.stdout is None:
            return None
        raw = await proc.stdout.readline()
        if not raw:
            return None
        return raw.decode("utf-8", "replace")


async def _wait_or_stop(stop: asyncio.Event, seconds: float) -> None:
    """Sleep, but return early once shutdown is requested."""
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)


async def _watch_birth(mq: aiomqtt.Client, pub: Publisher) -> None:
    """Republish discovery when HA announces it has restarted."""
    async for message in mq.messages:
        if (
            message.payload
            and bytes(message.payload).decode(errors="replace").strip() == "online"
        ):
            pub.on_ha_birth()


async def _sample_loop(
    mq: aiomqtt.Client, pub: Publisher, ts: Turbostat, stop: asyncio.Event
) -> int:
    """Drain turbostat and publish, until shutdown or a fault we cannot fix here.

    Returns an exit code; 0 means shutdown was requested. Broker faults are left
    to propagate so the caller reconnects rather than treating them as fatal.
    """
    while not stop.is_set():
        now = asyncio.get_running_loop().time()
        fault = pub.check_watchdogs(time.time(), now)
        if fault in _RESTART_FAULTS:
            await ts.restart(fault.value)
        elif fault is not Fault.NONE:
            return _FAULT_EXIT[fault]

        await pub.maybe_heartbeat(mq, time.time())

        # Bounded so the watchdogs and heartbeat above still run while turbostat
        # is quiet -- and so shutdown is noticed without a sample to carry it.
        try:
            line = await asyncio.wait_for(ts.readline(), timeout=pub.opts.interval)
        except TimeoutError:
            continue

        if line is None:
            rc = ts.proc.returncode if ts.proc is not None else None
            if rc is not None:
                log("ERROR", f"turbostat exited rc={rc}", pub.opts.log_level)
            # The stream is done either way; waiting on the pid would just stall
            # until a watchdog fired.
            await ts.restart("process_eof")
            continue

        try:
            await pub.publish_sample(mq, line)
        except aiomqtt.MqttError:
            raise  # the session is gone; _reconnect_loop reconnects
        except Exception as e:  # noqa: BLE001 one bad sample must not end them all
            # Anything else is our own bug -- a column we mishandle, a payload
            # that will not serialise. Skip it: letting it out would exit the
            # process over a single malformed line.
            log("ERROR", f"sample failed: {e!r}", pub.opts.log_level)
    return EXIT_OK


async def _session(pub: Publisher, ts: Turbostat, stop: asyncio.Event) -> int:
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
            # broker refusing -- an ACL granting publish on our base topic but
            # not subscribe elsewhere is a normal lockdown for a stats-only
            # client. Neither may end the session: letting them out means we
            # reconnect and die here again every time, so no sample is ever
            # published. Costs the HA birth message; discovery is republished on
            # every reconnect anyway.
            log(
                "ERROR",
                f"cannot subscribe to {o.discovery_prefix}/status: {e};"
                " HA restart will not trigger rediscovery",
                o.log_level,
            )
        birth = asyncio.create_task(_watch_birth(mq, pub), name="birth")
        try:
            return await _sample_loop(mq, pub, ts, stop)
        finally:
            birth.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await birth
            # Supersede the will while the session is still up, on a short leash
            # so an unresponsive broker cannot spend our whole stop budget here.
            try:
                await asyncio.wait_for(
                    pub.publish_availability(mq, "offline"),
                    timeout=_FAREWELL_TIMEOUT_SECONDS,
                )
            except Exception as e:  # noqa: BLE001 shutdown must not raise
                log("WARNING", f"error during shutdown publish: {e!r}", o.log_level)


async def _reconnect_loop(pub: Publisher, ts: Turbostat, stop: asyncio.Event) -> int:
    """Hold a session up, reconnecting with a capped backoff when it drops."""
    delay = _RECONNECT_MIN_SECONDS
    while not stop.is_set():
        connected_at = pub.health.last_connect_ok
        try:
            return await _session(pub, ts, stop)
        except aiomqtt.MqttError as e:
            pub.on_disconnected()
            # A session that got as far as connecting starts the ramp over:
            # backoff is for a broker that is down, not one that dropped a
            # client it was happy to serve a moment ago.
            if pub.health.last_connect_ok != connected_at:
                delay = _RECONNECT_MIN_SECONDS
            log("WARNING", f"MQTT: {e}; reconnecting in {delay}s", pub.opts.log_level)
            # Checked here too: with no session the sample loop is not running,
            # so this is the only place the disconnect watchdog gets to fire.
            fault = pub.check_watchdogs(time.time(), asyncio.get_running_loop().time())
            if fault in _FAULT_EXIT:
                return _FAULT_EXIT[fault]
            await _wait_or_stop(stop, delay)
            delay = min(delay * 2, _RECONNECT_MAX_SECONDS)
    return EXIT_OK


async def _run() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--options", required=True)
    args = ap.parse_args()

    opts = config_mod.from_mapping(config_mod.read(args.options))
    log("INFO", f"Turbostat to MQTT v{__version__} starting", opts.log_level)
    log("INFO", opts.summary(), opts.log_level)

    pub = Publisher(opts, TurbostatParser())
    ts = Turbostat(pub)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in EXIT_SIGNALS:
        loop.add_signal_handler(signum, stop.set)

    try:
        await ts.restart("initial_start")
    except FileNotFoundError:
        log(
            "ERROR",
            "turbostat not found in container; check package install.",
            opts.log_level,
        )
        return EXIT_UNEXPECTED

    try:
        return await _reconnect_loop(pub, ts, stop)
    finally:
        await ts.stop()


def main() -> int:
    try:
        return asyncio.run(_run())
    except Exception as e:  # noqa: BLE001 supervisor safety net
        print(f"[ERROR] Main loop exception: {e!r}", flush=True)
        return EXIT_UNEXPECTED
