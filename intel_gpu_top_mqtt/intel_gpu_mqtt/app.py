# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Process plumbing: run intel_gpu_top, drive the publisher, own the exit codes.

One asyncio loop: aiomqtt for the broker, create_subprocess_exec for
intel_gpu_top, asyncio.Event for shutdown. There are no threads and no
``signal.signal``, so a signal cannot be lost -- ``add_signal_handler`` rides
``set_wakeup_fd``, which wakes the selector whenever the signal lands rather than
needing the main thread to be between bytecodes. The MQTT surface lives in
publisher.py.

Exit codes are the interface to run.sh, which restarts us on any non-zero:
2 intel_gpu_top is missing, 11 MQTT down past its timeout, 14 an unexpected
fault. 0 means we were asked to stop.
"""

import argparse
import asyncio
import contextlib
import logging
import signal
import time

import aiomqtt

from . import __version__
from . import config as config_mod
from .device import (
    auto_select_device_arg,
    list_intel_gpu_top_devices,
    start_intel_gpu_top,
)
from .publisher import Fault, Publisher

EXIT_SIGNALS = (signal.SIGTERM, signal.SIGINT)

EXIT_OK = 0
EXIT_NO_BINARY = 2
EXIT_MQTT_DOWN = 11
EXIT_UNEXPECTED = 14

# Broker reconnect backoff, doubling to a cap. Capped rather than flat: a broker
# down for hours would otherwise draw a fresh TCP connect -- and a fresh DNS
# lookup, when mqtt_host is a name -- every few seconds for the whole outage.
_RECONNECT_MIN_SECONDS = 3
_RECONNECT_MAX_SECONDS = 60

# How long to wait for intel_gpu_top to die on terminate() before killing it.
_TERM_TIMEOUT_SECONDS = 3

# Ceiling on any single aiomqtt operation. aiomqtt defaults to 10s, which is the
# whole budget the supervisor gives us to stop: a qos=1 publish awaiting a PUBACK
# and the disconnect in __aexit__ can each burn the full 10s, so a broker holding
# the TCP connection open without acking would push us past SIGKILL.
_MQTT_TIMEOUT_SECONDS = 5

# SUBSCRIBE needs a SUBACK too, and it runs at session start where a SIGTERM
# cannot shorten it -- a signal landing mid-subscribe still pays the remaining
# wait before shutdown begins. Bounded separately so it stays inside the budget.
_SUBSCRIBE_TIMEOUT_SECONDS = 2

# The farewell is best-effort and rides an even shorter leash. If the broker is
# not acking we are being killed regardless, and the retained last will already
# says "offline" -- precisely the case it exists for.
_FAREWELL_TIMEOUT_SECONDS = 2

# Cap on stderr kept for the exit diagnostic, so a chatty failing binary cannot
# grow this without bound.
_STDERR_TAIL_BYTES = 4000

# Faults the loop fixes in place by restarting intel_gpu_top, rather than exiting.
_RESTART_FAULTS = {Fault.RENDER_NODE_GONE, Fault.SAMPLE_TIMEOUT}

_FAULT_EXIT = {Fault.MQTT_DOWN: EXIT_MQTT_DOWN}

# Returned when a pass produced no line: either the wait elapsed or shutdown was
# requested. Distinct from None, which means EOF and calls for a restart.
_NO_LINE = object()


class GpuTop:
    """The intel_gpu_top child process, its device selection, and its restarts.

    Restarts are debounced: a flapping binary must not be respawned faster than
    HA notices the gap it leaves.
    """

    def __init__(self, pub: Publisher, log: logging.Logger) -> None:
        self.pub = pub
        self.log = log
        self.proc: asyncio.subprocess.Process | None = None
        self._last_restart_attempt = 0.0
        self._stderr_task: asyncio.Task | None = None
        self._read_task: asyncio.Task | None = None
        self.stderr_tail = ""

    async def restart(self, reason: str) -> bool:
        """Replace the running intel_gpu_top. False if the debounce declined."""
        o = self.pub.opts
        # Monotonic: a pure duration must not be defeated by a wall-clock step.
        now = asyncio.get_running_loop().time()
        if (
            reason != "initial_start"
            and (now - self._last_restart_attempt) < o.restart_grace_seconds
        ):
            self.log.warning(
                "Skipping intel_gpu_top restart (grace period) reason=%s", reason
            )
            return False
        self._last_restart_attempt = now
        if reason != "initial_start":
            self.log.warning("Restarting intel_gpu_top reason=%s", reason)

        await self.stop()

        # Re-select: the GPU nodes may have changed, which is the whole point of
        # the render-node watchdog.
        listing = list_intel_gpu_top_devices(self.log)
        dev_arg, dev_path = auto_select_device_arg(
            listing, o.preferred_device_regex, self.log
        )
        self.log.info("Selected device arg: %s", dev_arg or "(none)")
        if dev_path:
            self.log.info("Selected render node: %s", dev_path)

        self.pub.on_gpu_started(dev_path)
        self.proc = await start_intel_gpu_top(o.interval_ms, dev_arg, self.log)
        self._stderr_task = asyncio.create_task(self._drain_stderr(), name="gpu-stderr")
        return True

    async def _drain_stderr(self) -> None:
        """Keep stderr's pipe empty and hold a tail for the exit diagnostic.

        Its own task because stdout can reach EOF while the process lives: a
        blocking read here would strand the sampler, and an undrained pipe would
        eventually block intel_gpu_top itself at 64 KiB.
        """
        proc = self.proc
        if proc is None or proc.stderr is None:
            return
        with contextlib.suppress(Exception):
            while chunk := await proc.stderr.read(4096):
                self.stderr_tail = (
                    self.stderr_tail + chunk.decode("utf-8", "replace")
                )[-_STDERR_TAIL_BYTES:]

    async def stop(self) -> None:
        proc, self.proc = self.proc, None
        for attr in ("_stderr_task", "_read_task"):
            task = getattr(self, attr)
            setattr(self, attr, None)
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
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

    async def next_line(
        self, stop: asyncio.Event, timeout: float
    ) -> str | None | object:
        """A line, EOF (None), or _NO_LINE, whichever comes first.

        Raced against ``stop`` rather than polled: waiting only on the read
        would leave a SIGTERM unnoticed for a whole sampling interval, which at
        the shipped default is longer than the supervisor's entire stop grace --
        so the bounded teardown below would never get to run at all.

        The read task outlives a timeout instead of being cancelled and
        restarted, so a line that is still arriving in pieces is never abandoned
        half-read.
        """
        if self._read_task is None:
            self._read_task = asyncio.create_task(self.readline(), name="gpu-read")
        stopped = asyncio.create_task(stop.wait(), name="gpu-stop")
        try:
            await asyncio.wait(
                {self._read_task, stopped},
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            stopped.cancel()
        if not self._read_task.done():
            return _NO_LINE
        task, self._read_task = self._read_task, None
        return task.result()


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
    mq: aiomqtt.Client, pub: Publisher, gpu: GpuTop, stop: asyncio.Event
) -> int:
    """Drain intel_gpu_top and publish, until shutdown or a fault we cannot fix.

    Returns an exit code; 0 means shutdown was requested. Broker faults are left
    to propagate so the caller reconnects rather than treating them as fatal.
    """
    while not stop.is_set():
        fault = pub.check_watchdogs(time.time(), asyncio.get_running_loop().time())
        if fault in _RESTART_FAULTS:
            await gpu.restart(fault.value)
        elif fault is not Fault.NONE:
            return _FAULT_EXIT[fault]

        await pub.maybe_heartbeat(mq, time.time())

        # Bounded so the watchdogs and heartbeat above still run while
        # intel_gpu_top is quiet, and raced against stop so shutdown does not
        # wait for a sample that may be a whole interval away.
        line = await gpu.next_line(stop, pub.opts.interval)
        if line is _NO_LINE:
            continue

        if line is None:
            rc = gpu.proc.returncode if gpu.proc is not None else None
            if rc is not None:
                pub.log.error(
                    "intel_gpu_top exited rc=%s stderr_tail=%s", rc, gpu.stderr_tail
                )
            # The stream is done either way; waiting on the pid would just stall
            # until a watchdog fired.
            await gpu.restart("intel_gpu_top_exited")
            continue

        try:
            await pub.feed(mq, str(line))
        except aiomqtt.MqttError:
            raise  # the session is gone; _reconnect_loop reconnects
        except Exception:
            # Anything else is our own bug -- a metric we mishandle, a payload
            # that will not serialise. Skip it: letting it out would exit the
            # process over a single malformed sample.
            pub.log.exception("sample failed")
    return EXIT_OK


async def _session(pub: Publisher, gpu: GpuTop, stop: asyncio.Event) -> int:
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
        pub.log.info("MQTT connected to %s:%d", o.mqtt_host, o.mqtt_port)
        pub.on_connected()
        try:
            await mq.subscribe(
                f"{o.discovery_prefix}/status",
                qos=1,
                timeout=_SUBSCRIBE_TIMEOUT_SECONDS,
            )
        except (ValueError, aiomqtt.MqttError) as e:
            # ValueError is a malformed prefix (a config typo); MqttError is the
            # broker refusing -- an ACL granting publish on our base topic but
            # not subscribe elsewhere is a normal lockdown for a stats-only
            # client. Neither may end the session: letting them out means we
            # reconnect and die here again every time, so no sample is ever
            # published. Costs the HA birth message; discovery is republished on
            # every reconnect anyway.
            pub.log.error(
                "cannot subscribe to %s/status: %s; HA restart will not trigger rediscovery",
                o.discovery_prefix,
                e,
            )
        birth = asyncio.create_task(_watch_birth(mq, pub), name="birth")
        try:
            return await _sample_loop(mq, pub, gpu, stop)
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
            except Exception:
                pub.log.warning("error during shutdown publish", exc_info=True)


async def _reconnect_loop(pub: Publisher, gpu: GpuTop, stop: asyncio.Event) -> int:
    """Hold a session up, reconnecting with a capped backoff when it drops."""
    delay = _RECONNECT_MIN_SECONDS
    while not stop.is_set():
        connected_at = pub.health.last_connect_ok
        try:
            return await _session(pub, gpu, stop)
        except aiomqtt.MqttError as e:
            pub.on_disconnected()
            # A session that got as far as connecting starts the ramp over:
            # backoff is for a broker that is down, not one that dropped a
            # client it was happy to serve a moment ago.
            if pub.health.last_connect_ok != connected_at:
                delay = _RECONNECT_MIN_SECONDS
            pub.log.warning("MQTT: %s; reconnecting in %ds", e, delay)
            # Checked here too: with no session the sample loop is not running,
            # so this is the only place the disconnect watchdog gets to fire.
            fault = pub.check_watchdogs(time.time(), asyncio.get_running_loop().time())
            if fault in _FAULT_EXIT:
                return _FAULT_EXIT[fault]
            await _wait_or_stop(stop, delay)
            delay = min(delay * 2, _RECONNECT_MAX_SECONDS)
    return EXIT_OK


def _parse_args(argv: list[str] | None = None) -> dict[str, object]:
    ap = argparse.ArgumentParser()
    ap.add_argument("--options", default="")
    ap.add_argument("--interval-seconds", type=int, default=None)
    ap.add_argument("--mqtt-host", default=None)
    ap.add_argument("--mqtt-port", type=int, default=None)
    ap.add_argument("--mqtt-username", default=None)
    ap.add_argument("--mqtt-password", default=None)
    ap.add_argument("--mqtt-discovery-prefix", default=None)
    ap.add_argument("--mqtt-base-topic", default=None)
    ap.add_argument("--client-id", default=None)
    ap.add_argument("--preferred-device-regex", default=None)
    ap.add_argument("--log-level", default=None)
    ap.add_argument("--publish-raw-sample", default=None)
    ap.add_argument("--expire-after-multiplier", type=int, default=None)
    ap.add_argument("--mqtt-disconnect-timeout-seconds", type=int, default=None)
    ap.add_argument("--intel-restart-grace-seconds", type=int, default=None)
    return vars(ap.parse_args(argv))


async def _run(argv: list[str] | None = None) -> int:
    cli = _parse_args(argv)
    opts = config_mod.read(str(cli["options"])) if cli["options"] else {}
    o = config_mod.from_sources(cli, opts)

    logging.basicConfig(
        level=getattr(logging, o.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    log = logging.getLogger("intel_gpu_mqtt")
    log.info("Intel GPU Top MQTT v%s starting", __version__)

    if not o.mqtt_host:
        log.error("mqtt_host is required (via --mqtt-host or --options)")
        return EXIT_UNEXPECTED

    log.info("%s", o.summary())

    pub = Publisher(o, log)
    gpu = GpuTop(pub, log)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in EXIT_SIGNALS:
        loop.add_signal_handler(signum, stop.set)

    try:
        await gpu.restart("initial_start")
    except FileNotFoundError:
        return EXIT_NO_BINARY

    try:
        return await _reconnect_loop(pub, gpu, stop)
    finally:
        await gpu.stop()


def main() -> int:
    try:
        return asyncio.run(_run())
    except Exception:
        logging.getLogger("intel_gpu_mqtt").exception("Main loop exception")
        return EXIT_UNEXPECTED
