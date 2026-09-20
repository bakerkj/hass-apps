# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""MQTT health publisher: aiomqtt session (connect + reconnect + LWT), one-shot
retained-scan reconciliation, HA MQTT-Discovery publish, and the periodic
health poll that emits state/attributes/summary per tracked container. Split
from ``app.py`` so the docker events loop stays focused and this module can
be unit-tested against a recording stub."""

import asyncio
import contextlib
import logging
import time

import aiodocker

try:  # pragma: no cover -- import guarded so tests can exercise pure logic
    import aiomqtt
except ImportError:  # pragma: no cover
    aiomqtt = None  # type: ignore[assignment]

from .config import Options, pre_start_files_dir, pre_start_log
from .health import HealthResult, check_applied, check_sentinel, render_binary_state
from .mqtt import (
    availability_topic,
    clear_container_entities,
    component_for,
    discovery_payload,
    keys_for,
    publish_discovery,
    publish_slug_availability,
    publish_state,
    slugify,
    summary_attributes,
    summary_state,
)

_BACKOFF_MIN = 2
_BACKOFF_MAX = 60
# Idle-window budget for the one-shot retained-scan on first connect.
_RETAINED_SCAN_TIMEOUT = 5.0


def _log() -> logging.Logger:
    return logging.getLogger("container_hooks.health")


def _friendly_name(container: str) -> str:
    """Strip Supervisor's ``app_<slug>_`` / ``addon_<slug>_`` prefix; titlecase."""
    for prefix in ("app_", "addon_"):
        if container.startswith(prefix):
            _, _, rest = container.partition("_")
            _, _, name = rest.partition("_")
            if name:
                return name.replace("_", " ").replace("-", " ").strip().title()
    return container


def _list_recipe_containers(options: Options) -> list[str]:
    """Container names under base_dir that carry a ``pre-start-files/`` recipe."""
    if not options.base_dir.is_dir():
        return []
    out: list[str] = []
    for entry in sorted(options.base_dir.iterdir()):
        if not entry.is_dir() or entry.name in options.skip_containers:
            continue
        if pre_start_files_dir(options, entry.name).is_dir():
            out.append(entry.name)
    return out


def _sentinel_for(options: Options, container: str) -> str | None:
    for override in options.container_overrides:
        if override.container == container:
            return override.success_sentinel
    return None


class HealthPublisher:
    """Owns the aiomqtt session and the per-container health poll."""

    def __init__(
        self,
        options: Options,
        docker: aiodocker.Docker,
        stop: asyncio.Event,
    ) -> None:
        self.options = options
        self.docker = docker
        self.stop = stop
        self.log = _log()
        # Non-zero when the disconnect watchdog trips; ``main_async``
        # returns this as the process rc so Supervisor restarts the addon.
        self.exit_code: int = 0
        # slug -> friendly name; rebuilt from disk on each connection so
        # a new recipe added while the addon runs shows up next connect.
        self._slugs: dict[str, str] = {}
        # slug -> container name, for the reverse lookup during polling
        self._container_for_slug: dict[str, str] = {}
        # Cross-restart-removal slugs from the one-shot retained-scan.
        # Consumed on the first _publish_discovery of each session.
        self._retained_slugs_at_start: set[str] = set()
        # Single source of truth for "publish discovery next iteration."
        # Set on session start and by the birth-message listener; cleared
        # by the poll loop before it calls _publish_discovery. If a birth
        # races into the middle of a publish, the listener re-sets it
        # and the next iteration republishes.
        self._republish_needed: asyncio.Event = asyncio.Event()
        # One-shot per process: scan only runs on the first connect.
        self._did_retained_scan: bool = False

    # -- lifecycle ---------------------------------------------------------

    async def run(self) -> None:
        """Outer reconnect loop; returns cleanly when ``stop`` is set."""
        if aiomqtt is None:
            self.log.error("aiomqtt not importable; health publisher disabled")
            return
        if not self.options.mqtt_host:
            self.log.info(
                "mqtt_host is empty; health publisher disabled (this is fine)"
            )
            return
        base_topic = self.options.mqtt_base_topic
        avail = availability_topic(base_topic)
        backoff = _BACKOFF_MIN
        # Broker-downtime watchdog: on trip we set stop + exit_code=11 so
        # main_async can drain in-flight docker work before returning.
        first_disconnect_at: float | None = None
        disconnect_timeout = max(5, self.options.mqtt_disconnect_timeout_seconds)
        while not self.stop.is_set():
            try:
                async with aiomqtt.Client(
                    hostname=self.options.mqtt_host,
                    port=self.options.mqtt_port,
                    username=self.options.mqtt_username or None,
                    password=self.options.mqtt_password or None,
                    identifier=self.options.client_id,
                    will=aiomqtt.Will(avail, payload="offline", qos=1, retain=True),
                    keepalive=60,
                ) as mq:
                    self.log.info(
                        "MQTT connected to %s:%d",
                        self.options.mqtt_host,
                        self.options.mqtt_port,
                    )
                    await mq.publish(avail, "online", qos=1, retain=True)
                    # Reset only after a successful publish so a
                    # publish-rejecting broker still trips the watchdog.
                    backoff = _BACKOFF_MIN
                    first_disconnect_at = None
                    # Retained-scan is one-shot per process and must run
                    # before birth-topic subscribe (it consumes from
                    # mq.messages with a timeout).
                    if not self._did_retained_scan:
                        self._retained_slugs_at_start = await self._scan_retained_slugs(
                            mq
                        )
                        self._did_retained_scan = True
                    await mq.subscribe(
                        f"{self.options.mqtt_discovery_prefix}/status", qos=1
                    )
                    self._republish_needed.set()
                    await self._session_body(mq)
                    # Session ended cleanly (stop set): best-effort offline.
                    with contextlib.suppress(aiomqtt.MqttError):
                        await mq.publish(avail, "offline", qos=1, retain=True)
                    return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                now = time.time()
                if first_disconnect_at is None:
                    first_disconnect_at = now
                downtime = now - first_disconnect_at
                if aiomqtt is not None and isinstance(e, aiomqtt.MqttError):
                    self.log.warning(
                        "MQTT session error: %s; reconnecting in %ds "
                        "(down for %.1fs / %ds threshold)",
                        e,
                        backoff,
                        downtime,
                        disconnect_timeout,
                    )
                else:
                    self.log.exception(
                        "unexpected error in health publisher; reconnecting in %ds",
                        backoff,
                    )
                if downtime >= disconnect_timeout:
                    self.log.error(
                        "MQTT broker unreachable for %.1fs (> %ds). "
                        "Exiting for Supervisor restart.",
                        downtime,
                        disconnect_timeout,
                    )
                    self.exit_code = 11
                    self.stop.set()
                    return
                try:
                    await asyncio.wait_for(self.stop.wait(), timeout=backoff)
                    return
                except TimeoutError:
                    pass
                backoff = min(backoff * 2, _BACKOFF_MAX)

    async def _session_body(self, mq: aiomqtt.Client) -> None:
        """Run poll + listen tasks concurrently; return when stop is set."""
        poll_task = asyncio.create_task(self._poll_forever(mq))
        listen_task = asyncio.create_task(self._listen_forever(mq))
        stop_task = asyncio.create_task(self.stop.wait())
        try:
            done, _pending = await asyncio.wait(
                {poll_task, listen_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            # If poll or listen ended first, they most likely raised MqttError
            # which the outer loop catches. Re-raise so reconnect kicks in.
            for t in done:
                if t is stop_task:
                    continue
                exc = t.exception()
                if exc is not None:
                    raise exc
        finally:
            for t in (poll_task, listen_task, stop_task):
                if not t.done():
                    t.cancel()
            for t in (poll_task, listen_task, stop_task):
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await t

    # -- retained scan -----------------------------------------------------

    async def _scan_retained_slugs(self, mq: aiomqtt.Client) -> set[str]:
        """Collect slugs the broker has retained discovery configs for.

        Subscribes to the fully-wildcarded discovery topic (per MQTT-4.7.1.3
        `+` must occupy an entire level, so a `{client_id}_+` filter would be
        rejected by mosquitto), drains retained deliveries under the timeout,
        then filters to our device_id prefix in code. Result feeds the
        first _publish_discovery so cross-restart removals get cleared."""
        device_prefix = f"{self.options.client_id}_"
        discovery_filter = f"{self.options.mqtt_discovery_prefix}/+/+/+/config"
        await mq.subscribe(discovery_filter, qos=0)
        collected: set[str] = set()
        try:
            while True:
                try:
                    msg = await asyncio.wait_for(
                        mq.messages.__anext__(), timeout=_RETAINED_SCAN_TIMEOUT
                    )
                except TimeoutError, StopAsyncIteration:
                    break
                topic = str(msg.topic)
                if not msg.payload:  # tombstone
                    continue
                parts = topic.split("/")
                # {prefix}/{component}/{device_id}_{slug}/{key}/config
                if len(parts) != 5 or not parts[2].startswith(device_prefix):
                    continue
                collected.add(parts[2][len(device_prefix) :])
        finally:
            with contextlib.suppress(Exception):
                await mq.unsubscribe(discovery_filter)
        if collected:
            self.log.info(
                "retained-scan found %d slug(s) with pre-existing discovery: %s",
                len(collected),
                ", ".join(sorted(collected)),
            )
        return collected

    # -- discovery ---------------------------------------------------------

    def _rebuild_slug_map(self) -> None:
        """Rescan disk. Idempotent; called at connect and on birth."""
        self._slugs = {}
        self._container_for_slug = {}
        for container in _list_recipe_containers(self.options):
            slug = slugify(container)
            # Slug collisions are extremely unlikely with docker container
            # names, but if they happen the first-wins policy is fine
            # (the second recipe will show up in the logs as "skipped").
            if slug in self._slugs:
                self.log.warning(
                    "slug collision: %s and %s both slugify to %r; skipping the latter",
                    self._container_for_slug[slug],
                    container,
                    slug,
                )
                continue
            self._slugs[slug] = _friendly_name(container)
            self._container_for_slug[slug] = container

    async def _publish_discovery(self, mq: aiomqtt.Client) -> None:
        """Publish HA MQTT-Discovery for each tracked slug; clear dropped ones.

        Dropped-slug detection unions in-memory _slugs (within-session removals)
        with _retained_slugs_at_start (cross-restart removals from the scan).
        Consumes the retained set AFTER the clear loop so a mid-loop raise
        leaves it intact for the retry."""
        prev_slugs = set(self._slugs) | self._retained_slugs_at_start
        prev_container_for_slug = dict(self._container_for_slug)
        self._rebuild_slug_map()
        for dropped_slug in prev_slugs - set(self._slugs):
            prev_container = prev_container_for_slug.get(dropped_slug, "")
            # keys_for(True) covers both possible past shapes; clear on a
            # key never published is a broker-side no-op.
            await clear_container_entities(
                mq,
                discovery_prefix=self.options.mqtt_discovery_prefix,
                device_id=self.options.client_id,
                slug=dropped_slug,
                keys=keys_for(sentinel_configured=True),
                base_topic=self.options.mqtt_base_topic,
            )
            self.log.info(
                "cleared discovery + retained state for removed slug=%s (was %s)",
                dropped_slug,
                prev_container or "?",
            )
        self._retained_slugs_at_start = set()  # consumed after clear loop
        expire = max(60, self.options.health_interval_seconds * 4)
        for slug, friendly in self._slugs.items():
            container = self._container_for_slug[slug]
            sentinel = _sentinel_for(self.options, container)
            active_keys = keys_for(sentinel_configured=sentinel is not None)
            if sentinel is None:
                # sentinel dropped for this container — clear any retained
                # discovery/state from a prior run where it was set.
                await clear_container_entities(
                    mq,
                    discovery_prefix=self.options.mqtt_discovery_prefix,
                    device_id=self.options.client_id,
                    slug=slug,
                    keys=("sentinel",),
                    base_topic=self.options.mqtt_base_topic,
                )
            for key in active_keys:
                await publish_discovery(
                    mq,
                    discovery_payload(
                        key,
                        device_id=self.options.client_id,
                        slug=slug,
                        friendly=friendly,
                        base_topic=self.options.mqtt_base_topic,
                        expire_after_s=expire,
                    ),
                    discovery_prefix=self.options.mqtt_discovery_prefix,
                    component=component_for(key),
                    device_id=self.options.client_id,
                    slug=slug,
                    key=key,
                )
        self.log.info(
            "Published discovery for %d container(s): %s",
            len(self._slugs),
            ", ".join(sorted(self._slugs)) or "(none)",
        )

    # -- poll --------------------------------------------------------------

    async def _poll_forever(self, mq: aiomqtt.Client) -> None:
        """Emit health state on an interval; inter-poll sleep races stop.wait()
        against _republish_needed.wait() so HA birth wakes the poll instantly."""
        interval = max(5, self.options.health_interval_seconds)
        while not self.stop.is_set():
            if self._republish_needed.is_set():
                # Clear before publish so a birth arriving mid-republish
                # re-sets the event and the next iteration republishes.
                self._republish_needed.clear()
                await self._publish_discovery(mq)
            await self._poll_once(mq)
            stop_task = asyncio.create_task(self.stop.wait())
            republish_task = asyncio.create_task(self._republish_needed.wait())
            try:
                done, _pending = await asyncio.wait(
                    {stop_task, republish_task},
                    timeout=interval,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if stop_task in done:
                    return
            finally:
                for t in (stop_task, republish_task):
                    if not t.done():
                        t.cancel()
                for t in (stop_task, republish_task):
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await t

    async def _poll_once(self, mq: aiomqtt.Client) -> None:
        for slug, _friendly in list(self._slugs.items()):
            if self.stop.is_set():
                return
            container = self._container_for_slug[slug]
            sentinel = _sentinel_for(self.options, container)
            applied = await check_applied(
                self.docker, container, pre_start_log(self.options, container)
            )
            sentinel_res: HealthResult | None = None
            if sentinel is not None:
                sentinel_res = await check_sentinel(self.docker, container, sentinel)
            # Both checks None → target unreachable → per-slug availability offline
            # instead of holding at ``unknown``. Any check returning a
            # real HealthResult keeps the target ``online`` — even a
            # False result is "reachable, and here's the diagnosis."
            target_reachable = applied is not None or sentinel_res is not None
            await publish_slug_availability(
                mq,
                base_topic=self.options.mqtt_base_topic,
                slug=slug,
                online=target_reachable,
            )
            await self._publish_snapshot(
                mq,
                slug,
                applied,
                sentinel_res,
                sentinel_configured=sentinel is not None,
            )

    async def _publish_snapshot(
        self,
        mq: aiomqtt.Client,
        slug: str,
        applied: HealthResult | None,
        sentinel_res: HealthResult | None,
        *,
        sentinel_configured: bool,
    ) -> None:
        applied_state = render_binary_state(applied)
        await publish_state(
            mq,
            base_topic=self.options.mqtt_base_topic,
            slug=slug,
            key="applied",
            state=applied_state,
            attributes={
                "reason": applied.reason if applied else "no data",
            },
        )
        sentinel_state = None
        if sentinel_configured:
            sentinel_state = render_binary_state(sentinel_res)
            await publish_state(
                mq,
                base_topic=self.options.mqtt_base_topic,
                slug=slug,
                key="sentinel",
                state=sentinel_state,
                attributes={
                    "reason": sentinel_res.reason if sentinel_res else "no data",
                },
            )
        summary = summary_state(applied_state, sentinel_state)
        await publish_state(
            mq,
            base_topic=self.options.mqtt_base_topic,
            slug=slug,
            key="summary",
            state=summary,
            attributes=summary_attributes(
                applied_reason=applied.reason if applied else None,
                sentinel_reason=(sentinel_res.reason if sentinel_res else None),
                sentinel_configured=sentinel_configured,
            ),
        )

    # -- listen -----------------------------------------------------------

    async def _listen_forever(self, mq: aiomqtt.Client) -> None:
        """Wake the poll on HA birth so entities re-appear after a HA restart."""
        birth_topic = f"{self.options.mqtt_discovery_prefix}/status"
        async for msg in mq.messages:
            if str(msg.topic) != birth_topic:
                continue
            payload = (msg.payload or b"").decode("utf-8", errors="replace").strip()
            if payload.lower() == "online":
                self.log.info(
                    "HA birth message received; scheduling discovery republish"
                )
                self._republish_needed.set()


async def run_publisher(
    options: Options,
    docker: aiodocker.Docker,
    stop: asyncio.Event,
) -> int:
    """Entrypoint used by ``app.py`` — a task-shaped coroutine.

    Returns the publisher's ``exit_code`` (0 on clean shutdown; 11 when
    the disconnect watchdog trips so Supervisor restarts the addon).
    """
    publisher = HealthPublisher(options, docker, stop)
    await publisher.run()
    return publisher.exit_code
