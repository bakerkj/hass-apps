# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""MQTT health publisher: the whole aiomqtt session, running as one task.

Owns:

* Broker session lifecycle (connect with backoff, LWT, availability
  publish, disconnect + best-effort offline publish on shutdown).
* HA MQTT Discovery: publish configs for every container the addon
  has a recipe for, retained. Republish on the HA birth message
  (``homeassistant/status = online``) so a restarted HA picks the
  entities up.
* Periodic health polling: for each container with a recipe, evaluate
  ``check_applied`` and — if the recipe declares ``success_sentinel``
  — ``check_sentinel``. Publish per-check binary states, a summary
  state, and a per-entity ``reason`` attribute. All state is retained
  so a late subscriber sees the last known value without waiting for
  the next poll.

Split from ``app.py`` so the Docker events loop stays focused and this
module can be unit-tested against a recording stub without needing an
aiomqtt client.
"""

import asyncio
import contextlib
import logging
import os
import time

import aiodocker

try:  # pragma: no cover -- import guarded so tests can exercise pure logic
    import aiomqtt
except ImportError:  # pragma: no cover
    aiomqtt = None  # type: ignore[assignment]

from .config import Options, pre_start_files_dir, pre_start_log
from .health import HealthResult, check_applied, check_sentinel, render_binary_state
from .mqtt import (
    applied_discovery_payload,
    availability_topic,
    clear_container_entities,
    publish_discovery,
    publish_state,
    sentinel_discovery_payload,
    slugify,
    summary_attributes,
    summary_discovery_payload,
    summary_state,
)

_BACKOFF_MIN = 2
_BACKOFF_MAX = 60


def _log() -> logging.Logger:
    return logging.getLogger("container_hooks.health")


def _friendly_name(container: str) -> str:
    """Derive a short human name from the docker container name.

    Supervisor addon containers are ``app_<slug>_<name>`` (or the
    legacy ``addon_<slug>_<name>``); everything else keeps its full
    docker name. The result is only used as the HA device's friendly
    name, so being a lossy prettifier is fine — the slug (a separate
    field) is the stable identifier.
    """
    for prefix in ("app_", "addon_"):
        if container.startswith(prefix):
            _, _, rest = container.partition("_")
            _, _, name = rest.partition("_")
            if name:
                return name.replace("_", " ").replace("-", " ").strip().title()
    return container


def _list_recipe_containers(options: Options) -> list[str]:
    """Every directory under ``base_dir`` that has a ``pre-start-files/``.

    That's the shape of a recipe we care about publishing health for —
    the ``applied`` check is meaningful only when there is a pre-start
    payload to apply. Post-start-only recipes (just ``scripts/``) are
    outside this signal's scope.
    """
    if not options.base_dir.is_dir():
        return []
    out: list[str] = []
    for entry in sorted(options.base_dir.iterdir()):
        if not entry.is_dir():
            continue
        if entry.name in options.skip_containers:
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
    """Encapsulates the MQTT health-publisher state.

    Held on the instance so the reconnect loop can restore state
    (discovered slugs, last published states) without re-scanning
    disk on every reconnect.
    """

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
        # slug -> friendly name; rebuilt from disk on each connection so
        # a new recipe added while the addon runs shows up next connect.
        self._slugs: dict[str, str] = {}
        # slug -> container name, for the reverse lookup during polling
        self._container_for_slug: dict[str, str] = {}
        # Track whether we published discovery this session; on birth
        # message we clear it so the next poll republishes.
        self._discovered = False

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
        # Wall-clock stamp of the first disconnect in the current outage.
        # Cleared whenever a session reaches "connected" (a successful publish
        # of availability=online). If the outage exceeds
        # ``mqtt_disconnect_timeout_seconds`` we exit non-zero so Supervisor
        # restarts the addon — the same pattern the sibling MQTT addons use to
        # avoid a silently-stuck broker session.
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
                    backoff = _BACKOFF_MIN
                    first_disconnect_at = None
                    await mq.publish(avail, "online", qos=1, retain=True)
                    await mq.subscribe(
                        f"{self.options.mqtt_discovery_prefix}/status", qos=1
                    )
                    self._discovered = False
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
                    os._exit(11)
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
        """Publish (or republish) HA MQTT Discovery configs.

        Called on initial connect and again when we see HA's birth
        message. Retained on the broker, so republishing is idempotent.
        expire_after is set generously (max(60, 4× poll interval)) so a
        broker or addon hiccup doesn't churn every sensor to unknown.
        """
        self._rebuild_slug_map()
        expire = max(60, self.options.health_interval_seconds * 4)
        for slug, friendly in self._slugs.items():
            container = self._container_for_slug[slug]
            sentinel = _sentinel_for(self.options, container)

            await publish_discovery(
                mq,
                applied_discovery_payload(
                    device_id=self.options.client_id,
                    slug=slug,
                    friendly=friendly,
                    base_topic=self.options.mqtt_base_topic,
                    expire_after_s=expire,
                ),
                discovery_prefix=self.options.mqtt_discovery_prefix,
                component="binary_sensor",
                device_id=self.options.client_id,
                slug=slug,
                key="applied",
            )
            if sentinel is not None:
                await publish_discovery(
                    mq,
                    sentinel_discovery_payload(
                        device_id=self.options.client_id,
                        slug=slug,
                        friendly=friendly,
                        base_topic=self.options.mqtt_base_topic,
                        expire_after_s=expire,
                    ),
                    discovery_prefix=self.options.mqtt_discovery_prefix,
                    component="binary_sensor",
                    device_id=self.options.client_id,
                    slug=slug,
                    key="sentinel",
                )
            else:
                # No sentinel configured now — drop any retained config
                # from a previous run where it may have been set.
                await clear_container_entities(
                    mq,
                    discovery_prefix=self.options.mqtt_discovery_prefix,
                    device_id=self.options.client_id,
                    slug=slug,
                    keys=("sentinel",),
                )
            await publish_discovery(
                mq,
                summary_discovery_payload(
                    device_id=self.options.client_id,
                    slug=slug,
                    friendly=friendly,
                    base_topic=self.options.mqtt_base_topic,
                    expire_after_s=expire,
                ),
                discovery_prefix=self.options.mqtt_discovery_prefix,
                component="sensor",
                device_id=self.options.client_id,
                slug=slug,
                key="summary",
            )
        self._discovered = True
        self.log.info(
            "Published discovery for %d container(s): %s",
            len(self._slugs),
            ", ".join(sorted(self._slugs)) or "(none)",
        )

    # -- poll --------------------------------------------------------------

    async def _poll_forever(self, mq: aiomqtt.Client) -> None:
        """Publish health state for every tracked container on an interval."""
        interval = max(5, self.options.health_interval_seconds)
        while not self.stop.is_set():
            if not self._discovered:
                await self._publish_discovery(mq)
            await self._poll_once(mq)
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=interval)
                return
            except TimeoutError:
                pass

    async def _poll_once(self, mq: aiomqtt.Client) -> None:
        for slug, _friendly in list(self._slugs.items()):
            if self.stop.is_set():
                # Bail early so a large poll set doesn't emit CancelledError
                # noise from mid-container docker exec calls at shutdown.
                return
            container = self._container_for_slug[slug]
            sentinel = _sentinel_for(self.options, container)
            applied = await check_applied(
                self.docker, container, pre_start_log(self.options, container)
            )
            sentinel_res: HealthResult | None = None
            if sentinel is not None:
                sentinel_res = await check_sentinel(self.docker, container, sentinel)
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
        """Watch HA birth messages so a restarted HA picks the entities up."""
        birth_topic = f"{self.options.mqtt_discovery_prefix}/status"
        async for msg in mq.messages:
            if str(msg.topic) != birth_topic:
                continue
            payload = (msg.payload or b"").decode("utf-8", errors="replace").strip()
            if payload.lower() == "online":
                self.log.info(
                    "HA birth message received; scheduling discovery republish"
                )
                self._discovered = False


async def run_publisher(
    options: Options,
    docker: aiodocker.Docker,
    stop: asyncio.Event,
) -> None:
    """Entrypoint used by ``app.py`` — a task-shaped coroutine."""
    await HealthPublisher(options, docker, stop).run()
