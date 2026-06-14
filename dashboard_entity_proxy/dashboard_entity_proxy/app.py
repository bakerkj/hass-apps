# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Process entry point.

Loads ``/data/options.json``, validates and parses it into a
``config.Config`` plus an optional ``customization.Customization``, then
constructs and runs two aiohttp web applications side by side on one
asyncio event loop:

  * The proxy app on loopback ``PROXY_BIND_PORT`` — handles
    browser/tablet traffic bound for Home Assistant. Forwards HTTP,
    tunnels passthrough WebSockets, intercepts ``/api/websocket`` into
    a ``Session``. nginx (running as a sibling service) listens on the
    host-facing port and forwards every request to this app.
  * The status app on ``INGRESS_PORT`` — serves the Ingress status UI
    (an HTML page + ``/api/sessions`` JSON snapshot) for the
    Supervisor-side panel. Reached directly by the Supervisor over the
    addon network, not through nginx.

Installs SIGTERM / SIGINT handlers for clean shutdown and cleans up both
runners on exit.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
from pathlib import Path

from aiohttp import web

from . import __version__, config, customization, http_traffic, proxy, statusui
from .const import HTTP_ACCESS_LOG_PATH, INGRESS_PORT, PROXY_BIND_PORT
from .session import SessionRegistry


def main() -> int:
    """Process entry point. Parses ``--options``, loads + validates the
    options file (any failure logs and returns 1), loads optional
    customization, then runs the asyncio event loop until shutdown.
    """
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--options", default="/data/options.json", help="add-on options file"
    )
    args = ap.parse_args()

    try:
        cfg = config.load(args.options)
    except (OSError, ValueError) as exc:
        logging.basicConfig(level=logging.ERROR)
        logging.error("failed to load options from %s: %s", args.options, exc)
        return 1

    logging.basicConfig(
        level=getattr(logging, cfg.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    log = logging.getLogger("dashboard_entity_proxy")
    log.info(
        "dashboard-entity-proxy v%s starting; ha=%s dashboard=%r "
        "throttle=%ss passthrough_all=%s",
        __version__,
        cfg.homeassistant_url,
        cfg.dashboard_url_path,
        cfg.state_update_interval,
        cfg.passthrough_all,
    )

    cust = customization.Customization()
    if cfg.customization_file:
        try:
            cust = customization.load(cfg.customization_file)
        except (OSError, ValueError) as exc:
            log.error(
                "failed to load customization from %s: %s",
                cfg.customization_file,
                exc,
            )
            return 1
        log.info(
            "loaded customization from %s: %d baseline, %d card(s), %d helper(s)",
            cfg.customization_file,
            len(cust.baseline_entities),
            len(cust.implicit_entities),
            len(cust.extra_helper_platforms),
        )

    try:
        asyncio.run(_serve(cfg, cust, log))
    except KeyboardInterrupt:
        pass
    log.info("stopped")
    return 0


async def _serve(
    cfg: config.Config,
    cust: customization.Customization,
    log: logging.Logger,
) -> None:
    """Stand up the proxy and status apps as a side-by-side pair of
    ``aiohttp`` runners, install signal handlers so SIGTERM / SIGINT cause
    a clean shutdown, and wait until that shutdown is requested. Tears
    both runners down on exit.
    """
    registry = SessionRegistry()

    proxy_app = proxy.create_app(
        proxy.Options(
            target_url=cfg.homeassistant_url,
            transparent=cfg.transparent,
            passthrough_all=cfg.passthrough_all,
            dashboard_url_path=cfg.dashboard_url_path,
            throttle=cfg.state_update_interval,
            extra_entities=cfg.extra_entities,
            include_globs=cfg.include_entity_globs,
            exclude_globs=cfg.exclude_entity_globs,
            registry=registry,
            logger=log.getChild("proxy"),
            customization=cust,
            registry_mode=cfg.registry_mode,
            registry_refetch_interval=cfg.registry_refetch_interval,
            registry_burst_threshold=cfg.registry_burst_threshold,
        )
    )
    status_app = statusui.create_app(registry, show_client_paths=cfg.show_client_paths)

    # HTTP traffic visibility: nginx writes its access log to a tmpfs file
    # (see HTTP_ACCESS_LOG_PATH); the tracker reads it and registers one
    # SessionRegistry entry per unique source IP, since plain HTTP requests
    # go nginx → HA directly without touching the Python proxy. Each entry
    # is removed from the registry when its rows all expire.
    http_tracker = http_traffic.HttpTrafficTracker(registry)
    http_tail_done = asyncio.Event()
    http_tail_task = asyncio.create_task(
        http_traffic.tail_access_log(
            Path(HTTP_ACCESS_LOG_PATH), http_tracker, done=http_tail_done
        )
    )

    runners = []
    for app, host, port, name in (
        # Proxy app is loopback-only; nginx talks to it on 127.0.0.1.
        (proxy_app, "127.0.0.1", PROXY_BIND_PORT, "proxy"),
        # Status app binds on every interface so Supervisor's ingress
        # proxy (which reaches the addon over the hassio docker bridge,
        # NOT the container's loopback) can hit it. ``ports:`` in
        # config.json does not publish this port, so the only reachable
        # paths are: Supervisor ingress (auth-gated by HA) and other
        # containers on the same bridge. Don't tighten this without
        # also reworking how Supervisor reaches the UI.
        (status_app, "0.0.0.0", INGRESS_PORT, "status"),  # noqa: S104
    ):
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host, port)
        await site.start()
        log.info("%s listening on %s:%d", name, host, port)
        runners.append(runner)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _on_signal(received: signal.Signals) -> None:
        # Log inside the handler so the level record carries the signal
        # name; logging it after ``stop.wait()`` would only show that
        # *something* set the event, not which signal arrived.
        log.info("signal %s received", received.name)
        stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _on_signal, sig)
        except NotImplementedError:
            # Windows / non-main thread — best-effort.
            pass
    try:
        await stop.wait()
    finally:
        log.info("shutting down")
        http_tail_done.set()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(http_tail_task, timeout=2.0)
        for runner in runners:
            await runner.cleanup()
