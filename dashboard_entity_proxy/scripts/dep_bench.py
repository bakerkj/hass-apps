#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Synthetic load test for the dashboard_entity_proxy addon.

Spins up an in-process fake Home Assistant and the real proxy app, opens N
client connections, drives M events/sec from the fake HA, and measures the
proxy's forward latency, throughput, and queue-full incidents.

The fake HA encodes the send timestamp into each entity's ``s`` (state)
value as ``v<i>|t<send_ns>``. The proxy filters events down to the
``a``/``c``/``r`` fields and strips any custom top-level keys, so the
timestamp has to travel inside a per-entity field that survives the
filter. The client extracts it on receive. Forward latency is
``receive - send``, covering the full path: fake-HA send → proxy scope
filter → proxy serialize → client recv. JSON serialize on the send side
and parse on the receive side are included in that number — they're part
of the real path.

Defaults aim at a typical HA install: 1000 entities, 50 events/sec, 5
clients, 30 seconds. Override with --entities / --rate / --clients /
--duration. Use --burst to spike rate during the second half.

Run::

    uv run dashboard_entity_proxy/scripts/dep_bench.py
    uv run dashboard_entity_proxy/scripts/dep_bench.py --entities 5000 --rate 500 --clients 10
"""

import argparse
import asyncio
import json
import logging
import re
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aiohttp
from aiohttp import web

# Add the addon dir (parent of this script's dir) to sys.path so
# ``from dashboard_entity_proxy import proxy`` resolves to the in-tree
# package next door. Same trick the integration suite uses.
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))

from dashboard_entity_proxy.session import SessionRegistry

from dashboard_entity_proxy import proxy

_TS_RE = re.compile(r"\|t(\d+)")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@dataclass
class BenchConfig:
    entities: int = 1000
    rate: int = 50  # events/sec from fake HA
    coalesce: int = 3  # entities updated per coalesced frame
    clients: int = 5
    duration: float = 30.0
    burst: bool = False
    # Fraction of entities each client's dashboard scopes (so the proxy
    # actually filters — passthrough_all would skip the hot path).
    scope_fraction: float = 0.25


@dataclass
class ClientStats:
    received: int = 0
    bytes_received: int = 0
    latencies_ns: list[int] = field(default_factory=list)
    queue_full_disconnect: bool = False


def _gen_entity_ids(n: int) -> list[str]:
    domains = ["sensor", "light", "switch", "binary_sensor", "climate", "cover"]
    return [f"{domains[i % len(domains)]}.entity_{i:05d}" for i in range(n)]


async def _fake_ha_handler(
    request: web.Request, *, entities: list[str], cfg: BenchConfig
) -> web.WebSocketResponse:
    ws = web.WebSocketResponse(max_msg_size=64 * 1024 * 1024)
    await ws.prepare(request)
    await ws.send_json({"type": "auth_required"})
    msg = await ws.receive()
    if msg.type != aiohttp.WSMsgType.TEXT:
        return ws
    await ws.send_json({"type": "auth_ok"})

    sub_id: int | None = None

    async def stream_changes() -> None:
        """Once the client subscribes, fire ``rate`` events/sec."""
        assert sub_id is not None
        start = time.monotonic()
        period = 1.0 / cfg.rate if cfg.rate > 0 else 0.0
        # ``next_t`` advances by fixed period so missed deadlines are not
        # silently absorbed — we measure how the proxy keeps up at the
        # demanded rate, not how its slowness paces the producer.
        next_t = start
        i = 0
        while not ws.closed:
            now = time.monotonic()
            if cfg.burst and (now - start) > cfg.duration / 2:
                effective_period = period / 4 if period else 0.0
            else:
                effective_period = period
            if next_t > now:
                await asyncio.sleep(next_t - now)
            send_ns = time.monotonic_ns()
            changes: dict[str, Any] = {}
            for _ in range(cfg.coalesce):
                eid = entities[i % len(entities)]
                i += 1
                changes[eid] = {
                    "+": {
                        "s": f"v{i}|t{send_ns}",
                        "lc": send_ns / 1e9,
                        "lu": send_ns / 1e9,
                    }
                }
            try:
                await ws.send_json(
                    {"id": sub_id, "type": "event", "event": {"c": changes}}
                )
            except ConnectionResetError, RuntimeError:
                return
            next_t += effective_period

    streamer: asyncio.Task[None] | None = None
    try:
        async for raw in ws:
            if raw.type != aiohttp.WSMsgType.TEXT:
                continue
            m: dict[str, Any] = json.loads(raw.data)
            mid = m.get("id")
            mtype = m.get("type")
            if mtype == "subscribe_entities":
                sub_id = mid
                await ws.send_json(
                    {"id": mid, "type": "result", "success": True, "result": None}
                )
                # Initial snapshot.
                snap = {
                    eid: {"s": "init", "a": {}, "c": "c", "lc": 0.0, "lu": 0.0}
                    for eid in entities
                }
                await ws.send_json({"id": mid, "type": "event", "event": {"a": snap}})
                streamer = asyncio.create_task(stream_changes())
            elif mtype == "lovelace/config":
                # A dashboard that scopes ``scope_fraction`` of the entities,
                # exercising the per-event filter path on each event.
                limit = max(1, int(len(entities) * cfg.scope_fraction))
                await ws.send_json(
                    {
                        "id": mid,
                        "type": "result",
                        "success": True,
                        "result": {
                            "views": [
                                {"cards": [{"entity": eid} for eid in entities[:limit]]}
                            ]
                        },
                    }
                )
            elif mtype in (
                "config/entity_registry/list",
                "config/device_registry/list",
            ):
                await ws.send_json(
                    {"id": mid, "type": "result", "success": True, "result": []}
                )
    finally:
        if streamer is not None:
            streamer.cancel()
    return ws


async def _start_fake_ha(
    entities: list[str], cfg: BenchConfig
) -> tuple[web.AppRunner, str]:
    app = web.Application()
    app.router.add_get(
        "/api/websocket",
        lambda r: _fake_ha_handler(r, entities=entities, cfg=cfg),
    )
    port = _free_port()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    return runner, f"http://127.0.0.1:{port}"


async def _start_proxy(target_url: str) -> tuple[web.AppRunner, str]:
    app = proxy.create_app(
        proxy.Options(
            target_url=target_url,
            registry=SessionRegistry(),
        )
    )
    port = _free_port()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    return runner, f"http://127.0.0.1:{port}"


_NGINX_OVERRIDE_TEMPLATE = """\
worker_processes auto;
pid /run/nginx.pid;
error_log /dev/stderr warn;
events { worker_connections 1024; }
http {
  client_max_body_size 100m;
  proxy_buffering off;
  proxy_request_buffering off;
  server_tokens off;
  include /etc/nginx/mime.types;
  default_type application/octet-stream;
  access_log off;
  map $http_upgrade $connection_upgrade { default upgrade; '' close; }
  upstream proxy_backend { server 127.0.0.1:8125; keepalive 32; }
  server {
    listen __LISTEN_PORT__ default_server;
    listen [::]:__LISTEN_PORT__ default_server;
    location / {
      proxy_pass http://proxy_backend;
      proxy_http_version 1.1;
      proxy_set_header Upgrade $http_upgrade;
      proxy_set_header Connection $connection_upgrade;
      proxy_set_header Host $host;
      proxy_set_header X-Real-IP $remote_addr;
      proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
      proxy_set_header X-Forwarded-Proto $scheme;
      proxy_read_timeout 86400s;
      proxy_send_timeout 86400s;
      proxy_connect_timeout 10s;
    }
  }
}
"""


def _wait_for_tcp(host: str, port: int, timeout: float) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except TimeoutError, OSError:
            time.sleep(0.5)
    raise TimeoutError(f"{host}:{port} not reachable within {timeout}s")


def _start_addon_container(target_url: str, image: str) -> tuple[str, str, list[Path]]:
    """Boot the addon container (nginx + s6 + python) with ``--network host``
    and an nginx override that listens on a free port. Returns
    ``(container_name, base_url, tempfile_paths_for_cleanup)``.
    """
    addon_port = _free_port()
    options_path = Path(tempfile.mkstemp(suffix=".json")[1])
    options_path.write_text(
        json.dumps(
            {
                "homeassistant_url": target_url,
                "transparent": True,
                "state_update_interval": "",
                "extra_entities": [],
                "include_entity_globs": [],
                "exclude_entity_globs": [],
                "passthrough_all": False,
                "log_level": "INFO",
                "customization_file": "",
                "show_client_paths": True,
                "registry_mode": "incremental",
                "registry_refetch_interval": 0,
                "registry_burst_threshold": 50,
            }
        )
    )
    nginx_conf_path = options_path.parent / f"dep_bench_nginx_{addon_port}.conf"
    nginx_conf_path.write_text(
        _NGINX_OVERRIDE_TEMPLATE.replace("__LISTEN_PORT__", str(addon_port))
    )

    container = f"dep_bench_addon_{addon_port}"
    subprocess.run(["docker", "rm", "-f", container], capture_output=True, check=False)
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            container,
            "--network",
            "host",
            "-v",
            f"{options_path}:/data/options.json:ro",
            "-v",
            f"{nginx_conf_path}:/etc/nginx/nginx.conf:ro",
            image,
        ],
        check=True,
        capture_output=True,
    )
    _wait_for_tcp("127.0.0.1", addon_port, 30)
    return container, f"http://127.0.0.1:{addon_port}", [options_path, nginx_conf_path]


async def _run_client(
    base_url: str, idx: int, cfg: BenchConfig, stats: ClientStats
) -> None:
    timeout = aiohttp.ClientTimeout(total=cfg.duration + 30)
    try:
        async with (
            aiohttp.ClientSession(timeout=timeout) as cs,
            cs.ws_connect(
                base_url + "/api/websocket",
                max_msg_size=64 * 1024 * 1024,
                receive_timeout=cfg.duration + 30,
            ) as ws,
        ):
            await ws.receive_json()  # auth_required
            await ws.send_json({"type": "auth", "access_token": "x"})
            await ws.receive_json()  # auth_ok
            await ws.send_json({"id": 1, "type": "subscribe_entities"})
            # Drain the result + initial snapshot before measuring;
            # snapshot delivery is bulk and dwarfs steady-state latency.
            await ws.receive_json()  # result
            await ws.receive_json()  # initial 'a' frame
            deadline = time.monotonic() + cfg.duration
            while time.monotonic() < deadline:
                try:
                    raw = await asyncio.wait_for(
                        ws.receive(), timeout=deadline - time.monotonic()
                    )
                except TimeoutError:
                    break
                if raw.type == aiohttp.WSMsgType.CLOSED:
                    # The proxy disconnects "slow" peers when the
                    # outbound queue saturates — record that.
                    stats.queue_full_disconnect = True
                    break
                if raw.type != aiohttp.WSMsgType.TEXT:
                    continue
                recv_ns = time.monotonic_ns()
                stats.bytes_received += len(raw.data)
                m = json.loads(raw.data)
                payloads = m if isinstance(m, list) else [m]
                for payload in payloads:
                    if not isinstance(payload, dict):
                        continue
                    ev = payload.get("event") or {}
                    changed = ev.get("c") or {}
                    for diff in changed.values():
                        plus = diff.get("+") or {}
                        state = plus.get("s")
                        if not isinstance(state, str):
                            continue
                        match = _TS_RE.search(state)
                        if not match:
                            continue
                        send_ns = int(match.group(1))
                        stats.latencies_ns.append(recv_ns - send_ns)
                        stats.received += 1
    except aiohttp.ClientError as e:
        print(f"client {idx} error: {e}", file=sys.stderr)


def _percentile(xs: list[int], p: float) -> int:
    if not xs:
        return 0
    k = max(0, min(len(xs) - 1, round((p / 100) * (len(xs) - 1))))
    return sorted(xs)[k]


def _fmt_ns(ns: int) -> str:
    if ns < 1_000:
        return f"{ns}ns"
    if ns < 1_000_000:
        return f"{ns / 1_000:.1f}us"
    if ns < 1_000_000_000:
        return f"{ns / 1_000_000:.1f}ms"
    return f"{ns / 1_000_000_000:.2f}s"


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--entities", type=int, default=1000)
    ap.add_argument("--rate", type=int, default=50, help="events/sec from fake HA")
    ap.add_argument("--coalesce", type=int, default=3)
    ap.add_argument("--clients", type=int, default=5)
    ap.add_argument("--duration", type=float, default=30.0)
    ap.add_argument("--burst", action="store_true", help="4x rate during second half")
    ap.add_argument(
        "--scope-fraction",
        type=float,
        default=0.25,
        help="fraction of entities each client's dashboard scopes",
    )
    ap.add_argument(
        "--addon-image",
        default=None,
        help=(
            "if set, run the bench against the real addon container "
            "(nginx + s6 + python) instead of an in-process Python proxy. "
            "Requires docker. The container is booted with --network host "
            "and pointed at the in-process fake HA."
        ),
    )
    args = ap.parse_args()

    cfg = BenchConfig(
        entities=args.entities,
        rate=args.rate,
        coalesce=args.coalesce,
        clients=args.clients,
        duration=args.duration,
        burst=args.burst,
        scope_fraction=args.scope_fraction,
    )

    # Quiet the proxy's own logger so its warnings don't pollute the report
    # — we surface the same signal (queue-full disconnects) via client stats.
    queue_full_warnings = 0

    class _Counter(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            nonlocal queue_full_warnings
            if "outbound queue full" in record.getMessage():
                queue_full_warnings += 1

    logging.basicConfig(level=logging.ERROR)
    logging.getLogger("dashboard_entity_proxy.proxy").addHandler(_Counter())

    entities = _gen_entity_ids(cfg.entities)
    print(
        f"bench: {cfg.entities} entities, {cfg.rate} ev/sec "
        f"(coalesce={cfg.coalesce}), {cfg.clients} clients, "
        f"{cfg.duration}s, scope_fraction={cfg.scope_fraction}"
        f"{', burst' if cfg.burst else ''}"
    )
    fake_runner, fake_url = await _start_fake_ha(entities, cfg)
    proxy_runner: web.AppRunner | None = None
    container_name: str | None = None
    tempfiles: list[Path] = []
    if args.addon_image:
        container_name, proxy_url, tempfiles = _start_addon_container(
            fake_url, args.addon_image
        )
        print(f"using addon container {container_name} at {proxy_url}")
    else:
        proxy_runner, proxy_url = await _start_proxy(fake_url)
    try:
        stats = [ClientStats() for _ in range(cfg.clients)]
        t0 = time.monotonic()
        await asyncio.gather(
            *(_run_client(proxy_url, i, cfg, stats[i]) for i in range(cfg.clients))
        )
        elapsed = time.monotonic() - t0

        all_latencies: list[int] = []
        total_received = 0
        total_bytes = 0
        disconnects = 0
        for s in stats:
            all_latencies.extend(s.latencies_ns)
            total_received += s.received
            total_bytes += s.bytes_received
            if s.queue_full_disconnect:
                disconnects += 1

        print()
        print("=== results ===")
        print(f"duration:               {elapsed:.2f}s")
        print(f"events received (all):  {total_received}")
        print(f"per-client mean:        {total_received / cfg.clients:.0f}")
        print(f"throughput (rx/sec):    {total_received / elapsed:.0f}")
        print(f"bytes received (all):   {total_bytes:,}")
        if all_latencies:
            print(
                f"latency p50/p95/p99:    "
                f"{_fmt_ns(_percentile(all_latencies, 50))} / "
                f"{_fmt_ns(_percentile(all_latencies, 95))} / "
                f"{_fmt_ns(_percentile(all_latencies, 99))}"
            )
            print(
                f"latency mean:           {_fmt_ns(int(statistics.mean(all_latencies)))}"
            )
            print(f"latency max:            {_fmt_ns(max(all_latencies))}")
        print(f"clients disconnected:   {disconnects} / {cfg.clients}")
        print(f"queue-full warnings:    {queue_full_warnings}")
        return 0
    finally:
        if proxy_runner is not None:
            await proxy_runner.cleanup()
        if container_name is not None:
            subprocess.run(  # noqa: ASYNC221 - teardown script; blocking wait is fine here
                ["docker", "rm", "-f", container_name],
                capture_output=True,
                check=False,
            )
            for path in tempfiles:
                path.unlink(missing_ok=True)
        await fake_runner.cleanup()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
