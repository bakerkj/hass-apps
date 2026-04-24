# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Docker access: CLI (ps/inspect/info) + Unix-socket engine API (stats).

``run_cmd`` + ``docker_api_get_json`` are the leaf I/O helpers — all the
higher-level ``fetch_*`` helpers funnel through them, so monkey-patching
those two in tests is enough to stand up a fake Docker.
"""

from __future__ import annotations

import asyncio
import http.client
import json
import logging
import socket
import subprocess
from typing import Any
from urllib.parse import quote

from .metrics import (
    cpu_percent_from_stats,
    parse_docker_timestamp,
    sum_blkio_totals,
    sum_network_totals,
)
from .util import DOCKER_SOCKET_PATH, cmd_error, safe_float, safe_int, safe_text


def run_cmd(cmd: list[str], timeout_seconds: int) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            timeout=max(1, timeout_seconds),
        )
    except FileNotFoundError as exc:
        raise RuntimeError(f"command not found: {cmd[0]}") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"command timed out after {timeout_seconds}s: {' '.join(cmd)}"
        ) from exc


def fetch_ps_containers(
    docker_timeout_seconds: int,
    log: logging.Logger,
) -> list[dict[str, str]]:
    cmd = ["docker", "ps", "--no-trunc", "--format", "{{json .}}"]
    proc = run_cmd(cmd, docker_timeout_seconds)
    if proc.returncode != 0:
        raise RuntimeError(f"docker ps failed: {cmd_error(proc)}")

    containers: list[dict[str, str]] = []
    for line in proc.stdout.splitlines():
        text = line.strip()
        if not text:
            continue

        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            log.warning("Skipping unparsable docker ps line: %s", text)
            continue

        if not isinstance(payload, dict):
            continue

        container_id = safe_text(payload.get("ID") or payload.get("Id"))
        raw_name = safe_text(payload.get("Names") or payload.get("Name"))
        name = raw_name.lstrip("/") if raw_name else None
        if container_id is None or name is None:
            continue

        status_text = safe_text(payload.get("Status")) or ""
        state = safe_text(payload.get("State")) or ""
        containers.append(
            {
                "id": container_id,
                "name": name,
                "status_text": status_text,
                "state": state,
            }
        )

    return containers


class UnixSocketHTTPConnection(http.client.HTTPConnection):
    def __init__(self, unix_socket_path: str, timeout_seconds: int):
        super().__init__("localhost", timeout=max(1, timeout_seconds))
        self.unix_socket_path = unix_socket_path

    def connect(self) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.settimeout(self.timeout)
            sock.connect(self.unix_socket_path)
        except Exception:
            sock.close()
            raise
        self.sock = sock


def docker_api_get_json(path: str, docker_timeout_seconds: int) -> Any:
    conn = UnixSocketHTTPConnection(DOCKER_SOCKET_PATH, docker_timeout_seconds)
    try:
        conn.request("GET", path, headers={"Host": "localhost"})
        response = conn.getresponse()
        body = response.read()
    except OSError as exc:
        raise RuntimeError(
            f"docker engine API request failed for {path}: {exc}"
        ) from exc
    finally:
        conn.close()

    if response.status >= 400:
        error_text = body.decode("utf-8", errors="replace").strip()
        raise RuntimeError(
            f"docker engine API {path} failed: HTTP {response.status} "
            f"{response.reason}: {error_text}"
        )

    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"docker engine API {path} returned invalid JSON: {exc}"
        ) from exc


async def _fetch_stats_for_container(
    container_id: str,
    docker_timeout_seconds: int,
    sem: asyncio.Semaphore,
    log: logging.Logger,
) -> tuple[str, dict[str, float]] | None:
    async with sem:
        endpoint = f"/containers/{quote(container_id, safe='')}/stats?stream=false"
        try:
            payload = await asyncio.to_thread(
                docker_api_get_json,
                endpoint,
                docker_timeout_seconds,
            )
        except Exception as exc:
            log.warning("Skipping docker stats for %s: %s", container_id[:12], exc)
            return None

        if not isinstance(payload, dict):
            log.warning(
                "Skipping docker stats for %s: unexpected payload type",
                container_id[:12],
            )
            return None

        container_stats: dict[str, float] = {}

        cpu_percent = cpu_percent_from_stats(payload)
        if cpu_percent is not None:
            container_stats["cpu_percent"] = cpu_percent

        memory_stats = payload.get("memory_stats")
        if isinstance(memory_stats, dict):
            memory_usage = safe_float(memory_stats.get("usage"))
            if memory_usage is not None:
                container_stats["memory_usage"] = memory_usage

        network_rx_total, network_tx_total = sum_network_totals(payload)
        if network_rx_total is not None:
            container_stats["network_rx_total"] = network_rx_total
        if network_tx_total is not None:
            container_stats["network_tx_total"] = network_tx_total

        io_read_total, io_write_total = sum_blkio_totals(payload)
        if io_read_total is not None:
            container_stats["io_read_total"] = io_read_total
        if io_write_total is not None:
            container_stats["io_write_total"] = io_write_total

        return container_id, container_stats


async def _fetch_stats_by_id_async(
    container_ids: list[str],
    docker_timeout_seconds: int,
    log: logging.Logger,
) -> dict[str, dict[str, float]]:
    if not container_ids:
        return {}

    max_concurrency = min(12, max(4, len(container_ids)))
    sem = asyncio.Semaphore(max_concurrency)

    tasks = [
        asyncio.create_task(
            _fetch_stats_for_container(
                container_id,
                docker_timeout_seconds,
                sem,
                log,
            )
        )
        for container_id in container_ids
    ]

    stats_by_id: dict[str, dict[str, float]] = {}
    for result in await asyncio.gather(*tasks):
        if result is None:
            continue
        cid, stats = result
        stats_by_id[cid] = stats

    return stats_by_id


def fetch_stats_by_id(
    container_ids: list[str],
    docker_timeout_seconds: int,
    log: logging.Logger,
) -> dict[str, dict[str, float]]:
    return asyncio.run(
        _fetch_stats_by_id_async(
            container_ids,
            docker_timeout_seconds,
            log,
        )
    )


def fetch_inspect_by_id(
    container_ids: list[str],
    docker_timeout_seconds: int,
    log: logging.Logger,
) -> dict[str, dict[str, Any]]:
    if not container_ids:
        return {}

    cmd = ["docker", "inspect", *container_ids]
    proc = run_cmd(cmd, docker_timeout_seconds)
    if proc.returncode != 0:
        raise RuntimeError(f"docker inspect failed: {cmd_error(proc)}")

    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"docker inspect returned invalid JSON: {exc}") from exc

    if not isinstance(payload, list):
        raise RuntimeError("docker inspect returned an unexpected payload")

    inspect_by_id: dict[str, dict[str, Any]] = {}
    for item in payload:
        if not isinstance(item, dict):
            continue

        container_id = safe_text(item.get("Id") or item.get("ID"))
        if container_id is None:
            continue

        state_obj = item.get("State")
        host_cfg = item.get("HostConfig")

        status = None
        started_at: float | None = None
        if isinstance(state_obj, dict):
            status = safe_text(state_obj.get("Status"))
            started_at_str = safe_text(state_obj.get("StartedAt"))
            if started_at_str:
                started_at = parse_docker_timestamp(started_at_str)

        cpuset_cpus = None
        cpu_shares = None
        blkio_weight = None
        if isinstance(host_cfg, dict):
            cpuset_cpus = safe_text(host_cfg.get("CpusetCpus"))
            cpu_shares = safe_int(host_cfg.get("CpuShares"))
            blkio_weight = safe_int(host_cfg.get("BlkioWeight"))

        inspect_by_id[container_id] = {
            "status": status,
            "started_at": started_at,
            "cpuset_cpus": cpuset_cpus,
            "cpu_shares": cpu_shares,
            "blkio_weight": blkio_weight,
        }

    if len(inspect_by_id) != len(container_ids):
        log.debug(
            "docker inspect returned %d records for %d IDs",
            len(inspect_by_id),
            len(container_ids),
        )

    return inspect_by_id


def fetch_containers(
    docker_timeout_seconds: int,
    log: logging.Logger,
) -> list[dict[str, Any]]:
    import time

    ps_containers = fetch_ps_containers(docker_timeout_seconds, log)
    if not ps_containers:
        return []

    container_ids = [entry["id"] for entry in ps_containers]
    inspect_by_id = fetch_inspect_by_id(container_ids, docker_timeout_seconds, log)
    stats_by_id = fetch_stats_by_id(container_ids, docker_timeout_seconds, log)

    containers: list[dict[str, Any]] = []
    for entry in ps_containers:
        container_id = entry["id"]
        name = entry["name"]
        inspect_info = inspect_by_id.get(container_id, {})
        stats_info = stats_by_id.get(container_id, {})

        status = (
            safe_text(inspect_info.get("status"))
            or safe_text(entry.get("state"))
            or safe_text(entry.get("status_text"))
            or "unknown"
        )

        cpuset_cpus = safe_text(inspect_info.get("cpuset_cpus")) or "all"
        cpu_shares = safe_int(inspect_info.get("cpu_shares"))
        blkio_weight = safe_int(inspect_info.get("blkio_weight"))
        started_at = inspect_info.get("started_at")

        now_wall = time.time()
        uptime_seconds: float | None = None
        if (
            status.lower() == "running"
            and isinstance(started_at, float)
            and started_at > 0
            and started_at <= now_wall
        ):
            uptime_seconds = now_wall - started_at

        container: dict[str, Any] = {
            "id": container_id,
            "name": name,
            "status": status.lower(),
            "cpuset_cpus": cpuset_cpus,
            "cpu_shares": cpu_shares if cpu_shares is not None else 0,
            "blkio_weight": blkio_weight if blkio_weight is not None else 0,
        }

        if uptime_seconds is not None:
            container["uptime_seconds"] = uptime_seconds

        cpu_percent = safe_float(stats_info.get("cpu_percent"))
        memory_usage = safe_float(stats_info.get("memory_usage"))
        network_rx_total = safe_float(stats_info.get("network_rx_total"))
        network_tx_total = safe_float(stats_info.get("network_tx_total"))
        io_read_total = safe_float(stats_info.get("io_read_total"))
        io_write_total = safe_float(stats_info.get("io_write_total"))

        if cpu_percent is not None:
            container["cpu_percent"] = cpu_percent
        if memory_usage is not None:
            container["memory_usage"] = memory_usage

        network: dict[str, float] = {}
        if network_rx_total is not None:
            network["cumulative_rx"] = network_rx_total
            container["network_rx_total"] = network_rx_total
        if network_tx_total is not None:
            network["cumulative_tx"] = network_tx_total
            container["network_tx_total"] = network_tx_total
        if network:
            container["network"] = network

        io: dict[str, float] = {}
        if io_read_total is not None:
            io["cumulative_ior"] = io_read_total
            container["io_read_total"] = io_read_total
        if io_write_total is not None:
            io["cumulative_iow"] = io_write_total
            container["io_write_total"] = io_write_total
        if io:
            container["io"] = io

        containers.append(container)

    return containers
