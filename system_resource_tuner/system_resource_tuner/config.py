# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Config dataclasses + options parsing + cpuset helpers."""

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Target:
    container: str
    cpuset_cpus: str | None = None
    cpu_shares: int | None = None
    blkio_weight: int | None = None


@dataclass(frozen=True)
class ProcessTuning:
    container: str | None = None
    process_match_regex: str = ""
    nice: int | None = None
    cpuset_cpus: str | None = None

    @property
    def is_host(self) -> bool:
        return self.container is None

    @property
    def container_label(self) -> str:
        return self.container if self.container is not None else "host"

    @property
    def is_configured(self) -> bool:
        return self.nice is not None or self.cpuset_cpus is not None


def parse_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        val = v.strip().lower()
        if val in {"1", "true", "yes", "on"}:
            return True
        if val in {"0", "false", "no", "off"}:
            return False
    return default


def load_options(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Options file not found: {path}")
    return json.loads(p.read_text(encoding="utf-8"))


def parse_cpuset_expression(cpus: str) -> set[int] | None:
    value = cpus.strip()
    if not value:
        return set()

    out: set[int] = set()
    for token in value.split(","):
        part = token.strip()
        if not part:
            return None

        if "-" in part:
            start_raw, end_raw = part.split("-", 1)
            if not start_raw.isdigit() or not end_raw.isdigit():
                return None
            start = int(start_raw)
            end = int(end_raw)
            if end < start:
                return None
            for cpu in range(start, end + 1):
                out.add(cpu)
            continue

        if not part.isdigit():
            return None
        out.add(int(part))

    return out


def cpuset_matches(current: str, desired: str) -> bool:
    cur = current.strip()
    des = desired.strip()

    cur_set = parse_cpuset_expression(cur)
    des_set = parse_cpuset_expression(des)
    if cur_set is not None and des_set is not None:
        return cur_set == des_set

    return cur == des


def parse_targets(raw_targets: Any, log: logging.Logger) -> list[Target]:
    if raw_targets is None:
        return []
    if not isinstance(raw_targets, list):
        raise ValueError("'targets' must be a list")

    targets: list[Target] = []
    for idx, raw in enumerate(raw_targets):
        if not isinstance(raw, dict):
            raise ValueError(f"targets[{idx}] must be an object")

        container = str(raw.get("container", "")).strip()
        if not container:
            raise ValueError(f"targets[{idx}].container is required")

        cpuset_raw = raw.get("cpuset_cpus")
        cpuset = None
        if cpuset_raw is not None:
            cpuset = str(cpuset_raw).strip()
            if not cpuset:
                cpuset = None
            elif parse_cpuset_expression(cpuset) is None:
                raise ValueError(f"targets[{idx}].cpuset_cpus is invalid: '{cpuset}'")

        cpu_shares_raw = raw.get("cpu_shares")
        cpu_shares = int(cpu_shares_raw) if cpu_shares_raw is not None else None

        blkio_raw = raw.get("blkio_weight")
        blkio_weight = int(blkio_raw) if blkio_raw is not None else None

        if cpuset is None and cpu_shares is None and blkio_weight is None:
            log.warning(
                "Skipping targets[%d] (%s): no tuning values specified",
                idx,
                container,
            )
            continue

        targets.append(
            Target(
                container=container,
                cpuset_cpus=cpuset,
                cpu_shares=cpu_shares,
                blkio_weight=blkio_weight,
            )
        )

    return targets


def parse_process_tuning(
    raw_cfg: Any,
    block_name: str,
    require_container: bool = True,
) -> ProcessTuning:
    if raw_cfg is None:
        return ProcessTuning()
    if not isinstance(raw_cfg, dict):
        raise ValueError(f"'{block_name}' must be an object")

    container: str | None = str(raw_cfg.get("container", "")).strip() or None
    pattern = str(raw_cfg.get("process_match_regex", "")).strip()

    if pattern:
        try:
            _ = re.compile(pattern)
        except re.error as e:
            raise ValueError(f"{block_name}.process_match_regex is invalid: {e}")

    nice: int | None = None
    if raw_cfg.get("nice") is not None:
        nice = int(raw_cfg["nice"])
        if nice < -20 or nice > 19:
            raise ValueError(f"{block_name}.nice must be between -20 and 19")

    cpuset_cpus: str | None = None
    if raw_cfg.get("cpuset_cpus") is not None:
        cpuset_cpus = str(raw_cfg["cpuset_cpus"]).strip()
        if not cpuset_cpus:
            cpuset_cpus = None
        elif parse_cpuset_expression(cpuset_cpus) is None:
            raise ValueError(
                f"{block_name}.cpuset_cpus must be a valid CPU set expression"
            )

    is_configured = nice is not None or cpuset_cpus is not None
    if is_configured and require_container and not container:
        raise ValueError(
            f"{block_name}.container is required when tuning is configured"
        )
    if is_configured and not pattern:
        raise ValueError(
            f"{block_name}.process_match_regex is required when tuning is configured"
        )

    return ProcessTuning(
        container=container,
        process_match_regex=pattern,
        nice=nice,
        cpuset_cpus=cpuset_cpus,
    )


def parse_process_targets(raw_cfg: Any, log: logging.Logger) -> list[ProcessTuning]:
    if raw_cfg is None:
        return []
    if not isinstance(raw_cfg, list):
        raise ValueError("'process_targets' must be a list")

    out: list[ProcessTuning] = []
    for idx, raw in enumerate(raw_cfg):
        tuning = parse_process_tuning(raw, block_name=f"process_targets[{idx}]")
        if not tuning.is_configured:
            log.warning(
                "Skipping process_targets[%d]: no process tuning values specified",
                idx,
            )
            continue
        out.append(tuning)

    return out


def parse_host_process_targets(
    raw_cfg: Any,
    log: logging.Logger,
) -> list[ProcessTuning]:
    if raw_cfg is None:
        return []
    if not isinstance(raw_cfg, list):
        raise ValueError("'host_process_targets' must be a list")

    out: list[ProcessTuning] = []
    for idx, raw in enumerate(raw_cfg):
        if not isinstance(raw, dict):
            raise ValueError(f"host_process_targets[{idx}] must be an object")

        tuning = parse_process_tuning(
            raw,
            block_name=f"host_process_targets[{idx}]",
            require_container=False,
        )
        if not tuning.is_configured:
            log.warning(
                "Skipping host_process_targets[%d]: no process tuning values specified",
                idx,
            )
            continue
        out.append(tuning)

    return out
