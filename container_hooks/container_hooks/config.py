# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Options dataclass + parser + per-container path helpers.

Layout: ``<base_dir>/<container>/`` holds everything for a single
container. The kind of hook lives in a fixed subdirectory under it:
``scripts/``, ``pre-start/``, ``pre-start-files/``,
``pre-start-patches/``, and ``logs/``. Files inside ``scripts/``,
``pre-start/``, and ``pre-start-patches/`` are lex-sorted to control
order; use ``00-``, ``10-``, ``20-`` prefixes.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

_log = logging.getLogger(__name__)

_KNOWN_OPTION_KEYS = frozenset(
    {
        "log_level",
        "base_dir",
        "initial_sweep",
        "debounce_seconds",
        "skip_containers",
        "container_overrides",
        "watch_create_events",
    }
)
_KNOWN_OVERRIDE_KEYS = frozenset({"container", "debounce_seconds"})


@dataclass(frozen=True)
class ContainerOverride:
    """Per-container overrides for global options.

    Only ``container`` is required. ``debounce_seconds`` (when set)
    replaces the global default just for matching events. More override
    fields can be added here without breaking existing config.
    """

    container: str
    debounce_seconds: int | None = None


@dataclass(frozen=True)
class Options:
    log_level: str = "INFO"
    base_dir: Path = Path("/homeassistant/container_hooks")
    initial_sweep: bool = True
    debounce_seconds: int = 2
    skip_containers: tuple[str, ...] = field(default_factory=tuple)
    container_overrides: tuple[ContainerOverride, ...] = field(default_factory=tuple)
    watch_create_events: bool = False


# --- per-container path helpers ---------------------------------------------


def container_dir(options: Options, container: str) -> Path:
    return options.base_dir / container


def scripts_dir(options: Options, container: str) -> Path:
    """Post-start hooks: lex-sorted ``*.sh`` files run inside the target."""
    return container_dir(options, container) / "scripts"


def pre_start_scripts_dir(options: Options, container: str) -> Path:
    """Pre-start scripts: lex-sorted ``*.sh`` files run in the addon container."""
    return container_dir(options, container) / "pre-start"


def pre_start_files_dir(options: Options, container: str) -> Path:
    """Pre-start file tree: tarred and ``put_archive``'d into the target."""
    return container_dir(options, container) / "pre-start-files"


def pre_start_patches_dir(options: Options, container: str) -> Path:
    """Pre-start patches: lex-sorted ``*.patch`` files applied to the target."""
    return container_dir(options, container) / "pre-start-patches"


def logs_dir(options: Options, container: str) -> Path:
    return container_dir(options, container) / "logs"


def post_start_log(options: Options, container: str) -> Path:
    return logs_dir(options, container) / "post-start.log"


def pre_start_log(options: Options, container: str) -> Path:
    return logs_dir(options, container) / "pre-start.log"


# --- options loader ---------------------------------------------------------


def _coerce_int(value: object, default: int, key: str) -> int:
    """Coerce a JSON scalar to ``int``, falling back to ``default`` on bad input.

    Logs a warning rather than crashing the addon — Supervisor passes
    options through a JSON-Schema layer that should catch most type
    errors, but defensive parsing keeps a hand-edited options.json from
    taking the whole addon down.
    """
    try:
        # ``int(object)`` isn't a typed overload, but the try/except below
        # catches the TypeError/ValueError that mypy is worried about.
        return max(0, int(value))  # type: ignore[call-overload]
    except TypeError, ValueError:
        _log.warning(
            "options: %s=%r is not an integer; using default %d", key, value, default
        )
        return default


def load_options(path: str) -> Options:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise TypeError(
            f"options file {path!r} must be a JSON object, got {type(raw).__name__}"
        )
    unknown = sorted(set(raw) - _KNOWN_OPTION_KEYS)
    if unknown:
        _log.warning(
            "options: ignoring unrecognized top-level key(s): %s", ", ".join(unknown)
        )
    skip = tuple(
        str(c).strip() for c in (raw.get("skip_containers") or []) if str(c).strip()
    )
    overrides: list[ContainerOverride] = []
    for entry in raw.get("container_overrides") or []:
        if not isinstance(entry, dict):
            continue
        unknown_o = sorted(set(entry) - _KNOWN_OVERRIDE_KEYS)
        if unknown_o:
            _log.warning(
                "options: ignoring unrecognized container_overrides key(s) on %r: %s",
                entry.get("container"),
                ", ".join(unknown_o),
            )
        name = str(entry.get("container", "")).strip()
        if not name:
            continue
        debounce_raw = entry.get("debounce_seconds")
        debounce = (
            _coerce_int(
                debounce_raw, 0, f"container_overrides[{name}].debounce_seconds"
            )
            if debounce_raw is not None
            else None
        )
        overrides.append(ContainerOverride(container=name, debounce_seconds=debounce))
    return Options(
        log_level=str(raw.get("log_level", "INFO")).upper(),
        base_dir=Path(str(raw.get("base_dir", "/homeassistant/container_hooks"))),
        initial_sweep=bool(raw.get("initial_sweep", True)),
        debounce_seconds=_coerce_int(
            raw.get("debounce_seconds", 2), 2, "debounce_seconds"
        ),
        skip_containers=skip,
        container_overrides=tuple(overrides),
        watch_create_events=bool(raw.get("watch_create_events", False)),
    )
