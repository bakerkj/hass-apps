# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Loads, defaults, and validates the add-on options file.

The Supervisor writes user-supplied options to ``/data/options.json``;
``load(path)`` reads that JSON, applies defaults from the ``Config``
dataclass, normalises a few fields (uppercased ``log_level``, parsed
``state_update_interval`` duration string), and validates the result.

Validation rejects:
  * non-http(s) or hostless ``homeassistant_url``
  * unknown ``log_level``
  * malformed ``extra_entities`` (must match ``domain.name``) and
    obviously-malformed ``include_entity_globs`` /
    ``exclude_entity_globs``
  * unparsable / non-positive ``state_update_interval``

Schema errors raise ``ValueError`` — the addon container exits non-zero
on bad config rather than booting with surprising defaults.
"""

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

import humanfriendly

from .const import DEFAULT_HA_URL, ENTITY_RE

# Read-only mounts the Supervisor exposes to add-ons. ``customization_file``
# is restricted to these prefixes so a misconfigured value can't be used to
# read arbitrary container paths (``/etc/passwd``, ``/data/options.json``,
# mounted SSL keys). Paths are resolved before comparison so ``..`` and
# symlink traversals are normalised away.
_ALLOWED_CUSTOMIZATION_PREFIXES = (Path("/homeassistant"), Path("/share"))

DEFAULT_LOG_LEVEL = "INFO"
VALID_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR"})

# Registry-tracking modes. ``incremental`` applies events as they arrive
# (per-entity gets for create/update, local drops for remove); ``full``
# refetches the entire list on any event (matches HA's own frontend
# pattern). See ARCHITECTURE.md for the trade-offs.
REGISTRY_MODES = frozenset({"full", "incremental"})

# Permissive shape check for entity-id globs. fnmatch accepts almost
# any string, so we only reject the obviously-malformed shapes
# (whitespace, path separators, empty) — bare patterns like
# ``*_battery`` (no domain prefix) and full ``light.*`` shapes both
# pass.
_VALID_GLOB_RE = re.compile(r"^[^\s/\\]+$")


@dataclass
class Config:
    """Resolved add-on options."""

    homeassistant_url: str = DEFAULT_HA_URL
    transparent: bool = True
    # state_update_interval in seconds; 0 means "send updates immediately".
    state_update_interval: float = 0.0
    extra_entities: list[str] = field(default_factory=list)
    include_entity_globs: list[str] = field(default_factory=list)
    exclude_entity_globs: list[str] = field(default_factory=list)
    passthrough_all: bool = False
    log_level: str = DEFAULT_LOG_LEVEL
    customization_file: str = ""
    show_client_paths: bool = True
    # Registry-update tracking. See ARCHITECTURE.md for details.
    registry_mode: str = "incremental"
    # Periodic full-refetch interval as a safety net for missed events;
    # 0 disables.
    registry_refetch_interval: float = 60.0
    # In incremental mode, if a single debounced burst contains more than
    # this many entity events, promote to one full list refetch instead
    # of issuing N per-entity gets. 0 disables (always per-entity).
    registry_burst_threshold: int = 50


def load(path: str) -> Config:
    """Read, parse, default, and validate the options file at ``path``."""
    with open(path, encoding="utf-8") as f:
        opts = json.load(f)
    if not isinstance(opts, dict):
        raise ValueError("options file must contain a JSON object")

    cfg = Config()
    if opts.get("homeassistant_url"):
        cfg.homeassistant_url = str(opts["homeassistant_url"])
    if "transparent" in opts:
        cfg.transparent = bool(opts["transparent"])
    cfg.extra_entities = [str(e) for e in (opts.get("extra_entities") or [])]
    cfg.include_entity_globs = [
        str(g) for g in (opts.get("include_entity_globs") or [])
    ]
    cfg.exclude_entity_globs = [
        str(g) for g in (opts.get("exclude_entity_globs") or [])
    ]
    if "passthrough_all" in opts:
        cfg.passthrough_all = bool(opts["passthrough_all"])
    if opts.get("log_level"):
        cfg.log_level = str(opts["log_level"]).upper()
    cfg.customization_file = str(opts.get("customization_file") or "")
    if "show_client_paths" in opts:
        cfg.show_client_paths = bool(opts["show_client_paths"])
    if opts.get("registry_mode"):
        cfg.registry_mode = str(opts["registry_mode"]).lower()
    if "registry_refetch_interval" in opts:
        cfg.registry_refetch_interval = float(opts["registry_refetch_interval"])
    if "registry_burst_threshold" in opts:
        cfg.registry_burst_threshold = int(opts["registry_burst_threshold"])

    interval = str(opts.get("state_update_interval") or "")
    if interval:
        cfg.state_update_interval = _parse_duration(interval)

    _validate(cfg)
    return cfg


def _parse_duration(s: str) -> float:
    """Parse a duration string into seconds (positive float), or raise
    ``ValueError`` on a malformed / non-positive value.
    """
    # Delegates the parsing to humanfriendly. Accepts the previous shapes
    # (``"2s"``, ``"500ms"``, ``"1h"``) plus fractional forms (``"1.5h"``),
    # plus days/weeks (``"2d"``, ``"1w"``). A bare numeric value (``"5"``)
    # is interpreted as seconds. Compound forms like ``"1h30m"`` are NOT
    # supported by humanfriendly's parser.
    try:
        seconds = humanfriendly.parse_timespan(s)
    except humanfriendly.InvalidTimespan as exc:
        raise ValueError(f"invalid state_update_interval {s!r}") from exc
    if seconds <= 0:
        raise ValueError("state_update_interval must be > 0 when set")
    return float(seconds)


def _validate(cfg: Config) -> None:
    """Reject obviously-broken option values. Validates the upstream URL,
    log level, entity-id shape of ``extra_entities``, and glob shape of
    the include/exclude lists. Raises ``ValueError`` with a message
    pointing at the offending field; the addon container then exits
    non-zero rather than booting with surprising defaults.
    """
    u = urlparse(cfg.homeassistant_url)
    if u.scheme not in ("http", "https"):
        raise ValueError(
            f"homeassistant_url {cfg.homeassistant_url!r} must use http or https"
        )
    if not u.netloc:
        raise ValueError(
            f"homeassistant_url {cfg.homeassistant_url!r} must include a host"
        )
    if cfg.log_level not in VALID_LOG_LEVELS:
        raise ValueError(f"invalid log_level {cfg.log_level!r}")
    for eid in cfg.extra_entities:
        if not ENTITY_RE.match(eid):
            raise ValueError(
                f"invalid entity_id {eid!r} in extra_entities "
                f"(expected domain.name with lowercase letters/digits/underscores)"
            )
    for glob in cfg.include_entity_globs:
        if not _VALID_GLOB_RE.match(glob):
            raise ValueError(
                f"invalid entity-id glob {glob!r} in include_entity_globs "
                f"(must be non-empty, no whitespace or path separators)"
            )
    for glob in cfg.exclude_entity_globs:
        if not _VALID_GLOB_RE.match(glob):
            raise ValueError(
                f"invalid entity-id glob {glob!r} in exclude_entity_globs "
                f"(must be non-empty, no whitespace or path separators)"
            )
    if cfg.registry_mode not in REGISTRY_MODES:
        raise ValueError(
            f"invalid registry_mode {cfg.registry_mode!r} "
            f"(expected one of {sorted(REGISTRY_MODES)})"
        )
    if cfg.registry_refetch_interval < 0:
        raise ValueError(
            f"registry_refetch_interval must be >= 0 "
            f"(got {cfg.registry_refetch_interval!r})"
        )
    if cfg.registry_burst_threshold < 0:
        raise ValueError(
            f"registry_burst_threshold must be >= 0 "
            f"(got {cfg.registry_burst_threshold!r})"
        )
    if cfg.customization_file:
        _validate_customization_path(cfg.customization_file)


def _validate_customization_path(raw: str) -> None:
    """Reject ``customization_file`` values that escape the read-only
    Supervisor mounts. ``Path.resolve`` canonicalises symlinks and ``..``
    segments so a traversal like ``/homeassistant/../etc/passwd`` collapses to
    ``/etc/passwd`` before the prefix check. The error message names the
    allowed roots but does NOT echo the rejected path beyond a repr — the
    file contents themselves are never read here, so nothing privileged
    leaks via the addon log.
    """
    try:
        resolved = Path(raw).resolve()
    except OSError as exc:
        # Symlink loops, EACCES on a path component, etc. surface as raw
        # tracebacks in the Supervisor log without this; wrap so operators
        # see a friendly message naming the offending option.
        raise ValueError(
            f"customization_file {raw!r} could not be resolved: {exc}"
        ) from exc
    for allowed in _ALLOWED_CUSTOMIZATION_PREFIXES:
        try:
            resolved.relative_to(allowed)
        except ValueError:
            continue
        return
    allowed_str = ", ".join(str(p) for p in _ALLOWED_CUSTOMIZATION_PREFIXES)
    raise ValueError(f"customization_file {raw!r} must be under one of: {allowed_str}")
