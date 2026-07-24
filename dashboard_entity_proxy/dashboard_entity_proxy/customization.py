# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Optional, site-specific customization loaded from a YAML file outside the
add-on package. Lets a deployment extend the built-in scope knowledge
(baseline entities, per-card implicit-entity registry, helper-platform list)
without modifying the add-on. An empty / missing file leaves every list empty,
which preserves today's behavior.
"""

import fnmatch
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import voluptuous as vol
import yaml

from .const import HELPER_PLATFORMS, SUPPORTED_VERSIONS

# Glob metacharacters recognised in ``entity_keys`` entries. A key
# containing any of these is compiled as a ``fnmatch``-style pattern;
# anything else stays a literal exact-match. Matches today's behaviour
# for plain strings while letting users write ``entity_*`` to cover
# numbered slots without enumerating them.
_GLOB_METACHARS = frozenset("*?[")


_HELPER_PLATFORMS_FROZEN = frozenset(HELPER_PLATFORMS)


def _is_glob(key: str) -> bool:
    return any(c in key for c in _GLOB_METACHARS)


def _compile_glob(key: str) -> re.Pattern[str]:
    """Translate a glob to an anchored regex. ``fnmatch.translate`` adds
    ``\\Z`` at the end so ``entity_*`` only matches a key that *starts*
    with ``entity_``, keeping the "exact match" superset semantics.
    """
    return re.compile(fnmatch.translate(key))


@dataclass(frozen=True)
class CardEntityKeys:
    """Per-card overrides telling the walker which extra dict keys hold
    entity references for one specific card ``type``. Scoped to a single
    card subtree — child cards do not inherit a parent's overrides.

    Plain key names land in ``singles`` / ``lists`` and use O(1) set
    lookup at walk time. Names containing ``*``, ``?``, or ``[`` are
    compiled into ``single_patterns`` / ``list_patterns`` via
    :func:`fnmatch.translate` (implicit ``^…$`` anchoring) and checked
    only when the literal-set lookup misses.
    """

    singles: frozenset[str] = frozenset()
    lists: frozenset[str] = frozenset()
    single_patterns: tuple[re.Pattern[str], ...] = ()
    list_patterns: tuple[re.Pattern[str], ...] = ()

    def matches_single(self, key: str) -> bool:
        return key in self.singles or any(p.match(key) for p in self.single_patterns)

    def matches_list(self, key: str) -> bool:
        return key in self.lists or any(p.match(key) for p in self.list_patterns)


@dataclass(frozen=True)
class Customization:
    """Site-specific extensions to the add-on's built-in scope knowledge.

    Every field is additive: the loader merges values on top of the in-code
    defaults (``FRONTEND_BASELINE``, ``HELPER_PLATFORMS``, and the built-in
    entity-key tables) rather than replacing them. ``implicit_entities`` and
    ``card_entity_keys`` are both indexed by card ``type`` verbatim.
    """

    baseline_entities: tuple[str, ...] = ()
    implicit_entities: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    card_entity_keys: Mapping[str, CardEntityKeys] = field(default_factory=dict)
    extra_helper_platforms: tuple[str, ...] = ()

    def helper_platforms(self) -> tuple[str, ...]:
        """Union of the built-in ``HELPER_PLATFORMS`` with the user's
        ``extra_helper_platforms``, deduped while preserving the
        built-in order followed by the order of the user's additions.
        A user entry that already exists in ``HELPER_PLATFORMS`` is dropped.
        """
        extras = tuple(
            p for p in self.extra_helper_platforms if p not in _HELPER_PLATFORMS_FROZEN
        )
        # Dedup within extras while preserving first-seen order.
        seen: set[str] = set()
        deduped_extras: list[str] = []
        for p in extras:
            if p not in seen:
                seen.add(p)
                deduped_extras.append(p)
        return (*HELPER_PLATFORMS, *deduped_extras)


# ---- Schema ----------------------------------------------------------------

# Non-empty string element used in every entity-id / domain list, so an
# empty literal in YAML is rejected early.
_NONEMPTY_STR = vol.All(str, vol.Length(min=1))


def _no_overlap(entity_keys: dict[str, Any]) -> dict[str, Any]:
    """Reject ``entity_keys`` blocks that declare the same key under
    ``singles`` and ``lists``, including glob/literal collisions where a
    glob on one side matches a literal on the other.

    Pure-literal overlap (``singles: [foo]`` + ``lists: [foo]``) and
    duplicate-glob overlap (``singles: [foo_*]`` + ``lists: [foo_*]``)
    are caught by the textual intersection. The cross-checks below add
    coverage for ``singles: [foo_*]`` + ``lists: [foo_1]`` and the
    mirror case. The walker checks literals first, so an unflagged
    ambiguity would silently route the colliding key to the literal side.
    """
    singles_all = set(entity_keys.get("singles") or [])
    lists_all = set(entity_keys.get("lists") or [])
    overlap = singles_all & lists_all
    if overlap:
        raise vol.Invalid(f"{sorted(overlap)!r} declared as both single and list")

    singles_lit = {k for k in singles_all if not _is_glob(k)}
    lists_lit = {k for k in lists_all if not _is_glob(k)}
    singles_globs = [(k, _compile_glob(k)) for k in singles_all if _is_glob(k)]
    lists_globs = [(k, _compile_glob(k)) for k in lists_all if _is_glob(k)]

    for lit in singles_lit:
        for glob_str, pat in lists_globs:
            if pat.match(lit):
                raise vol.Invalid(
                    f"single key {lit!r} also matched by list glob {glob_str!r}"
                )
    for lit in lists_lit:
        for glob_str, pat in singles_globs:
            if pat.match(lit):
                raise vol.Invalid(
                    f"list key {lit!r} also matched by single glob {glob_str!r}"
                )
    return entity_keys


_ENTITY_KEYS_SCHEMA = vol.All(
    vol.Schema(
        {
            vol.Optional("singles", default=list): [_NONEMPTY_STR],
            vol.Optional("lists", default=list): [_NONEMPTY_STR],
        },
        extra=vol.PREVENT_EXTRA,
    ),
    _no_overlap,
)

_CARD_SCHEMA = vol.Schema(
    {
        vol.Optional("implicit_entities", default=list): [_NONEMPTY_STR],
        vol.Optional("entity_keys"): _ENTITY_KEYS_SCHEMA,
    },
    extra=vol.PREVENT_EXTRA,
)

_BASELINE_SCHEMA = vol.Schema(
    {
        vol.Optional("entities", default=list): [_NONEMPTY_STR],
    },
    extra=vol.PREVENT_EXTRA,
)

_HELPERS_SCHEMA = vol.Schema(
    {
        vol.Optional("extra_platforms", default=list): [_NONEMPTY_STR],
    },
    extra=vol.PREVENT_EXTRA,
)

_FILE_SCHEMA = vol.Schema(
    {
        vol.Required("version"): vol.In(sorted(SUPPORTED_VERSIONS)),
        vol.Optional("baseline"): _BASELINE_SCHEMA,
        vol.Optional("cards"): {_NONEMPTY_STR: _CARD_SCHEMA},
        vol.Optional("helpers"): _HELPERS_SCHEMA,
    },
    extra=vol.PREVENT_EXTRA,
)


def load_builtins() -> Customization:
    """Load the per-card defaults shipped with the add-on. Same parser
    and schema as user customization; see ``builtin_cards.yaml`` next to
    this module. A failure here means the bundled file is missing or
    malformed (broken install); we raise rather than silently fall back.
    """
    builtin_path = Path(__file__).with_name("builtin_cards.yaml")
    return load(str(builtin_path))


def load(path: str) -> Customization:
    """Read, parse, and validate a customization YAML file.

    A missing file raises ``FileNotFoundError``; callers who want the
    "no customization configured" path should skip calling ``load`` at all.
    Silently returning an empty ``Customization`` for a path the user
    explicitly named hides typos (e.g. a stray trailing space) behind a
    success log line. An empty / whitespace-only file is parsed normally
    and returns an empty ``Customization``. Schema errors raise
    ``ValueError`` with a message that includes the offending path.
    """
    with open(path, encoding="utf-8") as f:
        try:
            raw = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            # YAMLError subclasses neither OSError nor ValueError; wrap
            # so the app-level ``(OSError, ValueError)`` arm catches it.
            raise ValueError(f"{path}: YAML parse error") from exc
    if raw is None:
        return Customization()
    if not isinstance(raw, dict):
        # ValueError keeps this under the app-level ``(OSError, ValueError)``
        # loader catch alongside YAML parse errors.
        raise ValueError(f"{path}: top-level must be a mapping")  # noqa: TRY004

    try:
        data = _FILE_SCHEMA(raw)
    except vol.Invalid as exc:
        # Generic message: voluptuous's ``str(exc)`` can embed snippets
        # of the offending value, and customization paths are user-named
        # files whose contents end up in the Supervisor-visible addon log
        # on validation failure. The original exception is preserved via
        # ``__cause__`` for local debugging but not formatted into the
        # message string.
        raise ValueError(f"{path}: validation failed") from exc

    return _to_customization(data)


def _to_customization(data: dict[str, Any]) -> Customization:
    """Convert a validated-by-voluptuous dict into a ``Customization``
    dataclass, dropping empty entries (a card declaring
    ``implicit_entities: []`` and no ``entity_keys`` registers nothing).
    """
    baseline = data.get("baseline") or {}
    baseline_entities = tuple(baseline.get("entities") or [])

    helpers = data.get("helpers") or {}
    extra_helper_platforms = tuple(helpers.get("extra_platforms") or [])

    implicit_entities: dict[str, tuple[str, ...]] = {}
    card_entity_keys: dict[str, CardEntityKeys] = {}
    for card_type, body in (data.get("cards") or {}).items():
        implicit = tuple(body.get("implicit_entities") or [])
        if implicit:
            implicit_entities[card_type] = implicit
        ekeys = body.get("entity_keys")
        if ekeys is not None:
            singles_raw: list[str] = ekeys.get("singles") or []
            lists_raw: list[str] = ekeys.get("lists") or []
            singles = frozenset(k for k in singles_raw if not _is_glob(k))
            lists_ = frozenset(k for k in lists_raw if not _is_glob(k))
            single_patterns = tuple(
                _compile_glob(k) for k in singles_raw if _is_glob(k)
            )
            list_patterns = tuple(_compile_glob(k) for k in lists_raw if _is_glob(k))
            if singles or lists_ or single_patterns or list_patterns:
                card_entity_keys[card_type] = CardEntityKeys(
                    singles=singles,
                    lists=lists_,
                    single_patterns=single_patterns,
                    list_patterns=list_patterns,
                )

    return Customization(
        baseline_entities=baseline_entities,
        implicit_entities=implicit_entities,
        card_entity_keys=card_entity_keys,
        extra_helper_platforms=extra_helper_platforms,
    )
