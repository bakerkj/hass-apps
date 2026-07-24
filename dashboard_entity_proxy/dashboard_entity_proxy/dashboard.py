# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Lovelace dashboard config parser: extracts the entity ids referenced by a dashboard.

The walker is deliberately structure-agnostic: it recurses through every
nested dict and list and harvests entity ids from a small set of well-known
keys wherever they appear. That covers built-in cards, container cards (stack,
grid, sections, panel, conditional, picture-elements, …), and the custom-card
ecosystem (mushroom, plotly, apexcharts, multiple-entity-row, battery-state,
history-explorer, mini-graph, flex-table, etc.) without a per-type rule book.

It also recognises two patterns that defer entity selection to runtime:

- ``custom:auto-entities`` ``filter`` blocks with ``include`` / ``exclude``
  lists whose items carry an ``entity_id`` (glob) or ``domain`` filter.
- ``custom:flex-table-card`` ``entities: {include: <regex>, exclude: <regex>}``
  blocks (anchored-at-start regex strings).

These cannot be resolved to concrete ids at parse time, so they are returned
alongside the static ids as patterns; the session expands them against the
state mirror at scope-resolution time. Filter conditions that depend on
runtime state (``state``, ``attributes``, ``last_changed``, etc.) are ignored —
we deliver a superset of what the card will actually render.
"""

import fnmatch
import functools
import re
from dataclasses import dataclass, field
from typing import Any

import voluptuous as vol

from . import customization as _customization
from .const import ENTITY_RE, FRONTEND_BASELINE
from .customization import CardEntityKeys, Customization

# Keys whose value is a single entity id string (or sometimes a list).
# Reserved for HA-canonical card schema conventions used across the whole
# ecosystem. Card-specific keys (e.g. ``custom:power-flow-card``'s slot
# names) belong in ``_BUILTIN_CARD_ENTITY_KEYS`` so they only apply within
# that card's subtree — generic English nouns like ``home`` or ``grid``
# don't otherwise collide with unrelated cards.
_SINGLE_ENTITY_KEYS = frozenset(
    {
        "entity",
        "entity_id",
        "camera_image",
        "camera_view",
        "image_entity",
        "person_entity",
    }
)

# Keys whose value is a list of entity refs — each item is either a string or
# an object that itself carries an `entity` / `entity_id` key.
_ENTITY_LIST_KEYS = frozenset(
    {
        "entities",
        "entity_ids",
        "chips",
        "series",
        "tiles",
        "widgets",
        "rows",
        "badges",
    }
)

# Keys whose value is an action-config dict; we extract entity_id from
# call-service / perform-action targets but never blindly recurse — service
# data fields named entity_id under a non-call-service action are intentional
# no-ops, not entity references. The tile card also accepts ``icon_tap_action``
# / ``icon_hold_action`` / ``icon_double_tap_action`` and the area card accepts
# ``image_tap_action``; matching any ``*_action`` key keeps the gate applied to
# the whole family without enumerating every variant.
_ACTION_KEYS = frozenset({"tap_action", "hold_action", "double_tap_action"})


def _is_action_key(key: object) -> bool:
    """Whether a dict key holds an action-config (``tap_action`` etc.).
    Matches the canonical names plus any ``*_action`` variant so card
    families that ship ``icon_tap_action`` / ``image_tap_action`` etc.
    are gated the same way without enumerating every variant.
    """
    return isinstance(key, str) and (key in _ACTION_KEYS or key.endswith("_action"))


# Cards whose YAML deliberately declares no entity references but whose
# renderer subscribes to a fixed, well-known entity id at runtime are
# declared in the optional customization file (see
# :class:`customization.Customization.implicit_entities`). Nothing is shipped
# in code; every site-specific entry lives in its own YAML so the addon
# stays free of one user's card choices.


# Per-card entity-key defaults shipped with the add-on, loaded from
# ``builtin_cards.yaml`` (same schema as the user customization file).
# Keys here apply only inside that card's subtree, so generic English
# nouns like ``home`` or ``grid`` don't false-positive on unrelated
# cards. Merged with a user's ``Customization.card_entity_keys``
# (additive union of ``singles`` / ``lists``) when the walker enters a
# typed dict. Lazy-loaded on first use so a malformed shipped file
# surfaces through the app/test logger rather than crashing at import
# before ``logging.basicConfig`` runs. A schema failure here is a broken
# install (programmer / packaging error), not user config — we re-raise
# as ``RuntimeError`` to keep that distinction at the call site.
@functools.cache
def _load_builtin_customization() -> Customization:
    """Return the addon's bundled per-card defaults, cached after first call.

    ``customization.load`` already converts ``voluptuous.Invalid`` to
    ``ValueError``; we also defend against a raw ``vol.Invalid`` leaking
    out of any future refactor and rewrap both into ``RuntimeError`` so
    callers can distinguish "shipped file is broken" from "user config
    is broken".
    """
    try:
        return _customization.load_builtins()
    except (vol.Invalid, ValueError) as exc:
        raise RuntimeError(f"builtin_cards.yaml is malformed: {exc}") from exc


def _merge_frames(
    a: CardEntityKeys | None, b: CardEntityKeys | None
) -> CardEntityKeys | None:
    """Additive union of two per-card key frames. ``None`` is identity."""
    if a is None:
        return b
    if b is None:
        return a
    return CardEntityKeys(
        singles=a.singles | b.singles,
        lists=a.lists | b.lists,
        single_patterns=a.single_patterns + b.single_patterns,
        list_patterns=a.list_patterns + b.list_patterns,
    )


@dataclass(frozen=True)
class ExtractResult:
    """Static analysis of a dashboard: explicit entity ids plus deferred
    selection rules for ``custom:auto-entities`` and ``custom:flex-table-card``.
    """

    ids: tuple[str, ...] = ()
    include_globs: tuple[str, ...] = ()
    include_regexes: tuple[str, ...] = ()
    include_domains: tuple[str, ...] = ()
    exclude_globs: tuple[str, ...] = ()
    exclude_regexes: tuple[str, ...] = ()
    exclude_domains: tuple[str, ...] = ()

    def empty(self) -> bool:
        """Whether the parser found nothing — neither static ids nor any
        include pattern that could expand to something. (Exclude-only is
        not a valid scope; treat that as empty too.)
        """
        return not (
            self.ids
            or self.include_globs
            or self.include_regexes
            or self.include_domains
        )

    def has_patterns(self) -> bool:
        """Whether any pattern rule (include or exclude) needs runtime
        evaluation against new entities as they arrive in the mirror.
        Static-id-only results don't.
        """
        return bool(
            self.include_globs
            or self.include_regexes
            or self.include_domains
            or self.exclude_globs
            or self.exclude_regexes
            or self.exclude_domains
        )


def extract_energy_entities(prefs: dict[str, Any]) -> set[str]:
    """Walk an ``energy/get_prefs`` response and collect every value
    that looks like an entity_id (``domain.name`` lowercase-shape).

    Energy preferences carry entity ids in several nested fields whose
    exact set evolves as HA adds new source types — instead of
    enumerating the known shape (energy_sources / device_consumption /
    flow_from / stat_energy_from / stat_cost / ...), we walk the whole
    tree and pick up everything :data:`ENTITY_RE` matches. Numeric
    config values (cost_adjustment_day, etc.) and config-entry ids
    don't match the regex and are skipped.

    Sibling to the typed Lovelace walkers above; lives here because
    "extract entity ids from input structure X" is this module's job.
    """
    found: set[str] = set()

    def _walk_anon(node: Any) -> None:
        if isinstance(node, str):
            if ENTITY_RE.match(node):
                found.add(node)
        elif isinstance(node, dict):
            for v in node.values():
                _walk_anon(v)
        elif isinstance(node, list):
            for v in node:
                _walk_anon(v)

    _walk_anon(prefs)
    return found


@dataclass
class ScopeSet:
    """A resolved scope: ``all`` entities, or a set of static ids plus
    optional pattern rules that the session expands against the state
    mirror at apply-time (and re-checks for newly-arriving entities).
    """

    all: bool = False
    ids: list[str] = field(default_factory=list)
    extract: ExtractResult | None = None

    @property
    def has_patterns(self) -> bool:
        """Whether this scope has runtime-resolved pattern rules (globs / regexes
        / domain filters) that need to be re-evaluated as new entities arrive.
        """
        return self.extract is not None and self.extract.has_patterns()


def is_strategy(config: dict[str, Any]) -> bool:
    """Report whether the config (or any view) is strategy-generated."""
    if "strategy" in config:
        return True
    return any(
        isinstance(v, dict) and "strategy" in v for v in (config.get("views") or [])
    )


def extract_scope(
    config: dict[str, Any], customization: Customization | None = None
) -> ExtractResult:
    """Walk the dashboard and return its full static analysis: explicit ids
    plus any glob / regex / domain pattern from dynamic-entity cards.
    """
    state = _State(customization or Customization())
    _walk(state, config)
    return ExtractResult(
        ids=tuple(sorted(state.ids)),
        include_globs=tuple(sorted(state.include_globs)),
        include_regexes=tuple(sorted(state.include_regexes)),
        include_domains=tuple(sorted(state.include_domains)),
        exclude_globs=tuple(sorted(state.exclude_globs)),
        exclude_regexes=tuple(sorted(state.exclude_regexes)),
        exclude_domains=tuple(sorted(state.exclude_domains)),
    )


def apply_filters(
    ids: list[str],
    extra: list[str],
    include: list[str],
    exclude: list[str],
    customization: Customization | None = None,
) -> list[str]:
    """Augment ids with extra and the frontend baseline, then constrain with
    include/exclude globs. The baseline bypasses include globs (the user
    didn't ask for it; we add it for them) but still respects excludes.
    """
    cust = customization or Customization()
    found = set(ids)
    found.update(e for e in extra if e)
    out = {
        eid
        for eid in found
        if (not include or _matches_any(eid, include))
        and not _matches_any(eid, exclude)
    }
    for eid in (*FRONTEND_BASELINE, *cust.baseline_entities):
        if not _matches_any(eid, exclude):
            out.add(eid)
    return sorted(out)


def expand_patterns(result: ExtractResult, candidates: list[str]) -> set[str]:
    """Resolve an ExtractResult against a set of candidate entity ids (a
    mirror snapshot). Returns the static ids plus every candidate that
    matches at least one include pattern and no exclude pattern.
    """
    matched: set[str] = set(result.ids)
    if not (result.include_globs or result.include_regexes or result.include_domains):
        return matched
    inc_globs = [_auto_glob_to_regex(g) for g in result.include_globs]
    exc_globs = [_auto_glob_to_regex(g) for g in result.exclude_globs]
    inc_regs = [c for c in (_safe_compile(r) for r in result.include_regexes) if c]
    exc_regs = [c for c in (_safe_compile(r) for r in result.exclude_regexes) if c]
    for eid in candidates:
        if eid in matched:
            continue
        if not (
            any(rx.match(eid) for rx in inc_globs)
            or any(rx.match(eid) for rx in inc_regs)
            or any(eid.startswith(d + ".") for d in result.include_domains)
        ):
            continue
        if (
            any(rx.match(eid) for rx in exc_globs)
            or any(rx.match(eid) for rx in exc_regs)
            or any(eid.startswith(d + ".") for d in result.exclude_domains)
        ):
            continue
        matched.add(eid)
    return matched


def matches_extract(result: ExtractResult, eid: str) -> bool:
    """Whether a single entity id satisfies the result's pattern set. Used
    to fold newly-arriving entities into a live scope without re-expanding
    the whole mirror.
    """
    if eid in result.ids:
        return True
    if not (result.include_globs or result.include_regexes or result.include_domains):
        return False
    inc = (
        any(_auto_glob_to_regex(g).match(eid) for g in result.include_globs)
        or any(
            c.match(eid)
            for c in (_safe_compile(r) for r in result.include_regexes)
            if c
        )
        or any(eid.startswith(d + ".") for d in result.include_domains)
    )
    if not inc:
        return False
    exc = (
        any(_auto_glob_to_regex(g).match(eid) for g in result.exclude_globs)
        or any(
            c.match(eid)
            for c in (_safe_compile(r) for r in result.exclude_regexes)
            if c
        )
        or any(eid.startswith(d + ".") for d in result.exclude_domains)
    )
    return not exc


@functools.cache
def _auto_glob_to_regex(pattern: str) -> re.Pattern[str]:
    """Convert a ``custom:auto-entities``-style entity_id glob to a compiled
    regex. Bug-compatible with auto-entities itself: ``*`` becomes ``.*`` and
    ``.`` is left as the regex any-char metacharacter (the upstream card
    does ``pattern.replace(/\\./g, ".").replace(/\\*/g, ".*")`` — the dot
    replacement is a deliberate no-op there too). That's why
    ``sensor.*.wifi_signal`` actually matches ``sensor.kitchen_wifi_signal``
    in real dashboards: the literal ``.`` in the pattern matches the
    ``_`` in the entity id. We mirror that so scope agrees with what the
    card will render.

    Cached so that a live scope with stable pattern set doesn't recompile
    the same regex on every newly-arriving entity.
    """
    return re.compile("^" + pattern.replace("*", ".*") + "$")


def _matches_any(value: str, patterns: list[str]) -> bool:
    """True if ``value`` matches at least one fnmatch glob in ``patterns``.
    Case-sensitive match — HA entity ids are lower-case by convention.
    """
    return any(fnmatch.fnmatchcase(value, p) for p in patterns)


@functools.cache
def _safe_compile(pattern: str) -> re.Pattern[str] | None:
    """Compile ``pattern`` as a regex, returning ``None`` on failure
    instead of raising. Used to filter out malformed regex strings
    pulled from card YAML. Cached so the live-scope hot path doesn't
    re-compile (or even re-validate) the same pattern per entity.
    """
    try:
        return re.compile(pattern)
    except re.error:
        return None


@dataclass
class _State:
    """Mutable accumulator threaded through the recursive walk.

    ``customization`` is captured so the walker can consult per-card
    overrides without threading it through every recursion.
    """

    customization: Customization
    ids: set[str] = field(default_factory=set)
    include_globs: set[str] = field(default_factory=set)
    include_regexes: set[str] = field(default_factory=set)
    include_domains: set[str] = field(default_factory=set)
    exclude_globs: set[str] = field(default_factory=set)
    exclude_regexes: set[str] = field(default_factory=set)
    exclude_domains: set[str] = field(default_factory=set)


def _walk(state: _State, node: Any, frame: CardEntityKeys | None = None) -> None:
    """Generic recursive descent. At each dict, harvest entity references from
    known keys; then descend into every value (dict or list). Action subtrees
    are handled by their dedicated extractor and not recursed into, so that
    entity_id fields under a non-call-service action don't leak through.

    ``frame`` carries per-card entity-key overrides from the active card's
    customization entry. Entering any dict with a ``type:`` resets the frame
    to that type's overrides (possibly ``None``) — overrides do not inherit
    from a parent card into a child card.
    """
    if isinstance(node, dict):
        type_name = node.get("type")
        if isinstance(type_name, str):
            implicit = state.customization.implicit_entities.get(type_name)
            if implicit is not None:
                state.ids.update(implicit)
            # Builtin per-card defaults + user-side customization, merged
            # additively. Entering a typed dict always resets the frame —
            # nested cards don't inherit a parent's overrides.
            frame = _merge_frames(
                _load_builtin_customization().card_entity_keys.get(type_name),
                state.customization.card_entity_keys.get(type_name),
            )
        for key, value in node.items():
            if key == "entities" and isinstance(value, dict):
                # custom:flex-table-card has the shape
                # ``entities: {include: "<regex>", exclude: "<regex>"}``;
                # other cards (``custom:power-flow-card-plus``) use
                # ``entities: {battery: {entity: ...}, grid: {...}, ...}``.
                # Detect the flex shape by the presence of ``include``/
                # ``exclude`` keys; otherwise recurse so the nested
                # ``entity:`` keys still get harvested.
                if "include" in value or "exclude" in value:
                    _harvest_flex_entities(state, value)
                else:
                    _walk(state, value, frame)
                continue
            if (
                key == "filter"
                and isinstance(value, dict)
                and type_name == "custom:auto-entities"
            ):
                # custom:auto-entities style:
                # filter: {include: [{entity_id|domain|state|...}, ...], exclude: [...]}
                # Other cards may have an unrelated ``filter:`` dict (e.g.
                # automations, generic config blocks); without the
                # ``type`` guard they would false-positive as auto-entities.
                _harvest_auto_entities_filter(state, value)
                continue
            if key in _SINGLE_ENTITY_KEYS:
                _add_one_or_many(state.ids, value)
                # ``custom:power-flow-card-plus`` (and siblings) use
                # ``entity: {consumption: ..., production: ...}`` — a dict
                # value carrying nested entity-shaped keys. ``_add_one_or_many``
                # ignores non-string-non-list values, so recurse to pick up
                # the nested ids that this branch would otherwise skip.
                if isinstance(value, dict):
                    _walk(state, value, frame)
                continue
            if key in _ENTITY_LIST_KEYS:
                _add_list(state.ids, value)
                _walk(state, value, frame)
                continue
            if _is_action_key(key):
                if isinstance(value, dict):
                    _add_from_action(state.ids, value)
                continue
            if key in ("include", "exclude") and isinstance(value, list):
                # custom:scheduler-card style: a card-root ``include:`` /
                # ``exclude:`` whose items are entity-id strings (or
                # entity-id globs). The auto-entities / flex-table-card
                # variants are handled above and ``continue`` before they
                # reach this branch, so this only runs for cards that ship
                # bare include / exclude lists.
                _harvest_bare_filter_list(state, key, value)
                continue
            if key == "customize" and isinstance(value, dict):
                # custom:scheduler-card ``customize:`` map whose KEYS are
                # entity ids or entity-id globs (with arbitrary per-entity
                # metadata as values). The walker doesn't otherwise inspect
                # dict keys against the entity shape.
                _harvest_scheduler_customize(state, value)
                _walk(state, value, frame)
                continue
            if frame is not None and frame.matches_single(key):
                _add_one_or_many(state.ids, value)
                # Same dict-recurse as the global ``_SINGLE_ENTITY_KEYS``
                # branch: ``power-flow-card-plus`` uses ``entity: {dict}``
                # for split consumption/production, and its slot keys
                # (``battery``, ``grid``, ...) follow the same convention.
                if isinstance(value, dict):
                    _walk(state, value, frame)
                continue
            if frame is not None and frame.matches_list(key):
                _add_list(state.ids, value)
                _walk(state, value, frame)
                continue
            if (
                isinstance(key, str)
                and (
                    (key.endswith(("_entity", "_sensor"))) or key.startswith("sensor_")
                )
                and key not in _SINGLE_ENTITY_KEYS
            ):
                # Generic ``<name>_entity`` / ``<name>_sensor`` / ``sensor_<name>``
                # keys. Examples we've seen in the wild:
                #   *_entity  — base_entity, camera_entity, azimuth_entity, ...
                #   *_sensor  — temperature_sensor, humidity_sensor, aqi_sensor
                #               (``custom:clock-weather-card`` and similar)
                #   sensor_*  — sensor_pv1, sensor_bat1_soc, sensor_grid_power
                #               (``custom:lumina-energy-card`` and similar)
                # String values are added if entity-id-shaped; dict / list
                # values get recursed so a wrapping config like
                # ``wind_direction_entity: {entity: ...}`` still resolves.
                # ``_looks_like_entity`` keeps random ``sensor_count: 5`` or
                # ``temperature_sensor: false`` style usages out of scope.
                if isinstance(value, str):
                    if _looks_like_entity(value):
                        state.ids.add(value)
                else:
                    _walk(state, value, frame)
                continue
            _walk(state, value, frame)
    elif isinstance(node, list):
        for item in node:
            _walk(state, item, frame)


def _harvest_bare_filter_list(state: _State, key: str, value: list[Any]) -> None:
    """``custom:scheduler-card``-style ``include:`` / ``exclude:`` as a flat
    list of entity-id strings (or entity-id globs). Items that are not
    entity-shaped are ignored so this stays safe when called against
    look-alike keys that happen to be lists of something else.
    """
    for item in value:
        if not isinstance(item, str) or not item:
            continue
        is_glob = any(c in item for c in "*?[")
        if is_glob:
            target = state.include_globs if key == "include" else state.exclude_globs
            target.add(item)
        elif ENTITY_RE.match(item):
            if key == "include":
                state.ids.add(item)
            else:
                # Excludes are degenerate globs matching only the literal id;
                # the runtime pattern resolver treats them as exact excludes.
                state.exclude_globs.add(item)


def _harvest_scheduler_customize(state: _State, value: dict[str, Any]) -> None:
    """``custom:scheduler-card`` ``customize:`` block — keys are entity ids
    or entity-id globs, values are per-entity metadata (name, icon, etc.).
    Non-entity-shaped keys are silently skipped so it's safe to invoke this
    against any ``customize:`` dict; the walker still recurses into the
    values to pick up any nested ``entity:`` refs inside the override.
    """
    for entity_key in value:
        if not isinstance(entity_key, str) or not entity_key:
            continue
        if any(c in entity_key for c in "*?["):
            state.include_globs.add(entity_key)
        elif ENTITY_RE.match(entity_key):
            state.ids.add(entity_key)


def _harvest_flex_entities(state: _State, value: dict[str, Any]) -> None:
    """``custom:flex-table-card`` and similar:

    ``entities: {include: "<regex>", exclude: "<regex>"}``

    The values are anchored-at-start regex strings (per the flex-table-card
    docs); they may also be a list of such regexes.
    """
    for key, dst in (
        ("include", state.include_regexes),
        ("exclude", state.exclude_regexes),
    ):
        pat = value.get(key)
        if isinstance(pat, str) and pat:
            dst.add(pat)
        elif isinstance(pat, list):
            for item in pat:
                if isinstance(item, str) and item:
                    dst.add(item)


def _harvest_auto_entities_filter(state: _State, value: dict[str, Any]) -> None:
    """``custom:auto-entities``:

    ``filter: {include: [{entity_id: "<glob>", domain: ..., state: ...}, ...],
               exclude: [...]}``

    Only the ``entity_id`` (glob, treated as :func:`fnmatch.fnmatchcase`) and
    ``domain`` rules are captured. ``state`` / ``attributes`` / ``last_*``
    rules are dynamic and ignored — we ship a superset and let the card do
    the runtime filtering itself.
    """
    inc = value.get("include")
    if isinstance(inc, list):
        for item in inc:
            if not isinstance(item, dict):
                continue
            eid = item.get("entity_id")
            if isinstance(eid, str) and eid:
                # Include: a literal id is a hard include; a glob expands at
                # mirror-resolve time.
                if any(c in eid for c in "*?["):
                    state.include_globs.add(eid)
                elif ENTITY_RE.match(eid):
                    state.ids.add(eid)
            dom = item.get("domain")
            if isinstance(dom, str) and dom:
                state.include_domains.add(dom)
            elif isinstance(dom, list):
                for d in dom:
                    if isinstance(d, str) and d:
                        state.include_domains.add(d)
            # ``group: group.<x>`` selects the group's members at runtime;
            # the static analyzer can't expand membership, but we MUST at
            # least subscribe to the group entity itself or the card sees
            # ``unavailable``. Member expansion is a separate enhancement
            # that needs the live state mirror's ``attributes.entity_id``.
            grp = item.get("group")
            if isinstance(grp, str) and ENTITY_RE.match(grp):
                state.ids.add(grp)

    exc = value.get("exclude")
    if isinstance(exc, list):
        for item in exc:
            if not isinstance(item, dict):
                continue
            eid = item.get("entity_id")
            if isinstance(eid, str) and eid:
                # Exclude: globs and literals both go to exclude_globs (a
                # literal is a degenerate glob matching only itself), so we
                # never accidentally pull an excluded id into the scope.
                state.exclude_globs.add(eid)
            dom = item.get("domain")
            if isinstance(dom, str) and dom:
                state.exclude_domains.add(dom)
            elif isinstance(dom, list):
                for d in dom:
                    if isinstance(d, str) and d:
                        state.exclude_domains.add(d)


def _add_one_or_many(found: set[str], value: Any) -> None:
    """A single-entity key can be a string or, occasionally, a list."""
    if isinstance(value, str):
        if _looks_like_entity(value):
            found.add(value)
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, str) and _looks_like_entity(item):
                found.add(item)


def _add_list(found: set[str], value: Any) -> None:
    """An entity-list key's items are strings or objects with `entity`/etc."""
    if not isinstance(value, list):
        return
    for item in value:
        if isinstance(item, str):
            if _looks_like_entity(item):
                found.add(item)
        elif isinstance(item, dict):
            # An object item; let the generic walk handle nested keys. We don't
            # recurse manually here because the caller will already descend.
            pass


def _add_from_action(found: set[str], action: dict[str, Any]) -> None:
    """Harvest entity ids from a tap/hold/double-tap action dict. Handles
    ``more-info`` and ``toggle`` (entity at the action top level) and
    ``call-service`` / ``perform-action`` (entity under ``target`` or
    ``data``). Other action shapes are intentionally ignored — their
    ``entity_id`` fields aren't entity references in the entity-scope
    sense.
    """
    kind = action.get("action")
    if kind in ("more-info", "toggle"):
        # These action shapes carry the entity at the action dict's top
        # level, not nested under target/data. Clicking opens the more-info
        # dialog (or toggles) the named entity, so it must be in scope.
        eid = action.get("entity")
        if isinstance(eid, str) and _looks_like_entity(eid):
            found.add(eid)
        return
    if kind not in ("call-service", "perform-action"):
        return
    for key in ("target", "data", "service_data"):
        sub = action.get(key)
        if not isinstance(sub, dict):
            continue
        eid = sub.get("entity_id")
        if isinstance(eid, str):
            if _looks_like_entity(eid):
                found.add(eid)
        elif isinstance(eid, list):
            for item in eid:
                if isinstance(item, str) and _looks_like_entity(item):
                    found.add(item)


def _looks_like_entity(value: str) -> bool:
    """Whether a string looks like an HA entity id (``domain.name`` with
    lowercase identifiers). Used to filter walker hits so non-entity dotted
    strings (paths, URLs, free text) don't slip into scope.
    """
    return bool(ENTITY_RE.match(value))
