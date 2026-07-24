#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Dashboard sweep: drive the proxy's extractor against a live HA instance.

For every Lovelace dashboard (and every view inside it), plus the well-known
system panels, run the same ``classify_path`` + ``extract_scope`` the live
addon uses. Emit a Markdown report flagging views whose scope decision looks
suspicious — typically dashboards classified as the wrong view kind, scopes
widening to all entities unexpectedly, or glob-vs-regex mismatches in
pattern-based scoping. Catches the same class of bug as live navigation
without needing a browser.

Usage:
    HA_URL=https://homeassistant.local:8123 HA_TOKEN=<long-lived-token> \\
        uv run python dashboard_entity_proxy/scripts/dep_sweep.py > report.md

Options:
    --dashboard <url_path>   only analyze this dashboard (e.g. ``lovelace``)
    --no-system              skip the system-path classification block
    --no-cards               skip the card-type frequency block
"""

import argparse
import asyncio
import os
import sys
from collections import Counter
from typing import Any, Self

import aiohttp

# Add the addon dir (parent of this script's dir) to sys.path so the
# ``from dashboard_entity_proxy...`` imports below resolve to the
# in-tree package.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dashboard_entity_proxy.dashboard import (
    FRONTEND_BASELINE,
    ExtractResult,
    apply_filters,
    expand_patterns,
    extract_scope,
    is_strategy,
)
from dashboard_entity_proxy.navigation import (
    ViewKind,
    classify_path,
)

_PROXY_OBSERVE_TIMEOUT = 20.0


# Locked-in classifications we want to confirm every run.
SYSTEM_PATHS = [
    "/calendar",
    "/todo",
    "/shopping-list",
    "/map",
    "/energy",
    "/media-browser",
    "/profile",
    "/history",
    "/logbook",
    "/developer-tools/state",
    "/config",
    "/config/dashboard",
    "/config/automation",
    "/config/integrations",
    "/config/devices",
    "/config/devices/device/abc123",
    "/config/integrations/integration/light",
    "/config/areas/area/kitchen",
    "/config/helpers",
]


class HAClient:
    def __init__(self, session: aiohttp.ClientSession, url: str, token: str) -> None:
        self._session = session
        self._url = url.rstrip("/")
        self._token = token
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._id = 0

    async def __aenter__(self) -> Self:
        scheme = "wss" if self._url.startswith("https://") else "ws"
        host = self._url.split("://", 1)[1]
        ws_url = f"{scheme}://{host}/api/websocket"
        self._ws = await self._session.ws_connect(
            ws_url, max_msg_size=0, heartbeat=None
        )
        hello = await self._ws.receive_json()
        if hello.get("type") != "auth_required":
            raise RuntimeError(f"unexpected hello: {hello}")
        await self._ws.send_json({"type": "auth", "access_token": self._token})
        ack = await self._ws.receive_json()
        if ack.get("type") != "auth_ok":
            raise RuntimeError(f"auth failed: {ack}")
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._ws is not None:
            await self._ws.close()

    async def call(self, **payload: Any) -> Any:
        assert self._ws is not None
        self._id += 1
        msg_id = self._id
        await self._ws.send_json({"id": msg_id, **payload})
        while True:
            msg = await self._ws.receive_json()
            if msg.get("id") != msg_id:
                continue
            if msg.get("type") != "result":
                continue
            if not msg.get("success", False):
                raise RuntimeError(f"{payload.get('type')} failed: {msg.get('error')}")
            return msg.get("result")

    async def observe_path_scope(self, path: str) -> set[str]:
        """Drive a system-panel navigation: send ``browser_mod/update`` with
        the target ``path`` (the proxy reads it before forwarding and sets
        scope synchronously), then ``subscribe_entities`` and collect the
        initial ``a:`` snapshot. Used for paths the proxy classifies as
        DOMAIN / ALL / DEVICE / etc. — anything not driven by a per-
        dashboard ``lovelace/config`` round-trip.
        """
        assert self._ws is not None
        self._id += 1
        nav_id = self._id
        await self._ws.send_json(
            {
                "id": nav_id,
                "type": "browser_mod/update",
                "data": {"path": path},
            }
        )
        # browser_mod/update is processed synchronously by the proxy before
        # the message is forwarded, but DOMAIN / ALL scopes are applied
        # without any HA round trip, so a tiny delay is enough to ensure
        # the scope is in place by the time subscribe_entities arrives.
        await asyncio.sleep(0.1)
        self._id += 1
        sub_id = self._id
        await self._ws.send_json({"id": sub_id, "type": "subscribe_entities"})
        entities: set[str] = set()

        async def _collect() -> None:
            nonlocal entities
            while True:
                assert self._ws is not None
                msg = await self._ws.receive_json()
                if msg.get("id") != sub_id:
                    # Ignore the (likely error) response to browser_mod/update.
                    continue
                mtype = msg.get("type")
                if mtype == "result":
                    if not msg.get("success", False):
                        raise RuntimeError(
                            f"subscribe_entities failed: {msg.get('error')}"
                        )
                    continue
                if mtype == "event":
                    event = msg.get("event") or {}
                    added = event.get("a")
                    if isinstance(added, dict):
                        entities = set(added.keys())
                    return

        await asyncio.wait_for(_collect(), timeout=_PROXY_OBSERVE_TIMEOUT)
        return entities

    async def observe_scope(self, url_path: str) -> set[str]:
        """Drive the proxy as a synthetic client: send ``lovelace/config`` for
        the target dashboard (which triggers ``_navigate(DASHBOARD, url)`` and
        populates the per-connection scope), then ``subscribe_entities`` and
        collect the initial ``a:`` snapshot. Returned set is the entity ids
        the proxy actually serves for a fresh connection scoped to that
        dashboard. Caller is responsible for using a fresh client per
        dashboard so navigations don't contaminate each other.
        """
        assert self._ws is not None
        cfg_kwargs: dict[str, Any] = {"type": "lovelace/config"}
        if url_path:
            cfg_kwargs["url_path"] = url_path
        try:
            await self.call(**cfg_kwargs)
        except RuntimeError:
            # Default-dashboard strategy or HA error — proxy still navigated;
            # carry on to subscribe and see what scope it picks.
            pass
        self._id += 1
        sub_id = self._id
        await self._ws.send_json({"id": sub_id, "type": "subscribe_entities"})
        entities: set[str] = set()

        async def _collect() -> None:
            nonlocal entities
            while True:
                assert self._ws is not None
                msg = await self._ws.receive_json()
                if msg.get("id") != sub_id:
                    continue
                mtype = msg.get("type")
                if mtype == "result":
                    if not msg.get("success", False):
                        raise RuntimeError(
                            f"subscribe_entities failed: {msg.get('error')}"
                        )
                    continue
                if mtype == "event":
                    event = msg.get("event") or {}
                    added = event.get("a")
                    if isinstance(added, dict):
                        entities = set(added.keys())
                    return

        await asyncio.wait_for(_collect(), timeout=_PROXY_OBSERVE_TIMEOUT)
        return entities


async def fetch_snapshot(client: HAClient) -> dict[str, Any]:
    states = await client.call(type="get_states")
    entities = sorted(
        {s["entity_id"] for s in states if isinstance(s, dict) and "entity_id" in s}
    )

    listed = await client.call(type="lovelace/dashboards/list") or []
    seen: set[str] = set()
    dashboards: list[dict[str, Any]] = [{"url_path": "", "title": "(default)"}]
    seen.add("")
    for d in listed:
        if not isinstance(d, dict):
            continue
        up = d.get("url_path") or ""
        if up in seen:
            continue
        seen.add(up)
        dashboards.append(d)

    configs: dict[str, dict[str, Any] | None] = {}
    errors: dict[str, str] = {}
    for d in dashboards:
        up = d.get("url_path") or ""
        kwargs: dict[str, Any] = {"type": "lovelace/config"}
        if up:
            kwargs["url_path"] = up
        try:
            cfg = await client.call(**kwargs)
            configs[up] = cfg if isinstance(cfg, dict) else None
        except RuntimeError as e:
            errors[up] = str(e)
            configs[up] = None

    return {
        "entities": entities,
        "dashboards": dashboards,
        "configs": configs,
        "errors": errors,
    }


def _iter_typed_dicts(node: Any) -> list[dict[str, Any]]:
    """Every dict with a string ``type`` field reachable from ``node``. Coarse —
    includes badges, elements, action sub-dicts — but useful as a histogram
    signal: types we never extract anything from are candidates for an
    extractor gap (or are entity-less by design, like ``markdown``).
    """
    found: list[dict[str, Any]] = []

    def visit(n: Any) -> None:
        if isinstance(n, dict):
            if isinstance(n.get("type"), str):
                found.append(n)
            for v in n.values():
                visit(v)
        elif isinstance(n, list):
            for v in n:
                visit(v)

    visit(node)
    return found


def _scope_size(result: ExtractResult) -> int:
    return (
        len(result.include_globs)
        + len(result.include_regexes)
        + len(result.include_domains)
    )


def predict_scope(cfg: Any, mirror: list[str]) -> set[str] | None:
    """Reproduce the proxy's ``_scope_from_config`` + ``_apply_scope`` for a
    fresh, option-free connection. Returns the predicted entity-id set, or
    ``None`` to signal "all entities" (strategy or empty extraction). Caller
    treats ``None`` the same as ``set(mirror)``.
    """
    if not isinstance(cfg, dict):
        return None
    if is_strategy(cfg):
        return None
    extract = extract_scope(cfg)
    if extract.empty():
        return None
    scoped = set(apply_filters(list(extract.ids), [], [], []))
    if extract.include_globs or extract.include_regexes or extract.include_domains:
        scoped |= expand_patterns(extract, mirror)
    return scoped


def predict_path_scope(path: str, mirror: list[str]) -> tuple[str, set[str] | None]:
    """Predict the proxy's scope for a non-dashboard path. Returns a
    ``(label, set_or_None)`` tuple: ``label`` is ``"all"`` / ``"domain
    <key>"`` / ``"skip <reason>"``; ``set_or_None`` is the predicted set
    (``None`` for "all" — caller treats as ``set(mirror)``), or ``None``
    when skipped. Registry-scoped kinds (DEVICE / INTEGRATION / AREA /
    HELPERS) need entity-registry state to predict, so the path test
    skips them rather than reporting fake drift.
    """
    view = classify_path(path)
    if view.kind is ViewKind.ALL:
        return ("all", None)
    if view.kind is ViewKind.DOMAIN:
        scope = {e for e in mirror if e.startswith(view.key + ".")}
        scope |= FRONTEND_BASELINE
        return (f"domain {view.key}", scope)
    if view.kind is ViewKind.DASHBOARD:
        return (f"skip (dashboard path {view.key!r}, covered elsewhere)", None)
    return (f"skip (registry-scoped {view.kind.value})", None)


# Paths whose proxy-side scope is predictable from classify_path alone, i.e.
# DOMAIN and ALL — registry-scoped paths are excluded here since they need
# real entity / device / area ids on the target instance.
PROXY_TEST_SYSTEM_PATHS = [
    "/calendar",
    "/todo",
    "/shopping-list",
    "/map",
    "/energy",
    "/media-browser",
    "/profile",
    "/history",
    "/logbook",
    "/developer-tools/state",
    "/config",
    "/config/dashboard",
    "/config/automation",
    "/config/integrations",
    "/config/devices",
]


async def run_proxy_panel_test(
    snapshot: dict[str, Any], proxy_url: str, token: str
) -> list[dict[str, Any]]:
    """End-to-end proxy test for system panels. For each path in
    ``PROXY_TEST_SYSTEM_PATHS``, open a fresh connection, navigate via
    ``browser_mod/update``, subscribe, and compare the observed snapshot
    against ``predict_path_scope``.
    """
    mirror: list[str] = snapshot["entities"]
    mirror_set = set(mirror)
    results: list[dict[str, Any]] = []
    async with aiohttp.ClientSession() as session:
        for path in PROXY_TEST_SYSTEM_PATHS:
            label, pred = predict_path_scope(path, mirror)
            pred_set: set[str] = (
                mirror_set if pred is None and label == "all" else (pred or set())
            )
            pred_n = len(pred_set) if pred is not None or label == "all" else 0
            entry: dict[str, Any] = {
                "path": path,
                "predicted": label if label != "all" else "all",
                "predicted_n": pred_n,
                "actual_n": 0,
                "extras": [],
                "missing": [],
                "error": "",
            }
            try:
                async with HAClient(session, proxy_url, token) as client:
                    actual = await client.observe_path_scope(path)
            except (TimeoutError, RuntimeError, aiohttp.ClientError) as e:
                entry["error"] = f"{type(e).__name__}: {e}"
                results.append(entry)
                continue
            entry["actual_n"] = len(actual)
            entry["extras"] = sorted(actual - pred_set)[:5]
            entry["missing"] = sorted(pred_set - actual)[:5]
            results.append(entry)
    return results


async def run_proxy_test(
    snapshot: dict[str, Any], proxy_url: str, token: str
) -> list[dict[str, Any]]:
    """For each dashboard, open a fresh proxy connection, navigate, and
    compare the proxy's observed snapshot against ``predict_scope``. Each
    entry: ``{url_path, title, predicted_n, actual_n, extras, missing, error}``
    where ``extras`` are ids the proxy served beyond prediction (5 samples)
    and ``missing`` are ids predicted but not served.
    """
    mirror: list[str] = snapshot["entities"]
    mirror_set: set[str] = set(mirror)
    results: list[dict[str, Any]] = []
    async with aiohttp.ClientSession() as session:
        for d in snapshot["dashboards"]:
            url_path = d.get("url_path") or ""
            title = d.get("title") or ""
            cfg = snapshot["configs"].get(url_path)
            pred = predict_scope(cfg, mirror)
            pred_set: set[str] = mirror_set if pred is None else pred
            pred_label = "all" if pred is None else f"{len(pred)}"
            entry: dict[str, Any] = {
                "url_path": url_path,
                "title": title,
                "predicted": pred_label,
                "predicted_n": len(pred_set),
                "actual_n": 0,
                "extras": [],
                "missing": [],
                "error": "",
            }
            try:
                async with HAClient(session, proxy_url, token) as client:
                    actual = await client.observe_scope(url_path)
            except (TimeoutError, RuntimeError, aiohttp.ClientError) as e:
                entry["error"] = f"{type(e).__name__}: {e}"
                results.append(entry)
                continue
            entry["actual_n"] = len(actual)
            entry["extras"] = sorted(actual - pred_set)[:5]
            entry["missing"] = sorted(pred_set - actual)[:5]
            results.append(entry)
    return results


def render_report(
    snapshot: dict[str, Any],
    *,
    include_system: bool = True,
    include_cards: bool = True,
    proxy_results: list[dict[str, Any]] | None = None,
    panel_results: list[dict[str, Any]] | None = None,
) -> str:
    mirror: list[str] = snapshot["entities"]
    mirror_set = set(mirror)
    dashboards: list[dict[str, Any]] = snapshot["dashboards"]
    configs: dict[str, dict[str, Any] | None] = snapshot["configs"]
    errors: dict[str, str] = snapshot["errors"]

    out: list[str] = ["# DEP-py sweep report", ""]
    total_views = sum(
        len(cfg.get("views") or []) for cfg in configs.values() if isinstance(cfg, dict)
    )
    out.append(f"- Entities in HA: **{len(mirror)}**")
    out.append(f"- Dashboards: **{len(dashboards)}**")
    out.append(f"- Total views: **{total_views}**")
    if errors:
        out.append(f"- Config-fetch errors: **{len(errors)}**")
    out.append("")

    if include_system:
        out.append("## System path classifications")
        out.append("")
        out.append("| Path | view_kind | key |")
        out.append("|---|---|---|")
        for p in SYSTEM_PATHS:
            v = classify_path(p)
            out.append(f"| `{p}` | {v.kind.value} | {v.key or ''} |")
        out.append("")

    suspicious: list[tuple[str, str, str]] = []
    type_count: Counter[str] = Counter()
    type_empty: Counter[str] = Counter()

    out.append("## Per-dashboard views")
    out.append("")
    for d in dashboards:
        url_path = d.get("url_path") or ""
        title = d.get("title") or ""
        header = url_path or "(default)"
        out.append(f"### `{header}` — {title}")
        out.append("")
        if url_path in errors:
            out.append(f"_Config fetch failed: {errors[url_path]}_")
            out.append("")
            continue
        cfg = configs.get(url_path)
        if not isinstance(cfg, dict):
            out.append("_No config available._")
            out.append("")
            continue
        if is_strategy(cfg):
            out.append(
                "_Top-level strategy dashboard. Widen-to-all at runtime is expected._"
            )
            out.append("")
        views = cfg.get("views") or []
        if not views:
            out.append("_No views._")
            out.append("")
            continue
        out.append(
            "| # | path | view_kind | strategy | static | inc pat | exc pat | expanded | stale |"
        )
        out.append("|---|---|---|---|---|---|---|---|---|")
        for i, view in enumerate(views):
            if not isinstance(view, dict):
                continue
            vpath = view.get("path") or str(i)
            full_path = (
                f"/{url_path}/{vpath}".replace("//", "/") if url_path else f"/{vpath}"
            )
            kind = classify_path(full_path)
            result = extract_scope(view)
            view_strategy = is_strategy(view) or "strategy" in view
            inc_n = _scope_size(result)
            exc_n = (
                len(result.exclude_globs)
                + len(result.exclude_regexes)
                + len(result.exclude_domains)
            )
            expanded = expand_patterns(result, mirror)
            stale = [e for e in result.ids if e not in mirror_set]
            strat_mark = "Y" if view_strategy else ""
            out.append(
                f"| {i} | `{vpath}` | {kind.kind.value} | {strat_mark} | "
                f"{len(result.ids)} | {inc_n} | {exc_n} | {len(expanded)} | {len(stale)} |"
            )

            if not view_strategy:
                if len(result.ids) == 0 and inc_n == 0:
                    suspicious.append(
                        (url_path, vpath, "empty extraction -> widen-to-all at runtime")
                    )
                elif inc_n > 0 and len(expanded) == len(result.ids):
                    suspicious.append(
                        (
                            url_path,
                            vpath,
                            "include patterns expanded to 0 (typo, stale, or extractor gap)",
                        )
                    )
                elif stale:
                    suspicious.append(
                        (
                            url_path,
                            vpath,
                            f"{len(stale)} static id(s) not in mirror: {', '.join(sorted(stale)[:5])}",
                        )
                    )

            if include_cards:
                for card in _iter_typed_dicts(view):
                    t = str(card.get("type") or "")
                    if not t:
                        continue
                    type_count[t] += 1
                    sub = extract_scope({"views": [{"cards": [card]}]})
                    if not sub.ids and _scope_size(sub) == 0:
                        type_empty[t] += 1
        out.append("")

    out.append("## Suspicious views")
    out.append("")
    if not suspicious:
        out.append("_None._")
    else:
        for url_path, vpath, reason in suspicious:
            head = url_path or "(default)"
            out.append(f"- `{head}` / `{vpath}` — {reason}")
    out.append("")

    if proxy_results is not None:
        out.append("## Proxy-observed vs predicted scope")
        out.append("")
        out.append(
            "_Per-dashboard end-to-end test: open a fresh connection to the "
            "proxy, send `lovelace/config` for the target url_path to trigger "
            "navigation, then `subscribe_entities` and read the initial "
            "snapshot. ``predicted`` is `predict_scope` applied to the same "
            "config (mirrors `_scope_from_config` + `_apply_scope` for an "
            "option-free connection). `all` means strategy or empty extraction "
            "(widen-to-all). Drift columns show how the proxy's served set "
            "differs from the prediction._"
        )
        out.append("")
        out.append(
            "| url_path | predicted | actual | extras (proxy>pred) | missing (pred>proxy) | error |"
        )
        out.append("|---|---|---|---|---|---|")
        drift: list[dict[str, Any]] = []
        for r in proxy_results:
            up = r["url_path"] or "(default)"
            extras = ", ".join(r["extras"][:3]) + (" …" if len(r["extras"]) > 3 else "")
            missing = ", ".join(r["missing"][:3]) + (
                " …" if len(r["missing"]) > 3 else ""
            )
            out.append(
                f"| `{up}` | {r['predicted']} | {r['actual_n']} | "
                f"{len(r['extras'])}{(' [' + extras + ']') if r['extras'] else ''} | "
                f"{len(r['missing'])}{(' [' + missing + ']') if r['missing'] else ''} | "
                f"{r['error']} |"
            )
            if r["error"] or r["extras"] or r["missing"]:
                drift.append(r)
        out.append("")
        out.append("### Proxy drift")
        out.append("")
        if not drift:
            out.append(
                "_No drift: proxy snapshot matched prediction on every dashboard._"
            )
        else:
            for r in drift:
                up = r["url_path"] or "(default)"
                if r["error"]:
                    out.append(f"- `{up}` — ERROR: {r['error']}")
                    continue
                bits: list[str] = []
                if r["extras"]:
                    bits.append(
                        f"proxy served {len(r['extras'])}+ unexpected: "
                        + ", ".join(r["extras"])
                    )
                if r["missing"]:
                    bits.append(
                        f"proxy omitted {len(r['missing'])}+ expected: "
                        + ", ".join(r["missing"])
                    )
                out.append(f"- `{up}` — {'; '.join(bits)}")
        out.append("")

    if panel_results is not None:
        out.append("## Proxy-observed vs predicted scope (system panels)")
        out.append("")
        out.append(
            "_Per-path end-to-end: open a fresh connection, send a synthetic "
            "`browser_mod/update` for the path (which the proxy reads "
            "synchronously to set scope before forwarding), then "
            "`subscribe_entities`. ``predicted`` is `predict_path_scope` "
            "for that path against the mirror — `all` for "
            "ViewKind.ALL paths, `domain <key>` for ViewKind.DOMAIN. "
            "Registry-scoped paths (DEVICE / INTEGRATION / AREA / HELPERS) "
            "are not in this list since predicting them needs real registry "
            "ids on the target instance._"
        )
        out.append("")
        out.append(
            "| path | predicted | actual | extras (proxy>pred) | missing (pred>proxy) | error |"
        )
        out.append("|---|---|---|---|---|---|")
        panel_drift: list[dict[str, Any]] = []
        for r in panel_results:
            extras = ", ".join(r["extras"][:3]) + (" …" if len(r["extras"]) > 3 else "")
            missing = ", ".join(r["missing"][:3]) + (
                " …" if len(r["missing"]) > 3 else ""
            )
            out.append(
                f"| `{r['path']}` | {r['predicted']} | {r['actual_n']} | "
                f"{len(r['extras'])}{(' [' + extras + ']') if r['extras'] else ''} | "
                f"{len(r['missing'])}{(' [' + missing + ']') if r['missing'] else ''} | "
                f"{r['error']} |"
            )
            if r["error"] or r["extras"] or r["missing"]:
                panel_drift.append(r)
        out.append("")
        out.append("### System-panel drift")
        out.append("")
        if not panel_drift:
            out.append("_No drift: proxy snapshot matched prediction on every path._")
        else:
            for r in panel_drift:
                if r["error"]:
                    out.append(f"- `{r['path']}` — ERROR: {r['error']}")
                    continue
                panel_bits: list[str] = []
                if r["extras"]:
                    panel_bits.append(
                        f"proxy served {len(r['extras'])}+ unexpected: "
                        + ", ".join(r["extras"])
                    )
                if r["missing"]:
                    panel_bits.append(
                        f"proxy omitted {len(r['missing'])}+ expected: "
                        + ", ".join(r["missing"])
                    )
                out.append(f"- `{r['path']}` — {'; '.join(panel_bits)}")
        out.append("")

    if include_cards and type_count:
        out.append("## Card / element type frequency")
        out.append("")
        out.append(
            "_Coarse: every dict with a string `type:` field anywhere in any view. "
            "High empty% on a card-shaped type may indicate an extractor gap; "
            "empty% on `markdown` / `picture` / `conditional` etc. is expected._"
        )
        out.append("")
        out.append("| type | total | empty | empty% |")
        out.append("|---|---|---|---|")
        for t, n in type_count.most_common(60):
            e = type_empty[t]
            pct = (100 * e // n) if n else 0
            out.append(f"| `{t}` | {n} | {e} | {pct}% |")
        out.append("")

    return "\n".join(out)


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dashboard", help="Limit to this url_path")
    parser.add_argument("--no-system", action="store_true")
    parser.add_argument("--no-cards", action="store_true")
    parser.add_argument(
        "--proxy-url",
        help="If set, also drive the proxy as a synthetic client per "
        "dashboard and compare its served snapshot to predict_scope.",
    )
    args = parser.parse_args()

    url = os.environ.get("HA_URL")
    token = os.environ.get("HA_TOKEN")
    if not url or not token:
        print("HA_URL and HA_TOKEN must be set in the environment.", file=sys.stderr)
        return 2

    async with (
        aiohttp.ClientSession() as session,
        HAClient(session, url, token) as client,
    ):
        snapshot = await fetch_snapshot(client)

    if args.dashboard is not None:
        wanted = args.dashboard
        snapshot["dashboards"] = [
            d for d in snapshot["dashboards"] if (d.get("url_path") or "") == wanted
        ]
        snapshot["configs"] = {
            k: v for k, v in snapshot["configs"].items() if k == wanted
        }
        snapshot["errors"] = {
            k: v for k, v in snapshot["errors"].items() if k == wanted
        }

    proxy_results: list[dict[str, Any]] | None = None
    panel_results: list[dict[str, Any]] | None = None
    if args.proxy_url:
        proxy_results = await run_proxy_test(snapshot, args.proxy_url, token)
        panel_results = await run_proxy_panel_test(snapshot, args.proxy_url, token)

    print(
        render_report(
            snapshot,
            include_system=not args.no_system,
            include_cards=not args.no_cards,
            proxy_results=proxy_results,
            panel_results=panel_results,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
