# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Serves the Ingress status UI: an HTML page (``index.html``) plus a
``/api/sessions`` JSON endpoint returning a snapshot of all active (and
recently-disconnected) sessions.

The HTML page polls the JSON endpoint and renders one card per session
showing kind (intercepted vs. passthrough/tunnel), connected duration,
resolved view, scope summary, message counts, and queue depth.

Honours the ``show_client_paths`` add-on option: when disabled, the JSON
snapshot omits ``current_path`` and ``target_path`` so users with
Ingress access can't see which dashboards other clients are on.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from aiohttp import web

from .session import SessionRegistry

_INDEX_HTML = (Path(__file__).with_name("index.html")).read_text(encoding="utf-8")

_PATH_FIELDS = ("current_path", "target_path")


def create_app(
    registry: SessionRegistry, show_client_paths: bool = True
) -> web.Application:
    """Build the aiohttp app for the Ingress status panel.

    Serves two routes:

      * ``GET /`` returns the static HTML page.
      * ``GET /api/sessions`` returns a JSON snapshot the page polls.

    When ``show_client_paths`` is False, the JSON snapshot has every
    session's path fields stripped via ``_redact_paths`` so users with
    Ingress access can't see what other clients are looking at.
    """
    app = web.Application()

    async def page(_request: web.Request) -> web.Response:
        """Serve the status UI HTML (loaded once at import from
        ``index.html`` next to this module).
        """
        return web.Response(body=_INDEX_HTML, content_type="text/html", charset="utf-8")

    async def sessions(request: web.Request) -> web.Response:
        """Return the current session-registry snapshot as JSON, with path
        fields redacted when ``show_client_paths`` is off.

        ``?detail=full`` requests the full ``scope_entities`` list per
        session. The default shape carries only ``scope_count`` and a
        ``scope_sample`` (up to 50 entity ids) so the polling JSON stays
        small on installs with thousands of entities and several sessions.
        """
        detail = "full" if request.query.get("detail") == "full" else "summary"
        snap = registry.snapshot(detail=detail)
        if not show_client_paths:
            snap = [{k: v for k, v in s.items() if k not in _PATH_FIELDS} for s in snap]
        payload: dict[str, Any] = {
            "now": datetime.now(timezone.utc).isoformat(),
            "sessions": snap,
        }
        return web.json_response(payload)

    app.router.add_get("/", page)
    app.router.add_get("/api/sessions", sessions)
    return app
