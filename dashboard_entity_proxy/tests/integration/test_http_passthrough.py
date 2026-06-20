# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""HTTP reverse-proxy pass-through.

Verifies that HA's compressed frontend assets reach a client through
the proxy with their ``Content-Encoding`` header intact and the body
untouched. ``auto_decompress=False`` on the proxy's upstream
``ClientSession`` is what allows this; the browser then decompresses
natively, no ``Brotli`` Python dep needed.
"""

from __future__ import annotations

import gzip
import urllib.request

import pytest

INTEGRATION = pytest.mark.integration


@INTEGRATION
def test_compressed_frontend_asset_arrives_compressed(proxy_url, ha):
    """Request a frontend JS bundle through the proxy with
    ``Accept-Encoding: gzip``. The proxy should forward the upstream
    response intact: ``Content-Encoding: gzip`` header preserved, body
    still gzip-compressed (decompressing it locally yields valid JS).
    """
    asset = "/manifest.json"

    req = urllib.request.Request(
        proxy_url.rstrip("/") + asset,
        headers={"Accept-Encoding": "gzip"},
    )
    # Bypass urllib's auto-decompression so we can inspect the wire body.
    with urllib.request.urlopen(req, timeout=10) as resp:
        encoding = resp.headers.get("Content-Encoding", "")
        body = resp.read()

    assert encoding == "gzip", (
        f"expected upstream compressed body to pass through; "
        f"Content-Encoding header was {encoding!r}. The proxy may have "
        f"reverted to decompress-and-strip behaviour."
    )
    # Body must still be the gzip wire format — decompressing succeeds.
    decompressed = gzip.decompress(body)
    assert decompressed.startswith(b"{"), (
        f"decompressed body doesn't look like JSON: {decompressed[:80]!r}"
    )


@INTEGRATION
def test_uncompressed_request_still_works(proxy_url, ha):
    """Without ``Accept-Encoding`` the proxy must still serve the
    asset (HA returns it uncompressed, proxy forwards verbatim).
    """
    asset = "/manifest.json"

    req = urllib.request.Request(
        proxy_url.rstrip("/") + asset,
        headers={"Accept-Encoding": "identity"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        encoding = resp.headers.get("Content-Encoding", "")
        body = resp.read()

    assert encoding in ("", "identity"), (
        f"expected uncompressed response; got Content-Encoding={encoding!r}"
    )
    assert body.startswith(b"{"), f"body doesn't look like JSON: {body[:80]!r}"
