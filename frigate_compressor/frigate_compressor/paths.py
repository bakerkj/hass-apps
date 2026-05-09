# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Helpers for tier-2 sibling files.

When tier-2 is encoded ahead of its activation date (camera's
``tier2.source == "direct"``), the encoded file is parked alongside the
tier-1 file using a ``.t2`` infix before the original extension. Once
a segment reaches ``tier2.min_days``, the sibling replaces the tier-1
file at the primary path.
"""

from __future__ import annotations

from pathlib import Path

T2_INFIX = ".t2"


def sibling_path(primary: Path) -> Path:
    """Return the sibling tier-2 path for a given primary segment path.

    ``/a/b/12.34.mp4`` → ``/a/b/12.34.t2.mp4``
    """
    return primary.with_suffix(T2_INFIX + primary.suffix)


def delete_sibling(primary: Path) -> bool:
    """Delete the sibling tier-2 file for ``primary`` if present.

    Returns ``True`` if a file was deleted, ``False`` if no sibling
    existed. Idempotent.
    """
    sib = sibling_path(primary)
    if sib.is_file():
        sib.unlink()
        return True
    return False
