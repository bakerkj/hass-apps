# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Helpers for tier-2 sibling files.

When tier-2 is encoded ahead of its activation date (camera's
``tier2.source == "direct"``), the encoded file is parked alongside the
tier-1 file using a ``.t2`` infix before the original extension. At the
day-30 swap boundary, the sibling replaces the tier-1 file at the
primary path.

This module exposes pure-path helpers (no I/O for ``sibling_path`` /
``primary_from_sibling`` / ``is_sibling_path``) and small wrappers
around the filesystem (``sibling_exists`` / ``delete_sibling``).
"""

from __future__ import annotations

from pathlib import Path

T2_INFIX = ".t2"


def sibling_path(primary: Path) -> Path:
    """Return the sibling tier-2 path for a given primary segment path.

    ``/a/b/12.34.mp4`` → ``/a/b/12.34.t2.mp4``
    """
    return primary.with_suffix(T2_INFIX + primary.suffix)


def is_sibling_path(path: Path) -> bool:
    """True if ``path`` looks like a tier-2 sibling (has ``.t2.<ext>`` infix)."""
    return path.name.endswith(T2_INFIX + path.suffix)


def primary_from_sibling(sibling: Path) -> Path:
    """Inverse of :func:`sibling_path`. Strips the ``.t2`` infix.

    ``/a/b/12.34.t2.mp4`` → ``/a/b/12.34.mp4``

    Raises ``ValueError`` if ``sibling`` is not a sibling path.
    """
    if not is_sibling_path(sibling):
        raise ValueError(f"not a tier-2 sibling path: {sibling}")
    name = sibling.name.removesuffix(T2_INFIX + sibling.suffix) + sibling.suffix
    return sibling.with_name(name)


def sibling_exists(primary: Path) -> bool:
    """True iff the sibling tier-2 file exists for ``primary``."""
    return sibling_path(primary).is_file()


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
