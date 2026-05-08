# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for tier-2 sibling-path helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

import frigate_compressor as fc


# ═══════════════════════════════════════════════════════════════════════════════
# sibling_path / primary_from_sibling round-trip
# ═══════════════════════════════════════════════════════════════════════════════


def test_sibling_path_inserts_t2_before_extension():
    p = Path("/media/frigate/recordings/2026-04-22/16/back-yard/27.18.mp4")
    assert fc.sibling_path(p) == Path(
        "/media/frigate/recordings/2026-04-22/16/back-yard/27.18.t2.mp4"
    )


def test_sibling_path_with_simple_name():
    assert fc.sibling_path(Path("/a/b/c.mp4")) == Path("/a/b/c.t2.mp4")


def test_sibling_path_handles_dots_in_filename():
    """Frigate filenames look like ``MM.SS.mp4`` — make sure the inner dot
    doesn't fool with_suffix into stripping the wrong piece."""
    p = Path("/x/27.18.mp4")
    assert fc.sibling_path(p) == Path("/x/27.18.t2.mp4")


def test_primary_from_sibling_round_trips():
    p = Path("/a/b/12.34.mp4")
    assert fc.primary_from_sibling(fc.sibling_path(p)) == p


def test_primary_from_sibling_rejects_non_sibling():
    with pytest.raises(ValueError, match="not a tier-2 sibling"):
        fc.primary_from_sibling(Path("/a/b/c.mp4"))


# ═══════════════════════════════════════════════════════════════════════════════
# is_sibling_path
# ═══════════════════════════════════════════════════════════════════════════════


def test_is_sibling_path_true_for_t2_files():
    assert fc.is_sibling_path(Path("/a/b/27.18.t2.mp4"))


def test_is_sibling_path_false_for_primary_files():
    assert not fc.is_sibling_path(Path("/a/b/27.18.mp4"))


def test_is_sibling_path_false_for_unrelated_files():
    assert not fc.is_sibling_path(Path("/a/b/temp.mp4"))
    assert not fc.is_sibling_path(Path("/a/b/27.18.bak.mp4"))


# ═══════════════════════════════════════════════════════════════════════════════
# sibling_exists
# ═══════════════════════════════════════════════════════════════════════════════


def test_sibling_exists_returns_true_when_present(tmp_path):
    primary = tmp_path / "27.18.mp4"
    primary.write_bytes(b"primary")
    sib = tmp_path / "27.18.t2.mp4"
    sib.write_bytes(b"sibling")
    assert fc.sibling_exists(primary) is True


def test_sibling_exists_returns_false_when_absent(tmp_path):
    primary = tmp_path / "27.18.mp4"
    primary.write_bytes(b"primary")
    # No sibling file created
    assert fc.sibling_exists(primary) is False


def test_sibling_exists_returns_false_when_neither_exists(tmp_path):
    primary = tmp_path / "missing.mp4"
    assert fc.sibling_exists(primary) is False


# ═══════════════════════════════════════════════════════════════════════════════
# delete_sibling
# ═══════════════════════════════════════════════════════════════════════════════


def test_delete_sibling_removes_existing(tmp_path):
    primary = tmp_path / "27.18.mp4"
    primary.write_bytes(b"primary")
    sib = tmp_path / "27.18.t2.mp4"
    sib.write_bytes(b"sibling")
    assert fc.delete_sibling(primary) is True
    assert not sib.exists()
    assert primary.exists()  # primary untouched


def test_delete_sibling_noop_when_absent(tmp_path):
    primary = tmp_path / "27.18.mp4"
    primary.write_bytes(b"primary")
    assert fc.delete_sibling(primary) is False
    assert primary.exists()


def test_delete_sibling_idempotent(tmp_path):
    primary = tmp_path / "27.18.mp4"
    primary.write_bytes(b"primary")
    sib = tmp_path / "27.18.t2.mp4"
    sib.write_bytes(b"sibling")
    assert fc.delete_sibling(primary) is True
    assert fc.delete_sibling(primary) is False
    assert fc.delete_sibling(primary) is False


def test_delete_sibling_only_touches_sibling(tmp_path):
    """delete_sibling on /a/b/x.mp4 must NOT delete /a/b/x.mp4 itself."""
    primary = tmp_path / "27.18.mp4"
    primary.write_bytes(b"primary")
    fc.delete_sibling(primary)  # no sibling exists; should be no-op
    assert primary.exists()
