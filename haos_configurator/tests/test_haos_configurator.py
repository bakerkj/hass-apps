# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Unit tests for ``haos_configurator``.

Covers the pure logic (default_mode_for, local_sha256, validate_manifest,
load_manifest) and one focused regression test on ``host.host_run``'s
argv shape — the central no-shell-injection guarantee of the host-side
rewrite.

Orchestration code (process_files / run_actions / main) is intentionally
not unit-tested: it's mostly subprocess plumbing where mock-heavy tests
would just re-state the implementation.  The behaviour that actually
matters there ("did the file land on the host with the right hash, did
udevadm fire") needs a real HAOS host.
"""

import hashlib
from unittest.mock import patch

import pytest

import haos_configurator.host as host
import haos_configurator.manifest as manifest


# ---------------------------------------------------------------------------
# default_mode_for
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name,expected",
    [
        ("foo.sh", "0755"),
        ("setup.SH", "0644"),  # match is case-sensitive (POSIX-style)
        ("80-haos-media.rules", "0644"),
        ("manifest.yaml", "0644"),
        ("noext", "0644"),
    ],
)
def test_default_mode_for(name: str, expected: str) -> None:
    assert manifest.default_mode_for(name) == expected


# ---------------------------------------------------------------------------
# local_sha256
# ---------------------------------------------------------------------------


def test_local_sha256_matches_hashlib(tmp_path) -> None:
    payload = b"hello\nworld\n" * 1000
    p = tmp_path / "x"
    p.write_bytes(payload)
    assert manifest.local_sha256(str(p)) == hashlib.sha256(payload).hexdigest()


def test_local_sha256_empty_file(tmp_path) -> None:
    p = tmp_path / "empty"
    p.write_bytes(b"")
    assert manifest.local_sha256(str(p)) == hashlib.sha256(b"").hexdigest()


# ---------------------------------------------------------------------------
# validate_manifest
# ---------------------------------------------------------------------------


def test_validate_manifest_happy_path() -> None:
    m = {
        "files": [
            {
                "src": "a.rules",
                "dst": "/etc/udev/rules.d/a.rules",
                "on_change": ["reload"],
            },
        ],
        "actions": {"reload": {"run": "udevadm control --reload-rules"}},
    }
    # No exception => happy path.
    manifest.validate_manifest(m)


def test_validate_manifest_missing_action() -> None:
    m = {
        "files": [
            {"src": "a", "dst": "/x", "on_change": ["never_defined"]},
        ],
        "actions": {},
    }
    with pytest.raises(SystemExit):
        manifest.validate_manifest(m)


def test_validate_manifest_relative_dst_rejected() -> None:
    m = {"files": [{"src": "a", "dst": "etc/udev/rules.d/a"}]}
    with pytest.raises(SystemExit):
        manifest.validate_manifest(m)


def test_validate_manifest_non_string_dst_rejected() -> None:
    m = {"files": [{"src": "a", "dst": None}]}
    with pytest.raises(SystemExit):
        manifest.validate_manifest(m)


def test_validate_manifest_empty_is_ok() -> None:
    # No files, no actions — nothing to validate.
    manifest.validate_manifest({})


def test_validate_manifest_run_list_of_strings_ok() -> None:
    m = {
        "files": [
            {"src": "a", "dst": "/x", "on_change": ["reload"]},
        ],
        "actions": {"reload": {"run": ["udevadm", "control", "--reload-rules"]}},
    }
    manifest.validate_manifest(m)


def test_validate_manifest_run_list_with_non_string_rejected() -> None:
    m = {
        "files": [],
        "actions": {"x": {"run": ["udevadm", 42]}},
    }
    with pytest.raises(SystemExit):
        manifest.validate_manifest(m)


def test_validate_manifest_run_non_str_non_list_rejected() -> None:
    m = {
        "files": [],
        "actions": {"x": {"run": {"not": "valid"}}},
    }
    with pytest.raises(SystemExit):
        manifest.validate_manifest(m)


# ---------------------------------------------------------------------------
# load_manifest
# ---------------------------------------------------------------------------


def _set_config_dir(monkeypatch, path) -> None:
    monkeypatch.setattr(manifest, "CONFIG_DIR", str(path))


def test_load_manifest_happy_path(tmp_path, monkeypatch) -> None:
    _set_config_dir(monkeypatch, tmp_path)
    (tmp_path / "manifest.yaml").write_text(
        "files:\n  - src: a\n    dst: /b\nactions: {}\n"
    )
    m = manifest.load_manifest()
    assert m["files"][0]["src"] == "a"
    assert m["files"][0]["dst"] == "/b"


def test_load_manifest_missing_exits(tmp_path, monkeypatch) -> None:
    _set_config_dir(monkeypatch, tmp_path)
    with pytest.raises(SystemExit):
        manifest.load_manifest()


def test_load_manifest_bad_yaml_exits(tmp_path, monkeypatch) -> None:
    _set_config_dir(monkeypatch, tmp_path)
    # Unbalanced bracket → yaml.YAMLError.
    (tmp_path / "manifest.yaml").write_text("files: [oops\n")
    with pytest.raises(SystemExit):
        manifest.load_manifest()


def test_load_manifest_non_mapping_exits(tmp_path, monkeypatch) -> None:
    _set_config_dir(monkeypatch, tmp_path)
    # Top-level YAML list, not a mapping.
    (tmp_path / "manifest.yaml").write_text("- a\n- b\n")
    with pytest.raises(SystemExit):
        manifest.load_manifest()


def test_load_manifest_empty_file_returns_empty_dict(tmp_path, monkeypatch) -> None:
    _set_config_dir(monkeypatch, tmp_path)
    (tmp_path / "manifest.yaml").write_text("")
    assert manifest.load_manifest() == {}


# ---------------------------------------------------------------------------
# host.host_run — argv shape lock
# ---------------------------------------------------------------------------


def test_host_run_argv_no_shell_no_concat() -> None:
    """The central security claim: host_run never shell-parses its
    arguments.  Each manifest-controlled string lands as its own argv
    element regardless of what shell metacharacters it contains.
    """
    weird = "weird path with 'single' \"double\" $vars && rm -rf /"
    with patch("subprocess.run") as run:
        run.return_value.returncode = 0
        host.host_run(["mkdir", "-p", weird])

    args, _ = run.call_args
    argv = args[0]
    # Exact prefix (the no-shell-injection guarantee comes from the
    # constant nsenter prefix + each arg as its own list element).
    assert argv == [
        "nsenter",
        "-t",
        "1",
        "-m",
        "--",
        "mkdir",
        "-p",
        weird,
    ]


def test_host_run_default_check_true_raises_on_nonzero() -> None:
    with patch("subprocess.run") as run:
        run.side_effect = __import__("subprocess").CalledProcessError(
            returncode=1, cmd=["x"]
        )
        with pytest.raises(__import__("subprocess").CalledProcessError):
            host.host_run(["false"])


def test_host_run_check_false_does_not_raise() -> None:
    with patch("subprocess.run") as run:
        run.return_value.returncode = 17
        # Should not raise even though returncode != 0.
        proc = host.host_run(["false"], check=False)
    assert proc.returncode == 17


# ---------------------------------------------------------------------------
# host.host_sha256 — happy-path parsing + missing-file → None
# ---------------------------------------------------------------------------


def test_host_sha256_happy_path() -> None:
    """sha256sum returns 0 + the standard ``<hash>  <path>\\n`` line."""
    with patch("subprocess.run") as run:
        proc = run.return_value
        proc.returncode = 0
        proc.stdout = (
            b"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            b"  /etc/hostname\n"
        )
        result = host.host_sha256("/etc/hostname")
    assert result == (
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )


def test_host_sha256_returns_none_on_missing_file() -> None:
    """``test -f`` returning non-zero (or ``sha256sum`` itself failing)
    surfaces as ``None`` rather than raising."""
    with patch("subprocess.run") as run:
        proc = run.return_value
        proc.returncode = 1
        proc.stdout = b""
        proc.stderr = b"sha256sum: /nope: No such file or directory\n"
        assert host.host_sha256("/nope") is None


# ---------------------------------------------------------------------------
# host.run_user_action — array form bypasses sh -c
# ---------------------------------------------------------------------------


def test_run_user_action_array_form_no_shell() -> None:
    """Array form: argv is exec'd directly via nsenter — no ``sh -c``."""
    with patch("subprocess.run") as run:
        run.return_value.returncode = 0
        host.run_user_action(["udevadm", "control", "--reload-rules"])

    args, _ = run.call_args
    argv = args[0]
    assert argv == [
        "nsenter",
        "-t",
        "1",
        "-m",
        "--",
        "udevadm",
        "control",
        "--reload-rules",
    ]
    # No "sh" / "-c" anywhere in the invocation.
    assert "sh" not in argv
    assert "-c" not in argv


def test_run_user_action_string_form_uses_sh_c() -> None:
    """String form: still goes through ``sh -c`` so pipes/redirects work."""
    with patch("subprocess.run") as run:
        run.return_value.returncode = 0
        host.run_user_action("echo hi | logger")

    args, _ = run.call_args
    argv = args[0]
    assert argv[-3:] == ["sh", "-c", "echo hi | logger"]
