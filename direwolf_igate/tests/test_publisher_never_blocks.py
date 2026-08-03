# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""The publisher must never stop draining direwolf's output.

It sits in a pipe between direwolf and the add-on log, so if it stops reading,
the pipe fills at 64 KiB and direwolf blocks on write -- a metrics fault taking
the radio gateway off the air.

Each test runs the real module as a subprocess with no broker reachable and
asserts a line fed to stdin still comes back on stdout.
"""

import json
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

ADDON_DIR = Path(__file__).resolve().parents[1]  # .../direwolf_igate
REPO_ROOT = ADDON_DIR.parent

SAMPLE = "[0.3] W1XM-15>APOT30:!4221.62N/07105.36Wr test\n"


def _dead_port() -> int:
    """A port with nothing listening, so connect() fails immediately."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _spawn(tmp_path: Path, options: dict) -> subprocess.Popen:
    opts = tmp_path / "options.json"
    opts.write_text(json.dumps(options))
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "direwolf_igate",
            "--options",
            str(opts),
            "--mycall",
            "N0CALL-10",
        ],
        cwd=str(ADDON_DIR),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )


def _expect_tee(proc: subprocess.Popen, timeout: float = 10.0) -> str:
    """Feed one line, return the first line teed back, or fail."""
    assert proc.stdin and proc.stdout
    proc.stdin.write(SAMPLE)
    proc.stdin.flush()

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        line = proc.stdout.readline()
        if not line:
            break
        if "W1XM-15" in line:
            return line
        # Startup banner / broker-retry warnings precede it; keep reading.
    pytest.fail("publisher never teed the input line back to stdout")


def test_tee_survives_an_unreachable_broker(tmp_path: Path) -> None:
    """connect_mqtt_with_retry never gives up, so it must not run on the thread
    that drains stdin."""
    proc = _spawn(
        tmp_path,
        {
            "mqtt_enabled": True,
            "mqtt_host": "127.0.0.1",
            "mqtt_port": _dead_port(),
            "interval_seconds": 5,
            "log_level": "INFO",
        },
    )
    try:
        assert "W1XM-15" in _expect_tee(proc)
        assert proc.poll() is None, "publisher exited instead of continuing"
    finally:
        proc.kill()
        proc.wait(timeout=5)


def test_tee_survives_a_broken_options_file(tmp_path: Path) -> None:
    """A malformed options file must degrade to a pass-through, not raise."""
    opts = tmp_path / "options.json"
    opts.write_text("{ this is not json")
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "direwolf_igate",
            "--options",
            str(opts),
            "--mycall",
            "N0CALL-10",
        ],
        cwd=str(ADDON_DIR),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    try:
        assert "W1XM-15" in _expect_tee(proc)
    finally:
        proc.kill()
        proc.wait(timeout=5)


def test_tee_survives_a_non_utf8_byte(tmp_path: Path) -> None:
    """A bearing report gated down from APRS-IS carries a Latin-1 degree sign
    (0xb0). Under strict decoding that raises out of the read loop and kills
    the gateway; the module asks for surrogateescape so it round-trips."""
    opts = tmp_path / "options.json"
    opts.write_text(json.dumps({"mqtt_enabled": False}))
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "direwolf_igate",
            "--options",
            str(opts),
            "--mycall",
            "N0CALL-10",
        ],
        cwd=str(ADDON_DIR),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=0,
    )
    try:
        assert proc.stdin and proc.stdout
        payload = b"DX=1*VE2BNE-3(134mi@10\xb0)\n"
        proc.stdin.write(payload)
        proc.stdin.flush()

        deadline = time.monotonic() + 10.0
        seen = b""
        while time.monotonic() < deadline and b"VE2BNE" not in seen:
            chunk = proc.stdout.readline()
            if not chunk:
                break
            seen += chunk
        assert b"VE2BNE" in seen, f"non-UTF-8 line was not teed back: {seen!r}"
        # Byte-exact: the 0xb0 must survive, not be replaced or dropped.
        assert b"\xb0" in seen, f"0xb0 byte was mangled in transit: {seen!r}"
        assert proc.poll() is None, "publisher died on a non-UTF-8 byte"
    finally:
        proc.kill()
        proc.wait(timeout=5)


def test_sigterm_exits_promptly_instead_of_hanging_in_the_read_loop(
    tmp_path: Path,
) -> None:
    """SIGTERM must terminate the publisher, not leave it blocked in the read
    loop until run.sh escalates to KILL.

    Broker deliberately unreachable: the handler must not need a live
    connection to get out of the way.
    """
    proc = _spawn(
        tmp_path,
        {
            "mqtt_enabled": True,
            "mqtt_host": "127.0.0.1",
            "mqtt_port": _dead_port(),
            "interval_seconds": 5,
            "log_level": "INFO",
        },
    )
    try:
        # Confirm it is up and draining before signalling.
        assert "W1XM-15" in _expect_tee(proc)

        proc.send_signal(signal.SIGTERM)
        # No live session, so ``shutdown()`` returns immediately and re-raises
        # SIGTERM; the only work between signal delivery and exit is Python
        # unwinding. 5s covers even a heavily-contended GHA runner and still
        # catches any regression that reintroduces a blocking teardown.
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pytest.fail("publisher ignored SIGTERM and stayed in the read loop")

        # Conventional signal status, not an invented exit code: the handler
        # restores the default disposition and re-raises.
        assert proc.returncode in (-signal.SIGTERM, 128 + signal.SIGTERM), (
            f"unexpected exit status {proc.returncode}"
        )
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


def test_sigterm_is_prompt_with_mqtt_disabled_too(tmp_path: Path) -> None:
    """The pass-through path has no handler of its own, so it must still die on
    the default disposition rather than linger."""
    proc = _spawn(tmp_path, {"mqtt_enabled": False})
    try:
        assert "W1XM-15" in _expect_tee(proc)
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            pytest.fail("pass-through publisher ignored SIGTERM")
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


def test_a_bad_mqtt_option_does_not_break_the_disabled_path(tmp_path: Path) -> None:
    """With MQTT off, the MQTT options are irrelevant: an unparsable one must
    not stop the tee, and must not be reported as an unreadable options file."""
    proc = _spawn(tmp_path, {"mqtt_enabled": False, "interval_seconds": "not-a-number"})
    try:
        assert "W1XM-15" in _expect_tee(proc)
    finally:
        proc.kill()
        proc.wait(timeout=5)


def test_a_bad_mqtt_option_degrades_to_the_tee_when_enabled(tmp_path: Path) -> None:
    """With MQTT on, an unparsable option disables statistics but must still
    leave direwolf's output flowing."""
    proc = _spawn(tmp_path, {"mqtt_enabled": True, "mqtt_port": "not-a-port"})
    try:
        assert "W1XM-15" in _expect_tee(proc)
        assert proc.poll() is None
    finally:
        proc.kill()
        proc.wait(timeout=5)
