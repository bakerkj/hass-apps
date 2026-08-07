# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""The main loop must stay responsive to signals while turbostat is quiet.

The handler only sets a stop flag, so the loop has to reach its own condition
to act on it. Parked in a blocking read that no longer produces lines it never
does: the interrupted read is simply retried once the handler returns, and
SIGTERM is ignored until the supervisor escalates to SIGKILL.

Each test runs the real module as a subprocess against a stub broker and a
turbostat that stops emitting.
"""

import json
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

ADDON_DIR = Path(__file__).resolve().parents[1]  # .../turbostat_mqtt

_CONNACK = b"\x20\x02\x00\x00"  # accepted, no session present

# Prelude for the fake child: die with the add-on rather than outliving it.
# PR_SET_PDEATHSIG (1) fires even when the parent is SIGKILLed, which is exactly
# the case these tests provoke.
_SELF_REAP = (
    "import ctypes, signal, sys, time\n"
    "try:\n"
    "    ctypes.CDLL('libc.so.6', use_errno=True).prctl(1, signal.SIGKILL)\n"
    "except Exception:\n"
    "    pass\n"
    "_deadline = time.monotonic() + 120\n"
)


def _stub_broker() -> tuple[int, socket.socket]:
    """Enough of a broker to reach a normally-connected paho client.

    Accepting alone would get main() into the loop -- connect() writes CONNECT
    without waiting -- but the client would then sit unacknowledged and
    loop_stop()'s join at shutdown would never return, hanging the teardown for
    a reason unrelated to what these tests cover.
    """
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)

    def serve() -> None:
        while True:
            try:
                conn, _ = srv.accept()
            except OSError:
                return
            threading.Thread(target=_session, args=(conn,), daemon=True).start()

    def _session(conn: socket.socket) -> None:
        try:
            conn.recv(4096)  # CONNECT
            conn.sendall(_CONNACK)
            while conn.recv(4096):  # drain PUBLISH/SUBSCRIBE/DISCONNECT
                pass
        except OSError:
            pass
        finally:
            conn.close()

    threading.Thread(target=serve, daemon=True).start()
    return int(srv.getsockname()[1]), srv


def _fake_turbostat(tmp_path: Path, *, lines: int = 2) -> Path:
    """A turbostat that emits a couple of lines and then goes silent forever.

    Self-reaping, because these tests exist to prove the add-on gets SIGKILLed
    when it is broken -- and a SIGKILLed parent never reaches the ``terminate()``
    that would clean this up. PR_SET_PDEATHSIG kills it the moment the add-on
    dies however it dies; the deadline is a backstop for anything that leaves it
    reparented instead.
    """
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    script = bindir / "turbostat"
    script.write_text(
        "#!/usr/bin/env python3\n"
        f"{_SELF_REAP}"
        f"for i in range({lines}):\n"
        "    sys.stdout.write('Busy%  Bzy_MHz  PkgWatt\\n' if i == 0 else "
        "'12.5  3200  45.2\\n')\n"
        "    sys.stdout.flush()\n"
        "while time.monotonic() < _deadline:\n"
        "    time.sleep(1)\n"
    )
    script.chmod(0o755)
    return bindir


def _spawn(tmp_path: Path, port: int, bindir: Path) -> subprocess.Popen:
    opts = tmp_path / "options.json"
    opts.write_text(
        json.dumps(
            {
                "mqtt_host": "127.0.0.1",
                "mqtt_port": port,
                "interval_seconds": 1,
                "log_level": "INFO",
            }
        )
    )
    import os

    env = dict(os.environ, PATH=f"{bindir}:{os.environ['PATH']}")
    return subprocess.Popen(
        [sys.executable, "-m", "turbostat_mqtt", "--options", str(opts)],
        cwd=str(ADDON_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
    )


def _wait_until_running(proc: subprocess.Popen, timeout: float = 15.0) -> None:
    """Block until the add-on reports turbostat started, so the loop is live."""
    assert proc.stdout
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        line = proc.stdout.readline()
        if not line:
            break
        if "Started turbostat" in line:
            return
    pytest.fail("add-on never reported turbostat starting")


def test_sigterm_is_prompt_while_turbostat_is_silent(tmp_path: Path) -> None:
    """A quiet turbostat must not pin the process past SIGTERM.

    Blocking readline() on the main thread makes this deterministic rather than
    racy: the handler sets the flag, the read is retried, and nothing rechecks
    the flag until a line that never comes.
    """
    port, srv = _stub_broker()
    proc = _spawn(tmp_path, port, _fake_turbostat(tmp_path))
    try:
        _wait_until_running(proc)
        time.sleep(1.0)  # well past the last emitted line

        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            pytest.fail("ignored SIGTERM while blocked reading a quiet turbostat")

        assert proc.returncode == 0, f"unexpected exit status {proc.returncode}"
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
        srv.close()


@pytest.mark.skipif(not Path("/proc").is_dir(), reason="needs procfs")
def test_main_thread_never_parks_in_the_blocking_read(tmp_path: Path) -> None:
    """The invariant behind the test above, asserted directly.

    The main thread is the only one that runs Python signal handlers, so it
    must not be the one sitting in turbostat's pipe.
    """
    port, srv = _stub_broker()
    proc = _spawn(tmp_path, port, _fake_turbostat(tmp_path))
    try:
        _wait_until_running(proc)

        wchan = Path(f"/proc/{proc.pid}/task/{proc.pid}/wchan")
        seen = set()
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            seen.add(wchan.read_text().strip())
            time.sleep(0.02)

        assert not [w for w in seen if "pipe" in w], (
            f"main thread blocked in the pipe read: {sorted(seen)}"
        )
    finally:
        proc.kill()
        proc.wait(timeout=5)
        srv.close()
