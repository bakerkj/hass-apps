# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""The main loop must stay responsive to signals between GPU samples.

The handler only sets a stop flag, so the loop has to reach its own condition
to act on it. Parked in a blocking read it never does: the interrupted read is
retried once the handler returns, and nothing rechecks the flag until the next
sample. intel_gpu_top is sampled once per ``interval_seconds`` -- 60s on the
deployed host -- against a 10s add-on stop grace, so SIGTERM was usually missed
outright and the add-on SIGKILLed.

Each test runs the real module as a subprocess against a stub broker and an
intel_gpu_top that stops emitting.
"""

import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

ADDON_DIR = Path(__file__).resolve().parents[1]  # .../intel_gpu_top_mqtt

_CONNACK = b"\x20\x02\x00\x00"  # accepted, no session present


def _stub_broker() -> tuple[int, socket.socket]:
    """Enough of a broker to reach a normally-connected paho client."""
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


def _fake_intel_gpu_top(tmp_path: Path) -> Path:
    """Emits one JSON sample, then goes silent forever like a 60s interval."""
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    script = bindir / "intel_gpu_top"
    sample = json.dumps(
        {
            "period": {"duration": 1000, "unit": "ms"},
            "engines": {"Render/3D/0": {"busy": 12.5, "unit": "%"}},
            "frequency": {"requested": 300, "actual": 300, "unit": "MHz"},
        }
    )
    script.write_text(
        "#!/usr/bin/env python3\n"
        "import sys, time\n"
        f"sys.stdout.write({sample!r} + '\\n')\n"
        "sys.stdout.flush()\n"
        "while True:\n"
        "    time.sleep(3600)\n"
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
                "interval_seconds": 60,
                "log_level": "INFO",
            }
        )
    )
    env = dict(os.environ, PATH=f"{bindir}:{os.environ['PATH']}")
    return subprocess.Popen(
        [sys.executable, "-m", "intel_gpu_mqtt", "--options", str(opts)],
        cwd=str(ADDON_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
    )


def _wait_until_running(proc: subprocess.Popen, timeout: float = 20.0) -> None:
    """Block until intel_gpu_top has been started, so the loop is live."""
    assert proc.stdout
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        line = proc.stdout.readline()
        if not line:
            break
        if "process started" in line:
            return
    pytest.fail("add-on never reported intel_gpu_top starting")


def test_sigterm_is_prompt_between_gpu_samples(tmp_path: Path) -> None:
    """A silent intel_gpu_top must not pin the process past SIGTERM.

    Deterministic rather than racy: the handler sets the flag, the read is
    retried, and nothing rechecks it until a sample that never comes.
    """
    port, srv = _stub_broker()
    proc = _spawn(tmp_path, port, _fake_intel_gpu_top(tmp_path))
    try:
        _wait_until_running(proc)
        time.sleep(1.0)  # well past the single emitted sample

        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            pytest.fail("ignored SIGTERM while blocked on a silent intel_gpu_top")

        assert proc.returncode == 0, f"unexpected exit status {proc.returncode}"
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
        srv.close()


@pytest.mark.skipif(not Path("/proc").is_dir(), reason="needs procfs")
def test_main_thread_never_parks_in_the_blocking_read(tmp_path: Path) -> None:
    """The invariant behind the test above, asserted directly."""
    port, srv = _stub_broker()
    proc = _spawn(tmp_path, port, _fake_intel_gpu_top(tmp_path))
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
