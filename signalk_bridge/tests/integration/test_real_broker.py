# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""End-to-end: run the real bridge process against a stub Signal K server and a
real Mosquitto broker, then assert the discovery configs and converted state
values actually reach the wire -- the publish path the unit suite stubs out.
"""

import json
import os
import signal
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.e2e

ADDON_DIR = Path(__file__).resolve().parents[2]  # .../signalk_bridge


def _wait_until(
    predicate: Callable[[], bool], timeout: float, proc: subprocess.Popen[str]
) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        if proc.poll() is not None:  # bridge died before it should have
            out = proc.stdout.read() if proc.stdout else ""
            pytest.fail(f"bridge exited early (rc={proc.returncode}):\n{out}")
        time.sleep(0.2)
    return False


def test_bridge_publishes_to_a_real_broker(
    signalk_stub: str,
    mosquitto: tuple[str, int],
    subscriber: Any,
    tmp_path: Path,
) -> None:
    host, port = mosquitto
    options = {
        "signalk_url": signalk_stub,
        "signalk_token": "e2e-token",  # non-empty -> skips the request/approve flow
        "mqtt_host": host,
        "mqtt_port": port,
        "interval_seconds": 1,
        "log_level": "INFO",
    }
    opts_file = tmp_path / "options.json"
    opts_file.write_text(json.dumps(options))

    env = {**os.environ, "PYTHONPATH": str(ADDON_DIR)}
    proc = subprocess.Popen(
        [sys.executable, "-m", "signalk_bridge", "--options", str(opts_file)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    cfg = "homeassistant/sensor/signalk/navigation_speedoverground/config"
    sog = "signalk/navigation_speedoverground/state"
    temp = "signalk/environment_water_temperature/state"
    try:
        arrived = _wait_until(
            lambda: (
                subscriber.get("signalk/availability") == "online"
                and subscriber.get(cfg) is not None
                and subscriber.get(sog) is not None
                and subscriber.get(temp) is not None
            ),
            timeout=30,
            proc=proc,
        )
        assert arrived, (
            f"expected messages never arrived; saw {sorted(subscriber.snapshot())}"
        )

        # discovery payload is well-formed
        disc_raw = subscriber.get(cfg)
        assert disc_raw is not None
        disc = json.loads(disc_raw)
        assert disc["unit_of_measurement"] == "m/s"
        assert disc["device_class"] == "speed"
        assert disc["device"]["identifiers"] == ["signalk_navigation"]
        assert disc["state_topic"] == sog

        # converted values reached the wire
        assert subscriber.get(sog) == "3.086"
        assert subscriber.get(temp) == "15"
        assert (
            subscriber.get("signalk/electrical_batteries_house_stateofcharge/state")
            == "87"
        )
        cog_raw = subscriber.get("signalk/navigation_courseovergroundtrue/state")
        assert cog_raw is not None
        assert abs(float(cog_raw) - 90.0) < 0.01

        # position -> device_tracker: lat/lon in attributes, no state topic (GPS mode)
        pos_cfg = "homeassistant/device_tracker/signalk/navigation_position/config"
        pos_raw = subscriber.get(pos_cfg)
        assert pos_raw is not None
        pos = json.loads(pos_raw)
        assert pos["source_type"] == "gps"
        assert "state_topic" not in pos
        attrs_raw = subscriber.get("signalk/navigation_position/attributes")
        assert attrs_raw is not None
        attrs = json.loads(attrs_raw)
        assert attrs["latitude"] == 40.0
        assert attrs["longitude"] == -70.0

        # SIGTERM -> a clean "offline" publish before exit
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=10)
        deadline = time.monotonic() + 5
        while (
            time.monotonic() < deadline
            and subscriber.get("signalk/availability") != "offline"
        ):
            time.sleep(0.2)
        assert subscriber.get("signalk/availability") == "offline"
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
