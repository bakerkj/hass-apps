# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""End-to-end: run the shipped run.sh and verify it renders a correct sdr.conf,
keeps the APRS-IS passcode out of the log, refuses configs that would corrupt
the generated file, and actually reaches the point of spawning the two
processes -- not just that the image builds.

Scaffolding (mock Supervisor, image build) lives in conftest.py.

Run: pytest direwolf_igate/tests/integration/ -m e2e   (requires docker; the
default pytest run excludes the e2e marker).
"""

import time

import pytest

from conftest import BASE_OPTIONS, PASSCODE, SupervisorStub, docker, require_docker

pytestmark = pytest.mark.e2e


def run_to_completion(supervisor: SupervisorStub, image: str, options: dict) -> str:
    """Run the add-on to exit and return its combined log.

    Every path through run.sh terminates without a dongle: a rejected config
    exits at the validation call, and an accepted one exits once rtl_fm fails
    and the surviving child is reaped (bounded by the script's 5s grace).
    """
    name = supervisor.start_addon(image, options)
    try:
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            running = docker(
                "inspect", "-f", "{{.State.Running}}", name, check=False
            ).strip()
            if running == "false":
                break
            time.sleep(0.5)
        else:
            pytest.fail(f"container never exited\n{docker('logs', name, check=False)}")
        return docker("logs", name, check=False)
    finally:
        docker("rm", "-f", name, check=False)


def sdr_conf_lines(logs: str) -> list[str]:
    """The rendered sdr.conf as run.sh echoes it, up to the next log line."""
    lines = logs.splitlines()
    for i, line in enumerate(lines):
        if "Rendered sdr.conf:" in line:
            rest = lines[i + 1 :]
            break
    else:
        pytest.fail(f"add-on never rendered sdr.conf\n{logs}")
    out = []
    for line in rest:
        if "Starting direwolf" in line:
            break
        out.append(line.strip())
    return [line for line in out if line]


def test_renders_sdr_conf_and_keeps_passcode_out_of_the_log(
    supervisor: SupervisorStub, addon_image: str
) -> None:
    require_docker()
    logs = run_to_completion(supervisor, addon_image, dict(BASE_OPTIONS))
    conf = sdr_conf_lines(logs)

    assert "MYCALL N0CALL-10" in conf, conf
    assert "IGSERVER noam.aprs2.net" in conf, conf
    assert "ARATE 24000" in conf, conf  # must match rtl_fm's demod rate
    assert any(line.startswith("IBEACON") for line in conf), conf

    beacon = [line for line in conf if line.startswith("PBEACON")]
    assert len(beacon) == 1, conf
    assert "lat=51.4779" in beacon[0] and "long=-0.0015" in beacon[0], beacon
    assert 'COMMENT="iGate | DireWolf"' in beacon[0], beacon

    # The passcode is a shared secret: it must reach sdr.conf but never the log.
    login = [line for line in conf if line.startswith("IGLOGIN")]
    assert login == ["IGLOGIN N0CALL-10 [redacted]"], conf
    assert PASSCODE not in logs, "APRS-IS passcode leaked into the add-on log"


def test_reaches_the_spawn_stage(supervisor: SupervisorStub, addon_image: str) -> None:
    """run.sh gets past the FIFO handoff to the lines that spawn the pair.

    Does NOT prove either process exec'd: both lines are emitted before the
    `&`, and without a dongle rtl_fm dies in milliseconds and direwolf emits
    nothing before teardown. What it catches is a failure *between* the render
    and the spawn -- the FIFO handoff. Config reachability is covered by
    test_direwolf_can_read_its_rendered_config.
    """
    require_docker()
    logs = run_to_completion(supervisor, addon_image, dict(BASE_OPTIONS))

    assert "Starting direwolf" in logs, logs
    assert "Starting rtl_fm" in logs, logs
    for noise in ("mktemp:", "mkfifo:", "Invalid argument", "Permission denied"):
        assert noise not in logs, f"{noise!r} in startup output:\n{logs}"


def test_direwolf_can_read_its_rendered_config(addon_image: str) -> None:
    """The rendered config must be readable by the account direwolf drops to,
    and that directory must not be writable by it.

    Both halves matter: 0700 would block the symlink race and also stop
    direwolf reading its config, which nothing else here would notice.
    """
    require_docker()

    # Evaluate the SHIPPED lines; restating them would verify our own copy.
    script = (
        "eval \"$(grep -m1 '^CONF_DIR=' /run.sh)\" && "
        "eval \"$(grep -m1 'install -d' /run.sh)\" && "
        "eval \"$(grep -m1 '^CONF_PATH=' /run.sh)\" && "
        "echo 'MYCALL N0CALL-10' > \"$CONF_PATH\" && "
        'chown direwolf:direwolf "$CONF_PATH" && chmod 400 "$CONF_PATH" && '
        'echo DIR="$CONF_DIR" && '
        'echo READ=$(su-exec direwolf:direwolf cat "$CONF_PATH" '
        "   >/dev/null 2>&1 && echo yes || echo no) && "
        "echo PLANT=$(su-exec direwolf:direwolf ln -s /etc/shadow "
        '   "$CONF_DIR/evil" >/dev/null 2>&1 && echo yes || echo no)'
    )
    out = docker("run", "--rm", "--entrypoint", "sh", addon_image, "-c", script)

    assert "READ=yes" in out, f"direwolf cannot read its own config:\n{out}"
    assert "PLANT=no" in out, f"direwolf can plant a symlink in the conf dir:\n{out}"


def test_conf_dir_mode_matches_what_run_sh_creates(addon_image: str) -> None:
    """The mode above is only meaningful if run.sh actually creates it that way."""
    require_docker()
    run_sh = docker("run", "--rm", "--entrypoint", "cat", addon_image, "/run.sh")
    install_line = next(
        line
        for line in run_sh.splitlines()
        if "install -d" in line and "CONF_DIR" in line
    )
    assert "-m 750" in install_line, install_line
    assert "-g direwolf" in install_line, install_line
    assert "/tmp/" not in run_sh.split("CONF_PATH=")[1].splitlines()[0], (
        "sdr.conf must not be rendered into world-writable /tmp"
    )


def test_beacon_comment_cannot_inject_a_direwolf_directive(
    supervisor: SupervisorStub, addon_image: str
) -> None:
    """Direwolf's config has no escape syntax, so a newline in any interpolated
    option would start a new directive. IGTXLIMIT is the one that matters: it
    would turn this receive-only IGate into a transmitter."""
    require_docker()
    options = dict(BASE_OPTIONS)
    options["beacon_comment"] = 'x"\nIGTXLIMIT 20 20\nPBEACON sendto=IG symbol="y'
    logs = run_to_completion(supervisor, addon_image, options)
    conf = sdr_conf_lines(logs)

    assert not any(line.startswith("IGTXLIMIT") for line in conf), conf
    # The payload survives only flattened into the single legitimate beacon.
    assert len([line for line in conf if line.startswith("PBEACON")]) == 1, conf


def test_gating_survives_an_unreachable_mqtt_broker(
    supervisor: SupervisorStub, addon_image: str
) -> None:
    """MQTT is secondary here. The sibling MQTT add-ons exit when the broker
    stays unreachable, which in this add-on would take the radio gateway down
    over a metrics outage -- so enabling MQTT with nothing listening must still
    reach the spawn."""
    require_docker()
    options = dict(BASE_OPTIONS) | {
        "mqtt_enabled": True,
        "mqtt_host": "127.0.0.1",
        "mqtt_port": 1,  # nothing listens on port 1
        "interval_seconds": 5,
    }
    logs = run_to_completion(supervisor, addon_image, options)

    assert "Starting direwolf" in logs, logs
    assert "Starting rtl_fm" in logs, logs


@pytest.mark.parametrize(
    "override,expected",
    [
        ({"mycall": "N0CALL-10 IGTXLIMIT 99"}, "mycall"),
        ({"mycall": ""}, "mycall"),
        ({"iglogin_passcode": "not-a-number"}, "iglogin_passcode"),
        ({"latitude": 0.0, "longitude": 0.0}, "latitude"),
    ],
    ids=["callsign-with-junk", "callsign-empty", "passcode-non-numeric", "null-island"],
)
def test_invalid_config_is_refused_before_anything_starts(
    supervisor: SupervisorStub, addon_image: str, override: dict, expected: str
) -> None:
    """Each of these would otherwise render a file direwolf silently misreads --
    a malformed MYCALL/IGLOGIN pair, or a beacon announcing Null Island."""
    require_docker()
    logs = run_to_completion(supervisor, addon_image, dict(BASE_OPTIONS) | override)

    assert expected in logs, logs
    assert "Rendered sdr.conf" not in logs, logs
    assert "Starting direwolf" not in logs, logs


def test_direwolf_does_not_run_as_root(addon_image: str) -> None:
    """Direwolf warns on every decode that running as root is an unnecessary
    risk, and it is right -- it only reads a pipe and opens an outbound socket.
    Asserted against the shipped image the way signalk's e2e checks its server
    runs as ``node``.

    Scope, honestly: the first half is a real observation -- ``id direwolf`` in
    the shipped image. The second half is a *text* check on run.sh, not an
    observation of any process's uid, because a dongle-less run tears the pair
    down before direwolf emits anything. So this catches su-exec being dropped
    from the invocation, but would not catch it being present and failing.
    rtl_fm deliberately stays root: it needs /dev/bus/usb.
    """
    require_docker()

    ids = docker("run", "--rm", "--entrypoint", "id", addon_image, "direwolf")
    assert "uid=" in ids, ids

    run_sh = docker("run", "--rm", "--entrypoint", "cat", addon_image, "/run.sh")
    direwolf_line = next(
        line for line in run_sh.splitlines() if "/usr/bin/direwolf" in line
    )
    assert "su-exec direwolf" in direwolf_line, direwolf_line
    rtl_line = next(line for line in run_sh.splitlines() if "/usr/bin/rtl_fm" in line)
    assert "su-exec" not in rtl_line, rtl_line


def test_optional_options_left_unset_do_not_render_the_word_null(
    supervisor: SupervisorStub, addon_image: str
) -> None:
    """bashio::config returns the literal "null" for an unset optional option.
    run.sh has branches for exactly that, and nothing covered them because the
    fixture always supplies every key. Delete the beacon_comment branch and the
    add-on beacons COMMENT="null" to APRS-IS worldwide.
    """
    require_docker()
    options = dict(BASE_OPTIONS)
    del options["beacon_comment"]  # str? -- legitimately omittable
    del options["rtl_gain"]  # str? -- empty means automatic gain
    logs = run_to_completion(supervisor, addon_image, options)
    conf = sdr_conf_lines(logs)

    beacon = [line for line in conf if line.startswith("PBEACON")]
    assert len(beacon) == 1, conf
    assert "null" not in beacon[0], f'literal "null" reached sdr.conf: {beacon[0]}'
    # Absent comment means no COMMENT= token at all, not an empty or "null" one.
    assert "COMMENT=" not in beacon[0], beacon[0]
    # Absent gain means rtl_fm is invoked without -g, letting the tuner decide.
    rtl = next(line for line in logs.splitlines() if "Starting rtl_fm" in line)
    assert " -g " not in rtl, rtl


def test_status_beacon_can_be_switched_off(
    supervisor: SupervisorStub, addon_image: str
) -> None:
    """Only the enabled case was covered, so inverting the condition passed."""
    require_docker()
    options = dict(BASE_OPTIONS) | {"send_status_beacon": False}
    conf = sdr_conf_lines(run_to_completion(supervisor, addon_image, options))

    assert not any(line.startswith("IBEACON") for line in conf), conf
    # The position beacon is independent and must survive.
    assert any(line.startswith("PBEACON") for line in conf), conf


def test_beacon_schedule_and_symbol_reach_sdr_conf(
    supervisor: SupervisorStub, addon_image: str
) -> None:
    """Only lat/long/COMMENT were asserted, so dropping every= -- turning a
    15-minute beacon into a one-shot -- slipped through."""
    require_docker()
    options = dict(BASE_OPTIONS) | {
        "beacon_delay": "0:45",
        "beacon_every": "20:00",
        "beacon_symbol": "igate",
        "beacon_overlay": "T",
    }
    conf = sdr_conf_lines(run_to_completion(supervisor, addon_image, options))
    beacon = next(line for line in conf if line.startswith("PBEACON"))

    assert "delay=0:45" in beacon, beacon
    assert "every=20:00" in beacon, beacon
    assert 'symbol="igate"' in beacon, beacon
    assert "overlay=T" in beacon, beacon


@pytest.mark.parametrize(
    "field,value",
    [
        ("beacon_every", "15:00 lat=0 long=0"),
        ("beacon_delay", "0:30 lat=0"),
        ("beacon_overlay", "R sendto=R0"),
    ],
    ids=["every-overrides-position", "delay-appends", "overlay-retargets"],
)
def test_unquoted_beacon_fields_cannot_smuggle_pbeacon_keywords(
    supervisor: SupervisorStub, addon_image: str, field: str, value: str
) -> None:
    """These three land unquoted on the PBEACON line, so a space appends further
    keywords rather than injecting a directive -- enough to override lat=/long=
    and walk straight past the Null Island guard, beaconing a false position."""
    require_docker()
    logs = run_to_completion(
        supervisor, addon_image, dict(BASE_OPTIONS) | {field: value}
    )

    assert field in logs, logs
    assert "Rendered sdr.conf" not in logs, logs


def test_igserver_injection_is_rejected_before_rendering(
    supervisor: SupervisorStub, addon_image: str
) -> None:
    """IGSERVER lands unquoted on its own line. The sanitising loop strips the
    newline, but the hostname shape check refuses the value outright, so nothing
    reaches sdr.conf to be interpreted."""
    require_docker()
    options = dict(BASE_OPTIONS) | {
        "igserver": 'rotate.aprs2.net"\nIGTXLIMIT 20 20',
    }
    logs = run_to_completion(supervisor, addon_image, options)

    assert "igserver" in logs, logs
    assert "Rendered sdr.conf" not in logs, logs
    assert "IGTXLIMIT" not in logs, logs


def test_teardown_signals_direwolf_and_not_only_the_publisher(
    addon_image: str,
) -> None:
    """Scope: a text check on the shipped run.sh, not an observed signal.

    A dongle-less container tears itself down in milliseconds, so the graceful
    path cannot be exercised here; the Python half is covered by
    tests/test_publisher_never_blocks.py, which sends a real SIGTERM.

    `$!` after a pipeline is its LAST process, so a teardown built on it alone
    signals the publisher and never direwolf.
    """
    require_docker()
    run_sh = docker("run", "--rm", "--entrypoint", "cat", addon_image, "/run.sh")

    assert "DIREWOLF_PID=$(jobs -p %%)" in run_sh, "direwolf's pid is not captured"
    kills = [line for line in run_sh.splitlines() if line.strip().startswith("kill -")]
    escalating = [line for line in kills if "TERM" in line or "KILL" in line]
    assert escalating, kills
    for line in escalating:
        if "jobs -p" in line:
            continue  # the cleanup() path signals the process group instead
        assert "DIREWOLF_PID" in line, f"teardown line omits direwolf: {line}"


def test_status_beacon_states_its_destination_and_interval(
    supervisor: SupervisorStub, addon_image: str
) -> None:
    """direwolf forces sendto=IG for IBEACON, but every= falls back to its own
    600s default. Both are stated so the counters this beacon carries refresh on
    the interval the user configured."""
    require_docker()
    options = dict(BASE_OPTIONS) | {"beacon_every": "20:00"}
    conf = sdr_conf_lines(run_to_completion(supervisor, addon_image, options))

    ibeacon = next(line for line in conf if line.startswith("IBEACON"))
    assert "sendto=IG" in ibeacon, ibeacon
    assert "every=20:00" in ibeacon, ibeacon


def test_audio_fifo_is_not_in_world_writable_tmp(addon_image: str) -> None:
    """A FIFO name under 1777 /tmp can be raced by anything in the container.
    CONF_DIR is root-owned and not writable by the direwolf account."""
    require_docker()
    run_sh = docker("run", "--rm", "--entrypoint", "cat", addon_image, "/run.sh")
    pipe_line = next(line for line in run_sh.splitlines() if line.startswith("PIPE="))

    assert "/tmp" not in pipe_line, pipe_line
    assert "CONF_DIR" in pipe_line, pipe_line


@pytest.mark.parametrize(
    "override,expected",
    [
        ({"beacon_symbol": "not a symbol!"}, "beacon_symbol"),
        ({"igserver": "no spaces allowed here"}, "igserver"),
    ],
    ids=["bad-symbol", "bad-igserver"],
)
def test_malformed_values_fail_at_startup_not_as_a_direwolf_warning(
    supervisor: SupervisorStub, addon_image: str, override: dict, expected: str
) -> None:
    require_docker()
    logs = run_to_completion(supervisor, addon_image, dict(BASE_OPTIONS) | override)

    assert expected in logs, logs
    assert "Rendered sdr.conf" not in logs, logs
