# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""End-to-end behavior of the container_hooks addon against a real
docker daemon.

The addon is built and run as a real container; a throwaway alpine
container serves as the hook target. We assert by:

  * checking files inside the target via ``docker exec`` (proves a
    post-start hook ran or a pre-start file/patch landed), and
  * watching the addon's container log on the host (proves dispatch,
    debounce, and skip decisions).
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.e2e


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _drop_post_start_script(
    hooks_dir: Path, container: str, name: str, body: str
) -> None:
    d = hooks_dir / container / "scripts"
    d.mkdir(parents=True, exist_ok=True)
    p = d / name
    p.write_text(body)
    p.chmod(0o755)


def _drop_pre_start_file(
    hooks_dir: Path, container: str, rel_path: str, content: str
) -> Path:
    p = hooks_dir / container / "pre-start-files" / rel_path
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content)
    return p


def _drop_pre_start_patch(
    hooks_dir: Path, container: str, name: str, diff: str
) -> None:
    d = hooks_dir / container / "pre-start-patches"
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_text(diff)


# ---------------------------------------------------------------------------
# post-start path
# ---------------------------------------------------------------------------


def test_post_start_hook_runs_on_event_start(
    addon, addon_image, target, hooks_dir, options_path, write_options, wait_for
):
    """A script under ``scripts/`` fires inside the target on docker start."""
    target_name = target.name
    _drop_post_start_script(
        hooks_dir,
        target_name,
        "00-marker.sh",
        "#!/bin/sh\ntouch /tmp/rocs-e2e\n",
    )
    write_options(options_path, debounce_seconds=0)
    addon.start(addon_image)

    target.run()

    assert wait_for(
        lambda: target.exec_check("test", "-f", "/tmp/rocs-e2e"), timeout=15
    ), f"hook never landed /tmp/rocs-e2e inside target. addon log:\n{addon.logs()}"
    log = hooks_dir / target_name / "logs" / "post-start.log"
    assert log.exists(), f"post-start.log missing under {log.parent}"


def test_initial_sweep_runs_for_already_running_target(
    addon, addon_image, target, hooks_dir, options_path, write_options, wait_for
):
    """``initial_sweep`` catches a container that came up before the addon."""
    target_name = target.name
    target.run()

    _drop_post_start_script(
        hooks_dir,
        target_name,
        "00-marker.sh",
        "#!/bin/sh\ntouch /tmp/rocs-sweep\n",
    )
    write_options(options_path, initial_sweep=True, debounce_seconds=0)
    addon.start(addon_image)

    assert wait_for(
        lambda: target.exec_check("test", "-f", "/tmp/rocs-sweep"), timeout=15
    ), f"initial sweep did not fire. addon log:\n{addon.logs()}"


def test_post_start_scripts_run_in_lex_order(
    addon, addon_image, target, hooks_dir, options_path, write_options, wait_for
):
    """Multiple scripts run in lex-sorted filename order, all on one dispatch."""
    target_name = target.name
    _drop_post_start_script(
        hooks_dir,
        target_name,
        "00-first.sh",
        "#!/bin/sh\necho a >> /tmp/rocs-order\n",
    )
    _drop_post_start_script(
        hooks_dir,
        target_name,
        "10-second.sh",
        "#!/bin/sh\necho b >> /tmp/rocs-order\n",
    )
    _drop_post_start_script(
        hooks_dir,
        target_name,
        "20-third.sh",
        "#!/bin/sh\necho c >> /tmp/rocs-order\n",
    )
    write_options(options_path, debounce_seconds=0)
    addon.start(addon_image)

    target.run()

    assert wait_for(
        lambda: target.exec_check("test", "-s", "/tmp/rocs-order"), timeout=15
    ), f"no /tmp/rocs-order in target. addon log:\n{addon.logs()}"
    # All three should have appended, in order.
    assert wait_for(
        lambda: target.exec_capture("cat", "/tmp/rocs-order") == "a\nb\nc\n",
        timeout=10,
    ), f"order file contents: {target.exec_capture('cat', '/tmp/rocs-order')!r}"


# ---------------------------------------------------------------------------
# pre-start path (create event)
# ---------------------------------------------------------------------------


def test_pre_start_files_land_in_target_before_entrypoint(
    addon, addon_image, target, hooks_dir, options_path, write_options, wait_for
):
    """``pre-start-files/`` content is put_archive'd before docker start."""
    target_name = target.name
    _drop_pre_start_file(hooks_dir, target_name, "opt/rocs-marker", "from-pre-start\n")
    write_options(options_path, watch_create_events=True, debounce_seconds=0)
    addon.start(addon_image)

    # Create then start, so the addon's `container_created` handler races
    # `docker start`. ``-d`` on ``run`` does both atomically — sufficient
    # for the fast path because the create event still fires first.
    target.run()

    assert wait_for(
        lambda: target.exec_check("test", "-f", "/opt/rocs-marker"), timeout=15
    ), f"pre-start file did not land. addon log:\n{addon.logs()}"
    assert target.exec_capture("cat", "/opt/rocs-marker") == "from-pre-start\n"


def test_pre_start_patch_preserves_executable_mode(
    addon, addon_image, target, hooks_dir, options_path, write_options, wait_for
):
    """Patch round-trip must keep the executable bit on a 0755 source.

    Real-world target: a script under ``/etc/cont-init.d/`` that s6
    skips silently if the +x bit is lost during the get/put round-trip.
    """
    target_name = target.name
    payload = _drop_pre_start_file(
        hooks_dir, target_name, "etc/cont-init.d/00-probe", "#!/bin/sh\n"
    )
    payload.chmod(0o755)
    _drop_pre_start_patch(
        hooks_dir,
        target_name,
        "00-rewrite.patch",
        "--- a/etc/cont-init.d/00-probe\n"
        "+++ b/etc/cont-init.d/00-probe\n"
        "@@ -1 +1 @@\n"
        "-#!/bin/sh\n"
        "+#!/bin/sh -e\n",
    )
    write_options(options_path, watch_create_events=True, debounce_seconds=0)
    addon.start(addon_image)

    target.run()

    assert wait_for(
        lambda: target.exec_check("test", "-x", "/etc/cont-init.d/00-probe"),
        timeout=15,
    ), (
        f"+x bit lost after patch round-trip. ls: "
        f"{target.exec_capture('ls', '-l', '/etc/cont-init.d/00-probe')!r}\n"
        f"addon log:\n{addon.logs()}"
    )
    # And the content must reflect the patch.
    assert target.exec_capture("cat", "/etc/cont-init.d/00-probe") == "#!/bin/sh -e\n"


def test_pre_start_patch_modifies_staged_file(
    addon, addon_image, target, hooks_dir, options_path, write_options, wait_for
):
    """A unified diff applied at create-time mutates the staged file.

    The patch runs after put_archive, so we stage a known file via
    ``pre-start-files/`` first, then patch it. This avoids depending on
    any specific byte of the target image's stock content.
    """
    target_name = target.name
    _drop_pre_start_file(hooks_dir, target_name, "opt/rocs-payload", "original\n")
    _drop_pre_start_patch(
        hooks_dir,
        target_name,
        "00-rewrite.patch",
        "--- a/opt/rocs-payload\n"
        "+++ b/opt/rocs-payload\n"
        "@@ -1 +1 @@\n"
        "-original\n"
        "+patched\n",
    )
    write_options(options_path, watch_create_events=True, debounce_seconds=0)
    addon.start(addon_image)

    target.run()

    assert wait_for(
        lambda: target.exec_check("test", "-f", "/opt/rocs-payload"), timeout=15
    ), f"staged file not present. addon log:\n{addon.logs()}"
    assert wait_for(
        lambda: target.exec_capture("cat", "/opt/rocs-payload") == "patched\n",
        timeout=10,
    ), f"file not patched. content: {target.exec_capture('cat', '/opt/rocs-payload')!r}"


# ---------------------------------------------------------------------------
# debounce + skip
# ---------------------------------------------------------------------------


def test_debounce_suppresses_second_rapid_start(
    addon,
    addon_image,
    target,
    control_target,
    hooks_dir,
    options_path,
    write_options,
    wait_for,
):
    """A second ``start`` event within the debounce window is skipped.

    Uses ``control_target`` started AFTER the debounced restart as the
    synchronization signal — once the control's hook has fired we know
    the addon has drained the events queue up through the restart, so
    the assertion on ``target``'s state is meaningful without coupling
    to any addon log wording.
    """
    target_name = target.name
    _drop_post_start_script(
        hooks_dir,
        target_name,
        "00-count.sh",
        "#!/bin/sh\nprintf x >> /tmp/rocs-count\n",
    )
    # Per-container debounce=0 on the control so its start always fires.
    _drop_post_start_script(
        hooks_dir,
        control_target.name,
        "00-ready.sh",
        "#!/bin/sh\ntouch /tmp/rocs-ctrl-ready\n",
    )
    write_options(
        options_path,
        debounce_seconds=10,
        container_overrides=[{"container": control_target.name, "debounce_seconds": 0}],
    )
    addon.start(addon_image)

    target.run()
    assert wait_for(
        lambda: target.exec_check("test", "-s", "/tmp/rocs-count"), timeout=15
    ), f"first hook never ran. addon log:\n{addon.logs()}"
    # Restart the target inside the debounce window; the second start
    # event should be suppressed. ``-t 0`` SIGKILLs immediately so the
    # restart-then-start sequence fits inside the 10 s window — alpine
    # with ``sleep`` ignores SIGTERM, so without -t 0 the default 10 s
    # graceful timeout would exceed the debounce_seconds=10 ceiling.
    import subprocess

    subprocess.run(
        ["docker", "restart", "-t", "0", target_name],
        check=True,
        capture_output=True,
    )

    # Positive control: starting after the restart, its marker proves
    # the addon has processed events up through the restart.
    control_target.run()
    assert wait_for(
        lambda: control_target.exec_check("test", "-f", "/tmp/rocs-ctrl-ready"),
        timeout=15,
    ), f"control hook never ran; addon log:\n{addon.logs()}"

    # And the debounced target's script should have run exactly once.
    assert target.exec_capture("cat", "/tmp/rocs-count") == "x"


def test_skip_containers_skips_listed_target(
    addon,
    addon_image,
    target,
    control_target,
    hooks_dir,
    options_path,
    write_options,
    wait_for,
):
    """Names in ``skip_containers`` get no dispatch.

    Uses ``control_target`` started AFTER the skipped target as the
    synchronization signal — once the control's hook has fired we know
    the addon has observed and processed both start events, so it's
    safe to assert the skipped one's marker file does not exist.
    """
    target_name = target.name
    _drop_post_start_script(
        hooks_dir,
        target_name,
        "00-marker.sh",
        "#!/bin/sh\ntouch /tmp/rocs-should-not-exist\n",
    )
    _drop_post_start_script(
        hooks_dir,
        control_target.name,
        "00-ready.sh",
        "#!/bin/sh\ntouch /tmp/rocs-ctrl-ready\n",
    )
    write_options(
        options_path,
        skip_containers=[target_name],
        debounce_seconds=0,
    )
    addon.start(addon_image)

    target.run()
    control_target.run()

    # Positive control: control's hook running proves both start events
    # have been processed by the addon.
    assert wait_for(
        lambda: control_target.exec_check("test", "-f", "/tmp/rocs-ctrl-ready"),
        timeout=15,
    ), f"control hook never ran; addon log:\n{addon.logs()}"
    assert not target.exec_check("test", "-f", "/tmp/rocs-should-not-exist")
