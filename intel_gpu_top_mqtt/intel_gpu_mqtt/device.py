# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Intel GPU device enumeration + subprocess management for intel_gpu_top."""

import logging
import re
import subprocess


def list_intel_gpu_top_devices(log: logging.Logger) -> str:
    """Return stdout of `intel_gpu_top -L` (or empty string)."""
    try:
        out = subprocess.check_output(
            ["intel_gpu_top", "-L"], text=True, stderr=subprocess.STDOUT, timeout=5
        )
        return out
    except (OSError, subprocess.SubprocessError) as e:
        log.warning("Failed to list devices with intel_gpu_top -L: %s", e)
        return ""


def auto_select_device_arg(
    device_listing: str, preferred_regex: str, log: logging.Logger
) -> tuple[str | None, str | None]:
    """Pick a -d argument for intel_gpu_top based on `intel_gpu_top -L` output.

    Returns:
      (device_arg, render_node_path)
      e.g. ("drm:/dev/dri/renderD128", "/dev/dri/renderD128")
    """
    lines = [ln.strip() for ln in device_listing.splitlines() if ln.strip()]
    render_candidates: list[tuple[str, str]] = []
    for ln in lines:
        m = re.search(r"(/dev/dri/renderD\d+)", ln)
        if m:
            render_candidates.append((ln, m.group(1)))

    if not render_candidates:
        return None, None

    if preferred_regex:
        try:
            rx = re.compile(preferred_regex, re.IGNORECASE)
            for ln, path in render_candidates:
                if rx.search(ln) or rx.search(path):
                    log.info(
                        "Auto-selected device by regex '%s': %s", preferred_regex, ln
                    )
                    return f"drm:{path}", path
        except re.error as e:
            log.warning("Invalid preferred_device_regex '%s': %s", preferred_regex, e)

    ln, path = render_candidates[0]
    log.info("Auto-selected first available device: %s", ln)
    return f"drm:{path}", path


def start_intel_gpu_top(
    interval_ms: int,
    dev_arg: str | None,
    log: logging.Logger,
) -> subprocess.Popen:
    cmd = ["intel_gpu_top", "-J", "-s", str(interval_ms), "-o", "-"]
    if dev_arg:
        cmd += ["-d", dev_arg]
    log.info("Starting: %s", " ".join(cmd))
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,  # line-buffered in text mode
        )
    except FileNotFoundError:
        log.error("intel_gpu_top not found in container; check package install.")
        raise
    log.info("intel_gpu_top process started pid=%s", proc.pid)
    return proc
