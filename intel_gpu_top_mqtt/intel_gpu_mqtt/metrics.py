# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Build per-sensor metric dicts from an intel_gpu_top JSON sample."""

from typing import Any

from .util import dig, find_engine_field, safe_float


def build_metrics(raw: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Return metrics dict keyed by sensor key with fields:
    - value (numeric or None)
    - unit
    - attrs (dict)
    - name (human name)
    """
    common_attrs: dict[str, Any] = {}

    for k in ["pci_id", "device", "driver", "card", "gt"]:
        v = raw.get(k)
        if v is not None and isinstance(v, (str, int, float)):
            common_attrs[k] = v

    def metric(
        key: str,
        name: str,
        value: float | None,
        unit: str,
        extra_attrs: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        attrs = dict(common_attrs)
        if extra_attrs:
            attrs.update(extra_attrs)
        return {"key": key, "name": name, "value": value, "unit": unit, "attrs": attrs}

    # Note: intel_gpu_top JSON schema varies a bit by version.
    # On your system, power keys are capitalized: power.GPU and power.Package.
    rc6 = safe_float(dig(raw, ["rc6", "value"])) or safe_float(raw.get("rc6"))
    freq_actual = safe_float(dig(raw, ["frequency", "actual"]))
    freq_requested = safe_float(dig(raw, ["frequency", "requested"]))

    p_gpu = safe_float(dig(raw, ["power", "GPU"]))
    if p_gpu is None:
        p_gpu = safe_float(dig(raw, ["power", "gpu"]))

    p_pkg = safe_float(dig(raw, ["power", "Package"]))
    if p_pkg is None:
        p_pkg = safe_float(dig(raw, ["power", "pkg"]))
    if p_pkg is None:
        p_pkg = safe_float(dig(raw, ["power", "package"]))

    metrics: dict[str, dict[str, Any]] = {
        "rc6_percent": metric("rc6_percent", "Intel GPU RC6", rc6, "%"),
        "freq_mhz": metric(
            "freq_mhz", "Intel GPU Frequency Actual", freq_actual, "MHz"
        ),
        "freq_requested_mhz": metric(
            "freq_requested_mhz", "Intel GPU Frequency Requested", freq_requested, "MHz"
        ),
        "interrupts_per_s": metric(
            "interrupts_per_s",
            "Intel GPU Interrupts",
            safe_float(dig(raw, ["interrupts", "count"])),
            "irq/s",
        ),
        "power_gpu_w": metric("power_gpu_w", "Intel GPU Power", p_gpu, "W"),
        "power_pkg_w": metric("power_pkg_w", "Intel Package Power", p_pkg, "W"),
        # Render/3D
        "engine_render_3d_busy_percent": metric(
            "engine_render_3d_busy_percent",
            "Intel GPU Engine Render/3D Busy",
            find_engine_field(raw, "Render/3D", "busy"),
            "%",
            {"engine": "Render/3D", "field": "busy"},
        ),
        "engine_render_3d_semaphore_percent": metric(
            "engine_render_3d_semaphore_percent",
            "Intel GPU Engine Render/3D Semaphore",
            find_engine_field(raw, "Render/3D", "sema"),
            "%",
            {"engine": "Render/3D", "field": "sema"},
        ),
        "engine_render_3d_wait_percent": metric(
            "engine_render_3d_wait_percent",
            "Intel GPU Engine Render/3D Wait",
            find_engine_field(raw, "Render/3D", "wait"),
            "%",
            {"engine": "Render/3D", "field": "wait"},
        ),
        # Video
        "engine_video_busy_percent": metric(
            "engine_video_busy_percent",
            "Intel GPU Engine Video Busy",
            find_engine_field(raw, "Video", "busy"),
            "%",
            {"engine": "Video", "field": "busy"},
        ),
        "engine_video_semaphore_percent": metric(
            "engine_video_semaphore_percent",
            "Intel GPU Engine Video Semaphore",
            find_engine_field(raw, "Video", "sema"),
            "%",
            {"engine": "Video", "field": "sema"},
        ),
        "engine_video_wait_percent": metric(
            "engine_video_wait_percent",
            "Intel GPU Engine Video Wait",
            find_engine_field(raw, "Video", "wait"),
            "%",
            {"engine": "Video", "field": "wait"},
        ),
        # VideoEnhance
        "engine_videoenhance_busy_percent": metric(
            "engine_videoenhance_busy_percent",
            "Intel GPU Engine VideoEnhance Busy",
            find_engine_field(raw, "VideoEnhance", "busy"),
            "%",
            {"engine": "VideoEnhance", "field": "busy"},
        ),
        "engine_videoenhance_semaphore_percent": metric(
            "engine_videoenhance_semaphore_percent",
            "Intel GPU Engine VideoEnhance Semaphore",
            find_engine_field(raw, "VideoEnhance", "sema"),
            "%",
            {"engine": "VideoEnhance", "field": "sema"},
        ),
        "engine_videoenhance_wait_percent": metric(
            "engine_videoenhance_wait_percent",
            "Intel GPU Engine VideoEnhance Wait",
            find_engine_field(raw, "VideoEnhance", "wait"),
            "%",
            {"engine": "VideoEnhance", "field": "wait"},
        ),
        # Blitter
        "engine_blitter_busy_percent": metric(
            "engine_blitter_busy_percent",
            "Intel GPU Engine Blitter Busy",
            find_engine_field(raw, "Blitter", "busy"),
            "%",
            {"engine": "Blitter", "field": "busy"},
        ),
        "engine_blitter_semaphore_percent": metric(
            "engine_blitter_semaphore_percent",
            "Intel GPU Engine Blitter Semaphore",
            find_engine_field(raw, "Blitter", "sema"),
            "%",
            {"engine": "Blitter", "field": "sema"},
        ),
        "engine_blitter_wait_percent": metric(
            "engine_blitter_wait_percent",
            "Intel GPU Engine Blitter Wait",
            find_engine_field(raw, "Blitter", "wait"),
            "%",
            {"engine": "Blitter", "field": "wait"},
        ),
    }

    return metrics
