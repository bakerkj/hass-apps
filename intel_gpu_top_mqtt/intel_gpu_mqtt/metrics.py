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
    # Explicit None-check, not `or`: rc6 == 0.0 (GPU 100 % busy) is falsy,
    # and the `or` fallback then reads raw["rc6"] as a dict on the nested
    # schema, which safe_float rejects -> None. That would blank both
    # rc6_percent and non_idle_percent at exactly the "GPU is pegged" moment.
    rc6 = safe_float(dig(raw, ["rc6", "value"]))
    if rc6 is None:
        rc6 = safe_float(raw.get("rc6"))
    freq_actual = safe_float(dig(raw, ["frequency", "actual"]))
    freq_requested = safe_float(dig(raw, ["frequency", "requested"]))

    render_busy = find_engine_field(raw, "Render/3D", "busy")
    video_busy = find_engine_field(raw, "Video", "busy")
    videoenhance_busy = find_engine_field(raw, "VideoEnhance", "busy")
    blitter_busy = find_engine_field(raw, "Blitter", "busy")

    # Non-idle: complement of the RC6 (render C6, deepest idle) residency.
    # Hardware-authoritative, never exceeds 100 %, and is the closest thing
    # intel_gpu_top publishes to "was the GPU out of its lowest power state?".
    non_idle = 100.0 - rc6 if rc6 is not None else None

    # Peak engine busy: max across the four engines intel_gpu_top reports.
    # 100 % here means at least one engine is the bottleneck for a serialized
    # workload, which is usually the useful "the GPU is full" signal.
    engine_busy_values = [
        v
        for v in (render_busy, video_busy, videoenhance_busy, blitter_busy)
        if v is not None
    ]
    peak_engine_busy = max(engine_busy_values) if engine_busy_values else None

    # QSV-style: mean of the Render/3D and Video engine busy%, matching
    # Frigate 0.17's get_intel_gpu_stats which averaged those two engines
    # (used by QSV encode and decode) into a single gpu_load number.
    if render_busy is not None and video_busy is not None:
        qsv_style = (render_busy + video_busy) / 2.0
    else:
        qsv_style = None

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
        # Aggregate "busy" proxies. Multiple, because there is no single true
        # answer for a multi-engine iGPU:
        #   non_idle_percent       -- hardware idle-residency complement (100 - RC6)
        #   peak_engine_busy_percent -- max across engines, i.e. bottleneck engine
        #   qsv_style_load_percent -- mean(Render/3D, Video) busy, matches the
        #                             number Frigate 0.17 published as gpu_load
        #                             for intel-qsv (dropped in Frigate 0.18)
        "non_idle_percent": metric(
            "non_idle_percent", "Intel GPU Non-Idle", non_idle, "%"
        ),
        "peak_engine_busy_percent": metric(
            "peak_engine_busy_percent",
            "Intel GPU Peak Engine Busy",
            peak_engine_busy,
            "%",
        ),
        "qsv_style_load_percent": metric(
            "qsv_style_load_percent",
            "Intel GPU QSV-Style Load",
            qsv_style,
            "%",
        ),
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
