# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Turbostat column → HA sensor metadata.

Single source of truth for every column we handle explicitly: display name,
unit, device class, icon, decimal precision, and the orthogonal opt-in
flags (``expected`` / ``diagnostic`` / ``count``). Public frozensets are
derived views over this table so a new column is a one-line addition,
not a four-file drift risk.

Unknown columns still land in ``guess_meta`` heuristics (endswith-``Watt``
→ ``W``, contains ``%`` → ``%``, ...) so turbostat can add a new column
tomorrow — on a new kernel or a new CPU — and get sensible defaults with
no code change.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class ColMeta:
    """Everything we know about one turbostat column, in one place."""

    name: str
    unit: str | None = None
    device_class: str | None = None
    icon: str = "mdi:chart-line"
    sdp: int = 2
    # Column absence is a strong signal of an upstream rename or kernel
    # regression on any supported platform.
    expected: bool = False
    # Published but ``enabled_by_default: false`` in HA — opt-in per sensor.
    diagnostic: bool = False
    # Per-interval integer count; render with 0-decimal precision.
    count: bool = False


# fmt: off
COLUMNS: dict[str, ColMeta] = {
    # Power
    "PkgWatt":   ColMeta("CPU Package Power", unit="W", device_class="power", icon="mdi:flash",  sdp=1, expected=True),
    "CorWatt":   ColMeta("CPU Cores Power",   unit="W", device_class="power", icon="mdi:flash",  sdp=1, expected=True),
    "GFXWatt":   ColMeta("CPU iGPU Power",    unit="W", device_class="power", icon="mdi:flash",  sdp=1),
    "RAMWatt":   ColMeta("CPU DRAM Power",    unit="W", device_class="power", icon="mdi:flash",  sdp=1),

    # Temperature
    "PkgTmp":    ColMeta("CPU Package Temperature", unit="°C", device_class="temperature", icon="mdi:thermometer", sdp=0),
    "CoreTmp":   ColMeta("CPU Core Temperature",    unit="°C", device_class="temperature", icon="mdi:thermometer", sdp=0),

    # Busy / activity
    "Busy%":     ColMeta("CPU Busy",              unit="%", icon="mdi:percent", sdp=1, expected=True),
    "CPU%":      ColMeta("CPU Busy",              unit="%", icon="mdi:percent", sdp=1),
    "GFX%":      ColMeta("CPU iGPU Busy",         unit="%", icon="mdi:percent", sdp=1),
    "Totl%C0":   ColMeta("CPU Total C0 (Active)", unit="%", icon="mdi:percent", sdp=1),
    "Any%C0":    ColMeta("CPU Any Core C0 (Active)", unit="%", icon="mdi:percent", sdp=1),
    "GFX%C0":    ColMeta("GPU C0 (Active)",       unit="%", icon="mdi:percent", sdp=1),
    "CPUGFX%":   ColMeta("CPU+GPU C0 (Active)",   unit="%", icon="mdi:percent", sdp=1),

    # Frequency
    "Bzy_MHz":   ColMeta("CPU Busy Frequency",              unit="MHz", device_class="frequency", icon="mdi:sine-wave", sdp=0, expected=True),
    "Avg_MHz":   ColMeta("CPU Average Frequency",           unit="MHz", device_class="frequency", icon="mdi:sine-wave", sdp=0, expected=True),
    "TSC_MHz":   ColMeta("CPU Time Stamp Counter Frequency", unit="MHz", device_class="frequency", icon="mdi:sine-wave", sdp=0, expected=True),
    "GFXAMHz":   ColMeta("GPU Frequency (Actual)",          unit="MHz", device_class="frequency", icon="mdi:sine-wave", sdp=0),
    "GFXMHz":    ColMeta("GPU Frequency (Requested)",       unit="MHz", device_class="frequency", icon="mdi:sine-wave", sdp=0),

    # Package C-states
    "Pkg%pc2":   ColMeta("CPU Package C2 Residency",  unit="%", icon="mdi:percent", sdp=1),
    "Pkg%pc3":   ColMeta("CPU Package C3 Residency",  unit="%", icon="mdi:percent", sdp=1),
    "Pkg%pc6":   ColMeta("CPU Package C6 Residency",  unit="%", icon="mdi:percent", sdp=1),
    "Pkg%pc7":   ColMeta("CPU Package C7 Residency",  unit="%", icon="mdi:percent", sdp=1),
    "Pkg%pc8":   ColMeta("CPU Package C8 Residency",  unit="%", icon="mdi:percent", sdp=1),
    "Pkg%pc9":   ColMeta("CPU Package C9 Residency",  unit="%", icon="mdi:percent", sdp=1),
    "Pkg%pc10":  ColMeta("CPU Package C10 Residency", unit="%", icon="mdi:percent", sdp=1),
    "Pk%pc10":   ColMeta("CPU Package C10 Residency", unit="%", icon="mdi:percent", sdp=1),

    # ACPI C-state residencies
    "C1ACPI%":   ColMeta("ACPI C1 Residency", unit="%", icon="mdi:percent", sdp=1),
    "C2ACPI%":   ColMeta("ACPI C2 Residency", unit="%", icon="mdi:percent", sdp=1),
    "C3ACPI%":   ColMeta("ACPI C3 Residency", unit="%", icon="mdi:percent", sdp=1),

    # CPU-level C-state residencies
    "CPU%c1":    ColMeta("CPU C1 Residency", unit="%", icon="mdi:percent", sdp=1),
    "CPU%c6":    ColMeta("CPU C6 Residency", unit="%", icon="mdi:percent", sdp=1),
    "CPU%c7":    ColMeta("CPU C7 Residency", unit="%", icon="mdi:percent", sdp=1),

    # Low-power idle
    "CPU%LPI":   ColMeta("CPU Low Power Idle Residency",    unit="%", icon="mdi:percent", sdp=1),
    "SYS%LPI":   ColMeta("System Low Power Idle Residency", unit="%", icon="mdi:percent", sdp=1),

    # GPU
    "GFX%rc6":   ColMeta("GPU RC6 Residency", unit="%", icon="mdi:percent", sdp=1),

    # Instructions per cycle
    "IPC":       ColMeta("Instructions per Cycle", sdp=2, expected=True),

    # LLC / L2 cache. Unit is M/s because the parser scales k-refs/sec
    # (or native M-refs/sec on newer turbostat, see COLUMN_RENAMES) into
    # mega-refs/sec before publish. sdp=2 keeps the ~decimal.dd precision
    # that made the k→M conversion legible.
    "LLCkRPS":   ColMeta("CPU Last-Level Cache References", unit="M/s", sdp=2, expected=True),
    "LLC%hi":    ColMeta("CPU Last-Level Cache Hit Rate",   unit="%", icon="mdi:percent", sdp=1),
    "LLC%hit":   ColMeta("CPU Last-Level Cache Hit Rate",   unit="%", icon="mdi:percent", sdp=1, expected=True),
    "L2kRPS":    ColMeta("CPU L2 Cache References",         unit="M/s", sdp=2, expected=True),
    "L2%hit":    ColMeta("CPU L2 Cache Hit Rate",           unit="%", icon="mdi:percent", sdp=1, expected=True),

    # Interrupt counters (skipped from publish today in app.py, but named
    # here so guess_meta gives them a friendly label if they're ever surfaced).
    "IRQ":       ColMeta("Interrupt Rate",                sdp=0),
    "NMI":       ColMeta("Non-maskable Interrupt Rate",   sdp=0),
    "SMI":       ColMeta("System Management Interrupt Rate", sdp=0),

    # POLL residency and cpuidle diagnostic counters. POLL/C{n}ACPI base +
    # ``-``/``+`` mispredict counters are new in turbostat 2026.04.21
    # (Linux 7.1). Each raw counter is a per-interval integer count;
    # published disabled-by-default so operators can opt in for cpuidle
    # tuning without cluttering everyone else's entity list.
    "POLL%":     ColMeta("CPU Polling Time",                     unit="%", icon="mdi:percent", sdp=1),
    "POLL":      ColMeta("CPU POLL Entries",                     icon="mdi:counter", sdp=0, diagnostic=True, count=True),
    "POLL-":     ColMeta("CPU POLL Over-Sleep",                  icon="mdi:counter", sdp=0, diagnostic=True, count=True),
    "C1ACPI":    ColMeta("CPU ACPI C1 Entries",                  icon="mdi:counter", sdp=0, diagnostic=True, count=True),
    "C1ACPI-":   ColMeta("CPU ACPI C1 Over-Sleep",               icon="mdi:counter", sdp=0, diagnostic=True, count=True),
    "C1ACPI+":   ColMeta("CPU ACPI C1 Under-Sleep",              icon="mdi:counter", sdp=0, diagnostic=True, count=True),
    "C2ACPI":    ColMeta("CPU ACPI C2 Entries",                  icon="mdi:counter", sdp=0, diagnostic=True, count=True),
    "C2ACPI-":   ColMeta("CPU ACPI C2 Over-Sleep",               icon="mdi:counter", sdp=0, diagnostic=True, count=True),
    "C2ACPI+":   ColMeta("CPU ACPI C2 Under-Sleep",              icon="mdi:counter", sdp=0, diagnostic=True, count=True),
    "C3ACPI":    ColMeta("CPU ACPI C3 Entries",                  icon="mdi:counter", sdp=0, diagnostic=True, count=True),
    "C3ACPI-":   ColMeta("CPU ACPI C3 Over-Sleep",               icon="mdi:counter", sdp=0, diagnostic=True, count=True),
    "C3ACPI+":   ColMeta("CPU ACPI C3 Under-Sleep",              icon="mdi:counter", sdp=0, diagnostic=True, count=True),
}
# fmt: on


# Derived views. Always in sync with COLUMNS — no drift risk between a
# frozenset and a friendly_name dict.
EXPECTED_COLS: frozenset[str] = frozenset(c for c, m in COLUMNS.items() if m.expected)
DIAGNOSTIC_COLS: frozenset[str] = frozenset(
    c for c, m in COLUMNS.items() if m.diagnostic
)
COUNT_COLS: frozenset[str] = frozenset(c for c, m in COLUMNS.items() if m.count)


# Source-name → (canonical-name, scale-to-canonical-unit) for columns whose
# upstream naming or unit drifted. The canonical name keeps the historical
# HA entity ID stable across the rename — even though the slug still says
# "kRPS" the published unit is mega/s (see the M/s override on LLCkRPS/L2kRPS
# above); the slug is name continuity, the unit is honest display.
#   - LLCMRPS native (new turbostat): name → LLCkRPS, value already M/s → ×1
#   - LLCkRPS native (old turbostat): name unchanged, value k/s → ÷1000 → M/s
# Both inputs converge on canonical name + M/s scale, so the publisher needs
# zero unit/scale logic — the parser delivers the right number.
COLUMN_RENAMES: dict[str, tuple[str, float]] = {
    "LLCMRPS": ("LLCkRPS", 1.0),
    "LLCkRPS": ("LLCkRPS", 0.001),
    "L2MRPS": ("L2kRPS", 1.0),
    "L2kRPS": ("L2kRPS", 0.001),
}


def missing_expected_columns(header: list[str]) -> list[str]:
    """Members of ``EXPECTED_COLS`` that aren't in this header. A non-empty
    result is how we catch upstream silently dropping a column we publish."""
    return sorted(EXPECTED_COLS - set(header))


def friendly_name(col: str) -> str:
    m = COLUMNS.get(col)
    return m.name if m is not None else f"Turbostat {col}"


def guess_meta(original_col: str) -> tuple[str | None, str | None, str, int]:
    col = original_col.strip()

    m = COLUMNS.get(col)
    if m is not None:
        return m.unit, m.device_class, m.icon, m.sdp

    # Heuristic fallback for columns not in the explicit table. Turbostat
    # emits ~200 possible columns depending on kernel/CPU; enumerating each
    # would be fragile. These patterns cover the common suffix conventions
    # so a new column (e.g. ``UMHz`` on Granite Rapids) still publishes
    # with a sensible unit on day zero without a code change.

    if "%" in col or col in ("CPU%", "GFX%"):
        return "%", None, "mdi:percent", 1

    if col.lower().endswith("tmp") or "temp" in col.lower():
        return "°C", "temperature", "mdi:thermometer", 0

    if "mhz" in col.lower():
        return "MHz", "frequency", "mdi:sine-wave", 0

    if "watt" in col.lower():
        return "W", "power", "mdi:flash", 1

    if col.lower().endswith("_j") or col.lower().endswith("j"):
        return "J", None, "mdi:counter", 0

    if col.lower().endswith("rps") or "/s" in col.lower() or col.lower().endswith("_s"):
        return "1/s", None, "mdi:chart-line", 0

    if col.lower() in {"sec", "seconds"} or col.lower().endswith("sec"):
        return "s", None, "mdi:timer-outline", 1

    if "irq" in col.lower():
        return None, None, "mdi:chart-line", 0

    return None, None, "mdi:chart-line", 2
