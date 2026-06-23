# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Turbostat column → HA sensor metadata (friendly names, units, icons),
plus the curated set of columns we treat as load-bearing."""

# Columns that should appear on every supported platform — turbostat without
# any of these is a strong signal of an upstream rename or kernel regression.
# Deliberately conservative: GFX/ACPI/POLL/CPU%c* are platform-conditional
# (iGPU presence, BIOS C-state exposure, CPUidle config) so they're not
# enforced — only entered here when a column's absence would always be a bug.
EXPECTED_COLS: frozenset[str] = frozenset(
    {
        "PkgWatt",
        "CorWatt",
        "Busy%",
        "Bzy_MHz",
        "Avg_MHz",
        "TSC_MHz",
        "IPC",
        "LLCkRPS",
        "LLC%hit",
        "L2kRPS",
        "L2%hit",
    }
)


# Source-name → (canonical-name, scale-to-canonical-unit) for the columns
# whose upstream naming or unit drifted. The canonical name keeps the
# historical HA entity ID stable across the rename — even though the slug
# still says "kRPS" the published unit is mega/s (see guess_meta override
# below); the slug is name continuity, the unit is honest display.
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

# Override the rps-suffix default ("1/s") for these columns: the value scale
# above lands in mega-refs/sec, so the displayed unit must match.
COLUMN_UNIT_OVERRIDES: dict[str, str] = {
    "LLCkRPS": "M/s",
    "L2kRPS": "M/s",
}


def missing_expected_columns(header: list[str]) -> list[str]:
    """Members of ``EXPECTED_COLS`` that aren't in this header. A non-empty
    result is how we catch upstream silently dropping a column we publish."""
    return sorted(EXPECTED_COLS - set(header))


def friendly_name(col: str) -> str:
    replacements = {
        "PkgWatt": "CPU Package Power",
        "CorWatt": "CPU Cores Power",
        "GFXWatt": "CPU iGPU Power",
        "RAMWatt": "CPU DRAM Power",
        "PkgTmp": "CPU Package Temperature",
        "Busy%": "CPU Busy",
        "CPU%": "CPU Busy",
        "GFX%": "CPU iGPU Busy",
        "CoreTmp": "CPU Core Temperature",
        "Bzy_MHz": "CPU Busy Frequency",
        "Avg_MHz": "CPU Average Frequency",
        "TSC_MHz": "CPU Time Stamp Counter Frequency",
        "Totl%C0": "CPU Total C0 (Active)",
        "Any%C0": "CPU Any Core C0 (Active)",
        "GFX%C0": "GPU C0 (Active)",
        "CPUGFX%": "CPU+GPU C0 (Active)",
        "Pkg%pc2": "CPU Package C2 Residency",
        "Pkg%pc3": "CPU Package C3 Residency",
        "Pkg%pc6": "CPU Package C6 Residency",
        "Pkg%pc7": "CPU Package C7 Residency",
        "Pkg%pc8": "CPU Package C8 Residency",
        "Pkg%pc9": "CPU Package C9 Residency",
        "Pkg%pc10": "CPU Package C10 Residency",
        "Pk%pc10": "CPU Package C10 Residency",
        "C1ACPI%": "ACPI C1 Residency",
        "C2ACPI%": "ACPI C2 Residency",
        "C3ACPI%": "ACPI C3 Residency",
        "CPU%c1": "CPU C1 Residency",
        "CPU%c6": "CPU C6 Residency",
        "CPU%c7": "CPU C7 Residency",
        "CPU%LPI": "CPU Low Power Idle Residency",
        "SYS%LPI": "System Low Power Idle Residency",
        "GFX%rc6": "GPU RC6 Residency",
        "GFXAMHz": "GPU Frequency (Actual)",
        "GFXMHz": "GPU Frequency (Requested)",
        "IPC": "Instructions per Cycle",
        "LLCkRPS": "CPU Last-Level Cache References",
        "LLC%hi": "CPU Last-Level Cache Hit Rate",
        "LLC%hit": "CPU Last-Level Cache Hit Rate",
        "L2kRPS": "CPU L2 Cache References",
        "L2%hit": "CPU L2 Cache Hit Rate",
        "IRQ": "Interrupt Rate",
        "NMI": "Non-maskable Interrupt Rate",
        "SMI": "System Management Interrupt Rate",
        "POLL%": "CPU Polling Time",
    }
    return replacements.get(col, f"Turbostat {col}")


def guess_meta(original_col: str) -> tuple[str | None, str | None, str, int]:
    col = original_col.strip()

    override = COLUMN_UNIT_OVERRIDES.get(col)
    if override is not None:
        return override, None, "mdi:chart-line", 2

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
