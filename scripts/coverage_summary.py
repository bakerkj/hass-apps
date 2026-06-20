# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Render the combined coverage report as Markdown for CI.

Reads a coverage JSON report (``coverage json``) from the path given as the
first argument (default ``coverage.json``) and prints the per-module roll-up
as a visible table, followed by the full per-file breakdown in a collapsed
``<details>`` block:

    ## Coverage
    ### By module
    <per-module table>
    <details> per-file table </details>

The per-file rows and every TOTAL reuse coverage's own ``percent_covered_display``
so they match ``coverage report`` exactly; per-module percentages (which coverage
does not compute) are derived from the summed statement counts.
"""

import json
import sys
from pathlib import Path


def _esc(name: str) -> str:
    """Escape underscores so names like ``__init__.py`` are not rendered as
    bold/italic inside a GitHub-flavored Markdown table (matches coverage)."""
    return name.replace("_", r"\_")


def _row(cells: list[str], *, bold: bool = False) -> str:
    if bold:
        cells = [f"**{c}**" for c in cells]
    return "| " + " | ".join(cells) + " |"


def _data_row(name: str, stmts: int, miss: int, pct: str) -> str:
    return _row([_esc(name), str(stmts), str(miss), f"{pct}%"])


def _total_row(stmts: int, miss: int, pct: str) -> str:
    return _row(["TOTAL", str(stmts), str(miss), f"{pct}%"], bold=True)


def _table(header: str, body: list[str], total: str) -> list[str]:
    return [
        _row([header, "Stmts", "Miss", "Cover"]),
        "| --- | ---: | ---: | ---: |",
        *body,
        total,
    ]


def main() -> None:
    report = Path(sys.argv[1] if len(sys.argv) > 1 else "coverage.json")
    data = json.loads(report.read_text())
    files = data["files"]
    totals = data["totals"]

    overall = _total_row(
        totals["num_statements"],
        totals["missing_lines"],
        totals["percent_covered_display"],
    )

    # Per-file rows reuse coverage's own display percentage.
    file_body: list[str] = []
    # Per-module aggregation by top-level add-on directory.
    modules: dict[str, list[int]] = {}
    for path in sorted(files):
        summary = files[path]["summary"]
        stmts = summary["num_statements"]
        miss = summary["missing_lines"]
        file_body.append(
            _data_row(path, stmts, miss, summary["percent_covered_display"])
        )
        agg = modules.setdefault(Path(path).parts[0], [0, 0])
        agg[0] += stmts
        agg[1] += summary["covered_lines"]

    module_body: list[str] = []
    for name in sorted(modules):
        stmts, covered = modules[name]
        pct = "100" if stmts == 0 else str(round(100 * covered / stmts))
        module_body.append(_data_row(name, stmts, stmts - covered, pct))

    lines = ["## Coverage", ""]
    # Per-module roll-up, shown expanded as the headline.
    lines += ["### By module", ""]
    lines += _table("Module", module_body, overall)
    # Full per-file breakdown, collapsed by default (blank lines around the
    # table so GitHub renders it inside the <details>).
    lines += ["", "<details><summary>By file</summary>", ""]
    lines += _table("Name", file_body, overall)
    lines += ["", "</details>"]
    print("\n".join(lines))


if __name__ == "__main__":
    main()
