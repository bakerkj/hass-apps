# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Render a per-module coverage summary as a Markdown table.

coverage.py reports per file; the CI `coverage` job rolls those up to one row
per top-level add-on directory (the "module"), which is the headline shown in
the GitHub step summary and the PR comment. Reads a coverage JSON report
(``coverage json``) from the path given as the first argument, defaulting to
``coverage.json``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def main() -> None:
    report_path = Path(sys.argv[1] if len(sys.argv) > 1 else "coverage.json")
    files = json.loads(report_path.read_text())["files"]

    # module name -> [num_statements, covered_lines]
    modules: dict[str, list[int]] = {}
    for path, info in files.items():
        module = Path(path).parts[0]
        summary = info["summary"]
        agg = modules.setdefault(module, [0, 0])
        agg[0] += summary["num_statements"]
        agg[1] += summary["covered_lines"]

    def row(name: str, stmts: int, covered: int, *, bold: bool = False) -> str:
        miss = stmts - covered
        pct = 100 if stmts == 0 else round(100 * covered / stmts)
        cells = [name, str(stmts), str(miss), f"{pct}%"]
        if bold:
            cells = [f"**{c}**" for c in cells]
        return "| " + " | ".join(cells) + " |"

    lines = ["| Module | Stmts | Miss | Cover |", "| --- | ---: | ---: | ---: |"]
    total_stmts = total_covered = 0
    for name in sorted(modules):
        stmts, covered = modules[name]
        total_stmts += stmts
        total_covered += covered
        lines.append(row(name, stmts, covered))
    lines.append(row("TOTAL", total_stmts, total_covered, bold=True))
    print("\n".join(lines))


if __name__ == "__main__":
    main()
