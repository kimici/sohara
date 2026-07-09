#!/usr/bin/env python3
"""Check that no Rust function exceeds 30 lines (excluding blank lines and comments)."""

import re
import sys
from pathlib import Path

MAX_FN_LINES = 30
FN_RE = re.compile(
    r"^\s*(pub\s+)?(async\s+)?fn\s+\w+",
)

def count_fn_lines(lines: list[str], start: int) -> tuple[int, str]:
    """Count non-blank, non-comment lines of a function starting at `start`."""
    depth = 0
    count = 0
    name = lines[start].strip()
    for line in lines[start:]:
        stripped = line.strip()
        if not stripped or stripped.startswith("//"):
            # Still count the line for brace tracking but skip in body count
            depth += stripped.count("{") - stripped.count("}")
            if depth <= 0 and count > 0:
                break
            continue
        depth += stripped.count("{") - stripped.count("}")
        count += 1
        if depth <= 0 and count > 1:
            break
    return count, name


def check_file(path: Path) -> list[str]:
    violations = []
    lines = path.read_text().splitlines()
    i = 0
    while i < len(lines):
        if FN_RE.match(lines[i]):
            count, name = count_fn_lines(lines, i)
            if count > MAX_FN_LINES:
                violations.append(
                    f"  {path}:{i+1} — {name[:60]} ({count} lines, max {MAX_FN_LINES})"
                )
        i += 1
    return violations


def main():
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")
    all_violations = []
    for rs in root.rglob("*.rs"):
        if "target" in rs.parts:
            continue
        all_violations.extend(check_file(rs))

    if all_violations:
        print(f"FAIL: {len(all_violations)} function(s) exceed {MAX_FN_LINES} lines:")
        for v in all_violations:
            print(v)
        sys.exit(1)
    else:
        print(f"OK: All Rust functions are within {MAX_FN_LINES} lines.")


if __name__ == "__main__":
    main()
