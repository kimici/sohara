#!/usr/bin/env bash
# Check that no Rust source file exceeds 300 lines.
# Usage: ./scripts/check-file-length.sh [directory]
# Exit code 0 = all files OK, 1 = violations found.

set -euo pipefail

DIR="${1:-.}"
MAX_LINES=300
EXIT_CODE=0

while IFS= read -r -d '' file; do
    lines=$(wc -l < "$file")
    if (( lines > MAX_LINES )); then
        echo "FAIL: $file ($lines lines, max $MAX_LINES)"
        EXIT_CODE=1
    fi
done < <(find "$DIR" -name '*.rs' -not -path '*/target/*' -print0)

if (( EXIT_CODE == 0 )); then
    echo "OK: All Rust files are within $MAX_LINES lines."
fi

exit "$EXIT_CODE"
