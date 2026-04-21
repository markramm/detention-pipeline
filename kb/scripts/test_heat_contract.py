#!/usr/bin/env python3
"""
Data-contract test for heat_data.json.

This is the file produced by build.sh (county_heat_score.py) and consumed by:
  - hugo/layouts/map_page/single.html (D3 visualization)
  - hugo/generate_content.py (copied to hugo/static/ + hugo/data/)
  - kb/scripts/ingest_diff.py (gainers/losers table in PR body)

Every consumer assumes a specific shape. This script validates that shape so
schema drift fails CI instead of silently breaking the site.

Usage:
    python3 test_heat_contract.py                            # defaults
    python3 test_heat_contract.py --path docs/heat_data.json # explicit
    python3 test_heat_contract.py --strict                   # fail on warnings too

Exit codes:
    0  contract satisfied
    1  contract violated
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent.parent

# Fields every row must have with the expected Python type. Missing -> ERROR.
REQUIRED_FIELDS: dict[str, type | tuple[type, ...]] = {
    "fips": str,
    "county": str,
    "state": str,
    "score": int,
    "signal_types": int,
    "signals": dict,
}

FIPS_RE = re.compile(r"^\d{5}$")
STATE_RE = re.compile(r"^[A-Z]{2}$")

# Known consumers assume these invariants. Tightening them later should break
# CI so we remember to update the consumers too.
MIN_ROWS = 100              # sanity — we've had 1000+ counties for months
MAX_SCORE_SEEN = 500        # current max is ~194; 500 leaves headroom
SIGNAL_ENTRY_KEYS = {"count", "entries", "propagated"}


def validate_row(row: dict, idx: int) -> list[tuple[str, str]]:
    """Return list of (severity, message) for one row."""
    issues: list[tuple[str, str]] = []

    for key, typ in REQUIRED_FIELDS.items():
        if key not in row:
            issues.append(("ERROR", f"row[{idx}] missing required field '{key}'"))
            continue
        if not isinstance(row[key], typ):
            issues.append(
                ("ERROR", f"row[{idx}] field '{key}' is {type(row[key]).__name__}, expected {typ.__name__}")
            )

    fips = row.get("fips", "")
    if isinstance(fips, str) and not FIPS_RE.match(fips):
        issues.append(("ERROR", f"row[{idx}] fips={fips!r} is not 5 digits"))

    state = row.get("state", "")
    if isinstance(state, str) and state and not STATE_RE.match(state):
        issues.append(("ERROR", f"row[{idx}] state={state!r} is not 2 uppercase letters"))

    score = row.get("score")
    if isinstance(score, int):
        if score < 0:
            issues.append(("ERROR", f"row[{idx}] score={score} is negative"))
        if score > MAX_SCORE_SEEN:
            issues.append(("WARN", f"row[{idx}] score={score} exceeds MAX_SCORE_SEEN={MAX_SCORE_SEEN}"))

    # signals is dict[signal_type] -> {count, entries, [propagated]}
    signals = row.get("signals")
    if isinstance(signals, dict):
        signal_types_count = row.get("signal_types")
        if isinstance(signal_types_count, int) and signal_types_count != len(signals):
            issues.append(
                ("WARN", f"row[{idx}] signal_types={signal_types_count} != len(signals)={len(signals)}")
            )

        for stype, detail in signals.items():
            if not isinstance(detail, dict):
                issues.append(("ERROR", f"row[{idx}] signals[{stype!r}] is not a dict"))
                continue
            missing = {"count", "entries"} - set(detail.keys())
            if missing:
                issues.append(
                    ("ERROR", f"row[{idx}] signals[{stype!r}] missing {sorted(missing)}")
                )
            extra = set(detail.keys()) - SIGNAL_ENTRY_KEYS
            if extra:
                issues.append(
                    ("ERROR", f"row[{idx}] signals[{stype!r}] has unexpected keys {sorted(extra)}")
                )
            if "count" in detail and not isinstance(detail["count"], int):
                issues.append(
                    ("ERROR", f"row[{idx}] signals[{stype!r}].count is not int")
                )
            if "entries" in detail and not isinstance(detail["entries"], list):
                issues.append(
                    ("ERROR", f"row[{idx}] signals[{stype!r}].entries is not list")
                )
            if "propagated" in detail and not isinstance(detail["propagated"], int):
                issues.append(
                    ("ERROR", f"row[{idx}] signals[{stype!r}].propagated is not int")
                )

    return issues


def validate(rows: list, strict: bool) -> int:
    """Validate the whole file. Return number of ERRORs (+WARNs if strict)."""
    if not isinstance(rows, list):
        print(f"ERROR: heat_data.json root is {type(rows).__name__}, expected list")
        return 1

    if len(rows) < MIN_ROWS:
        print(f"ERROR: only {len(rows)} rows (< MIN_ROWS={MIN_ROWS}) — ingest likely broken")
        return 1

    # Global checks
    error_count = 0
    warn_count = 0
    seen_fips: dict[str, int] = {}

    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            print(f"ERROR: row[{idx}] is {type(row).__name__}, expected dict")
            error_count += 1
            continue

        fips = row.get("fips")
        if isinstance(fips, str):
            if fips in seen_fips:
                print(f"ERROR: duplicate fips {fips!r} at rows {seen_fips[fips]} and {idx}")
                error_count += 1
            else:
                seen_fips[fips] = idx

        for severity, msg in validate_row(row, idx):
            print(f"{severity}: {msg}")
            if severity == "ERROR":
                error_count += 1
            else:
                warn_count += 1

    # Ordering: build.sh sorts by -score. Verify.
    scores = [r.get("score", 0) for r in rows if isinstance(r, dict)]
    if scores != sorted(scores, reverse=True):
        print("ERROR: rows are not sorted by score descending")
        error_count += 1

    print()
    print(f"Validated {len(rows)} rows: {error_count} errors, {warn_count} warnings")
    print(f"Score range: {min(scores)}..{max(scores)}")
    print(f"Unique FIPS: {len(seen_fips)}")

    return error_count + (warn_count if strict else 0)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--path", default=str(REPO_ROOT / "docs" / "heat_data.json"))
    p.add_argument("--strict", action="store_true", help="Fail on warnings too")
    args = p.parse_args()

    path = Path(args.path)
    if not path.exists():
        print(f"ERROR: {path} does not exist")
        return 1

    with open(path) as f:
        data = json.load(f)

    return 1 if validate(data, args.strict) else 0


if __name__ == "__main__":
    sys.exit(main())
