#!/usr/bin/env python3
"""
Validate KB entries against schema and data integrity rules.

Checks:
  - Required fields per entry type (from kb.yaml)
  - FIPS codes are 5 digits
  - State abbreviations are valid 2-letter codes
  - Titles are non-empty
  - source_url present for auto-ingested entries
  - No duplicate entry IDs

Usage:
    python validate_entries.py                # full scan, report only
    python validate_entries.py --strict       # exit 1 on any error (for CI/hooks)
    python validate_entries.py --files f1 f2  # validate specific files (for pre-commit)
    python validate_entries.py --fix          # auto-fix where possible
"""

import argparse
import re
import sys
from pathlib import Path

KB_PATH = Path(__file__).parent.parent

VALID_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","GU","HI","ID",
    "IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT",
    "NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","PR","RI",
    "SC","SD","TN","TX","UT","VT","VA","VI","WA","WV","WI","WY","CU","MP",
    "US",  # national/remote entries
}

FIPS_PATTERN = re.compile(r"^\d{5}$")

# Required fields by entry type (from kb.yaml)
REQUIRED_FIELDS = {
    "commission-activity": ["title", "county", "state", "fips"],
    "budget-distress": ["title", "county", "state", "fips"],
    "real-estate-trace": ["title", "county", "state", "fips"],
    "job-posting": ["title", "state"],
    "sheriff-network": ["title", "county", "state", "fips"],
    "anc-contract": ["title", "county", "state", "fips"],
    "comms-discipline": ["title", "county", "state", "fips"],
    "287g-agreement": ["title", "state"],
    "legislative-trace": ["title", "state"],
    "igsa": ["title", "state"],
    "facility": ["title", "state"],
    "contractor": ["title"],
    "person": ["title"],
    "organization": ["title"],
    "county-fight": ["title", "state"],
    "financial-flow": ["title"],
    "analysis": ["title"],
    "contract": ["title"],
    "event": ["title"],
    "note": ["title"],
}

# Entry types that should have source_url (auto-ingested)
SHOULD_HAVE_SOURCE_URL = {
    "igsa", "287g-agreement", "anc-contract", "budget-distress",
    "commission-activity", "job-posting",
}

# Known source URLs by entry type
SOURCE_URLS = {
    "igsa": "https://github.com/vera-institute/ice-detention-trends",
    "287g-agreement": "https://www.prisonpolicy.org/blog/2026/02/23/ice_county_collaboration/",
    "anc-contract": "https://www.usaspending.gov",
    "budget-distress": "https://www.ers.usda.gov/data-products/county-typology-codes/",
    "commission-activity": "https://webapi.legistar.com",
}


def parse_frontmatter(filepath):
    """Parse YAML frontmatter from a markdown file. Returns (fields_dict, raw_text)."""
    text = filepath.read_text(encoding="utf-8")
    if not text.startswith("---"):
        return None, text
    try:
        end = text.index("---", 3)
    except ValueError:
        return None, text

    fields = {}
    for line in text[3:end].split("\n"):
        line = line.strip()
        if not line or line.startswith("-") or line.startswith("#"):
            continue
        if ":" in line:
            key, val = line.split(":", 1)
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key and val:
                fields[key] = val
    return fields, text


def validate_entry(filepath, fields):
    """Validate a single entry. Returns list of (severity, message) tuples."""
    errors = []

    entry_type = fields.get("type", "")
    entry_id = fields.get("id", filepath.stem)

    # Check required fields
    required = REQUIRED_FIELDS.get(entry_type, ["title"])
    for field in required:
        if field not in fields or not fields[field]:
            errors.append(("ERROR", f"missing required field: {field}"))

    # Validate FIPS
    fips = fields.get("fips", "")
    if fips and not FIPS_PATTERN.match(fips):
        errors.append(("ERROR", f"invalid FIPS code: {fips!r} (must be 5 digits)"))

    # Validate state
    state = fields.get("state", "")
    if state and state not in VALID_STATES:
        errors.append(("ERROR", f"invalid state: {state!r}"))
    if state and len(state) > 2:
        errors.append(("ERROR", f"state not abbreviated: {state!r}"))

    # Check source_url for auto-ingested types
    if entry_type in SHOULD_HAVE_SOURCE_URL:
        if "source_url" not in fields:
            errors.append(("WARN", f"missing source_url (type: {entry_type})"))

    # Check title not empty
    if not fields.get("title"):
        errors.append(("ERROR", "empty title"))

    return errors


def fix_entry(filepath, fields, text):
    """Auto-fix issues where possible. Returns (fixed_text, fixes_applied)."""
    fixes = []
    entry_type = fields.get("type", "")

    # Fix missing source_url
    if entry_type in SHOULD_HAVE_SOURCE_URL and "source_url" not in fields:
        url = SOURCE_URLS.get(entry_type)
        if url:
            # Insert source_url before the closing ---
            end = text.index("---", 3)
            text = text[:end] + f'source_url: "{url}"\n' + text[end:]
            fixes.append(f"added source_url: {url}")

    return text, fixes


def main():
    parser = argparse.ArgumentParser(description="Validate KB entries")
    parser.add_argument("--strict", action="store_true", help="Exit 1 on any error")
    parser.add_argument("--fix", action="store_true", help="Auto-fix where possible")
    parser.add_argument("--files", nargs="*", help="Validate specific files (for pre-commit)")
    parser.add_argument("--quiet", action="store_true", help="Only show errors")
    args = parser.parse_args()

    if args.files:
        files = [Path(f) for f in args.files if f.endswith(".md")]
    else:
        files = sorted(KB_PATH.rglob("*.md"))
        # Exclude non-entry files
        files = [f for f in files if f.name != "kb.yaml" and "/scripts/" not in str(f)]

    total = 0
    error_count = 0
    warn_count = 0
    fixed_count = 0
    ids_seen = {}

    for filepath in files:
        fields, text = parse_frontmatter(filepath)
        if fields is None:
            continue

        total += 1
        entry_id = fields.get("id", filepath.stem)

        # Check for duplicate IDs
        if entry_id in ids_seen:
            errors = [("ERROR", f"duplicate ID (also in {ids_seen[entry_id]})")]
        else:
            ids_seen[entry_id] = str(filepath.relative_to(KB_PATH)) if filepath.is_relative_to(KB_PATH) else str(filepath)
            errors = validate_entry(filepath, fields)

        if errors:
            for severity, msg in errors:
                if severity == "ERROR":
                    error_count += 1
                else:
                    warn_count += 1
                if not args.quiet or severity == "ERROR":
                    rel = filepath.relative_to(KB_PATH) if filepath.is_relative_to(KB_PATH) else filepath
                    print(f"  [{severity}] {rel}: {msg}", flush=True)

        # Auto-fix
        if args.fix and fields:
            new_text, fixes = fix_entry(filepath, fields, text)
            if fixes:
                filepath.write_text(new_text, encoding="utf-8")
                fixed_count += 1
                for fix in fixes:
                    rel = filepath.relative_to(KB_PATH) if filepath.is_relative_to(KB_PATH) else filepath
                    print(f"  [FIXED] {rel}: {fix}", flush=True)

    print(f"\nValidated {total} entries: {error_count} errors, {warn_count} warnings"
          + (f", {fixed_count} fixed" if args.fix else ""), flush=True)

    if args.strict and error_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
