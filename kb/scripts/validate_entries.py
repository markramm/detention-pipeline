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

sys.path.insert(0, str(Path(__file__).parent))
from frontmatter import parse as parse_frontmatter_yaml
from schema import load_schema

KB_PATH = Path(__file__).parent.parent
SCHEMA = load_schema()

VALID_STATES = {
    "AL","AK","AS","AZ","AR","CA","CO","CT","DE","DC","FL","GA","GU","HI","ID",
    "IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT",
    "NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","PR","RI",
    "SC","SD","TN","TX","UT","VT","VA","VI","WA","WV","WI","WY","CU","MP",
    "US",  # national/remote entries
}

FIPS_PATTERN = re.compile(r"^\d{5}$")

REQUIRED_FIELDS = SCHEMA.required_fields()
SHOULD_HAVE_SOURCE_URL = SCHEMA.source_url_required()
SOURCE_URLS = SCHEMA.source_url_defaults()


def parse_frontmatter(filepath):
    """Parse YAML frontmatter via the shared yaml.safe_load-based parser.
    Returns (fields_dict, raw_text) to keep the historical call-shape."""
    text = filepath.read_text(encoding="utf-8")
    parsed = parse_frontmatter_yaml(text)
    if parsed is None:
        return None, text
    # Strip scalar-empty entries so validation "field missing" checks behave
    # the same way they did with the old hand-rolled parser.
    fields = {k: v for k, v in parsed.fields.items()
              if v != "" or isinstance(v, (list, dict))}
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


def default_files(kb_path=None):
    """Every entry file under kb/ that a full scan covers."""
    kb_path = Path(kb_path) if kb_path else KB_PATH
    files = sorted(kb_path.rglob("*.md"))
    # Exclude non-entry files
    return [f for f in files if f.name != "kb.yaml" and "/scripts/" not in str(f)]


def _rel(filepath, kb_path):
    return filepath.relative_to(kb_path) if filepath.is_relative_to(kb_path) else filepath


def scan(files=None, kb_path=None):
    """Validate `files` (default: the whole KB). Yields
    (filepath, fields, text, [(severity, message), ...]) for every entry
    file with frontmatter, in scan order. The duplicate-ID check depends on
    that order: the second file to carry an ID is the one flagged, and its
    message names the first ("also in <path relative to kb/>")."""
    kb_path = Path(kb_path) if kb_path else KB_PATH
    if files is None:
        files = default_files(kb_path)
    ids_seen = {}
    for filepath in files:
        fields, text = parse_frontmatter(filepath)
        if fields is None:
            continue
        entry_id = fields.get("id", filepath.stem)
        if entry_id in ids_seen:
            errors = [("ERROR", f"duplicate ID (also in {ids_seen[entry_id]})")]
        else:
            ids_seen[entry_id] = str(_rel(filepath, kb_path))
            errors = validate_entry(filepath, fields)
        yield filepath, fields, text, errors


def error_records(kb_path=None):
    """Every ERROR in a full scan, as sorted [path-relative-to-kb, message]
    pairs. The ingest gate compares two of these (before and after a batch)
    to tell the batch's errors from the ones already on HEAD."""
    kb_path = Path(kb_path) if kb_path else KB_PATH
    out = []
    for filepath, _fields, _text, errors in scan(kb_path=kb_path):
        for severity, msg in errors:
            if severity == "ERROR":
                out.append([str(_rel(filepath, kb_path)), msg])
    return sorted(out)


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
        files = default_files()

    total = 0
    error_count = 0
    warn_count = 0
    fixed_count = 0

    for filepath, fields, text, errors in scan(files):
        total += 1

        if errors:
            for severity, msg in errors:
                if severity == "ERROR":
                    error_count += 1
                else:
                    warn_count += 1
                if not args.quiet or severity == "ERROR":
                    print(f"  [{severity}] {_rel(filepath, KB_PATH)}: {msg}", flush=True)

        # Auto-fix
        if args.fix and fields:
            new_text, fixes = fix_entry(filepath, fields, text)
            if fixes:
                filepath.write_text(new_text, encoding="utf-8")
                fixed_count += 1
                for fix in fixes:
                    print(f"  [FIXED] {_rel(filepath, KB_PATH)}: {fix}", flush=True)

    print(f"\nValidated {total} entries: {error_count} errors, {warn_count} warnings"
          + (f", {fixed_count} fixed" if args.fix else ""), flush=True)

    if args.strict and error_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
