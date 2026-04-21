#!/usr/bin/env python3
"""
Convert staged ingest JSON files into markdown entries in kb/<signal>/.

Replaces the Pyrite `kb import` step in CI contexts where Pyrite isn't
available. Takes the JSON that ingest scripts already emit (list of dicts
with entry_type, title, body, plus structured fields) and writes one .md
per entry with YAML frontmatter.

Usage:
    python3 json_to_entries.py <json_file> [<json_file> ...]
    python3 json_to_entries.py /tmp/commission_items.json /tmp/287g_agreements.json
"""

import argparse
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from schema import load_schema

KB_ROOT = Path(__file__).parent.parent
ENTRY_TYPE_TO_DIR = load_schema().subdirectories()

# Fields that belong in frontmatter (in this order) when present.
# Other keys from the ingest JSON get dropped unless listed here — matches
# the practical shape of entries Pyrite has been producing.
FRONTMATTER_FIELDS = [
    "type", "county", "state", "fips",
    "agency", "agency_type", "model", "signed_date",
    "contractor", "contractor_type", "contract_class", "contract_value",
    "contract_type", "award_date", "usaspending_id",
    "employer", "position_title", "location", "posting_date", "posting_url",
    "shortfall_amount", "tax_action", "population_trend",
    "address", "sqft", "owner", "owner_type", "property_type", "status",
    "sheriff_name", "conference", "indicator_type", "speaker",
    "bill_number", "bill_title", "sponsor", "effect",
    "source", "source_url", "signal_strength", "notes",
]

SLUG_MAX = 100


def slugify(text):
    """Match Pyrite's slug convention so CI-created entries don't collide
    with entries previously created via `kb import`."""
    text = text.lower()
    text = re.sub(r"[\u2014\u2013\u2212]", "-", text)  # em/en/minus dash
    text = re.sub(r"[\u2018\u2019\u201c\u201d]", "", text)  # curly quotes
    text = text.replace("$", "")  # currency sign stripped outright
    # Every non-alphanumeric becomes a separator (hyphen / apostrophe /
    # parens / colon / comma / period all collapse here).
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")[:SLUG_MAX].rstrip("-")


def yaml_escape(value):
    """Quote a scalar value for YAML frontmatter."""
    if value is None:
        return '""'
    if isinstance(value, (int, float)):
        return str(value)
    s = str(value)
    # Always single-quote; double internal single-quotes per YAML spec
    return "'" + s.replace("'", "''") + "'"


def render_frontmatter(entry):
    """Return list of frontmatter lines (without the --- delimiters)."""
    lines = []
    entry_type = entry.get("entry_type") or entry.get("type") or "note"
    entry_id = entry.get("id") or slugify(entry.get("title", "untitled"))
    title = entry.get("title", "")
    tags = entry.get("tags") or [entry_type]
    importance = entry.get("importance", 5)

    lines.append(f"id: {entry_id}")
    lines.append(f"title: {yaml_escape(title)}")
    lines.append(f"type: {entry_type}")

    for key in FRONTMATTER_FIELDS:
        if key in ("type",):
            continue
        if key not in entry:
            continue
        val = entry[key]
        if val is None or val == "":
            continue
        lines.append(f"{key}: {yaml_escape(val)}")

    lines.append("tags:")
    for t in tags:
        lines.append(f"- {t}")
    lines.append(f"importance: {importance}")
    return lines, entry_id, entry_type


def write_entry(entry, dry_run=False):
    entry_type = entry.get("entry_type") or entry.get("type") or "note"
    subdir = ENTRY_TYPE_TO_DIR.get(entry_type)
    if not subdir:
        print(f"  SKIP: unknown entry_type {entry_type!r}", file=sys.stderr)
        return None

    fm_lines, entry_id, _ = render_frontmatter(entry)
    body = entry.get("body", "").rstrip()

    content = "---\n" + "\n".join(fm_lines) + "\n---\n\n" + body + "\n"

    out_dir = KB_ROOT / subdir
    out_path = out_dir / f"{entry_id}.md"

    if dry_run:
        status = "UPDATE" if out_path.exists() else "CREATE"
        print(f"  [{status}] {out_path.relative_to(KB_ROOT)}")
        return out_path

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path.write_text(content, encoding="utf-8")
    return out_path


def main():
    p = argparse.ArgumentParser(description="Convert staged ingest JSON to KB entries")
    p.add_argument("files", nargs="+", help="JSON files produced by ingest scripts")
    p.add_argument("--dry-run", action="store_true", help="Preview without writing")
    args = p.parse_args()

    total_created = 0
    total_skipped = 0
    for json_file in args.files:
        path = Path(json_file)
        if not path.exists():
            print(f"  MISS: {json_file}", file=sys.stderr)
            continue
        try:
            entries = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            print(f"  BAD JSON: {json_file}: {e}", file=sys.stderr)
            continue
        if not isinstance(entries, list):
            print(f"  BAD SHAPE: {json_file}: expected list", file=sys.stderr)
            continue

        print(f"── {path.name}: {len(entries)} entries ──")
        for entry in entries:
            result = write_entry(entry, dry_run=args.dry_run)
            if result:
                total_created += 1
            else:
                total_skipped += 1

    print(f"\nWrote {total_created} entries, skipped {total_skipped}")
    return 0 if total_skipped == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
