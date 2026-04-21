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

# Pyrite's max slug length — entries previously imported through Pyrite
# have slugs up to ~180 chars. Match so re-ingest doesn't create a
# shorter-slug duplicate of an existing longer-slug file.
SLUG_MAX = 200


def slugify(text):
    """Match Pyrite's slug convention so CI-created entries don't collide
    with entries previously created via `kb import`."""
    text = text.lower()
    text = re.sub(r"[\u2014\u2013\u2212]", "-", text)  # em/en/minus dash
    # Curly *double* quotes get stripped (Pyrite behaviour). Curly single
    # quotes / apostrophes are replaced with ' so they collapse to the
    # same separator the straight apostrophe does — otherwise Prison
    # Policy's "Sheriff's" (with U+2019) yields sheriffs-office while
    # the existing entries have sheriff-s-office.
    text = re.sub(r"[\u201c\u201d]", "", text)      # curly double quotes
    text = re.sub(r"[\u2018\u2019]", "'", text)     # curly single -> straight
    text = text.replace("$", "")  # currency sign stripped outright
    # Every non-alphanumeric becomes a separator (hyphen / apostrophe /
    # parens / colon / comma / period all collapse here).
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")[:SLUG_MAX].rstrip("-")


def stable_slug(entry: dict) -> str | None:
    """Compute a stable slug for entry types whose title contains volatile
    data (scores, dollar amounts). Returns None if no stable form applies,
    letting the caller fall back to title-based slugging.

    Rules:
      budget-distress  -> <county>-<state>-usda-distress   (drop score)
      anc-contract     -> <recipient>-<award-id>           (drop amount/location)
      ice-contract     -> <recipient>-<award-id>           (drop amount/state)
    """
    etype = entry.get("entry_type") or entry.get("type")

    if etype == "budget-distress":
        county = (entry.get("county") or "").strip()
        state = (entry.get("state") or "").strip()
        if county and state:
            return slugify(f"{county} {state}") + "-usda-distress"
        return None

    if etype in ("anc-contract", "ice-contract"):
        award_id = entry.get("usaspending_id") or entry.get("award_id") or ""
        contractor = entry.get("contractor") or ""
        if award_id and contractor:
            return f"{slugify(contractor)}-{slugify(award_id)}"
        return None

    return None


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
    # Prefer explicit id, then a stable per-type slug (so URLs don't churn
    # when scores recompute or contract amounts are modified on USAspending),
    # then fall back to slugifying the title.
    entry_id = (
        entry.get("id")
        or stable_slug(entry)
        or slugify(entry.get("title", "untitled"))
    )
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


def write_entry(entry, dry_run=False, stats=None):
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

    # Skip writes when the file exists with identical content. This keeps
    # re-ingest cheap (no mtime churn) and the CI diff small (only genuine
    # changes land in the weekly PR).
    existing = out_path.read_text(encoding="utf-8") if out_path.exists() else None
    if existing == content:
        if stats is not None:
            stats["unchanged"] += 1
        return out_path

    status = "UPDATE" if existing is not None else "CREATE"
    if stats is not None:
        stats["created" if existing is None else "updated"] += 1

    if dry_run:
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

    stats = {"created": 0, "updated": 0, "unchanged": 0, "unroutable": 0}
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
            result = write_entry(entry, dry_run=args.dry_run, stats=stats)
            if result is None:
                stats["unroutable"] += 1

    total_touched = stats["created"] + stats["updated"]
    print(
        f"\n{stats['created']} created, {stats['updated']} updated, "
        f"{stats['unchanged']} unchanged, {stats['unroutable']} unroutable "
        f"(touched {total_touched}/{total_touched + stats['unchanged']})"
    )
    return 0 if stats["unroutable"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
