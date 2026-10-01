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


# entry_id -> subdir, for every entry already in a signal directory. Built
# lazily once per run. An ID must be unique across the whole KB (the
# validator enforces it), so a CREATE whose ID already lives in another
# directory is a duplicate record of the same award, not a new one: the
# existing entry wins and the new one is skipped. Without this, two award
# IDs re-emitted under kb/ice-contracts/ that already exist under kb/anc/
# failed validation and rolled back every weekly ingest from 2026-07-14.
_ID_DIRS = None


def _id_dirs():
    global _ID_DIRS
    if _ID_DIRS is None:
        _ID_DIRS = {}
        for subdir in set(ENTRY_TYPE_TO_DIR.values()):
            d = KB_ROOT / subdir
            if d.is_dir():
                for f in d.glob("*.md"):
                    _ID_DIRS.setdefault(f.stem, subdir)
    return _ID_DIRS


# Keys this converter itself writes. An existing entry carrying any other
# frontmatter key was enriched by hand (e.g. parent_idv, idv_ceiling from a
# verified USAspending pull) and is left alone: the ingest source knows less
# about that record than the entry does.
_GENERATED_KEYS = set(FRONTMATTER_FIELDS) | {"id", "title", "type", "tags", "importance"}

# Location keys the source may lack (USAspending gives ICE contracts a state,
# not a county) but an earlier pass resolved. Kept on update when the new
# record does not supply them, so a re-ingest cannot drop an entry out of
# its county's heat score.
_PRESERVE_IF_MISSING = ("county", "fips")

# Fields worth carrying over when a cross-directory duplicate is resolved by
# migrating the record, so neither copy's enrichment is lost.
_MIGRATE_MERGE_FIELDS = (
    "county", "fips", "contract_value", "award_date", "usaspending_id", "notes",
)


def _migrate_to_subdir(entry_id, old_dir, old_text, new_subdir, new_entry):
    """Resolve a cross-directory duplicate by moving the record to
    `new_subdir`: merge any `_MIGRATE_MERGE_FIELDS` the old copy has that the
    new one lacks, write the new copy, and delete the old one. Returns the
    new path."""
    old_fields = _existing_frontmatter(old_text)
    merged = dict(new_entry)
    for key in _MIGRATE_MERGE_FIELDS:
        if not merged.get(key) and old_fields.get(key):
            merged[key] = old_fields[key]

    fm_lines, _, _ = render_frontmatter(merged)
    body = merged.get("body", "").rstrip()
    content = "---\n" + "\n".join(fm_lines) + "\n---\n\n" + body + "\n"

    new_dir = KB_ROOT / new_subdir
    new_dir.mkdir(parents=True, exist_ok=True)
    new_path = new_dir / f"{entry_id}.md"
    new_path.write_text(content, encoding="utf-8")
    (KB_ROOT / old_dir / f"{entry_id}.md").unlink()
    if _ID_DIRS is not None:
        _ID_DIRS[entry_id] = new_subdir
    return new_path


def _existing_frontmatter(text):
    if not text.startswith("---\n"):
        return {}
    end = text.find("\n---", 4)
    if end < 0:
        return {}
    try:
        import yaml
        data = yaml.safe_load(text[4:end])
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def write_entry(entry, dry_run=False, stats=None):
    entry_type = entry.get("entry_type") or entry.get("type") or "note"
    subdir = ENTRY_TYPE_TO_DIR.get(entry_type)
    if not subdir:
        print(f"  SKIP: unknown entry_type {entry_type!r}", file=sys.stderr)
        return None

    _, entry_id, _ = render_frontmatter(entry)
    out_dir = KB_ROOT / subdir
    out_path = out_dir / f"{entry_id}.md"
    existing = out_path.read_text(encoding="utf-8") if out_path.exists() else None

    if existing is not None:
        old_fm = _existing_frontmatter(existing)
        if set(old_fm) - _GENERATED_KEYS:
            if stats is not None:
                stats["curated"] = stats.get("curated", 0) + 1
            return out_path
        # Read the raw scalar, not the YAML value: an unquoted FIPS like
        # 05119 must keep its leading zero.
        missing = {}
        for k in _PRESERVE_IF_MISSING:
            m = re.search(rf"^{k}:[ \t]*(.+?)[ \t]*$", existing.split("\n---", 1)[0], re.M)
            raw = m.group(1).strip("'\"") if m else ""
            if raw and entry.get(k) in (None, ""):
                missing[k] = raw
        if missing:
            entry = {**entry, **missing}

    fm_lines, entry_id, _ = render_frontmatter(entry)
    body = entry.get("body", "").rstrip()

    content = "---\n" + "\n".join(fm_lines) + "\n---\n\n" + body + "\n"

    # Skip writes when the file exists with identical content. This keeps
    # re-ingest cheap (no mtime churn) and the CI diff small (only genuine
    # changes land in the weekly PR).
    if existing == content:
        if stats is not None:
            stats["unchanged"] += 1
        return out_path

    if existing is None:
        other = _id_dirs().get(entry_id)
        if other is not None and other != subdir:
            # The April cleanup's convention: a contractor classified 'anc'
            # (an Alaska Native Corporation) lives in anc/; every other
            # contractor (private-prison, security, transport, 'other', ...)
            # lives in ice-contracts/. A record ingested before that
            # convention existed can still be sitting in anc/ under the
            # wrong classification; when the new record identifies itself as
            # a non-ANC contractor arriving in ice-contracts/, resolve
            # toward ice-contracts/ instead of leaving it stuck wherever it
            # happened to be created first.
            contractor_type = entry.get("contractor_type")
            if subdir == "ice-contracts" and other == "anc" and contractor_type not in (None, "", "anc"):
                old_path = KB_ROOT / other / f"{entry_id}.md"
                old_text = old_path.read_text(encoding="utf-8")
                if stats is not None:
                    stats["migrated"] = stats.get("migrated", 0) + 1
                print(f"  MIGRATE: {entry_id} from {other}/ to {subdir}/ "
                      f"(contractor_type={contractor_type!r})", file=sys.stderr)
                if dry_run:
                    return old_path
                return _migrate_to_subdir(entry_id, other, old_text, subdir, entry)

            if stats is not None:
                stats["duplicate"] = stats.get("duplicate", 0) + 1
            print(f"  DUPLICATE: {entry_id} already in {other}/, not creating in {subdir}/",
                  file=sys.stderr)
            return KB_ROOT / other / f"{entry_id}.md"

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

    stats = {"created": 0, "updated": 0, "unchanged": 0, "unroutable": 0, "duplicate": 0,
             "curated": 0, "migrated": 0}
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
        f"{stats['unchanged']} unchanged, {stats['unroutable']} unroutable, "
        f"{stats['duplicate']} skipped as cross-directory duplicates, "
        f"{stats['migrated']} migrated to their correct directory, "
        f"{stats['curated']} hand-curated left as is "
        f"(touched {total_touched}/{total_touched + stats['unchanged']})"
    )
    return 0 if stats["unroutable"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
