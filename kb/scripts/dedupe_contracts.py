#!/usr/bin/env python3
"""
Dedupe contract entries appearing in both kb/anc/ and kb/ice-contracts/.

Root cause: ingest_usaspending.py (ANC broad net) and ingest_ice_contracts.py
(ICE-specific) both ingested many of the same contracts. With stable slugs
now enforced, these collide — same <recipient>-<award-id>.

Resolution per pair:
  - If the contractor is classified as 'anc' in ice-contracts/ (i.e. the
    ICE ingester tagged them ANC), keep anc/ version and delete ice/.
  - Otherwise (private-prison, security, etc.) keep ice-contracts/ and
    delete anc/. Merge any unique frontmatter fields (county, fips,
    contract_value, award_date, usaspending_id, notes) from the anc/
    copy into the ice-contracts/ copy before deleting.

Also handles within-anc duplicates (same award filed twice in anc/ with
different filename formats — old Pyrite output vs. backfill output).

Usage:
    python3 dedupe_contracts.py --dry-run     # preview
    python3 dedupe_contracts.py               # apply
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

KB_ROOT = Path(__file__).parent.parent

ANC_DIR = KB_ROOT / "anc"
ICE_DIR = KB_ROOT / "ice-contracts"


def parse_frontmatter(text: str):
    """Return (fields_dict, body_str, fm_end_index) or (None, text, -1)."""
    if not text.startswith("---"):
        return None, text, -1
    try:
        end = text.index("\n---", 3)
    except ValueError:
        return None, text, -1
    fields = {}
    for line in text[4:end].split("\n"):
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or stripped.startswith("-"):
            continue
        if ":" in stripped:
            k, v = stripped.split(":", 1)
            v = v.strip().strip('"').strip("'")
            if k.strip() and v:
                fields[k.strip()] = v
    body = text[end + 4:].lstrip("\n")
    return fields, body, end


def extract_award_id(fields: dict, body: str) -> str | None:
    v = (fields.get("usaspending_id") or "").strip()
    if v:
        return v.upper()
    for line in body.split("\n")[:40]:
        m = re.match(r"^Award ID:\s*(\S+)", line)
        if m:
            return m.group(1).upper()
    return None


def extract_tags(fm_raw: str) -> list[str]:
    """Pull the tags list from a raw frontmatter block (post-'tags:' lines)."""
    tags = []
    in_tags = False
    for line in fm_raw.split("\n"):
        if line.startswith("tags:"):
            in_tags = True
            continue
        if in_tags:
            if line.startswith("- "):
                tags.append(line[2:].strip().strip('"').strip("'"))
            elif line.strip() and not line.startswith(" "):
                break
    return tags


def classify_contractor(ice_path: Path) -> str:
    """Return the contractor_type tag from the ice-contracts/ entry (e.g. 'anc',
    'private-prison', 'air-operations'). Falls back to 'other'."""
    text = ice_path.read_text(encoding="utf-8")
    fm = text.split("---", 2)[1] if "---" in text else ""
    tags = extract_tags(fm)
    # ingest_ice_contracts.py tag order: ["ice-contract", contractor_type, contract_class, state]
    if len(tags) >= 2 and tags[0] == "ice-contract":
        return tags[1]
    return "other"


MERGE_FIELDS = [
    "county", "state", "fips",
    "contractor", "parent_anc", "contract_value", "contract_type",
    "award_date", "usaspending_id", "source", "signal_strength", "notes",
]


def merge_frontmatter_into(target_path: Path, source_fields: dict) -> bool:
    """Add any MERGE_FIELDS from source to target frontmatter that target
    doesn't already have. Returns True if target was modified."""
    text = target_path.read_text(encoding="utf-8")
    fields, _, fm_end = parse_frontmatter(text)
    if fields is None:
        return False

    added = []
    for key in MERGE_FIELDS:
        if key in fields and fields[key]:
            continue
        if key not in source_fields or not source_fields[key]:
            continue
        val = source_fields[key]
        added.append((key, val))

    if not added:
        return False

    insert_at = fm_end  # index of "\n---" closing delimiter
    insert_lines = ""
    for key, val in added:
        escaped = "'" + val.replace("'", "''") + "'"
        insert_lines += f"{key}: {escaped}\n"
    new_text = text[:insert_at] + "\n" + insert_lines.rstrip("\n") + text[insert_at:]
    target_path.write_text(new_text, encoding="utf-8")
    return True


def find_collisions():
    """Return dict: award_id -> {'anc': [paths], 'ice': [paths]}."""
    awards: dict[str, dict[str, list[Path]]] = {}

    for p in sorted(ANC_DIR.glob("*.md")):
        text = p.read_text(encoding="utf-8")
        fields, body, _ = parse_frontmatter(text)
        if fields is None:
            continue
        aid = extract_award_id(fields, body)
        if aid:
            awards.setdefault(aid, {"anc": [], "ice": []})["anc"].append(p)

    for p in sorted(ICE_DIR.glob("*.md")):
        text = p.read_text(encoding="utf-8")
        fields, body, _ = parse_frontmatter(text)
        if fields is None:
            continue
        aid = extract_award_id(fields, body)
        if aid:
            awards.setdefault(aid, {"anc": [], "ice": []})["ice"].append(p)

    return awards


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--samples", type=int, default=5)
    args = p.parse_args()

    awards = find_collisions()

    cross_dir = {a: fs for a, fs in awards.items()
                 if fs["anc"] and fs["ice"]}
    intra_anc = {a: fs["anc"] for a, fs in awards.items()
                 if len(fs["anc"]) > 1 and not fs["ice"]}

    print(f"Cross-dir collisions (anc/ vs ice-contracts/): {len(cross_dir)}")
    print(f"Within-anc duplicates:                          {len(intra_anc)}")

    # Resolve cross-dir
    kept_anc = 0
    kept_ice = 0
    merged = 0
    dry_samples = []
    for award, paths in sorted(cross_dir.items()):
        anc_path = paths["anc"][0]
        ice_path = paths["ice"][0]
        ctype = classify_contractor(ice_path)
        if ctype == "anc":
            # Keep anc/, delete ice/
            action = "keep_anc"
            kept_anc += 1
            target, to_delete = anc_path, ice_path
        else:
            # Keep ice/, merge frontmatter from anc/, delete anc/
            action = "keep_ice"
            kept_ice += 1
            target, to_delete = ice_path, anc_path

        if args.dry_run:
            if len(dry_samples) < args.samples:
                dry_samples.append((award, action, target, to_delete))
            continue

        # Merge richer fields from the doomed copy into the keeper.
        doomed_fields, _, _ = parse_frontmatter(to_delete.read_text(encoding="utf-8"))
        if doomed_fields and merge_frontmatter_into(target, doomed_fields):
            merged += 1
        to_delete.unlink()

    # Resolve within-anc: keep whichever has the stable slug format
    # (<contractor>-<awardid>, no '-ice-' infix, no trailing location bits).
    # The other format is the old Pyrite output.
    intra_resolved = 0
    for award, paths in sorted(intra_anc.items()):
        def is_stable(p):
            # Stable slug contains the lowercased award id and no "-ice-" infix.
            name = p.stem.lower()
            return award.lower() in name and "-ice-" not in name

        scored = [(1 if is_stable(p) else 0, p.name, p) for p in paths]
        scored.sort(reverse=True)
        keep = scored[0][2]
        dupes = [s[2] for s in scored[1:]]

        if args.dry_run:
            if len(dry_samples) < args.samples + 2:
                dry_samples.append((award, "keep_in_anc", keep, dupes[0]))
            continue

        for d in dupes:
            # Merge first, then delete
            doomed_fields, _, _ = parse_frontmatter(d.read_text(encoding="utf-8"))
            if doomed_fields:
                merge_frontmatter_into(keep, doomed_fields)
            d.unlink()
            intra_resolved += 1

    print()
    print(f"{'Would keep' if args.dry_run else 'Kept'} in anc/:          {kept_anc}")
    print(f"{'Would keep' if args.dry_run else 'Kept'} in ice-contracts/: {kept_ice}")
    print(f"{'Would merge frontmatter on' if args.dry_run else 'Merged frontmatter on'}: {merged}")
    print(f"{'Would drop intra-anc dupes:' if args.dry_run else 'Dropped intra-anc dupes:  '} {intra_resolved or len(intra_anc)}")

    if args.dry_run and dry_samples:
        print()
        print("── Samples ──")
        for award, action, target, doomed in dry_samples:
            print(f"  award={award}")
            print(f"    action: {action}")
            print(f"    keep:   {target.relative_to(KB_ROOT)}")
            print(f"    drop:   {doomed.relative_to(KB_ROOT)}")


if __name__ == "__main__":
    main()
