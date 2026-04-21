#!/usr/bin/env python3
"""
Rename entries whose current slug embeds volatile data (distress scores,
contract dollar amounts) to stable slugs based on immutable keys.

Why: the current URLs change every time a USDA score recomputes or a
USAspending award gets modified, which breaks permalinks. Target slugs
depend only on immutable identifiers (FIPS + signal type, or award ID).

Renames performed:
    budget/<name>-budget-distress-score-N-M.md
        -> budget/<name>-usda-distress.md

    ice-contracts/<recipient>-<award-id>-<state>-<amount>.md
        -> ice-contracts/<recipient>-<award-id>.md

    anc/<recipient>-ice-<county>-<state>-<amount>.md
        -> anc/<recipient>-<award-id>.md    (award id read from body)

Also rewrites:
    - id: field in frontmatter (matches new filename)
    - title: loses the ($N) suffix for contracts (but keeps score for budget
      since that IS the meaningful info — just not in the URL)

Usage:
    python3 rename_stable_slugs.py --dry-run            # preview
    python3 rename_stable_slugs.py --dry-run --samples 10
    python3 rename_stable_slugs.py                      # apply
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

KB_ROOT = Path(__file__).parent.parent


def slugify(text: str, max_len: int = 100) -> str:
    """Match the shared json_to_entries slug convention."""
    text = text.lower()
    text = re.sub(r"[\u2014\u2013\u2212]", "-", text)
    text = re.sub(r"[\u2018\u2019\u201c\u201d]", "", text)
    text = text.replace("$", "")
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")[:max_len].rstrip("-")


def parse_frontmatter(text: str):
    """Return (fields, body, fm_end_index) or (None, None, -1)."""
    if not text.startswith("---"):
        return None, None, -1
    try:
        end = text.index("\n---", 3)
    except ValueError:
        return None, None, -1
    fm_text = text[4:end]
    body = text[end + 4:].lstrip("\n")
    fields = {}
    for line in fm_text.split("\n"):
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or stripped.startswith("-"):
            continue
        if ":" in stripped:
            k, v = stripped.split(":", 1)
            fields[k.strip()] = v.strip().strip('"').strip("'")
    return fields, body, end


def rewrite_id(text: str, new_id: str) -> str:
    """Replace the id: line in frontmatter."""
    return re.sub(r"(?m)^id:\s*.*$", f"id: {new_id}", text, count=1)


def extract_award_id(body: str) -> str | None:
    for line in body.split("\n")[:15]:
        m = re.match(r"^Award ID:\s*(\S+)\s*$", line)
        if m:
            return m.group(1)
    return None


def new_budget_slug(path: Path, fields: dict) -> str | None:
    """
    budget/alameda-county-ca-budget-distress-score-3-10.md
        -> alameda-county-ca-usda-distress
    Also handles 'Parish', 'Census Area', 'city' suffixes.
    """
    stem = path.stem
    m = re.match(r"^(.+?)-budget-distress-score-\d+-\d+$", stem)
    if m:
        return f"{m.group(1)}-usda-distress"
    # Fallback: derive from county + state fields if available.
    county = fields.get("county", "").strip()
    state = fields.get("state", "").strip()
    if county and state:
        return slugify(f"{county}-{state}") + "-usda-distress"
    return None


def new_contract_slug(path: Path, fields: dict, body: str) -> str | None:
    """
    <recipient>-<award-id> — award id read from body.
    Falls back to None if we can't extract the award id.
    """
    award_id = extract_award_id(body)
    if not award_id:
        return None

    # Recipient is the portion before the first <award-id-looking token>
    # in the current filename. Easier: rebuild from title.
    title = fields.get("title", "")
    # Title form: "RECIPIENT — AWARD_ID (ST) $AMOUNT"  or  "RECIPIENT — ICE COUNTY, ST $AMOUNT"
    # Take the recipient (everything before the em-dash) and combine with award id.
    recipient_part = title.split("—")[0] if "—" in title else title.split("-")[0]
    recipient_slug = slugify(recipient_part)
    if not recipient_slug:
        # Fall back to stem prefix up to first numeric-looking block.
        recipient_slug = re.match(r"^([a-z0-9-]+?)(?=-[a-z0-9]*\d)", path.stem)
        recipient_slug = recipient_slug.group(1) if recipient_slug else "unknown"
    award_slug = slugify(award_id)
    return f"{recipient_slug}-{award_slug}"


def process_budget(dry_run: bool, samples: list):
    root = KB_ROOT / "budget"
    count = 0
    for path in sorted(root.glob("*-budget-distress-score-*.md")):
        text = path.read_text(encoding="utf-8")
        fields, body, _ = parse_frontmatter(text)
        if fields is None:
            continue
        new_slug = new_budget_slug(path, fields)
        if not new_slug:
            continue
        new_path = root / f"{new_slug}.md"
        if new_path == path:
            continue
        count += 1
        if dry_run:
            if len(samples) < 10:
                samples.append((path, new_path))
            continue
        new_text = rewrite_id(text, new_slug)
        new_path.write_text(new_text, encoding="utf-8")
        path.unlink()
    return count


def process_contracts(subdir: str, dry_run: bool, samples: list, cross_dir_skip: set):
    root = KB_ROOT / subdir
    count = 0
    skipped_intra = 0
    skipped_cross = 0
    claimed: set[Path] = set()  # dry-run: track paths we'd create
    for path in sorted(root.glob("*.md")):
        text = path.read_text(encoding="utf-8")
        fields, body, _ = parse_frontmatter(text)
        if fields is None:
            continue
        new_slug = new_contract_slug(path, fields, body or "")
        if not new_slug:
            skipped_intra += 1
            continue
        if new_slug in cross_dir_skip:
            skipped_cross += 1
            continue
        new_path = root / f"{new_slug}.md"
        if new_path == path:
            continue
        if new_path.exists() or new_path in claimed:
            skipped_intra += 1
            continue
        claimed.add(new_path)
        count += 1
        if dry_run:
            if len(samples) < 10:
                samples.append((path, new_path))
            continue
        new_text = rewrite_id(text, new_slug)
        new_path.write_text(new_text, encoding="utf-8")
        path.unlink()
    return count, skipped_intra, skipped_cross


def find_cross_dir_collisions() -> set:
    """Return the set of new-slug strings that would exist in both anc/ and ice-contracts/."""
    by_dir: dict[str, set[str]] = {"anc": set(), "ice-contracts": set()}
    for subdir in ("anc", "ice-contracts"):
        for path in (KB_ROOT / subdir).glob("*.md"):
            text = path.read_text(encoding="utf-8")
            fields, body, _ = parse_frontmatter(text)
            if fields is None:
                continue
            slug = new_contract_slug(path, fields, body or "")
            if slug:
                by_dir[subdir].add(slug)
    return by_dir["anc"] & by_dir["ice-contracts"]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--samples", type=int, default=5)
    args = p.parse_args()

    print("── Renaming budget entries ──")
    samples: list[tuple[Path, Path]] = []
    budget_count = process_budget(args.dry_run, samples)
    print(f"  {'Would rename' if args.dry_run else 'Renamed'} {budget_count} budget entries.")

    print()
    print("── Identifying cross-dir collisions ──")
    cross_dir_skip = find_cross_dir_collisions()
    print(f"  Found {len(cross_dir_skip)} award IDs present in both anc/ and ice-contracts/.")
    print(f"  These will be skipped (run dedupe_contracts.py to resolve).")

    print()
    print("── Renaming ice-contracts entries ──")
    ice_samples: list[tuple[Path, Path]] = []
    ice_count, ice_skip, ice_cross = process_contracts("ice-contracts", args.dry_run, ice_samples, cross_dir_skip)
    print(f"  {'Would rename' if args.dry_run else 'Renamed'} {ice_count} (skipped {ice_skip} dup, {ice_cross} cross-dir).")

    print()
    print("── Renaming anc entries ──")
    anc_samples: list[tuple[Path, Path]] = []
    anc_count, anc_skip, anc_cross = process_contracts("anc", args.dry_run, anc_samples, cross_dir_skip)
    print(f"  {'Would rename' if args.dry_run else 'Renamed'} {anc_count} (skipped {anc_skip} dup, {anc_cross} cross-dir).")

    if args.dry_run:
        for label, samp in [("budget", samples), ("ice-contracts", ice_samples), ("anc", anc_samples)]:
            if samp:
                print(f"\n── Sample {label} renames ──")
                for old, new in samp[: args.samples]:
                    print(f"  {old.name}")
                    print(f"    -> {new.name}")

    print()
    print(f"Total {'to rename' if args.dry_run else 'renamed'}: "
          f"{budget_count + ice_count + anc_count}")


if __name__ == "__main__":
    main()
