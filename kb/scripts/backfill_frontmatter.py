#!/usr/bin/env python3
"""
Backfill county / state / fips frontmatter on entries where these fields
are missing but recoverable from the entry's body, title, id, or tags.

Root cause: older Pyrite `kb import` runs dropped structured fields from
the ingest JSON. The field values still live in the entry body (e.g.
"State: TX", "Location: Foo County, MD") or are embedded in the id/title.
This script extracts them and patches the frontmatter in place.

Also normalizes:
  - full state names ("Minnesota" -> "MN")

Usage:
    python3 backfill_frontmatter.py --dry-run              # show summary + 5 samples
    python3 backfill_frontmatter.py --dry-run --samples 20 # more samples
    python3 backfill_frontmatter.py                        # apply changes
    python3 backfill_frontmatter.py --only anc             # one subdir
"""

from __future__ import annotations

import argparse
import csv
import difflib
import os
import re
import sys
from pathlib import Path

KB_ROOT = Path(__file__).parent.parent

STATE_NAME_TO_ABBR = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "district of columbia": "DC", "florida": "FL", "georgia": "GA", "hawaii": "HI",
    "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "puerto rico": "PR", "rhode island": "RI",
    "south carolina": "SC", "south dakota": "SD", "tennessee": "TN", "texas": "TX",
    "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
}
VALID_STATES = set(STATE_NAME_TO_ABBR.values()) | {"DC", "PR", "VI", "GU", "MP", "AS", "US"}

FIPS_TO_COUNTY: dict[str, tuple[str, str]] = {}      # fips -> (county, state)
COUNTY_STATE_TO_FIPS: dict[tuple[str, str], str] = {} # (county_lower, state) -> fips


def load_fips():
    """Load Census FIPS mapping from /tmp/county_fips.txt if present."""
    path = Path("/tmp/county_fips.txt")
    if not path.exists():
        return
    with open(path) as f:
        reader = csv.DictReader(f, delimiter="|")
        for row in reader:
            fips = row["STATEFP"] + row["COUNTYFP"]
            county = row["COUNTYNAME"]
            state = row["STATE"]
            FIPS_TO_COUNTY[fips] = (county, state)
            # Normalize "X County" -> "X" for lookup
            base = re.sub(r"\s+(County|Parish|Borough|Census Area|Municipality|City and Borough)$", "", county, flags=re.I)
            COUNTY_STATE_TO_FIPS[(base.lower(), state)] = fips
            COUNTY_STATE_TO_FIPS[(county.lower(), state)] = fips


def parse_frontmatter(text: str):
    """Return (fields_dict, body_text, frontmatter_raw) or (None, text, '') if no frontmatter."""
    if not text.startswith("---"):
        return None, text, ""
    try:
        end = text.index("\n---", 3)
    except ValueError:
        return None, text, ""
    fm_raw = text[4:end].rstrip()
    body = text[end + 4:].lstrip("\n")

    fields = {}
    current_list_key = None
    for line in fm_raw.split("\n"):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            current_list_key = None
            continue
        if stripped.startswith("-") and current_list_key:
            item = stripped[1:].strip().strip('"').strip("'")
            fields.setdefault(current_list_key + "_list", []).append(item)
            continue
        if ":" in stripped:
            key, val = stripped.split(":", 1)
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key:
                fields[key] = val
                current_list_key = key if val == "" else None
    return fields, body, fm_raw


STATE_ABBR_RE = r"(?:A[LKZR]|C[AOT]|D[EC]|FL|GA|HI|I[DLNA]|K[SY]|LA|M[EDADNSOT]|N[EVHJMYCD]|O[HKR]|PA|PR|RI|S[CD]|T[NX]|U[TV]|V[AT]|W[AVIY])"


# Cities that the legistar ingester mislabeled as counties via the slug
# `<city>-county-<st>-<body>-...`. Maps slug-prefix -> (real county, state).
# This is a workaround for an ingester bug in ingest_legistar.py:
# Legistar portals for city governments got the "-county-" infix anyway.
# Known offenders are listed here so backfill can still recover FIPS.
CITY_TO_COUNTY = {
    ("columbus-county", "OH"): ("Franklin County", "OH"),
    ("columbus-county", "GA"): ("Muscogee County", "GA"),
    ("kansas-city-county", "MO"): ("Jackson County", "MO"),
    ("st-paul-county", "MN"): ("Ramsey County", "MN"),
    ("seattle-county", "WA"): ("King County", "WA"),
    ("bellevue-county", "WA"): ("King County", "WA"),
    ("concord-county", "NH"): ("Merrimack County", "NH"),
    ("huntsville-county", "AL"): ("Madison County", "AL"),
    ("mesa-county", "AZ"): ("Maricopa County", "AZ"),   # Mesa is in Maricopa
    ("madison-county", "WI"): ("Dane County", "WI"),    # Madison (city) is in Dane
    ("pittsburgh-county", "PA"): ("Allegheny County", "PA"),
    ("round-rock-county", "TX"): ("Williamson County", "TX"),
}


_NONSTD_STATE = {
    "w. va.": "WV", "w.va.": "WV", "w va": "WV",
    "n. y.": "NY", "n.y.": "NY",
    "n. j.": "NJ", "n.j.": "NJ",
    "n. h.": "NH", "n.h.": "NH",
    "n. c.": "NC", "n.c.": "NC",
    "n. d.": "ND", "n.d.": "ND",
    "n. m.": "NM", "n.m.": "NM",
    "s. c.": "SC", "s.c.": "SC",
    "s. d.": "SD", "s.d.": "SD",
    "r. i.": "RI", "r.i.": "RI",
    "d. c.": "DC", "d.c.": "DC",
}


def extract_from_body(body: str) -> dict:
    """Pull county/state/fips out of structured body lines like 287g/anc entries."""
    out = {}
    for line in body.split("\n")[:25]:
        m = re.match(r"^County:\s*(.+?)\s*$", line)
        if m and m.group(1) and m.group(1).lower() not in ("none", "unresolved", ""):
            out.setdefault("county", m.group(1).strip())
        m = re.match(r"^State:\s*(.+?)\s*$", line)
        if m:
            raw = m.group(1).strip()
            if re.fullmatch(r"[A-Z]{2}", raw):
                out.setdefault("state", raw)
            else:
                abbr = _NONSTD_STATE.get(raw.lower()) or STATE_NAME_TO_ABBR.get(raw.lower())
                if abbr:
                    out.setdefault("state", abbr)
        m = re.match(r"^FIPS:\s*(\d{5})\s*$", line)
        if m:
            out.setdefault("fips", m.group(1))
        # ANC pattern: "Location: County Name, ST"
        m = re.match(r"^Location:\s*(.+?),\s*([A-Z]{2})\s*$", line)
        if m:
            county = m.group(1).strip()
            if county.lower() not in ("none", ""):
                out.setdefault("county", county)
            out.setdefault("state", m.group(2))
    return out


def extract_from_id_or_title(entry_id: str, title: str) -> dict:
    """Pull state (and maybe county) from id or title.

    Title forms seen:
      'Alameda County County CA — City Council ...'        (commission)
      'Ziebach County, SD — Budget Distress ...'           (budget)
      'ICE Detention ... — Florida (hybrid)'               (jobs, full state)
      '287(g) TFM: Cabell County Sheriff's Office (W-VA)'  (287g, hyphenated state)
      'Yukon-Koyukuk Census Area, AK — ...'                (budget, AK census area)
      'Radford City, VA — ...'                             (budget, VA indep. city)
    """
    out = {}

    # First: is the id/slug a known mislabeled-city form?
    # e.g. 'columbus-county-oh-columbus-city-council-...'
    if entry_id:
        m = re.match(r"^([a-z0-9-]+?-county)-([a-z]{2})-", entry_id)
        if m:
            key = (m.group(1), m.group(2).upper())
            if key in CITY_TO_COUNTY:
                county, state = CITY_TO_COUNTY[key]
                out["county"] = county
                out["state"] = state
                return out

    # Allow unicode letters (Doña Ana, Saint-Étienne-style) in county names.
    name_run = r"[A-Z][\w.\-' ]+?"

    # 'Name County County ST —' (commission, doubled 'County') must come first.
    m = re.search(rf"({name_run})\s+County\s+County\s+([A-Z]{{2}})\s*(?:—|-)", title)
    if m:
        out["county"] = m.group(1).strip() + " County"
        out["state"] = m.group(2)
        return out

    # Alaska variants: 'Name Census Area, AK —', 'Name Borough, AK —'
    m = re.search(rf"({name_run})\s+(Census Area|Borough|Municipality|City and Borough)\s*,?\s+AK\s*(?:—|-)", title)
    if m:
        out["county"] = (m.group(1).strip() + " " + m.group(2)).strip()
        out["state"] = "AK"
        return out

    # Louisiana parishes: 'Name Parish, LA —'
    m = re.search(rf"({name_run})\s+Parish,?\s+([A-Z]{{2}})\s*(?:—|-)", title)
    if m:
        out["county"] = m.group(1).strip() + " Parish"
        out["state"] = m.group(2)
        return out

    # Independent cities: 'Name City, ST —' (Virginia, plus Baltimore City MD).
    # Case-insensitive on the City token — 'baltimore city, MD' in titles.
    m = re.search(rf"({name_run})\s+[Cc]ity,?\s+([A-Z]{{2}})\s*(?:—|-)", title)
    if m:
        out["county"] = m.group(1).strip() + " city"
        out["state"] = m.group(2)
        return out

    # Generic 'Name County, ST —' (budget, well-formed commission)
    m = re.search(rf"({name_run})\s+County,?\s+([A-Z]{{2}})\s*(?:—|-)", title)
    if m:
        out["county"] = m.group(1).strip() + " County"
        out["state"] = m.group(2)
        return out

    # Special 'W-VA' / 'W. Va.' forms
    m = re.search(r"\(\s*W[-.\s]*VA\.?\s*\)", title, flags=re.I)
    if m:
        out["state"] = "WV"

    # Full state in title ('— Florida (hybrid)' or 'Florida (hybrid)')
    for name, abbr in STATE_NAME_TO_ABBR.items():
        if re.search(rf"—\s*{re.escape(name)}\b", title, flags=re.I):
            out.setdefault("state", abbr)
            return out

    # National / remote / no-state entries: fall back to "US" marker.
    # Signals: contract title contains "(None)", id contains "-none-",
    # or title says "Remote" / "National".
    if (
        re.search(r"\(None\)", title)
        or re.search(r"-none-\d", entry_id or "")
        or re.search(r"\b(Remote|National)\b", title, flags=re.I)
    ):
        out.setdefault("state", "US")

    return out


def resolve_fips(county: str, state: str) -> str | None:
    if not county or not state:
        return None
    base = re.sub(r"\s+(County|Parish|Borough|Census Area|Municipality|City and Borough)$", "", county, flags=re.I).strip()
    return (
        COUNTY_STATE_TO_FIPS.get((county.lower(), state))
        or COUNTY_STATE_TO_FIPS.get((base.lower(), state))
    )


def normalize_state(val: str) -> str | None:
    if not val:
        return None
    val = val.strip().strip('"').strip("'")
    if len(val) == 2 and val.upper() in VALID_STATES:
        return val.upper()
    abbr = STATE_NAME_TO_ABBR.get(val.lower())
    return abbr


def yaml_quote(v: str) -> str:
    return "'" + v.replace("'", "''") + "'"


def patch_frontmatter(text: str, patches: dict) -> str:
    """Patch existing fields in place; add missing ones before closing ---."""
    # Find end of frontmatter
    end = text.index("\n---", 3)
    fm = text[4:end]
    rest = text[end:]

    lines = fm.split("\n")
    handled = set()
    new_lines = []
    for line in lines:
        m = re.match(r"^(\s*)([A-Za-z_][A-Za-z0-9_]*)\s*:\s*(.*)$", line)
        if not m:
            new_lines.append(line)
            continue
        indent, key, _ = m.groups()
        if key in patches:
            new_lines.append(f"{indent}{key}: {yaml_quote(patches[key])}")
            handled.add(key)
        else:
            new_lines.append(line)
    # Append new keys that weren't present
    for key, val in patches.items():
        if key not in handled:
            new_lines.append(f"{key}: {yaml_quote(val)}")

    return "---\n" + "\n".join(new_lines).rstrip("\n") + "\n" + rest[1:]


def process_file(path: Path, reasons_out: dict) -> dict | None:
    """Return dict of patches applied (or None if no patch needed)."""
    text = path.read_text(encoding="utf-8")
    fields, body, _ = parse_frontmatter(text)
    if fields is None:
        return None

    patches = {}

    # 1. Normalize full state names to abbreviations.
    state_val = fields.get("state", "").strip().strip('"').strip("'")
    if state_val and state_val not in VALID_STATES:
        normalized = normalize_state(state_val)
        if normalized:
            patches["state"] = normalized
            reasons_out.setdefault("state_normalized", 0)
            reasons_out["state_normalized"] += 1

    # 2. Fill missing county/state/fips from body, then title/id.
    body_extract = extract_from_body(body)
    title_extract = extract_from_id_or_title(fields.get("id", path.stem), fields.get("title", ""))

    # 3. Entries tagged 'national' without a state get US as a fallback.
    tag_list = fields.get("tags_list", [])
    if "national" in tag_list:
        title_extract.setdefault("state", "US")

    current = {k: fields.get(k, "").strip().strip('"').strip("'") for k in ("county", "state", "fips")}
    current.update({k: v for k, v in patches.items()})  # any normalization applied

    want = {}
    for key in ("state", "county", "fips"):
        if current.get(key):
            continue
        val = body_extract.get(key) or title_extract.get(key)
        if val:
            want[key] = val

    # 3. If we got county+state but not fips, try to resolve.
    have_county = want.get("county") or current.get("county")
    have_state = want.get("state") or current.get("state")
    if have_county and have_state and not (want.get("fips") or current.get("fips")):
        fips = resolve_fips(have_county, have_state)
        if fips:
            want["fips"] = fips

    # 4. Merge into patches.
    for k, v in want.items():
        # Avoid overwriting if nothing changes
        if fields.get(k, "").strip().strip('"').strip("'") != v:
            patches[k] = v

    if not patches:
        return None
    return patches


def render_diff(path: Path, patches: dict) -> str:
    before = path.read_text(encoding="utf-8").splitlines()
    after = patch_frontmatter(path.read_text(encoding="utf-8"), patches).splitlines()
    diff = difflib.unified_diff(
        before, after,
        fromfile=str(path.relative_to(KB_ROOT)),
        tofile=str(path.relative_to(KB_ROOT)) + " (after)",
        lineterm="", n=2,
    )
    # Limit to header+frontmatter region only
    return "\n".join(list(diff)[:30])


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--only", help="Restrict to one subdir (e.g. anc, 287g, commission)")
    p.add_argument("--samples", type=int, default=5, help="Number of sample diffs to print in dry-run")
    p.add_argument("--quiet", action="store_true")
    args = p.parse_args()

    load_fips()
    if not FIPS_TO_COUNTY:
        print("WARNING: /tmp/county_fips.txt not found — FIPS resolution will be skipped.", file=sys.stderr)

    dirs = ["287g", "anc", "ice-contracts", "budget", "commission", "jobs",
            "industry/county-fights", "industry/facilities"]
    if args.only:
        dirs = [d for d in dirs if d == args.only or d.endswith("/" + args.only)]

    touched = 0
    scanned = 0
    per_dir: dict[str, int] = {}
    reasons: dict[str, int] = {}
    samples = []

    for rel in dirs:
        root = KB_ROOT / rel
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.md")):
            scanned += 1
            patches = process_file(path, reasons)
            if not patches:
                continue
            touched += 1
            per_dir[rel] = per_dir.get(rel, 0) + 1
            if args.dry_run:
                if len(samples) < args.samples:
                    samples.append((path, patches))
            else:
                text = path.read_text(encoding="utf-8")
                path.write_text(patch_frontmatter(text, patches), encoding="utf-8")

    print(f"\nScanned {scanned} entries.")
    print(f"{'Would patch' if args.dry_run else 'Patched'} {touched} entries.\n")
    if per_dir:
        print("By subdirectory:")
        for d, n in sorted(per_dir.items(), key=lambda x: -x[1]):
            print(f"  {d:<28} {n}")
    if reasons:
        print("\nNormalizations:")
        for r, n in sorted(reasons.items(), key=lambda x: -x[1]):
            print(f"  {r:<28} {n}")

    if args.dry_run and samples:
        print("\n── Sample diffs ──")
        for path, patches in samples:
            print()
            print(render_diff(path, patches))


if __name__ == "__main__":
    main()
