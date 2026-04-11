#!/usr/bin/env python3
"""
Ingest the full Vera Institute ICE detention facility dataset.

Replaces the existing IGSA-only facility list with the complete Vera dataset
(~1,490 facilities) including metadata (type, address, lat/lng, AOR) and
recent population data.

Source: https://github.com/vera-institute/ice-detention-trends

Usage:
    python ingest_vera_facilities.py              # full import
    python ingest_vera_facilities.py --dry-run     # preview
    python ingest_vera_facilities.py --type IGSA   # only IGSA type
"""

import argparse
import csv
import io
import re
import json
import sys
from datetime import datetime
from pathlib import Path
from urllib.request import Request, urlopen

VERA_META_URL = "https://raw.githubusercontent.com/vera-institute/ice-detention-trends/main/metadata/facilities.csv"
VERA_POP_URL = "https://raw.githubusercontent.com/vera-institute/ice-detention-trends/main/facilities/by_fiscal_year/FY2026.csv"

KB_FACILITIES_DIR = Path(__file__).parent.parent / "facilities"

# Facility types to include (skip hospitals, holds, unknown)
INCLUDE_TYPES = {
    "IGSA", "DIGSA", "CDF", "SPC", "BOP",
    "USMS IGA", "USMS CDF", "USMS CDF / USMS IGA",
    "Family Staging", "Staging", "Family",
    "DOD", "TAP-ICE", "MOC",
}

# Types that are clearly not detention facilities
EXCLUDE_TYPES = {
    "Hospital", "Medical", "BOP/Medical",
    "Hotel",  # temporary pandemic-era usage
}

# FIPS lookup
FIPS_URL = "https://www2.census.gov/geo/docs/reference/codes2020/national_county2020.txt"
FIPS_LOOKUP = {}

STATE_FIPS = {
    "AL":"01","AK":"02","AZ":"04","AR":"05","CA":"06","CO":"08","CT":"09",
    "DE":"10","DC":"11","FL":"12","GA":"13","HI":"15","ID":"16","IL":"17",
    "IN":"18","IA":"19","KS":"20","KY":"21","LA":"22","ME":"23","MD":"24",
    "MA":"25","MI":"26","MN":"27","MS":"28","MO":"29","MT":"30","NE":"31",
    "NV":"32","NH":"33","NJ":"34","NM":"35","NY":"36","NC":"37","ND":"38",
    "OH":"39","OK":"40","OR":"41","PA":"42","RI":"44","SC":"45","SD":"46",
    "TN":"47","TX":"48","UT":"49","VT":"50","VA":"51","WA":"53","WV":"54",
    "WI":"55","WY":"56","PR":"72","GU":"66","VI":"78",
}


def download(url, cache_path, max_age_hours=168):
    """Download with caching."""
    p = Path(cache_path)
    if p.exists():
        age = (datetime.now().timestamp() - p.stat().st_mtime) / 3600
        if age < max_age_hours:
            return p.read_text(encoding="utf-8")
    try:
        print(f"  Downloading {url.split('/')[-1]}...", flush=True)
        req = Request(url)
        req.add_header("User-Agent", "detention-pipeline-research/1.0")
        with urlopen(req, timeout=30) as resp:
            data = resp.read().decode("utf-8")
            p.write_text(data, encoding="utf-8")
            return data
    except Exception as e:
        print(f"  Download failed: {e}", file=sys.stderr, flush=True)
        if p.exists():
            return p.read_text(encoding="utf-8")
        return None


def load_fips():
    """Load FIPS lookup from Census county reference file."""
    data = download(FIPS_URL, "/tmp/county_fips.txt", max_age_hours=8760)
    if not data:
        return
    for line in data.strip().split("\n"):
        parts = line.split("|")
        # Format: STATE|STATEFP|COUNTYFP|COUNTYNS|COUNTYNAME|CLASSFP|FUNCSTAT
        if len(parts) >= 5 and parts[0] != "STATE":
            state_abbr = parts[0]
            fips = parts[1] + parts[2]
            county_name = parts[4]
            FIPS_LOOKUP[(county_name.lower(), state_abbr)] = fips
            # Also index without "County"/"Parish" suffix for matching Vera's format
            short = county_name.lower()
            for suffix in [" county", " parish", " borough", " census area",
                           " municipality", " city and borough", " city"]:
                if short.endswith(suffix):
                    short = short[:-len(suffix)]
                    break
            FIPS_LOOKUP[(short, state_abbr)] = fips


def resolve_fips(county_name, state):
    """Resolve county name + state to FIPS."""
    if not county_name or not state:
        return ""
    cn = county_name.lower().strip()
    # Try exact match (Vera uses short names like "Ada", Census has "Ada County")
    if (cn, state) in FIPS_LOOKUP:
        return FIPS_LOOKUP[(cn, state)]
    # Try with suffixes
    for suffix in [" county", " parish", " borough"]:
        if (cn + suffix, state) in FIPS_LOOKUP:
            return FIPS_LOOKUP[(cn + suffix, state)]
    return ""


def slugify(name):
    """Generate a URL-safe slug from facility name."""
    s = name.lower()
    s = re.sub(r"[^a-z0-9\s-]", "", s)
    s = re.sub(r"[\s]+", "-", s)
    s = re.sub(r"-+", "-", s)
    return s.strip("-")[:80]


def main():
    parser = argparse.ArgumentParser(description="Ingest Vera Institute facility dataset")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--type", type=str, help="Only this facility type (e.g. IGSA, CDF)")
    parser.add_argument("--include-all", action="store_true", help="Include all types except Hospital/Medical")
    args = parser.parse_args()

    print("Loading FIPS lookup...", flush=True)
    load_fips()
    print(f"  {len(FIPS_LOOKUP)} counties", flush=True)

    print("Loading Vera facility metadata...", flush=True)
    meta_data = download(VERA_META_URL, "/tmp/vera_facilities_meta.csv")
    if not meta_data:
        print("Failed to download metadata")
        sys.exit(1)

    print("Loading Vera population data...", flush=True)
    pop_data = download(VERA_POP_URL, "/tmp/vera_facilities_FY2026.csv")

    # Parse population data
    pops = {}
    if pop_data:
        reader = csv.DictReader(io.StringIO(pop_data))
        for row in reader:
            code = row.get("detention_facility_code", "")
            try:
                pop = int(row.get("daily_pop", 0) or 0)
            except ValueError:
                pop = 0
            if code and pop > 0:
                pops.setdefault(code, []).append(pop)
        # Average last 30 days
        avg_pops = {code: round(sum(vals[-30:]) / len(vals[-30:])) for code, vals in pops.items()}
        print(f"  {len(avg_pops)} facilities with population data", flush=True)
    else:
        avg_pops = {}

    # Parse metadata
    reader = csv.DictReader(io.StringIO(meta_data))
    facilities = []
    for row in reader:
        ftype = row.get("type_detailed", "")
        name = row.get("detention_facility_name", "")
        state = row.get("state", "")
        code = row.get("detention_facility_code", "")

        if not name or not state:
            continue

        # Type filtering
        if args.type:
            if ftype != args.type:
                continue
        elif not args.include_all:
            if ftype in EXCLUDE_TYPES:
                continue
            if not ftype and ftype not in INCLUDE_TYPES:
                # Include unknown types too — they might be relevant
                pass

        county = row.get("county", "")
        fips = resolve_fips(county, state)

        # If no FIPS from county name, try state FIPS prefix
        if not fips and state in STATE_FIPS:
            # Can't resolve to county level — skip FIPS
            pass

        avg_pop = avg_pops.get(code, 0)

        facilities.append({
            "code": code,
            "name": name,
            "state": state,
            "county": county,
            "fips": fips,
            "city": row.get("city", ""),
            "address": row.get("address", ""),
            "lat": row.get("latitude", ""),
            "lng": row.get("longitude", ""),
            "aor": row.get("aor", ""),
            "type": ftype,
            "type_grouped": row.get("type_grouped", ""),
            "avg_pop": avg_pop,
        })

    print(f"  {len(facilities)} facilities after filtering", flush=True)

    # Type breakdown
    from collections import Counter
    types = Counter(f["type"] for f in facilities)
    for t, c in types.most_common():
        print(f"    {t or '(empty)'}: {c}", flush=True)

    if args.dry_run:
        # Show sample
        for f in facilities[:10]:
            pop_str = f" pop={f['avg_pop']}" if f['avg_pop'] else ""
            fips_str = f" FIPS={f['fips']}" if f['fips'] else " NO-FIPS"
            print(f"  {f['name']}, {f['state']} [{f['type']}]{fips_str}{pop_str}")
        print(f"\n  ... and {len(facilities) - 10} more")
        print(f"\n  Would write {len(facilities)} entries to {KB_FACILITIES_DIR}/")
        return

    # Clear existing facilities directory and rewrite
    if KB_FACILITIES_DIR.exists():
        existing = list(KB_FACILITIES_DIR.glob("*.md"))
        print(f"  Removing {len(existing)} existing entries...", flush=True)
        for f in existing:
            f.unlink()
    else:
        KB_FACILITIES_DIR.mkdir(parents=True)

    written = 0
    for f in facilities:
        slug = slugify(f"{f['name']}-{f['county']}-{f['state']}")
        if not slug:
            slug = slugify(f['name'])

        # Build entry
        title = f"{f['name']} — {f['county']}, {f['state']}" if f['county'] else f"{f['name']} — {f['state']}"

        lines = [
            "---",
            f'id: {slug}',
            f'title: "{title}"',
            f'type: igsa',
            f'county: "{f["county"]}"',
            f'state: "{f["state"]}"',
            f'fips: "{f["fips"]}"',
            f'facility_name: "{f["name"]}"',
            f'facility_type: "{f["type"]}"',
        ]
        if f["city"]:
            lines.append(f'city: "{f["city"]}"')
        if f["address"]:
            lines.append(f'address: "{f["address"]}"')
        if f["aor"]:
            lines.append(f'aor: "{f["aor"]}"')
        if f["lat"]:
            lines.append(f'latitude: {f["lat"]}')
            lines.append(f'longitude: {f["lng"]}')
        if f["avg_pop"]:
            lines.append(f'avg_daily_pop: {f["avg_pop"]}')

        lines.extend([
            'operator: ""',
            'status: "active"',
            f'source: "Vera Institute ICE Detention Trends (vera-institute/ice-detention-trends)"',
            'tags:',
            f'- {f["type"].lower().replace(" ", "-") or "detention"}',
            f'- {f["state"].lower()}',
            'importance: 5',
            '---',
            '',
        ])

        # Body
        type_label = f["type"] or "Detention"
        body = f'{type_label} facility: {f["name"]} in {f["county"] + " County, " if f["county"] else ""}{f["state"]}.'
        if f["avg_pop"]:
            body += f' Average daily population: {f["avg_pop"]}.'

        filepath = KB_FACILITIES_DIR / f"{slug}.md"
        filepath.write_text("\n".join(lines) + body + "\n", encoding="utf-8")
        written += 1

    print(f"\n  Wrote {written} facility entries to {KB_FACILITIES_DIR}/", flush=True)
    print(f"  Run generate_content.py to rebuild the site.", flush=True)


if __name__ == "__main__":
    main()
