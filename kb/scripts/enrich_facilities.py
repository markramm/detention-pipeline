#!/usr/bin/env python3
"""
Enrich IGSA facility entries with data from Vera Institute's ICE Detention Trends.

Downloads Vera's facility metadata (1,490 facilities with type, location, AOR)
and recent population data, then matches against our IGSA entries by facility
name + state fuzzy matching.

Enriches entries with:
  - facility_type (Vera's type_detailed classification)
  - address, city, lat/lng
  - aor (ICE Area of Responsibility)
  - recent_population (average daily population from most recent data)

Usage:
    python enrich_facilities.py                 # enrich all IGSA entries
    python enrich_facilities.py --dry-run        # preview matches
    python enrich_facilities.py --state FL       # only Florida
"""

import argparse
import csv
import io
import json
import os
import re
import sys
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from urllib.request import Request, urlopen

VERA_META_URL = "https://raw.githubusercontent.com/vera-institute/ice-detention-trends/main/metadata/facilities.csv"
VERA_POP_URL = "https://raw.githubusercontent.com/vera-institute/ice-detention-trends/main/facilities/by_fiscal_year/FY2026.csv"

KB_PATH = Path(__file__).parent.parent


def download(url, cache_path, max_age_hours=168):
    """Download file with caching."""
    p = Path(cache_path)
    if p.exists():
        age = (datetime.now().timestamp() - p.stat().st_mtime) / 3600
        if age < max_age_hours:
            return p.read_text(encoding="utf-8")
    try:
        print(f"  Downloading {url.split('/')[-1]}...")
        req = Request(url)
        req.add_header("User-Agent", "detention-pipeline-research/1.0")
        with urlopen(req, timeout=30) as resp:
            data = resp.read().decode("utf-8")
            p.write_text(data, encoding="utf-8")
            return data
    except Exception as e:
        print(f"  Download failed: {e}", file=sys.stderr)
        if p.exists():
            return p.read_text(encoding="utf-8")
        return None


def load_vera_metadata():
    """Load Vera facility metadata into a lookup."""
    data = download(VERA_META_URL, "/tmp/vera_facilities_meta.csv")
    if not data:
        return {}

    facilities = {}
    reader = csv.DictReader(io.StringIO(data))
    for row in reader:
        code = row.get("detention_facility_code", "")
        name = row.get("detention_facility_name", "")
        state = row.get("state", "")
        if not name or not state:
            continue
        key = f"{name}|{state}"
        facilities[key] = {
            "code": code,
            "name": name,
            "state": state,
            "county": row.get("county", ""),
            "city": row.get("city", ""),
            "address": row.get("address", ""),
            "lat": row.get("latitude", ""),
            "lng": row.get("longitude", ""),
            "aor": row.get("aor", ""),
            "type_detailed": row.get("type_detailed", ""),
            "type_grouped": row.get("type_grouped", ""),
        }
    return facilities


def load_vera_population():
    """Load recent facility population data."""
    data = download(VERA_POP_URL, "/tmp/vera_facilities_fy2026.csv")
    if not data:
        return {}

    # Compute average daily population per facility (last 30 days of data)
    pops = {}
    reader = csv.DictReader(io.StringIO(data))
    for row in reader:
        code = row.get("detention_facility_code", "")
        try:
            pop = int(row.get("daily_pop", 0) or 0)
        except ValueError:
            pop = 0
        if code and pop > 0:
            if code not in pops:
                pops[code] = []
            pops[code].append(pop)

    # Average of last 30 data points per facility
    averages = {}
    for code, vals in pops.items():
        recent = vals[-30:] if len(vals) > 30 else vals
        averages[code] = round(sum(recent) / len(recent))
    return averages


def normalize_name(name):
    """Normalize facility name for matching."""
    n = name.lower()
    # Remove common suffixes/prefixes
    for word in ["detention center", "detention facility", "county jail",
                 "county det center", "county det. center", "processing center",
                 "correctional", "jail", "— ", " - "]:
        n = n.replace(word, "")
    # Remove state abbreviation patterns
    n = re.sub(r",\s*[A-Z]{2}$", "", n)
    # Remove punctuation
    n = re.sub(r"[^a-z0-9\s]", "", n)
    return n.strip()


def fuzzy_match(our_name, our_state, vera_facilities):
    """Find best Vera facility match for our entry."""
    our_norm = normalize_name(our_name)
    best_match = None
    best_score = 0

    for key, vera in vera_facilities.items():
        # Must be same state
        if vera["state"] != our_state:
            continue

        vera_norm = normalize_name(vera["name"])
        score = SequenceMatcher(None, our_norm, vera_norm).ratio()

        if score > best_score:
            best_score = score
            best_match = vera

    if best_score >= 0.5:
        return best_match, best_score
    return None, 0


def parse_entry(filepath):
    """Parse a KB markdown entry."""
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
        if ":" in line and not line.startswith("-") and not line.startswith("#"):
            key, val = line.split(":", 1)
            fields[key.strip()] = val.strip().strip('"').strip("'")

    body = text[end + 3:].strip()
    return fields, body


def main():
    parser = argparse.ArgumentParser(description="Enrich IGSA facility entries with Vera data")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--state", type=str, help="Only this state")
    parser.add_argument("--min-score", type=float, default=0.5, help="Minimum match score")
    args = parser.parse_args()

    print("Loading Vera facility metadata...")
    vera = load_vera_metadata()
    print(f"  {len(vera)} Vera facilities loaded")

    print("Loading Vera population data...")
    pops = load_vera_population()
    print(f"  {len(pops)} facilities with population data")

    # Find our IGSA entries
    facilities_dir = KB_PATH / "facilities"
    if not facilities_dir.exists():
        print(f"No facilities directory at {facilities_dir}")
        sys.exit(1)

    entries = list(facilities_dir.glob("*.md"))
    print(f"  {len(entries)} IGSA entries to enrich")

    matched = 0
    enriched = 0
    unmatched = []

    for md in sorted(entries):
        fields, body = parse_entry(md)
        if not fields:
            continue

        state = fields.get("state", "")
        if args.state and state != args.state:
            continue

        facility_name = fields.get("facility_name", fields.get("title", md.stem))
        match, score = fuzzy_match(facility_name, state, vera)

        if match:
            matched += 1
            vera_code = match["code"]
            avg_pop = pops.get(vera_code, 0)

            if args.dry_run:
                marker = "*" if avg_pop > 0 else " "
                print(f"  {marker} [{score:.2f}] {facility_name} → {match['name']}"
                      f"  type={match['type_detailed']}"
                      f"  pop={avg_pop}" if avg_pop else "")
            else:
                # Enrich the entry
                enrichments = []
                if match.get("type_detailed") and "type_detailed" not in fields:
                    enrichments.append(f"facility_type: \"{match['type_detailed']}\"")
                if match.get("city") and "city" not in fields:
                    enrichments.append(f"city: \"{match['city']}\"")
                if match.get("address") and "address" not in fields:
                    enrichments.append(f"address: \"{match['address']}\"")
                if match.get("aor") and "aor" not in fields:
                    enrichments.append(f"aor: \"{match['aor']}\"")
                if match.get("lat") and "latitude" not in fields:
                    enrichments.append(f"latitude: {match['lat']}")
                    enrichments.append(f"longitude: {match['lng']}")
                if avg_pop > 0 and "avg_daily_pop" not in fields:
                    enrichments.append(f"avg_daily_pop: {avg_pop}")

                if enrichments:
                    # Insert enrichments before the closing ---
                    text = md.read_text(encoding="utf-8")
                    end_idx = text.index("---", 3)
                    new_text = text[:end_idx] + "\n".join(enrichments) + "\n" + text[end_idx:]
                    md.write_text(new_text, encoding="utf-8")
                    enriched += 1
        else:
            unmatched.append((facility_name, state))

    print(f"\nResults: {matched} matched, {enriched} enriched, {len(unmatched)} unmatched")

    if unmatched and args.dry_run:
        print(f"\nUnmatched ({len(unmatched)}):")
        for name, state in unmatched[:20]:
            print(f"  {name}, {state}")
        if len(unmatched) > 20:
            print(f"  ... and {len(unmatched) - 20} more")


if __name__ == "__main__":
    main()
