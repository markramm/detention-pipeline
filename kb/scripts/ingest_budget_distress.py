#!/usr/bin/env python3
"""
Ingest county-level fiscal distress indicators from federal data sources.

Combines multiple datasets to identify counties vulnerable to detention
facility pitches — the ones with budget shortfalls, population loss,
high unemployment, and persistent poverty.

Data sources:
  1. USDA ERS County Typology — persistent poverty, population loss flags
  2. Census SAIPE — poverty rates and median household income
  3. BLS LAUS — unemployment rates

All sources are FIPS-coded. Output is scored by distress severity.

Usage:
    python ingest_budget_distress.py                # generate distress entries
    python ingest_budget_distress.py --dry-run       # preview only
    python ingest_budget_distress.py --min-score 3   # only high-distress counties
"""

import argparse
import csv
import io
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError

# USDA ERS County Typology Codes (2025 edition)
# Contains: persistent poverty, population loss, low employment, low education
USDA_TYPOLOGY_URL = "https://www.ers.usda.gov/media/6174/ers-county-typology-codes-2025-edition.csv?v=15553"

# Census SAIPE — Small Area Income and Poverty Estimates
# Most recent year's county-level poverty rates
SAIPE_API = "https://api.census.gov/data/2023/acs/acs5"

# BLS LAUS — Local Area Unemployment Statistics
# County-level unemployment (latest annual)
# NOTE: BLS blocks programmatic access as of 2026. Data must be downloaded
# manually from https://www.bls.gov/lau/tables.htm and saved to /tmp/laus_county.txt
LAUS_URL = None  # Set to URL if BLS restores API access

# FIPS lookup
FIPS_FILE = "/tmp/county_fips.txt"
FIPS_URL = "https://www2.census.gov/geo/docs/reference/codes2020/national_county2020.txt"


def download_if_missing(url, path, max_age_hours=168):
    """Download a file if missing or older than max_age_hours."""
    p = Path(path)
    if p.exists():
        age = time.time() - p.stat().st_mtime
        if age < max_age_hours * 3600:
            return True
    try:
        print(f"  Downloading {url}...")
        req = Request(url)
        req.add_header("User-Agent", "detention-pipeline-research/1.0")
        with urlopen(req, timeout=30) as resp:
            p.write_bytes(resp.read())
        return True
    except Exception as e:
        print(f"  Download failed: {e}", file=sys.stderr)
        return False


def load_fips_lookup():
    """Load FIPS → county name mapping."""
    fips_map = {}
    if not download_if_missing(FIPS_URL, FIPS_FILE):
        return fips_map
    with open(FIPS_FILE) as f:
        reader = csv.DictReader(f, delimiter="|")
        for r in reader:
            fips = r["STATEFP"] + r["COUNTYFP"]
            fips_map[fips] = f"{r['COUNTYNAME']}, {r['STATE']}"
    return fips_map


def load_usda_typology():
    """Load USDA ERS county typology codes (2025 long format).
    Returns dict of FIPS → {persistent_poverty, population_loss, low_employment, ...}
    """
    cache = "/tmp/usda_typology.csv"
    if not download_if_missing(USDA_TYPOLOGY_URL, cache):
        return {}

    # 2025 format is long: one row per (FIPS, Attribute, Value)
    # Attributes: Persistent_Poverty_1721, Population_Loss_2025,
    #   Low_Employment_2025, Low_PostSecondary_Ed_2025, Housing_Stress_2025
    ATTR_MAP = {
        "Persistent_Poverty_1721": "persistent_poverty",
        "Population_Loss_2025": "population_loss",
        "Low_Employment_2025": "low_employment",
        "Low_PostSecondary_Ed_2025": "low_education",
        "Housing_Stress_2025": "housing_stress",
    }

    counties = {}
    try:
        with open(cache) as f:
            reader = csv.DictReader(f)
            for row in reader:
                fips = row.get("FIPStxt", "").zfill(5)
                if len(fips) != 5:
                    continue
                attr = row.get("Attribute", "")
                val = row.get("Value", "0")
                metro = row.get("Metro2023", "")

                if fips not in counties:
                    counties[fips] = {
                        "persistent_poverty": False,
                        "population_loss": False,
                        "low_employment": False,
                        "low_education": False,
                        "housing_stress": False,
                        "metro": metro,
                    }

                if attr in ATTR_MAP and val == "1":
                    counties[fips][ATTR_MAP[attr]] = True
    except Exception as e:
        print(f"  Error parsing USDA data: {e}", file=sys.stderr)

    return counties


def load_laus():
    """Load BLS LAUS unemployment data.
    Returns dict of FIPS → unemployment_rate
    """
    cache = "/tmp/laus_county.txt"
    if LAUS_URL:
        if not download_if_missing(LAUS_URL, cache, max_age_hours=720):
            return {}
    elif not Path(cache).exists():
        return {}

    rates = {}
    try:
        with open(cache) as f:
            for line in f:
                # LAUS format: pipe-delimited, skip header rows
                if "|" not in line:
                    continue
                parts = [p.strip() for p in line.split("|")]
                if len(parts) < 10:
                    continue
                # LAUS code format: CN{SSFIPS}{CCFIPS}
                laus_code = parts[1].strip()
                if not laus_code.startswith("CN"):
                    continue
                state_fips = parts[2].strip().zfill(2)
                county_fips = parts[3].strip().zfill(3)
                fips = state_fips + county_fips
                try:
                    rate = float(parts[9].strip())
                    rates[fips] = rate
                except (ValueError, IndexError):
                    pass
    except Exception as e:
        print(f"  Error parsing LAUS data: {e}", file=sys.stderr)

    return rates


def score_distress(typology, unemployment_rate):
    """Score fiscal distress 0-10 based on available indicators."""
    score = 0
    reasons = []

    if typology:
        if typology.get("persistent_poverty"):
            score += 3
            reasons.append("persistent poverty (USDA ERS)")
        if typology.get("population_loss"):
            score += 2
            reasons.append("population loss")
        if typology.get("low_employment"):
            score += 2
            reasons.append("low employment")
        if typology.get("low_education"):
            score += 1
            reasons.append("low education attainment")
        # Non-metro (rural) counties more vulnerable to detention pitches
        if typology.get("metro") == "0":
            score += 1
            reasons.append("non-metro (rural)")

    if unemployment_rate is not None:
        if unemployment_rate >= 8.0:
            score += 2
            reasons.append(f"high unemployment ({unemployment_rate}%)")
        elif unemployment_rate >= 6.0:
            score += 1
            reasons.append(f"elevated unemployment ({unemployment_rate}%)")

    return score, reasons


def main():
    parser = argparse.ArgumentParser(description="Ingest county fiscal distress indicators")
    parser.add_argument("--min-score", type=int, default=3, help="Minimum distress score to include (default: 3)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without creating entries")
    parser.add_argument("--output", type=str, default="/tmp/budget_distress.json", help="Output JSON file")
    parser.add_argument("--state", type=str, help="Filter by state abbreviation")
    args = parser.parse_args()

    print("Loading FIPS lookup...")
    fips_map = load_fips_lookup()
    print(f"  {len(fips_map)} counties")

    print("Loading USDA county typology...")
    typology = load_usda_typology()
    print(f"  {len(typology)} counties with typology data")

    print("Loading BLS unemployment data...")
    laus = load_laus()
    if laus:
        print(f"  {len(laus)} counties with unemployment data")
    else:
        print("  Skipped (BLS blocks automated access; manually download from bls.gov/lau/tables.htm to /tmp/laus_county.txt)")

    # Score all counties
    entries = []
    for fips, county_name in fips_map.items():
        typ = typology.get(fips, {})
        urate = laus.get(fips)

        score, reasons = score_distress(typ, urate)
        if score < args.min_score:
            continue

        # Parse state from county name
        parts = county_name.split(", ")
        state = parts[-1] if len(parts) > 1 else ""
        county = parts[0] if parts else county_name

        if args.state and state != args.state:
            continue

        title = f"{county_name} — Budget Distress (score {score}/10)"
        body_lines = [
            f"County-level fiscal distress indicators for {county_name}.",
            "",
            f"Distress score: {score}/10",
            f"Indicators: {', '.join(reasons)}",
            "",
        ]
        if typ.get("persistent_poverty"):
            body_lines.append("USDA ERS classifies this county as persistently impoverished.")
        if typ.get("population_loss"):
            body_lines.append("Population declining — shrinking tax base increases vulnerability to 'economic development' pitches.")
        if urate:
            body_lines.append(f"Unemployment rate: {urate}%")

        body_lines.extend([
            "",
            "Sources: USDA ERS County Typology Codes (2025), BLS Local Area Unemployment Statistics",
        ])

        entry = {
            "entry_type": "budget-distress",
            "title": title,
            "body": "\n".join(body_lines),
            "county": county,
            "state": state,
            "fips": fips,
            "distress_score": score,
            "signal_strength": "strong" if score >= 5 else "moderate",
            "tags": ["budget-distress", state.lower()] + [r.split(" (")[0].replace(" ", "-") for r in reasons],
        }

        if args.dry_run:
            print(f"  [{score}/10] {county_name}: {', '.join(reasons)}")

        entries.append(entry)

    entries.sort(key=lambda x: -x["distress_score"])
    print(f"\nTotal: {len(entries)} counties with distress score >= {args.min_score}")

    if args.dry_run:
        return

    if entries:
        # Write directly to KB as markdown files (not JSON for kb import)
        budget_dir = Path(__file__).parent.parent / "budget"
        budget_dir.mkdir(parents=True, exist_ok=True)

        # Clear existing auto-generated budget entries (keep manually created ones)
        for existing in budget_dir.glob("*-usda-distress-*.md"):
            existing.unlink()

        written = 0
        for entry in entries:
            slug = re.sub(r"[^a-z0-9]+", "-", entry["title"].lower().split("—")[0].strip())[:60].strip("-")
            slug = f"{slug}-usda-distress"
            filepath = budget_dir / f"{slug}.md"

            lines = [
                "---",
                f'id: {slug}',
                f'title: "{entry["title"]}"',
                f'type: budget-distress',
                f'county: "{entry["county"]}"',
                f'state: "{entry["state"]}"',
                f'fips: "{entry["fips"]}"',
                f'signal_strength: "{entry["signal_strength"]}"',
                f'source: "USDA ERS County Typology Codes 2025"',
                f'source_url: "https://www.ers.usda.gov/data-products/county-typology-codes/"',
                "tags:",
            ]
            for tag in entry["tags"]:
                lines.append(f"- {tag}")
            lines.extend([
                f"importance: 5",
                "---",
                "",
                entry["body"],
                "",
            ])
            filepath.write_text("\n".join(lines), encoding="utf-8")
            written += 1

        print(f"Wrote {written} entries to {budget_dir}/")

        # Also write JSON for reference
        with open(args.output, "w") as f:
            json.dump(entries, f, indent=2)
        print(f"JSON copy saved to {args.output}")


if __name__ == "__main__":
    main()
