#!/usr/bin/env python3
"""
Monitor job postings for detention pipeline signals.

Searches Google (via SerpAPI or direct) for job postings from Sabot Consulting
and similar detention consultants. Geographic specificity in postings reveals
where the pipeline is active.

Usage:
    python ingest_jobs.py                    # search all configured queries
    python ingest_jobs.py --dry-run          # preview only
    python ingest_jobs.py --source sabot     # search only Sabot postings

Key signals:
  - "ICE Detention Compliance Operations Consultant" = active project
  - "Project Manager - Detention & Corrections" = construction phase
  - Requirements mentioning "NDS 2025" = ICE-specific work
  - Geographic specificity (e.g. "Florida hybrid", "Laredo TX") = local pitch
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.parse import quote_plus

# State abbreviation to FIPS state code mapping
STATE_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
    "CO": "08", "CT": "09", "DE": "10", "FL": "12", "GA": "13",
    "HI": "15", "ID": "16", "IL": "17", "IN": "18", "IA": "19",
    "KS": "20", "KY": "21", "LA": "22", "ME": "23", "MD": "24",
    "MA": "25", "MI": "26", "MN": "27", "MS": "28", "MO": "29",
    "MT": "30", "NE": "31", "NV": "32", "NH": "33", "NJ": "34",
    "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
    "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45",
    "SD": "46", "TN": "47", "TX": "48", "UT": "49", "VT": "50",
    "VA": "51", "WA": "53", "WV": "54", "WI": "55", "WY": "56",
}

# Search queries — company name + keywords
SEARCH_QUERIES = [
    {
        "name": "sabot",
        "queries": [
            '"Sabot Consulting" job',
            '"Sabot Consulting" hiring',
            '"Sabot Consulting" "ICE detention"',
            '"Sabot Consulting" "compliance operations"',
            '"Sabot" "detention" "consultant"',
        ],
    },
    {
        "name": "detention-consultant",
        "queries": [
            '"ICE Detention Compliance Operations Consultant"',
            '"detention compliance" consultant job',
            '"NDS 2025" job OR hiring',
            '"national detention standards" consultant',
            '"Project Manager" "detention" "corrections" job',
            '"detention facility" "project manager" consultant',
        ],
    },
    {
        "name": "anc-detention",
        "queries": [
            'Akima detention OR corrections job',
            '"Nana Management" detention OR corrections',
            '"ASRC Federal" detention facility',
        ],
    },
]

# Known Sabot job postings (from research) to seed the KB
KNOWN_POSTINGS = [
    {
        "title": "ICE Detention Compliance Operations Consultant — Florida (hybrid)",
        "employer": "Sabot Consulting",
        "position_title": "ICE Detention Compliance Operations Consultant",
        "location": "Florida (hybrid)",
        "state": "FL",
        "requirements": "NDS 2025, ICE detention facility compliance, IGSA agreements",
        "source": "LinkedIn/Indeed, identified via RAMM research",
        "signal_strength": "strong",
        "notes": "Florida hybrid position indicates active FL pipeline. NDS 2025 requirement is ICE-specific.",
    },
    {
        "title": "ICE Detention Compliance Operations Consultant — Laredo, TX",
        "employer": "Sabot Consulting",
        "position_title": "ICE Detention Compliance Operations Consultant",
        "location": "Laredo, TX",
        "state": "TX",
        "county": "Webb",
        "fips": "48479",
        "requirements": "NDS 2025, ICE detention facility compliance",
        "source": "LinkedIn/Indeed, identified via RAMM research",
        "signal_strength": "strong",
        "notes": "Laredo TX is a border city. Webb County already has an IGSA. This may indicate expansion or new facility.",
    },
    {
        "title": "Project Manager - Detention & Corrections",
        "employer": "Sabot Consulting",
        "position_title": "Project Manager - Detention & Corrections",
        "location": "United States",
        "state": "",
        "requirements": "Detention facility construction/renovation project management",
        "source": "LinkedIn/Indeed, identified via RAMM research",
        "signal_strength": "moderate",
        "notes": "Generic location but detention-specific PM role indicates active construction pipeline.",
    },
]


def extract_state(text):
    """Try to extract a US state from text."""
    # Check for state abbreviations
    for abbr in STATE_FIPS:
        if re.search(rf'\b{abbr}\b', text):
            return abbr
    # Check for common state names
    state_names = {
        "Florida": "FL", "Texas": "TX", "Georgia": "GA",
        "Louisiana": "LA", "Arizona": "AZ", "Wyoming": "WY",
        "California": "CA", "New York": "NY", "New Mexico": "NM",
    }
    for name, abbr in state_names.items():
        if name.lower() in text.lower():
            return abbr
    return ""


def create_entry_from_known(posting):
    """Create a KB entry from a known job posting."""
    state = posting.get("state", "")
    return {
        "entry_type": "job-posting",
        "title": posting["title"],
        "body": f"Job posting: {posting['position_title']}\n\nEmployer: {posting['employer']}\nLocation: {posting['location']}\nRequirements: {posting['requirements']}\n\n{posting['notes']}",
        "state": state,
        "county": posting.get("county", ""),
        "fips": posting.get("fips", ""),
        "employer": posting["employer"],
        "position_title": posting["position_title"],
        "location": posting["location"],
        "requirements": posting["requirements"],
        "source": posting["source"],
        "signal_strength": posting["signal_strength"],
        "notes": posting["notes"],
        "tags": ["job-posting", state.lower() if state else "national", posting["employer"].lower().replace(" ", "-")],
    }


def main():
    parser = argparse.ArgumentParser(description="Monitor detention consultant job postings")
    parser.add_argument("--dry-run", action="store_true", help="Preview without creating entries")
    parser.add_argument("--source", type=str, help="Search only this source (sabot, detention-consultant, anc-detention)")
    parser.add_argument("--seed-known", action="store_true", help="Seed KB with known postings from research")
    parser.add_argument("--output", type=str, default="/tmp/job_postings.json", help="Output JSON file")
    args = parser.parse_args()

    all_entries = []

    if args.seed_known:
        print(f"Seeding {len(KNOWN_POSTINGS)} known job postings...")
        for posting in KNOWN_POSTINGS:
            entry = create_entry_from_known(posting)
            all_entries.append(entry)
            if args.dry_run:
                print(f"  [{entry['signal_strength'].upper()}] {entry['title']}")
                print(f"    State: {entry['state']}, Employer: {entry['employer']}")

    # Note: Live search requires SerpAPI key or similar.
    # For now, this script seeds known postings and provides the framework
    # for live monitoring once an API key is configured.
    if not args.seed_known:
        print("Live job search requires SerpAPI key (not yet configured).")
        print("Use --seed-known to import known postings from research.")
        print("\nConfigured search queries:")
        sources = SEARCH_QUERIES
        if args.source:
            sources = [s for s in sources if s["name"] == args.source]
        for source in sources:
            print(f"\n  {source['name']}:")
            for q in source["queries"]:
                print(f"    {q}")

    print(f"\nTotal: {len(all_entries)} entries")

    if not args.dry_run and all_entries:
        with open(args.output, "w") as f:
            json.dump(all_entries, f, indent=2)
        print(f"Saved to {args.output}")
        print(f"\nTo import: kb import {args.output} --kb detention-pipeline-research")


if __name__ == "__main__":
    main()
