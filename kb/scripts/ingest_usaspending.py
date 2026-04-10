#!/usr/bin/env python3
"""
Ingest ANC/Akima contract awards from USAspending.gov API.

Searches for contracts awarded to Alaska Native Corporation subsidiaries
(Akima, Nana, ASRC, etc.) in the detention/corrections/security space.
Creates anc-contract entries in the detention-pipeline-research KB.

Usage:
    python ingest_usaspending.py                  # default: last 365 days
    python ingest_usaspending.py --days 90        # last 90 days
    python ingest_usaspending.py --dry-run        # preview without creating entries
    python ingest_usaspending.py --since 2025-01-01
"""

import argparse
import csv
import json
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import Request, urlopen

API_BASE = "https://api.usaspending.gov/api/v2"

# Alaska Native Corporation subsidiaries involved in federal contracting
# Akima (Nana Regional Corp) is the primary target — Kemmerer WY partner
ANC_RECIPIENTS = [
    "AKIMA",
    "NANA REGIONAL",
    "NANA MANAGEMENT",
    "ARCTIC SLOPE REGIONAL",
    "ASRC",
    "CALISTA",
    "DOYON",
    "SEALASKA",
    "AHTNA",
    "CHUGACH",
    "COOK INLET REGION",
    "CIRI",
    "BRISTOL BAY NATIVE",
    "BBNC",
    "ALEUT",
    "KONIAG",
]

# NAICS codes relevant to detention/corrections/security
RELEVANT_NAICS = [
    "922140",  # Correctional Institutions
    "561210",  # Facilities Support Services
    "561612",  # Security Guards and Patrol Services
    "561613",  # Armored Car Services
    "236220",  # Commercial/Institutional Building Construction
    "531120",  # Lessors of Nonresidential Buildings
    "488510",  # Freight Transportation Arrangement
    "561110",  # Office Administrative Services
]

# FIPS lookup — load from Census file if available, otherwise skip
FIPS_MAP = {}

def load_fips_map():
    """Load county FIPS lookup from Census reference file."""
    global FIPS_MAP
    fips_path = Path("/tmp/county_fips.txt")
    if not fips_path.exists():
        return
    import re
    with open(fips_path) as f:
        reader = csv.DictReader(f, delimiter="|")
        for r in reader:
            state = r["STATE"]
            county = r["COUNTYNAME"]
            normalized = re.sub(
                r"\s+(County|Parish|Borough|Census Area|Municipality|city|City and Borough)$",
                "", county
            ).strip().lower()
            fips = r["STATEFP"] + r["COUNTYFP"]
            FIPS_MAP[(state, normalized)] = fips


def search_awards(recipient_text, start_date, end_date, page=1):
    """Search USAspending for awards by recipient name."""
    url = f"{API_BASE}/search/spending_by_award/"
    payload = {
        "subawards": False,
        "limit": 50,
        "page": page,
        "order": "desc",
        "sort": "Award Amount",
        "filters": {
            "recipient_search_text": [recipient_text],
            "time_period": [
                {"start_date": start_date, "end_date": end_date}
            ],
            "award_type_codes": ["A", "B", "C", "D"],  # Contracts only
        },
        "fields": [
            "Award ID",
            "Recipient Name",
            "Award Amount",
            "Total Outlays",
            "Description",
            "Start Date",
            "End Date",
            "Awarding Agency",
            "Awarding Sub Agency",
            "Place of Performance State Code",
            "Place of Performance County",
            "generated_internal_id",
        ],
    }

    req = Request(url, data=json.dumps(payload).encode(), method="POST")
    req.add_header("Content-Type", "application/json")

    try:
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except Exception as e:
        print(f"  API error for '{recipient_text}': {e}", file=sys.stderr)
        return {"results": []}


def resolve_fips(state_code, county_name):
    """Resolve state+county to FIPS code."""
    if not FIPS_MAP or not county_name:
        return ""
    county_norm = county_name.lower().strip()
    fips = FIPS_MAP.get((state_code, county_norm))
    if not fips:
        # Try without common suffixes
        for suffix in [" county", " parish", " borough"]:
            if county_norm.endswith(suffix):
                fips = FIPS_MAP.get((state_code, county_norm[:-len(suffix)]))
                if fips:
                    break
    return fips or ""


def create_entry(award, dry_run=False):
    """Create a detention-pipeline-research entry from a USAspending award."""
    state = award.get("Place of Performance State Code", "")
    county = award.get("Place of Performance County", "")
    fips = resolve_fips(state, county)
    recipient = award.get("Recipient Name", "Unknown")
    award_id = award.get("Award ID", "")
    amount = award.get("Award Amount", 0)
    agency = award.get("Awarding Agency", "")
    sub_agency = award.get("Awarding Sub Agency", "")
    description = award.get("Description", "")
    start_date = award.get("Start Date", "")
    end_date = award.get("End Date", "")

    # Determine which ANC parent this is
    parent_anc = ""
    recipient_upper = recipient.upper()
    if "AKIMA" in recipient_upper:
        parent_anc = "Nana Regional Corporation"
    elif "NANA" in recipient_upper:
        parent_anc = "Nana Regional Corporation"
    elif "ARCTIC SLOPE" in recipient_upper or "ASRC" in recipient_upper:
        parent_anc = "Arctic Slope Regional Corporation"
    elif "CALISTA" in recipient_upper:
        parent_anc = "Calista Corporation"
    elif "DOYON" in recipient_upper:
        parent_anc = "Doyon Limited"
    elif "CHUGACH" in recipient_upper:
        parent_anc = "Chugach Alaska Corporation"

    # Signal strength based on agency relevance
    signal = "weak"
    if sub_agency and any(kw in sub_agency.upper() for kw in ["ICE", "IMMIGRATION", "CUSTOMS", "HOMELAND"]):
        signal = "strong"
    elif agency and "HOMELAND" in agency.upper():
        signal = "moderate"
    elif any(kw in description.upper() for kw in ["DETENTION", "CORRECTIONS", "SECURITY", "FACILITY"]):
        signal = "moderate"

    title = f"{recipient} — {award_id} ({state})"
    if amount:
        title += f" ${amount:,.0f}"

    entry = {
        "entry_type": "anc-contract",
        "title": title,
        "body": f"USAspending contract award.\n\nRecipient: {recipient}\nAward ID: {award_id}\nAmount: ${amount:,.2f}\nAgency: {agency}\nSub-Agency: {sub_agency}\nDescription: {description}\nPeriod: {start_date} to {end_date}\nLocation: {county}, {state}",
        "county": county,
        "state": state,
        "fips": fips,
        "contractor": recipient,
        "parent_anc": parent_anc,
        "contract_value": f"${amount:,.2f}" if amount else "",
        "contract_type": "federal-contract",
        "award_date": start_date,
        "usaspending_id": award_id,
        "source": f"USAspending.gov (award {award_id})",
        "signal_strength": signal,
        "notes": description,
        "tags": ["anc-contract", state.lower() if state else "unknown"],
    }

    if dry_run:
        print(f"  [DRY RUN] {title}")
        print(f"    FIPS: {fips}, Signal: {signal}, Agency: {sub_agency or agency}")
        return entry

    # Write as JSON for kb import
    return entry


def main():
    parser = argparse.ArgumentParser(description="Ingest ANC contracts from USAspending.gov")
    parser.add_argument("--days", type=int, default=365, help="Look back N days (default: 365)")
    parser.add_argument("--since", type=str, help="Start date (YYYY-MM-DD), overrides --days")
    parser.add_argument("--dry-run", action="store_true", help="Preview without creating entries")
    parser.add_argument("--output", type=str, default="/tmp/anc_contracts.json", help="Output JSON file")
    args = parser.parse_args()

    load_fips_map()

    if args.since:
        start_date = args.since
    else:
        start_date = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")

    print(f"Searching USAspending.gov for ANC contracts: {start_date} to {end_date}")

    all_entries = []
    seen_award_ids = set()

    for recipient in ANC_RECIPIENTS:
        print(f"\n  Searching: {recipient}")
        page = 1
        while True:
            data = search_awards(recipient, start_date, end_date, page=page)
            results = data.get("results", [])
            if not results:
                break

            for award in results:
                award_id = award.get("Award ID", "")
                if award_id in seen_award_ids:
                    continue
                seen_award_ids.add(award_id)
                entry = create_entry(award, dry_run=args.dry_run)
                all_entries.append(entry)

            has_next = data.get("page_metadata", {}).get("hasNext", False)
            if not has_next:
                break
            page += 1
            time.sleep(0.5)  # Rate limiting

        time.sleep(0.3)

    print(f"\nFound {len(all_entries)} contract awards")

    if not args.dry_run and all_entries:
        with open(args.output, "w") as f:
            json.dump(all_entries, f, indent=2)
        print(f"Saved to {args.output}")
        print(f"\nTo import: kb import {args.output} --kb detention-pipeline-research")

    return all_entries


if __name__ == "__main__":
    main()
