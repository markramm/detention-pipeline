#!/usr/bin/env python3
"""
Ingest ALL ICE contract awards from USAspending.gov API.

Searches for all contracts where the awarding sub-agency is
U.S. Immigration and Customs Enforcement, regardless of contractor.
This is a superset of the ANC-only ingest_usaspending.py.

Usage:
    python ingest_ice_contracts.py                  # default: last 365 days
    python ingest_ice_contracts.py --days 90        # last 90 days
    python ingest_ice_contracts.py --dry-run        # preview without creating entries
    python ingest_ice_contracts.py --since 2025-01-01
    python ingest_ice_contracts.py --summary        # print contractor summary table
"""

import argparse
import csv
import json
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import Request, urlopen

API_BASE = "https://api.usaspending.gov/api/v2"

# NAICS codes relevant to detention/corrections/security/facilities
DETENTION_NAICS = {
    "922140": "Correctional Institutions",
    "561210": "Facilities Support Services",
    "561612": "Security Guards and Patrol Services",
    "561613": "Armored Car Services",
    "236220": "Commercial/Institutional Building Construction",
    "531120": "Lessors of Nonresidential Buildings",
    "621999": "Miscellaneous Health Practitioners (medical staffing)",
    "621111": "Offices of Physicians",
    "488510": "Freight Transportation Arrangement",
    "481211": "Nonscheduled Chartered Passenger Air",
    "561110": "Office Administrative Services",
    "541611": "Management Consulting",
    "541519": "Other Computer Related Services",
    "518210": "Data Processing & Hosting",
    "561320": "Temporary Staffing Services",
}

# Keywords in contract descriptions that signal detention relevance
DETENTION_KEYWORDS = [
    r"detention", r"detainee", r"corrections", r"correctional",
    r"custody", r"processing center", r"processing facility",
    r"bed space", r"bed capacity", r"IGSA",
    r"removal operation", r"ERO", r"deportation",
    r"guard service", r"security guard",
    r"facility management", r"facility operations",
    r"medical staff", r"health service",
    r"ankle monitor", r"electronic monitor", r"ISAP",
    r"transport.*detain", r"transport.*alien",
    r"charter.*flight", r"air operations",
    r"case management.*immig",
]

# Known major detention contractors for enhanced classification
KNOWN_DETENTION_CONTRACTORS = {
    "GEO GROUP": "private-prison",
    "CORECIVIC": "private-prison",
    "MANAGEMENT & TRAINING": "private-prison",
    "GARDAWORLD": "private-security",
    "KVG LLC": "private-security",
    "G4S": "private-security",
    "PARAGON": "guard-services",
    "AHTNA": "anc",
    "AKIMA": "anc",
    "NANA": "anc",
    "ARCTIC SLOPE": "anc",
    "ASRC": "anc",
    "CALISTA": "anc",
    "DOYON": "anc",
    "SEALASKA": "anc",
    "CHUGACH": "anc",
    "COOK INLET": "anc",
    "CIRI": "anc",
    "BRISTOL BAY": "anc",
    "BBNC": "anc",
    "ALEUT": "anc",
    "KONIAG": "anc",
    "B.I. INCORPORATED": "monitoring",
    "BI INCORPORATED": "monitoring",
    "MVM": "transport",
    "CSI AVIATION": "air-operations",
    "CLASSIC AIR": "air-operations",
    "GLOBAL AVIATION": "air-operations",
    "SWIFT AIR": "air-operations",
    "WORLD ATLANTIC": "air-operations",
    "PALANTIR": "technology",
    "DELOITTE": "consulting",
    "BOOZ ALLEN": "consulting",
    "LEIDOS": "technology",
    "PERATON": "technology",
    "AMENTUM": "facilities",
    "STG INTERNATIONAL": "medical",
    "IMMIGRATION CENTERS OF AMERICA": "private-prison",
    "LASALLE": "private-prison",
    "SABOT": "consulting",
    "ACQUISITION LOGISTICS": "facilities",
}

# FIPS lookup
FIPS_MAP = {}


def load_fips_map():
    """Load county FIPS lookup from Census reference file."""
    global FIPS_MAP
    fips_path = Path("/tmp/county_fips.txt")
    if not fips_path.exists():
        return
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


def search_ice_awards(start_date, end_date, page=1):
    """Search USAspending for all ICE contract awards."""
    url = f"{API_BASE}/search/spending_by_award/"
    payload = {
        "subawards": False,
        "limit": 100,
        "page": page,
        "order": "desc",
        "sort": "Award Amount",
        "filters": {
            "agencies": [
                {
                    "type": "awarding",
                    "tier": "subtier",
                    "name": "U.S. Immigration and Customs Enforcement",
                }
            ],
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
        print(f"  API error (page {page}): {e}", file=sys.stderr)
        return {"results": []}


def resolve_fips(state_code, county_name):
    """Resolve state+county to FIPS code."""
    if not FIPS_MAP or not county_name:
        return ""
    county_norm = county_name.lower().strip()
    fips = FIPS_MAP.get((state_code, county_norm))
    if not fips:
        for suffix in [" county", " parish", " borough"]:
            if county_norm.endswith(suffix):
                fips = FIPS_MAP.get((state_code, county_norm[:-len(suffix)]))
                if fips:
                    break
    return fips or ""


def classify_contractor(recipient_name):
    """Classify a contractor into a category based on known names."""
    upper = recipient_name.upper()
    for pattern, category in KNOWN_DETENTION_CONTRACTORS.items():
        if pattern in upper:
            return category
    return "other"


def classify_contract(award):
    """Classify a contract by detention relevance based on description."""
    description = (award.get("Description") or "").upper()

    # Check for detention keywords in description
    for kw in DETENTION_KEYWORDS:
        if re.search(kw, description, re.IGNORECASE):
            return "detention-related"

    return "other-ice"


def create_entry(award, dry_run=False):
    """Create a detention-pipeline-research entry from a USAspending award."""
    state = award.get("Place of Performance State Code", "")
    county = award.get("Place of Performance County", "")
    fips = resolve_fips(state, county)
    recipient = award.get("Recipient Name", "Unknown")
    award_id = award.get("Award ID", "")
    amount = award.get("Award Amount", 0) or 0
    agency = award.get("Awarding Agency", "")
    sub_agency = award.get("Awarding Sub Agency", "")
    description = award.get("Description", "") or ""
    start_date = award.get("Start Date", "")
    end_date = award.get("End Date", "")
    contractor_type = classify_contractor(recipient)
    contract_class = classify_contract(award)

    # Signal strength based on contract classification
    if contract_class == "detention-related":
        signal = "strong"
    elif contract_class == "detention-adjacent":
        signal = "moderate"
    else:
        signal = "weak"

    # Boost signal for known detention contractors
    if contractor_type in ("private-prison", "private-security", "guard-services", "monitoring"):
        signal = "strong"

    title = f"{recipient} — {award_id} ({state})"
    if amount:
        title += f" ${amount:,.0f}"

    entry = {
        "entry_type": "ice-contract",
        "title": title,
        "body": (
            f"ICE contract award.\n\n"
            f"Recipient: {recipient}\n"
            f"Award ID: {award_id}\n"
            f"Amount: ${amount:,.2f}\n"
            f"Agency: {agency}\n"
            f"Sub-Agency: {sub_agency}\n"
            f"Description: {description}\n"
            f"Period: {start_date} to {end_date}\n"
            f"Location: {county}, {state}"
        ),
        "county": county,
        "state": state,
        "fips": fips,
        "contractor": recipient,
        "contractor_type": contractor_type,
        "contract_class": contract_class,
        "contract_value": f"${amount:,.2f}" if amount else "",
        "contract_type": "federal-contract",
        "award_date": start_date,
        "usaspending_id": award_id,
        "source": f"USAspending.gov (award {award_id})",
        "signal_strength": signal,
        "notes": description,
        "tags": ["ice-contract", contractor_type, contract_class,
                 state.lower() if state else "unknown"],
    }

    if dry_run:
        print(f"  [{signal.upper():8s}] [{contractor_type:15s}] [{contract_class:18s}] "
              f"${amount:>15,.0f}  {recipient[:40]}")

    return entry


def print_summary(entries):
    """Print summary tables of contractors and contract classifications."""
    # By contractor
    contractor_totals = defaultdict(lambda: {"count": 0, "value": 0, "type": ""})
    for e in entries:
        name = e["contractor"]
        val = float(e["contract_value"].replace("$", "").replace(",", "")) if e["contract_value"] else 0
        contractor_totals[name]["count"] += 1
        contractor_totals[name]["value"] += val
        contractor_totals[name]["type"] = e["contractor_type"]

    print("\n" + "=" * 100)
    print("  CONTRACTOR SUMMARY")
    print("=" * 100)
    print(f"  {'Contractor':<45} {'Type':<18} {'Contracts':>10} {'Total Value':>18}")
    print("-" * 100)

    sorted_contractors = sorted(
        contractor_totals.items(), key=lambda x: x[1]["value"], reverse=True
    )
    for name, info in sorted_contractors[:50]:
        print(f"  {name[:44]:<45} {info['type']:<18} {info['count']:>10} "
              f"${info['value']:>16,.0f}")

    if len(sorted_contractors) > 50:
        print(f"  ... and {len(sorted_contractors) - 50} more contractors")

    # By classification
    class_totals = defaultdict(lambda: {"count": 0, "value": 0})
    for e in entries:
        cls = e["contract_class"]
        val = float(e["contract_value"].replace("$", "").replace(",", "")) if e["contract_value"] else 0
        class_totals[cls]["count"] += 1
        class_totals[cls]["value"] += val

    print(f"\n  {'Classification':<25} {'Contracts':>10} {'Total Value':>18}")
    print("-" * 60)
    for cls, info in sorted(class_totals.items(), key=lambda x: x[1]["value"], reverse=True):
        print(f"  {cls:<25} {info['count']:>10} ${info['value']:>16,.0f}")

    # By contractor type
    type_totals = defaultdict(lambda: {"count": 0, "value": 0, "contractors": set()})
    for e in entries:
        ct = e["contractor_type"]
        val = float(e["contract_value"].replace("$", "").replace(",", "")) if e["contract_value"] else 0
        type_totals[ct]["count"] += 1
        type_totals[ct]["value"] += val
        type_totals[ct]["contractors"].add(e["contractor"])

    print(f"\n  {'Contractor Type':<20} {'Contractors':>12} {'Contracts':>10} {'Total Value':>18}")
    print("-" * 70)
    for ct, info in sorted(type_totals.items(), key=lambda x: x[1]["value"], reverse=True):
        print(f"  {ct:<20} {len(info['contractors']):>12} {info['count']:>10} "
              f"${info['value']:>16,.0f}")

    print(f"\n  TOTAL: {len(entries)} contracts from {len(contractor_totals)} contractors, "
          f"${sum(c['value'] for c in contractor_totals.values()):,.0f}")


def main():
    parser = argparse.ArgumentParser(description="Ingest all ICE contracts from USAspending.gov")
    parser.add_argument("--days", type=int, default=365, help="Look back N days (default: 365)")
    parser.add_argument("--since", type=str, help="Start date (YYYY-MM-DD), overrides --days")
    parser.add_argument("--dry-run", action="store_true", help="Preview without creating entries")
    parser.add_argument("--summary", action="store_true", help="Print contractor summary table")
    parser.add_argument("--output", type=str, default="/tmp/ice_contracts.json",
                        help="Output JSON file")
    args = parser.parse_args()

    load_fips_map()

    if args.since:
        start_date = args.since
    else:
        start_date = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")

    print(f"Searching USAspending.gov for ALL ICE contracts: {start_date} to {end_date}")

    all_entries = []
    seen_award_ids = set()
    page = 1

    while True:
        print(f"\n  Fetching page {page}...")
        data = search_ice_awards(start_date, end_date, page=page)
        results = data.get("results", [])

        if not results:
            break

        total = data.get("page_metadata", {}).get("total", "?")
        if page == 1:
            print(f"  Total awards found: {total}")

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

    print(f"\nFound {len(all_entries)} contract awards")

    if args.summary or args.dry_run:
        print_summary(all_entries)

    if not args.dry_run and all_entries:
        with open(args.output, "w") as f:
            json.dump(all_entries, f, indent=2)
        print(f"\nSaved to {args.output}")
        print(f"To import: kb import {args.output} -k detention-pipeline-research")

    return all_entries


if __name__ == "__main__":
    main()
