#!/usr/bin/env python3
"""
Discover and validate Legistar client IDs for US counties and cities.

Legistar (by Granicus) powers meeting management for hundreds of local
governments, but there is no public directory of client IDs. This script
discovers them using three strategies:

1. Web search: Search for "{county} legistar" and extract subdomains
   from *.legistar.com URLs in results
2. URL probing: Test common naming patterns against legistar.com
3. API validation: Confirm discovered IDs have working API access

The output is a JSON mapping file suitable for use by civic tech tools,
journalists, and researchers.

Usage:
    python discover_legistar.py                        # validate known portals
    python discover_legistar.py --discover             # search for new portals
    python discover_legistar.py --discover --state FL  # search specific state
    python discover_legistar.py --validate-only        # re-validate existing
    python discover_legistar.py --census-file fips.csv --discover  # full discovery

Requires: requests, beautifulsoup4
    pip install requests beautifulsoup4
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import requests
    from bs4 import BeautifulSoup
    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False

# Default output location
DEFAULT_OUTPUT = "/Users/markr/tcp-kb-internal/detention-pipeline-research/data/legistar_map.json"

# Rate limiting
SEARCH_DELAY = 2.0    # seconds between web searches (be polite to Google)
PROBE_DELAY = 0.15    # seconds between URL probes
API_DELAY = 0.2       # seconds between API validation calls

# States with active detention infrastructure (priority for discovery)
PRIORITY_STATES = [
    "FL", "TX", "GA", "AZ", "CA", "VA", "CO", "NM", "LA",
    "NJ", "IL", "NC", "MI", "PA", "MD", "NY", "OH", "MO",
    "IN", "UT", "WA", "OR", "SC", "AL", "MS", "TN", "KY",
    "WI", "MN", "OK", "KS", "NE", "WY", "MT", "ID", "NH",
]


# --- Known validated portals (as of 2026-04-11) ---
KNOWN_PORTALS = [
    # County-level — Florida
    ("broward", "Broward County", "FL", "12011", "county"),
    ("miamidade", "Miami-Dade County", "FL", "12086", "county"),
    ("hillsboroughcounty", "Hillsborough County", "FL", "12057", "county"),
    ("martin", "Martin County", "FL", "12085", "county"),
    ("pinellas", "Pinellas County", "FL", "12103", "county"),
    ("brevardfl", "Brevard County", "FL", "12009", "county"),
    ("ircgov", "Indian River County", "FL", "12061", "county"),
    ("hernandocountyfl", "Hernando County", "FL", "12053", "county"),
    ("occompt", "Orange County", "FL", "12095", "county"),
    ("seminolecountyfl", "Seminole County", "FL", "12117", "county"),
    ("polkcountyfl", "Polk County", "FL", "12105", "county"),
    # County-level — California
    ("sdcounty", "San Diego County", "CA", "06073", "county"),
    ("lacounty", "Los Angeles County", "CA", "06037", "county"),
    ("sacramento", "Sacramento County", "CA", "06067", "county"),
    ("sanbernardino", "San Bernardino County", "CA", "06071", "county"),
    # County-level — Arizona
    ("maricopa", "Maricopa County", "AZ", "04013", "county"),
    # County-level — Georgia
    ("fulton", "Fulton County", "GA", "13121", "county"),
    ("dekalbcountyga", "DeKalb County", "GA", "13089", "county"),
    # County-level — Virginia
    ("richmondva", "Richmond", "VA", "51760", "county"),
    ("albemarle", "Albemarle County", "VA", "51003", "county"),
    ("salem", "Salem", "VA", "51775", "county"),
    # County-level — Colorado
    ("arapahoe", "Arapahoe County", "CO", "08005", "county"),
    # County-level — Texas
    ("harriscountytx", "Harris County", "TX", "48201", "county"),
    ("brazoriacountytx", "Brazoria County", "TX", "48039", "county"),
    ("galvestoncountytx", "Galveston County", "TX", "48167", "county"),
    ("lubbockcounty", "Lubbock County", "TX", "48303", "county"),
    # County-level — Illinois
    ("cook-county", "Cook County", "IL", "17031", "county"),
    # County-level — North Carolina
    ("guilford", "Guilford County", "NC", "37081", "county"),
    ("cumberlandcounty", "Cumberland County", "NC", "37051", "county"),
    # City-level
    ("detroit", "Detroit", "MI", "26163", "city"),
    ("pompano", "Pompano Beach", "FL", "12011", "city"),
    ("fortlauderdale", "Fort Lauderdale", "FL", "12011", "city"),
    ("delraybeach", "Delray Beach", "FL", "12099", "city"),
    ("miramar", "Miramar", "FL", "12011", "city"),
    ("mesa", "Mesa", "AZ", "04013", "city"),
    ("atlantaga", "Atlanta", "GA", "13121", "city"),
    ("marietta", "Marietta", "GA", "13067", "city"),
    ("columbus", "Columbus", "GA", "13215", "city"),
    ("newark", "Newark", "NJ", "34013", "city"),
    ("elpasotexas", "El Paso", "TX", "48141", "city"),
    ("sanantonio", "San Antonio", "TX", "48029", "city"),
    ("kansascity", "Kansas City", "MO", "29095", "city"),
    ("chicago", "Chicago", "IL", "17031", "city"),
    ("roundrock", "Round Rock", "TX", "48491", "city"),
]


def extract_legistar_subdomains(html):
    """Extract Legistar subdomains from HTML search results."""
    subdomains = set()
    # Match URLs like https://something.legistar.com
    pattern = r'https?://([a-zA-Z0-9_-]+)\.legistar\.com'
    for match in re.finditer(pattern, html):
        subdomain = match.group(1).lower()
        # Skip non-client subdomains
        if subdomain in ('webapi', 'support', 'www', 'help', 'api'):
            continue
        subdomains.add(subdomain)
    return subdomains


def search_google(query, session):
    """Search Google and return HTML of results page."""
    url = "https://www.google.com/search"
    params = {"q": query, "num": 20}
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36",
    }
    try:
        resp = session.get(url, params=params, headers=headers, timeout=10)
        if resp.status_code == 200:
            return resp.text
        elif resp.status_code == 429:
            print("    Rate limited by Google, waiting 30s...")
            time.sleep(30)
            return None
    except Exception as e:
        print(f"    Search error: {e}")
    return None


def search_bing(query, session):
    """Search Bing and return HTML of results page."""
    url = "https://www.bing.com/search"
    params = {"q": query, "count": 20}
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36",
    }
    try:
        resp = session.get(url, params=params, headers=headers, timeout=10)
        if resp.status_code == 200:
            return resp.text
    except Exception as e:
        print(f"    Bing search error: {e}")
    return None


def discover_via_search(county_name, state, session):
    """Try to find a Legistar portal for a county via web search."""
    queries = [
        f'"{county_name}" "{state}" site:legistar.com',
        f'"{county_name}" legistar.com county commission',
    ]

    all_subdomains = set()
    for query in queries:
        # Try Bing first (less aggressive rate limiting)
        html = search_bing(query, session)
        if html:
            subdomains = extract_legistar_subdomains(html)
            all_subdomains.update(subdomains)

        if not all_subdomains:
            # Fall back to Google
            html = search_google(query, session)
            if html:
                subdomains = extract_legistar_subdomains(html)
                all_subdomains.update(subdomains)

        if all_subdomains:
            break  # Found something, no need for more queries

        time.sleep(SEARCH_DELAY)

    return all_subdomains


def probe_legistar_web(candidate):
    """Check if a Legistar web portal exists (faster than API check)."""
    url = f"https://{candidate}.legistar.com"
    try:
        resp = requests.head(url, timeout=5, allow_redirects=True)
        # Legistar returns 200 for valid clients, various errors for invalid
        return resp.status_code == 200
    except Exception:
        return False


def validate_api(client_id):
    """Validate a Legistar client ID has working API access."""
    url = f"https://webapi.legistar.com/v1/{client_id}/Bodies"
    try:
        resp = requests.get(url, headers={"Accept": "application/json"}, timeout=8)
        return resp.status_code == 200
    except Exception:
        return False


def check_has_events(client_id):
    """Check if a Legistar client has any events (meetings)."""
    url = f"https://webapi.legistar.com/v1/{client_id}/Events?$top=1"
    try:
        resp = requests.get(url, headers={"Accept": "application/json"}, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            return len(data) > 0
    except Exception:
        pass
    return False


def generate_candidates(name, state, entity_type="county"):
    """Generate candidate Legistar client IDs for a county or city."""
    clean = name.lower()
    for suffix in [' county', ' parish', ' city', ' borough', ' municipality']:
        clean = clean.replace(suffix, '')
    clean = clean.strip()

    nospace = re.sub(r'[^a-z0-9]', '', clean)
    dashed = re.sub(r'[^a-z0-9-]', '', clean.replace(' ', '-'))
    st = state.lower()

    if entity_type == "county":
        candidates = [
            nospace, f"{nospace}county", f"{nospace}county{st}",
            f"{nospace}{st}", f"{nospace}gov", f"{nospace}countygov",
            f"{nospace}parish", f"{nospace}parish{st}",
            dashed, f"{dashed}-county",
        ]
    else:
        candidates = [
            nospace, f"{nospace}{st}", f"{nospace}gov",
            dashed, f"cityof{nospace}",
        ]

    # Handle St./Saint variations
    if 'saint' in clean or clean.startswith('st ') or clean.startswith('st.'):
        st_ver = re.sub(r'^(saint|st\.?)\s*', 'st', nospace)
        candidates.extend([st_ver, f"{st_ver}county", f"{st_ver}parish"])

    # Deduplicate
    seen = set()
    return [c for c in candidates if not (c in seen or seen.add(c))]


def make_portal_entry(client_id, name, state, fips, entity_type, has_events=None):
    """Create a portal entry dict."""
    if has_events is None:
        has_events = check_has_events(client_id)
    return {
        "client_id": client_id,
        "name": name,
        "state": state,
        "fips": fips,
        "entity_type": entity_type,
        "legistar_url": f"https://{client_id}.legistar.com",
        "api_url": f"https://webapi.legistar.com/v1/{client_id}",
        "validated": datetime.now().strftime("%Y-%m-%d"),
        "has_events": has_events,
    }


def save_mapping(portals, output_file):
    """Save the mapping file."""
    mapping = {
        "metadata": {
            "description": (
                "Legistar client ID mapping for US counties and cities. "
                "Legistar (by Granicus) powers meeting management for local "
                "governments but has no public client directory. This file maps "
                "validated client IDs to FIPS codes for civic tech use."
            ),
            "last_updated": datetime.now().strftime("%Y-%m-%d"),
            "total_validated": len(portals),
            "how_to_use": {
                "api_events": "GET https://webapi.legistar.com/v1/{client_id}/Events",
                "api_items": "GET https://webapi.legistar.com/v1/{client_id}/Events/{id}/EventItems",
                "web_portal": "https://{client_id}.legistar.com",
                "api_docs": "https://webapi.legistar.com/Help",
            },
            "source": "https://detention-pipeline.transparencycascade.org",
            "repository": "https://github.com/transparencycascade/detention-pipeline",
            "license": "CC0 — public domain",
        },
        "portals": sorted(portals, key=lambda p: (p["state"], p["name"])),
    }

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w") as f:
        json.dump(mapping, f, indent=2)

    print(f"\nSaved {len(portals)} validated portals to {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Discover and validate Legistar client IDs"
    )
    parser.add_argument("--discover", action="store_true",
                       help="Search for new portals (requires requests + bs4)")
    parser.add_argument("--state", nargs="+",
                       help="Only discover for these states")
    parser.add_argument("--validate-only", action="store_true",
                       help="Only re-validate existing entries")
    parser.add_argument("--census-file", type=str,
                       help="CSV with columns: fips, county/NAME, state/STUSAB")
    parser.add_argument("--output", type=str, default=DEFAULT_OUTPUT,
                       help="Output JSON path")
    args = parser.parse_args()

    if args.discover and not HAS_DEPS:
        print("Discovery mode requires: pip install requests beautifulsoup4")
        sys.exit(1)

    # --- Validate known portals ---
    print(f"Validating {len(KNOWN_PORTALS)} known portals...")
    validated = []
    for client_id, name, state, fips, etype in KNOWN_PORTALS:
        if validate_api(client_id):
            has_events = check_has_events(client_id)
            validated.append(make_portal_entry(client_id, name, state, fips, etype, has_events))
            mark = "✓" if has_events else "○"
            print(f"  {mark} {name}, {state} -> {client_id}")
        else:
            print(f"  ✗ {name}, {state} -> {client_id} (INVALID)")
        time.sleep(API_DELAY)

    if args.validate_only:
        save_mapping(validated, args.output)
        return

    if not args.discover:
        save_mapping(validated, args.output)
        print("\nTo search for additional portals, re-run with --discover")
        return

    # --- Discovery mode ---
    print(f"\nDiscovering new portals via web search...")
    session = requests.Session()
    known_ids = {p["client_id"] for p in validated}

    # Load counties to search
    if args.census_file:
        import csv
        counties = []
        with open(args.census_file) as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = row.get("county") or row.get("NAME") or row.get("County Name", "")
                state = row.get("state") or row.get("STUSAB") or row.get("State", "")
                fips = row.get("fips") or row.get("FIPS") or ""
                if name and state:
                    counties.append({"name": name, "state": state, "fips": fips})
    else:
        # Default: search for high-priority heatmap counties not yet found
        print("No census file — searching for high-priority missing counties")
        counties = [
            {"name": "Wayne County", "state": "MI", "fips": "26163"},
            {"name": "Palm Beach County", "state": "FL", "fips": "12099"},
            {"name": "Pinal County", "state": "AZ", "fips": "04021"},
            {"name": "Webb County", "state": "TX", "fips": "48479"},
            {"name": "Charlton County", "state": "GA", "fips": "13049"},
            {"name": "Bradford County", "state": "FL", "fips": "12007"},
            {"name": "Chatham County", "state": "GA", "fips": "13051"},
            {"name": "Gwinnett County", "state": "GA", "fips": "13135"},
            {"name": "Cobb County", "state": "GA", "fips": "13067"},
            {"name": "Orange County", "state": "CA", "fips": "06059"},
            {"name": "Kern County", "state": "CA", "fips": "06029"},
            {"name": "Bernalillo County", "state": "NM", "fips": "35001"},
            {"name": "Essex County", "state": "NJ", "fips": "34013"},
            {"name": "Williamson County", "state": "TX", "fips": "48491"},
            {"name": "Cameron County", "state": "TX", "fips": "48061"},
            {"name": "El Paso County", "state": "TX", "fips": "48141"},
            {"name": "Hidalgo County", "state": "TX", "fips": "48215"},
            {"name": "Montgomery County", "state": "TX", "fips": "48339"},
            {"name": "Dallas County", "state": "TX", "fips": "48113"},
            {"name": "Bexar County", "state": "TX", "fips": "48029"},
            {"name": "Duval County", "state": "FL", "fips": "12031"},
            {"name": "Osceola County", "state": "FL", "fips": "12097"},
            {"name": "Lee County", "state": "FL", "fips": "12071"},
        ]

    if args.state:
        counties = [c for c in counties if c["state"] in args.state]

    newly_found = []
    for county in counties:
        name = county["name"]
        state = county["state"]
        fips = county.get("fips", "")

        print(f"\n  Searching: {name}, {state}...")

        # Strategy 1: Web search
        subdomains = discover_via_search(name, state, session)

        # Strategy 2: Pattern probe for any subdomains not yet found
        if not subdomains:
            candidates = generate_candidates(name, state)
            for candidate in candidates:
                if candidate in known_ids:
                    continue
                if probe_legistar_web(candidate):
                    subdomains.add(candidate)
                    break
                time.sleep(PROBE_DELAY)

        # Strategy 3: Validate discovered subdomains
        for subdomain in subdomains:
            if subdomain in known_ids:
                print(f"    Already known: {subdomain}")
                continue

            if validate_api(subdomain):
                has_events = check_has_events(subdomain)
                portal = make_portal_entry(subdomain, name, state, fips, "county", has_events)
                validated.append(portal)
                newly_found.append(portal)
                known_ids.add(subdomain)
                mark = "✓" if has_events else "○"
                print(f"    NEW {mark} {name}, {state} -> {subdomain}")
            time.sleep(API_DELAY)

        time.sleep(SEARCH_DELAY)

    if newly_found:
        print(f"\nDiscovered {len(newly_found)} new portals:")
        for p in newly_found:
            print(f"  {p['name']}, {p['state']} -> {p['client_id']}")

    save_mapping(validated, args.output)


if __name__ == "__main__":
    main()
