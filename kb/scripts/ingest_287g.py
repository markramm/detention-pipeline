#!/usr/bin/env python3
"""
Ingest 287(g) agreement data from Prison Policy Initiative.

Primary source: prisonpolicy.org appendix table of 287(g) agreements,
which includes agency, county, state, and per-model signing dates.
As of Feb 2026 this has 1,200+ entries with county already broken out.

287(g) agreements indicate local law enforcement willingness to cooperate
with ICE — a signal that the jurisdiction is receptive to detention expansion.

Usage:
    python ingest_287g.py                          # scrape Prison Policy
    python ingest_287g.py --dry-run                # preview without saving
    python ingest_287g.py --state FL               # Florida only
    python ingest_287g.py --output /tmp/287g.json  # custom output path
"""

import argparse
import csv
import json
import re
import sys
from html.parser import HTMLParser
from pathlib import Path
from urllib.request import Request, urlopen

SOURCE_URL = "https://www.prisonpolicy.org/blog/2026/02/23/ice_county_collaboration/"

# FIPS lookup
FIPS_MAP = {}

# State abbreviation variations used by Prison Policy (e.g. "Ala." -> "AL")
STATE_ABBR_NORMALIZE = {
    "Ala.": "AL", "Alaska": "AK", "Ariz.": "AZ", "Ark.": "AR",
    "Calif.": "CA", "Colo.": "CO", "Conn.": "CT", "Del.": "DE",
    "Fla.": "FL", "Ga.": "GA", "Hawaii": "HI", "Idaho": "ID",
    "Ill.": "IL", "Ind.": "IN", "Iowa": "IA", "Kan.": "KS",
    "Ky.": "KY", "La.": "LA", "Maine": "ME", "Md.": "MD",
    "Mass.": "MA", "Mich.": "MI", "Minn.": "MN", "Miss.": "MS",
    "Mo.": "MO", "Mont.": "MT", "Neb.": "NE", "Nev.": "NV",
    "N.H.": "NH", "N.J.": "NJ", "N.M.": "NM", "N.Y.": "NY",
    "N.C.": "NC", "N.D.": "ND", "Ohio": "OH", "Okla.": "OK",
    "Ore.": "OR", "Pa.": "PA", "R.I.": "RI", "S.C.": "SC",
    "S.D.": "SD", "Tenn.": "TN", "Texas": "TX", "Tex.": "TX",
    "Utah": "UT", "Vt.": "VT", "Va.": "VA", "Wash.": "WA",
    "W.Va.": "WV", "Wis.": "WI", "Wyo.": "WY", "D.C.": "DC",
    "Guam": "GU", "P.R.": "PR", "V.I.": "VI",
    # Also accept standard abbreviations
    "AL": "AL", "AK": "AK", "AZ": "AZ", "AR": "AR", "CA": "CA",
    "CO": "CO", "CT": "CT", "DE": "DE", "FL": "FL", "GA": "GA",
    "HI": "HI", "ID": "ID", "IL": "IL", "IN": "IN", "IA": "IA",
    "KS": "KS", "KY": "KY", "LA": "LA", "ME": "ME", "MD": "MD",
    "MA": "MA", "MI": "MI", "MN": "MN", "MS": "MS", "MO": "MO",
    "MT": "MT", "NE": "NE", "NV": "NV", "NH": "NH", "NJ": "NJ",
    "NM": "NM", "NY": "NY", "NC": "NC", "ND": "ND", "OH": "OH",
    "OK": "OK", "OR": "OR", "PA": "PA", "RI": "RI", "SC": "SC",
    "SD": "SD", "TN": "TN", "TX": "TX", "UT": "UT", "VT": "VT",
    "VA": "VA", "WA": "WA", "WV": "WV", "WI": "WI", "WY": "WY",
    "DC": "DC", "GU": "GU", "PR": "PR", "VI": "VI",
}


def load_fips_map():
    """Load county FIPS lookup from Census reference file."""
    global FIPS_MAP
    fips_path = Path("/tmp/county_fips.txt")
    if not fips_path.exists():
        try:
            from urllib.request import urlretrieve
            print("Downloading Census FIPS reference file...")
            urlretrieve(
                "https://www2.census.gov/geo/docs/reference/codes2020/national_county2020.txt",
                str(fips_path),
            )
        except Exception as e:
            print(f"  Warning: Could not download FIPS file: {e}", file=sys.stderr)
            return

    with open(fips_path) as f:
        reader = csv.DictReader(f, delimiter="|")
        for r in reader:
            state = r["STATE"]
            county = r["COUNTYNAME"]
            fips = r["STATEFP"] + r["COUNTYFP"]
            # Store multiple normalizations for matching
            normalized = re.sub(
                r"\s+(County|Parish|Borough|Census Area|Municipality|city|City and Borough)$",
                "", county
            ).strip().lower()
            FIPS_MAP[(state, normalized)] = fips
            FIPS_MAP[(state, county.lower())] = fips


def resolve_fips_from_county(state_abbr, county_name):
    """Resolve FIPS from state abbreviation + county name."""
    if not FIPS_MAP or not state_abbr or not county_name:
        return ""

    # Clean up non-breaking spaces and extra whitespace
    county_clean = county_name.replace("\xa0", " ").strip().lower()

    # Direct lookup
    fips = FIPS_MAP.get((state_abbr, county_clean))
    if fips:
        return fips

    # Strip "County" / "Parish" suffix and retry
    for suffix in [" county", " parish", " borough"]:
        if county_clean.endswith(suffix):
            fips = FIPS_MAP.get((state_abbr, county_clean[:-len(suffix)]))
            if fips:
                return fips

    # Try adding "county" suffix
    fips = FIPS_MAP.get((state_abbr, county_clean + " county"))
    if fips:
        return fips

    # Louisiana parishes
    if state_abbr == "LA":
        fips = FIPS_MAP.get((state_abbr, county_clean + " parish"))
        if fips:
            return fips

    return ""


class TableExtractor(HTMLParser):
    """Parse HTML tables."""

    def __init__(self):
        super().__init__()
        self.in_table = False
        self.in_row = False
        self.in_cell = False
        self.in_header = False
        self.current_row = []
        self.current_cell = ""
        self.tables = []
        self.current_table = []
        self.current_headers = []

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self.in_table = True
            self.current_table = []
            self.current_headers = []
        elif tag == "tr" and self.in_table:
            self.in_row = True
            self.current_row = []
        elif tag == "th" and self.in_row:
            self.in_cell = True
            self.in_header = True
            self.current_cell = ""
        elif tag == "td" and self.in_row:
            self.in_cell = True
            self.current_cell = ""

    def handle_endtag(self, tag):
        if tag == "table" and self.in_table:
            self.in_table = False
            self.tables.append((self.current_headers, self.current_table))
        elif tag == "tr" and self.in_row:
            self.in_row = False
            if self.current_row:
                if self.in_header and not self.current_headers:
                    self.current_headers = self.current_row
                else:
                    self.current_table.append(self.current_row)
            self.in_header = False
        elif tag in ("td", "th") and self.in_cell:
            self.in_cell = False
            self.current_row.append(self.current_cell.strip())

    def handle_data(self, data):
        if self.in_cell:
            self.current_cell += data


def fetch_prison_policy_data():
    """Fetch and parse 287(g) data from Prison Policy Initiative."""
    # Check for cached HTML first
    cache_path = Path("/tmp/prisonpolicy_287g.html")
    if cache_path.exists():
        print(f"Using cached HTML from {cache_path}")
        with open(cache_path) as f:
            html = f.read()
    else:
        print(f"Fetching {SOURCE_URL}")
        req = Request(SOURCE_URL)
        req.add_header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
        try:
            with urlopen(req, timeout=60) as resp:
                html = resp.read().decode("utf-8", errors="replace")
            # Cache for re-runs
            with open(cache_path, "w") as f:
                f.write(html)
        except Exception as e:
            print(f"Error fetching: {e}", file=sys.stderr)
            sys.exit(1)

    parser = TableExtractor()
    parser.feed(html)

    print(f"  Found {len(parser.tables)} tables")
    for i, (headers, rows) in enumerate(parser.tables):
        print(f"    Table {i+1}: {len(rows)} rows, headers: {headers[:3]}...")

    # Table 1 is the 287(g) agreements table
    # Structure: row[0] has sub-headers (Agency, County, State, TFM date, WSO date, JEM date)
    # The actual headers span two rows in the HTML
    if not parser.tables:
        return []

    headers, rows = parser.tables[0]

    # First data row is actually the sub-headers
    if rows and rows[0][0].startswith("Law"):
        sub_headers = rows[0]
        data_rows = rows[1:]
    else:
        sub_headers = headers
        data_rows = rows

    print(f"  Sub-headers: {sub_headers}")
    print(f"  Data rows: {len(data_rows)}")

    entries = []
    for row in data_rows:
        if len(row) < 3:
            continue

        # Columns: Agency, County, State, TFM date, WSO date, JEM date
        agency = row[0].replace("\xa0", " ").strip() if len(row) > 0 else ""
        county = row[1].replace("\xa0", " ").strip() if len(row) > 1 else ""
        state_raw = row[2].replace("\xa0", " ").strip() if len(row) > 2 else ""
        tfm_date = row[3].strip() if len(row) > 3 else ""
        wso_date = row[4].strip() if len(row) > 4 else ""
        jem_date = row[5].strip() if len(row) > 5 else ""

        if not agency or not state_raw:
            continue

        state = STATE_ABBR_NORMALIZE.get(state_raw, state_raw)

        # Create one entry per model type that has a date
        models = []
        if tfm_date:
            models.append(("TFM", tfm_date))
        if wso_date:
            models.append(("WSO", wso_date))
        if jem_date:
            models.append(("JEM", jem_date))

        # If no model dates, still record the agency (might have empty dates)
        if not models:
            models.append(("unknown", ""))

        for model, signed_date in models:
            entries.append({
                "agency": agency,
                "county": county,
                "state": state,
                "model": model,
                "signed_date": signed_date,
            })

    return entries


def create_entry(raw, dry_run=False):
    """Create a detention-pipeline-research entry from a 287(g) record."""
    agency = raw["agency"]
    county = raw["county"]
    state = raw["state"]
    model = raw["model"]
    signed_date = raw.get("signed_date", "")

    fips = resolve_fips_from_county(state, county)

    # Signal strength:
    # - County sheriff with JEM = strong (jail-level immigration screening)
    # - County sheriff with TFM = strong (patrol-level enforcement)
    # - Municipal PD = moderate
    # - Multiple models = strong (deep cooperation)
    if "sheriff" in agency.lower():
        signal = "strong"
    elif model == "JEM":
        signal = "strong"  # JEM = jail enforcement, always significant
    elif "police" in agency.lower() or "marshal" in agency.lower():
        signal = "moderate"
    else:
        signal = "moderate"

    title = f"287(g) {model}: {agency} ({state})"

    entry = {
        "entry_type": "287g-agreement",
        "title": title,
        "body": (
            f"287(g) agreement between ICE and {agency}.\n\n"
            f"Model: {model}\n"
            f"Signed: {signed_date}\n"
            f"County: {county}\n"
            f"State: {state}\n"
            f"FIPS: {fips or 'unresolved'}\n\n"
            f"Source: Prison Policy Initiative appendix table, "
            f"compiled from ICE data as of Feb 17, 2026."
        ),
        "county": county,
        "state": state,
        "fips": fips,
        "agency": agency,
        "model": model,
        "signed_date": signed_date,
        "source": "Prison Policy Initiative (prisonpolicy.org), compiled from ICE data, as of 2026-02-17",
        "signal_strength": signal,
        "notes": f"{model} model agreement signed {signed_date}" if signed_date else f"{model} model agreement",
        "tags": ["287g", model.lower(), state.lower() if state else "unknown"],
    }

    if dry_run:
        fips_status = f"FIPS: {fips}" if fips else "FIPS: unresolved"
        print(f"  [DRY RUN] {title} | {county} | {fips_status} | {signal}")

    return entry


def main():
    parser = argparse.ArgumentParser(description="Ingest 287(g) agreements from Prison Policy Initiative")
    parser.add_argument("--dry-run", action="store_true", help="Preview without creating entries")
    parser.add_argument("--state", type=str, help="Filter to one state (e.g. FL, TX)")
    parser.add_argument("--output", type=str, default="/tmp/287g_agreements.json", help="Output JSON file")
    parser.add_argument("--no-cache", action="store_true", help="Force re-fetch (ignore cached HTML)")
    args = parser.parse_args()

    if args.no_cache:
        cache = Path("/tmp/prisonpolicy_287g.html")
        if cache.exists():
            cache.unlink()

    load_fips_map()

    raw_entries = fetch_prison_policy_data()

    if not raw_entries:
        print("No entries found. Check the URL or try again.", file=sys.stderr)
        sys.exit(1)

    print(f"\nProcessing {len(raw_entries)} raw entries...")

    all_entries = []
    seen = set()
    fips_resolved = 0
    fips_unresolved = 0

    for raw in raw_entries:
        # State filter
        if args.state and raw["state"] != args.state.upper():
            continue

        entry = create_entry(raw, dry_run=args.dry_run)

        # Deduplicate by agency+model
        dedup_key = (entry["agency"], entry["model"])
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        if entry["fips"]:
            fips_resolved += 1
        else:
            fips_unresolved += 1

        all_entries.append(entry)

    print(f"\n{len(all_entries)} unique entries")
    print(f"  FIPS resolved: {fips_resolved}")
    print(f"  FIPS unresolved: {fips_unresolved}")

    # Stats by model type
    models = {}
    for e in all_entries:
        m = e["model"]
        models[m] = models.get(m, 0) + 1
    print(f"  By model: {models}")

    # Stats by state
    states = {}
    for e in all_entries:
        s = e["state"]
        states[s] = states.get(s, 0) + 1
    top_states = sorted(states.items(), key=lambda x: -x[1])[:10]
    print(f"  Top states: {top_states}")

    if not args.dry_run and all_entries:
        with open(args.output, "w") as f:
            json.dump(all_entries, f, indent=2)
        print(f"\nSaved to {args.output}")
        print(f"\nTo import: kb import {args.output} --kb detention-pipeline-research")

    return all_entries


if __name__ == "__main__":
    main()
