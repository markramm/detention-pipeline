#!/usr/bin/env python3
"""
County heat score — rank IGSA counties by converging pipeline signals.

Reads both igsa-holders and detention-pipeline-research KBs, scores each
county FIPS code by how many signal types are present, and outputs a
prioritized list for targeting Legistar scans, LoopNet queries, and
news monitoring.

Scoring:
  - Existing IGSA facility:        +2 per facility (base signal)
  - ANC/ICE contract in county:    +5 per contract (money flowing)
  - Job posting mentioning state:  +3 (active recruitment)
  - Real estate trace:             +3 per property (physical infrastructure)
  - Commission activity:           +4 (democratic process engaged)
  - Comms discipline signal:       +3 (playbook in motion)
  - Budget distress:               +4 (vulnerability indicator)
  - Sheriff network:               +3 (recruitment channel)
  - Legislative trace in state:    +1 (state-level context)

Usage:
    python county_heat_score.py                    # all counties
    python county_heat_score.py --state FL         # Florida only
    python county_heat_score.py --top 50           # top 50 hottest
    python county_heat_score.py --min-score 5      # only score >= 5
    python county_heat_score.py --output csv       # CSV output
    python county_heat_score.py --with-legistar    # check Legistar availability
"""

import argparse
import csv
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

# Signal type weights
# Tier 1: Proves federal detention relationship exists or is being built
# Tier 2: Active pipeline signals — someone is working on a deal
# Tier 3: Conditions that make a county a target
# Tier 4: Context — useful but not predictive alone
WEIGHTS = {
    "igsa": 10,              # Tier 1: existing federal detention agreement
    "anc-contract": 8,       # Tier 1: federal money flowing for detention services
    "287g-agreement": 7,     # Tier 1.5: local LE already cooperating with ICE
    "commission-activity": 7, # Tier 2: democratic process engaged on detention
    "job-posting": 7,        # Tier 2: consultant actively hiring for this geography
    "sheriff-network": 6,    # Tier 2: sheriff recruited at conference, pitching commissioners
    "comms-discipline": 6,   # Tier 2: playbook communications pattern detected
    "budget-distress": 5,    # Tier 3: county is financially vulnerable to the pitch
    "real-estate-trace": 2,  # Tier 3: building exists, but buildings are everywhere
    "legislative-trace": 1,  # Tier 4: state-level context
}

# Per-entry caps to prevent one signal type from dominating
# (9 warehouses shouldn't outscore 1 IGSA)
MAX_ENTRIES_PER_TYPE = {
    "igsa": 5,               # Multiple IGSAs in a county is meaningful (up to a point)
    "anc-contract": 3,       # Multiple contracts = deeper relationship
    "287g-agreement": 3,     # Multiple models (JEM+TFM+WSO) = deeper cooperation
    "real-estate-trace": 2,  # Cap at 2 — more warehouses doesn't mean more likely
    "commission-activity": 5, # Each meeting/vote is distinct signal
    "job-posting": 3,
    "comms-discipline": 3,
    "budget-distress": 2,
    "sheriff-network": 3,
    "legislative-trace": 2,
}

# FIPS to county name lookup
FIPS_TO_COUNTY = {}
STATE_FIPS_TO_ABBR = {}


def load_fips_lookup():
    """Load FIPS -> county name mapping from Census file."""
    fips_path = Path("/tmp/county_fips.txt")
    if not fips_path.exists():
        # Try to download it
        try:
            from urllib.request import urlretrieve
            urlretrieve(
                "https://www2.census.gov/geo/docs/reference/codes2020/national_county2020.txt",
                str(fips_path),
            )
        except Exception:
            return

    with open(fips_path) as f:
        reader = csv.DictReader(f, delimiter="|")
        for r in reader:
            state_abbr = r["STATE"]
            county_name = r["COUNTYNAME"]
            state_fips = r["STATEFP"]
            county_fips = r["COUNTYFP"]
            full_fips = state_fips + county_fips
            FIPS_TO_COUNTY[full_fips] = f"{county_name}, {state_abbr}"
            STATE_FIPS_TO_ABBR[state_fips] = state_abbr


def parse_frontmatter(filepath):
    """Extract fields from YAML frontmatter and body text."""
    with open(filepath) as f:
        content = f.read()
    if not content.startswith("---"):
        return {}
    try:
        end = content.index("---", 3)
    except ValueError:
        return {}
    fields = {}
    for line in content[3:end].split("\n"):
        if ":" in line and not line.startswith(" ") and not line.startswith("-"):
            key, val = line.split(":", 1)
            fields[key.strip()] = val.strip().strip("'\"")

    # If fips/state not in frontmatter, try to extract from body text
    # (287g-agreement entries store these in the body)
    if not fields.get("fips") or not fields.get("state"):
        body = content[end + 3:]
        for line in body.split("\n"):
            line = line.strip()
            if line.startswith("FIPS:") and not fields.get("fips"):
                val = line.split(":", 1)[1].strip()
                if val and val != "unresolved":
                    fields["fips"] = val
            elif line.startswith("State:") and not fields.get("state"):
                val = line.split(":", 1)[1].strip()
                if val:
                    fields["state"] = val
            elif line.startswith("County:") and not fields.get("county"):
                val = line.split(":", 1)[1].strip()
                if val:
                    fields["county"] = val

    return fields


def scan_kb(kb_path, entry_type_override=None):
    """Scan a KB directory for entries with FIPS codes. Returns list of (fips, state, entry_type, title)."""
    entries = []
    for root, dirs, files in os.walk(kb_path):
        # Skip hidden dirs
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for fname in files:
            if not fname.endswith(".md"):
                continue
            filepath = os.path.join(root, fname)
            fields = parse_frontmatter(filepath)
            fips = fields.get("fips", "")
            state = fields.get("state", "")
            entry_type = entry_type_override or fields.get("type", "unknown")
            title = fields.get("title", fname)

            if fips or state:
                entries.append({
                    "fips": fips,
                    "state": state,
                    "entry_type": entry_type,
                    "title": title,
                })
    return entries


def score_counties(igsa_path, pipeline_path):
    """Score all counties by signal convergence."""
    # county_data[fips] = {signals: {type: [titles]}, state: str, score: int}
    county_data = defaultdict(lambda: {"signals": defaultdict(list), "state": "", "score": 0})

    # Scan IGSA holders
    for entry in scan_kb(igsa_path, entry_type_override="igsa"):
        fips = entry["fips"]
        if not fips or fips == "00000":
            continue
        county_data[fips]["signals"]["igsa"].append(entry["title"])
        county_data[fips]["state"] = entry["state"]

    # Scan pipeline research
    for entry in scan_kb(pipeline_path):
        fips = entry["fips"]
        state = entry["state"]
        etype = entry["entry_type"]

        if fips and fips != "00000":
            county_data[fips]["signals"][etype].append(entry["title"])
            county_data[fips]["state"] = state
        elif state:
            # State-level signals (job postings, legislative traces) — apply to all counties in state
            for cfips, cdata in county_data.items():
                if cdata["state"] == state:
                    cdata["signals"][etype].append(entry["title"])

    # Calculate scores
    for fips, data in county_data.items():
        score = 0
        for signal_type, entries in data["signals"].items():
            weight = WEIGHTS.get(signal_type, 1)
            cap = MAX_ENTRIES_PER_TYPE.get(signal_type, 5)
            capped_count = min(len(entries), cap)
            score += weight * capped_count
        data["score"] = score
        # Bonus for signal diversity (multiple signal types = convergence)
        # This is the key insight: 3 different signal types in one county
        # is far more predictive than 10 entries of the same type
        signal_types = len([t for t in data["signals"] if data["signals"][t]])
        if signal_types >= 3:
            data["score"] += 10 * (signal_types - 2)  # +10 per type beyond 2
        if signal_types >= 5:
            data["score"] += 15  # extra bonus for 5+ types — very high convergence

    return county_data


def check_legistar_availability(county_name, state):
    """Heuristic check for Legistar availability. Returns potential client ID."""
    # Common patterns for Legistar client IDs
    name = county_name.lower().replace(" county", "").replace(" parish", "").strip()
    name = re.sub(r"[^a-z]", "", name)
    candidates = [
        f"{name}county{state.lower()}",
        f"{name}{state.lower()}",
        f"{name}county",
        name,
    ]
    return candidates[0]  # Best guess — needs verification


def main():
    parser = argparse.ArgumentParser(description="Score IGSA counties by pipeline signal convergence")
    parser.add_argument("--state", type=str, help="Filter to one state (e.g. FL, MI)")
    parser.add_argument("--top", type=int, default=50, help="Show top N counties (default: 50)")
    parser.add_argument("--min-score", type=int, default=0, help="Minimum score to display")
    parser.add_argument("--output", type=str, default="table", choices=["table", "csv", "json"], help="Output format")
    parser.add_argument("--with-legistar", action="store_true", help="Include Legistar client ID guesses")
    parser.add_argument("--igsa-path", type=str, default="/Users/markr/tcp-kb-internal/igsa-holders")
    parser.add_argument("--pipeline-path", type=str, default="/Users/markr/tcp-kb-internal/detention-pipeline-research")
    args = parser.parse_args()

    load_fips_lookup()

    county_data = score_counties(args.igsa_path, args.pipeline_path)

    # Sort by score descending
    ranked = sorted(county_data.items(), key=lambda x: -x[1]["score"])

    # Filter
    if args.state:
        ranked = [(f, d) for f, d in ranked if d["state"] == args.state]
    if args.min_score:
        ranked = [(f, d) for f, d in ranked if d["score"] >= args.min_score]
    ranked = ranked[:args.top]

    if args.output == "json":
        output = []
        for fips, data in ranked:
            county_name = FIPS_TO_COUNTY.get(fips, fips)
            output.append({
                "fips": fips,
                "county": county_name,
                "state": data["state"],
                "score": data["score"],
                "signal_types": len([t for t in data["signals"] if data["signals"][t]]),
                "signals": {k: len(v) for k, v in data["signals"].items() if v},
            })
        print(json.dumps(output, indent=2))

    elif args.output == "csv":
        writer = csv.writer(sys.stdout)
        header = ["fips", "county", "state", "score", "signal_types", "igsa", "287g", "anc_contract", "real_estate", "commission", "comms", "budget", "sheriff", "jobs", "legislative"]
        if args.with_legistar:
            header.append("legistar_client_guess")
        writer.writerow(header)
        for fips, data in ranked:
            county_name = FIPS_TO_COUNTY.get(fips, fips)
            row = [
                fips,
                county_name,
                data["state"],
                data["score"],
                len([t for t in data["signals"] if data["signals"][t]]),
                len(data["signals"].get("igsa", [])),
                len(data["signals"].get("287g-agreement", [])),
                len(data["signals"].get("anc-contract", [])),
                len(data["signals"].get("real-estate-trace", [])),
                len(data["signals"].get("commission-activity", [])),
                len(data["signals"].get("comms-discipline", [])),
                len(data["signals"].get("budget-distress", [])),
                len(data["signals"].get("sheriff-network", [])),
                len(data["signals"].get("job-posting", [])),
                len(data["signals"].get("legislative-trace", [])),
            ]
            if args.with_legistar:
                parts = county_name.split(",")
                cname = parts[0].strip() if parts else fips
                row.append(check_legistar_availability(cname, data["state"]))
            writer.writerow(row)

    else:  # table
        print(f"{'Rank':>4}  {'Score':>5}  {'Signals':>7}  {'FIPS':>5}  {'County':<35}  {'Signal Breakdown'}")
        print("-" * 110)
        for i, (fips, data) in enumerate(ranked, 1):
            county_name = FIPS_TO_COUNTY.get(fips, f"{fips} ({data['state']})")
            signal_types = len([t for t in data["signals"] if data["signals"][t]])
            # Build breakdown string
            parts = []
            for stype in ["igsa", "287g-agreement", "anc-contract", "real-estate-trace", "commission-activity",
                          "comms-discipline", "budget-distress", "sheriff-network", "job-posting"]:
                count = len(data["signals"].get(stype, []))
                if count:
                    short = stype.split("-")[0][:4]
                    parts.append(f"{short}:{count}")
            breakdown = " ".join(parts)

            print(f"{i:>4}  {data['score']:>5}  {signal_types:>7}  {fips:>5}  {county_name:<35}  {breakdown}")

            if args.with_legistar:
                cparts = county_name.split(",")
                cname = cparts[0].strip() if cparts else fips
                lid = check_legistar_availability(cname, data["state"])
                print(f"{'':>4}  {'':>5}  {'':>7}  {'':>5}  {'':35}  legistar: {lid}")


if __name__ == "__main__":
    main()
