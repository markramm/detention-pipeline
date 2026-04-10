#!/usr/bin/env python3
"""
Generate Hugo content pages from the detention-pipeline KB.

Reads KB entries and heat score data, generates:
- /entry/{id}/ pages (one per KB entry)
- /county/{fips}/ pages (one per scored county)
- /fights/{id}/ pages (county fights)
- /players/contractors/{id}/, /players/people/{id}/, /players/money/{id}/
- /facilities/{id}/ pages
- /signals/{type}/ taxonomy pages
- /state/{abbr}/ taxonomy pages
- /map/ page
- Static pages (methodology, contribute)

Run from the hugo/ directory:
    python3 generate_content.py
"""

import csv
import json
import os
import re
import shutil
import sys
from pathlib import Path

KB_PATH = Path("../kb")
CONTENT_PATH = Path("content")
STATIC_PATH = Path("static")
DATA_PATH = Path("data")

# Metadata for all entry types — signals, industry, facilities
ENTRY_TYPE_META = {
    # Pipeline signals (scored in heat map)
    "287g-agreement":      {"label": "287(g) Agreement",    "color": "#d46a2f", "weight": 7,  "section": "signals"},
    "anc-contract":        {"label": "ANC Contract",        "color": "#c49025", "weight": 8,  "section": "signals"},
    "budget-distress":     {"label": "Budget Distress",     "color": "#9a4fb5", "weight": 5,  "section": "signals"},
    "commission-activity": {"label": "Commission Activity", "color": "#8a9f2a", "weight": 7,  "section": "signals"},
    "comms-discipline":    {"label": "Comms Discipline",    "color": "#6a5fb5", "weight": 6,  "section": "signals"},
    "job-posting":         {"label": "Job Posting",         "color": "#2a9f6f", "weight": 7,  "section": "signals"},
    "legislative-trace":   {"label": "Legislative Trace",   "color": "#5f6a7a", "weight": 1,  "section": "signals"},
    "real-estate-trace":   {"label": "Real Estate Trace",   "color": "#b54f8a", "weight": 2,  "section": "signals"},
    "sheriff-network":     {"label": "Sheriff Network",     "color": "#2a7fb5", "weight": 6,  "section": "signals"},
    # Industry types
    "contractor":          {"label": "Contractor",          "color": "#8a5a2a", "section": "players"},
    "person":              {"label": "Person",              "color": "#5a2a8a", "section": "players"},
    "financial-flow":      {"label": "Financial Flow",      "color": "#8a2a5a", "section": "players"},
    "analysis":            {"label": "Analysis",            "color": "#5a6a8a", "section": "players"},
    "contract":            {"label": "Contract",            "color": "#c49025", "section": "players"},
    "organization":        {"label": "Organization",        "color": "#4a7ab5", "section": "players"},
    "personnel-flow":      {"label": "Personnel Flow",      "color": "#7a4ab5", "section": "players"},
    "county-fight":        {"label": "County Fight",        "color": "#2a8a5a", "section": "fights"},
    "event":               {"label": "Event",               "color": "#5a7a6a", "section": "players"},
    "note":                {"label": "Research Note",       "color": "#6a6a6a", "section": "entry"},
    # Facilities
    "igsa":                {"label": "IGSA Facility",       "color": "#c93b3b", "section": "facilities"},
    "facility":            {"label": "Facility",            "color": "#c93b3b", "section": "facilities"},
}

# Signal types only (for signal index pages and heat score)
SIGNAL_META = {k: v for k, v in ENTRY_TYPE_META.items() if v.get("weight")}

# Coverage depth classification
AUTOMATED_TYPES = {"287g-agreement", "anc-contract", "igsa", "budget-distress"}
HUMAN_TYPES = {"commission-activity", "comms-discipline", "sheriff-network",
               "real-estate-trace", "job-posting", "county-fight", "legislative-trace"}

STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming",
}

FIPS_TO_COUNTY = {}


def load_fips():
    fips_path = Path("/tmp/county_fips.txt")
    if not fips_path.exists():
        return
    with open(fips_path) as f:
        reader = csv.DictReader(f, delimiter="|")
        for r in reader:
            fips = r["STATEFP"] + r["COUNTYFP"]
            FIPS_TO_COUNTY[fips] = f"{r['COUNTYNAME']}, {r['STATE']}"


def parse_entry(filepath):
    """Parse a KB markdown entry into frontmatter dict + body."""
    with open(filepath) as f:
        content = f.read()
    if not content.startswith("---"):
        return None, content
    try:
        end = content.index("---", 3)
    except ValueError:
        return None, content

    fields = {}
    list_fields = {}
    current_list_key = None
    for line in content[3:end].split("\n"):
        stripped = line.strip()
        if not stripped:
            current_list_key = None
            continue
        if stripped.startswith("- ") and current_list_key:
            list_fields.setdefault(current_list_key, []).append(stripped[2:])
            continue
        if ":" in stripped and not stripped.startswith("-"):
            key, val = stripped.split(":", 1)
            key = key.strip()
            val = val.strip().strip("'\"")
            if not val:
                current_list_key = key
            else:
                current_list_key = None
                fields[key] = val

    fields["tags"] = list_fields.get("tags", [])
    fields["_list_fields"] = list_fields

    body = content[end + 3:].strip()

    # Extract FIPS/state/county from body if not in frontmatter
    if not fields.get("fips") or not fields.get("state"):
        for bline in body.split("\n"):
            bline = bline.strip()
            if bline.startswith("FIPS:") and not fields.get("fips"):
                v = bline.split(":", 1)[1].strip()
                if v and v != "unresolved":
                    fields["fips"] = v
            elif bline.startswith("State:") and not fields.get("state"):
                v = bline.split(":", 1)[1].strip()
                if v:
                    fields["state"] = v
            elif bline.startswith("County:") and not fields.get("county"):
                v = bline.split(":", 1)[1].strip()
                if v:
                    fields["county"] = v

    return fields, body


def esc(s):
    """Escape string for YAML frontmatter."""
    return s.replace('"', '\\"').replace('\n', ' ')


def scan_all_entries():
    """Scan all KB entries and return parsed list."""
    entries = []
    skip_dirs = {"scripts", ".pyrite", "__pycache__"}
    for md_file in sorted(KB_PATH.rglob("*.md")):
        if any(part in skip_dirs for part in md_file.parts):
            continue
        if md_file.name == "kb.yaml" or md_file.name.startswith("_"):
            continue
        fields, body = parse_entry(md_file)
        if not fields:
            continue
        try:
            rel_path = str(md_file.relative_to(KB_PATH.parent))
        except ValueError:
            rel_path = str(md_file)
        entries.append({
            "fields": fields,
            "body": body,
            "rel_path": rel_path,
            "md_file": md_file,
        })
    return entries


def generate_all_pages(parsed_entries, heat_data):
    """Generate all Hugo content from parsed entries."""
    entries_by_fips = {}
    entries_by_state = {}
    entries_by_type = {}
    all_entries_meta = []

    # Section directories
    for d in ["entry", "fights", "players/contractors", "players/people",
              "players/money", "facilities", "signals", "county", "state", "map"]:
        (CONTENT_PATH / d).mkdir(parents=True, exist_ok=True)

    for parsed in parsed_entries:
        fields = parsed["fields"]
        body = parsed["body"]
        rel_path = parsed["rel_path"]

        entry_id = fields.get("id", parsed["md_file"].stem)
        entry_type = fields.get("type", "unknown")
        title = fields.get("title", entry_id)
        fips = fields.get("fips", "")
        state = fields.get("state", "")
        county = fields.get("county", "")

        meta = ENTRY_TYPE_META.get(entry_type, {"label": entry_type, "color": "#666", "section": "entry"})
        section = meta["section"]

        # Track indexes
        if fips:
            entries_by_fips.setdefault(fips, []).append({
                "id": entry_id, "type": entry_type, "title": title,
                "state": state, "county": county, "fips": fips,
            })
        if state:
            entries_by_state.setdefault(state, []).append({
                "id": entry_id, "type": entry_type, "title": title,
                "fips": fips, "county": county,
            })
        entries_by_type.setdefault(entry_type, []).append({
            "id": entry_id, "title": title, "state": state,
            "county": county, "fips": fips,
        })
        all_entries_meta.append({
            "id": entry_id, "type": entry_type, "title": title,
            "state": state, "county": county, "fips": fips, "section": section,
        })

        # Common frontmatter
        fm = {
            "title": esc(title),
            "entry_type": entry_type,
            "signal_label": meta["label"],
            "signal_color": meta["color"],
            "fips": fips,
            "state": state,
            "county": esc(county),
            "repo_path": rel_path,
        }

        # Determine output path based on section
        if entry_type == "county-fight":
            # Fight pages go to /fights/
            page_path = CONTENT_PATH / "fights" / f"{entry_id}.md"
            fm["type"] = "fights"
            fm["layout"] = "single"
            fm["status"] = fields.get("status", "")
            fm["outcome"] = esc(fields.get("outcome", ""))
            fm["vote_tally"] = esc(fields.get("vote_tally", ""))
            fm["facility"] = fields.get("facility", "")
        elif entry_type == "contractor":
            page_path = CONTENT_PATH / "players" / "contractors" / f"{entry_id}.md"
            fm["type"] = "players"
            fm["layout"] = "single"
            fm["player_type"] = "contractor"
            fm["contractor_type"] = fields.get("contractor_type", "")
            fm["headquarters"] = esc(fields.get("headquarters", ""))
            fm["founded"] = fields.get("founded", "")
            fm["status"] = fields.get("status", "")
        elif entry_type == "person":
            page_path = CONTENT_PATH / "players" / "people" / f"{entry_id}.md"
            fm["type"] = "players"
            fm["layout"] = "single"
            fm["player_type"] = "person"
            fm["role"] = fields.get("role", "")
            fm["government_service"] = esc(fields.get("government_service", ""))
            fm["private_role"] = esc(fields.get("private_role", ""))
        elif entry_type in ("financial-flow", "analysis"):
            page_path = CONTENT_PATH / "players" / "money" / f"{entry_id}.md"
            fm["type"] = "players"
            fm["layout"] = "single"
            fm["player_type"] = "money"
        elif entry_type in ("igsa", "facility"):
            page_path = CONTENT_PATH / "facilities" / f"{entry_id}.md"
            fm["type"] = "facility_page"
            fm["layout"] = "single"
            fm["facility_name"] = esc(fields.get("facility_name", ""))
            fm["operator"] = esc(fields.get("operator", ""))
            fm["status"] = fields.get("status", "")
            fm["bed_count"] = fields.get("bed_count", fields.get("capacity", ""))
        else:
            # Default: entry page
            page_path = CONTENT_PATH / "entry" / f"{entry_id}.md"
            fm["type"] = "entry"
            fm["layout"] = "single"

        # Write the page
        fm_lines = "\n".join(f'{k}: "{v}"' if isinstance(v, str) else f'{k}: {v}'
                             for k, v in fm.items())
        with open(page_path, "w") as f:
            f.write(f"---\n{fm_lines}\n---\n\n{body}\n")

        # Also write to entry/ for types that have dedicated sections
        # so cross-referencing by FIPS still works on county pages
        if entry_type in ("county-fight", "contractor", "person", "financial-flow",
                          "analysis", "igsa", "facility") and page_path.parent != CONTENT_PATH / "entry":
            entry_page = CONTENT_PATH / "entry" / f"{entry_id}.md"
            entry_fm = dict(fm)
            entry_fm["type"] = "entry"
            entry_fm["layout"] = "single"
            # Add canonical URL to the dedicated section
            if entry_type == "county-fight":
                entry_fm["canonical"] = f"/fights/{entry_id}/"
            elif entry_type == "contractor":
                entry_fm["canonical"] = f"/players/contractors/{entry_id}/"
            elif entry_type == "person":
                entry_fm["canonical"] = f"/players/people/{entry_id}/"
            elif entry_type in ("financial-flow", "analysis"):
                entry_fm["canonical"] = f"/players/money/{entry_id}/"
            elif entry_type in ("igsa", "facility"):
                entry_fm["canonical"] = f"/facilities/{entry_id}/"
            efm_lines = "\n".join(f'{k}: "{v}"' if isinstance(v, str) else f'{k}: {v}'
                                  for k, v in entry_fm.items())
            with open(entry_page, "w") as f:
                f.write(f"---\n{efm_lines}\n---\n\n{body}\n")

    return entries_by_fips, entries_by_state, entries_by_type, all_entries_meta


def generate_county_pages(entries_by_fips, heat_data):
    """Generate county pages with coverage depth."""
    county_dir = CONTENT_PATH / "county"
    county_dir.mkdir(parents=True, exist_ok=True)
    heat_by_fips = {d["fips"]: d for d in heat_data}

    for fips, entries in entries_by_fips.items():
        heat = heat_by_fips.get(fips, {})
        county_name = heat.get("county", FIPS_TO_COUNTY.get(fips, f"County {fips}"))
        score = heat.get("score", 0)
        signal_types = heat.get("signal_types", 0)
        state = entries[0].get("state", "")

        rank = 0
        for i, d in enumerate(heat_data):
            if d["fips"] == fips:
                rank = i + 1
                break
        total = len(heat_data)
        percentile = round((1 - rank / total) * 100) if total > 0 and rank > 0 else 0

        # Coverage depth
        entry_types = set(e["type"] for e in entries)
        human_count = len(entry_types & HUMAN_TYPES)
        if human_count >= 2:
            coverage = "well-researched"
        elif human_count >= 1:
            coverage = "partially-researched"
        else:
            coverage = "unresearched"

        page_path = county_dir / f"{fips}.md"
        with open(page_path, "w") as f:
            f.write(f"""---
title: "{esc(county_name)}"
type: county
layout: single
fips: "{fips}"
state: "{state}"
score: {score}
signal_types: {signal_types}
rank: {rank}
total_counties: {total}
percentile: {percentile}
coverage: "{coverage}"
states: ["{state}"]
---
""")


def generate_section_indexes(entries_by_type, all_entries, heat_data):
    """Generate section index pages."""

    # Fights index
    fight_count = len(entries_by_type.get("county-fight", []))
    with open(CONTENT_PATH / "fights" / "_index.md", "w") as f:
        f.write(f"""---
title: "County Fights"
layout: list
fight_count: {fight_count}
---
""")

    # Players index
    contractor_count = len(entries_by_type.get("contractor", []))
    people_count = len(entries_by_type.get("person", []))
    money_count = len(entries_by_type.get("financial-flow", []))
    money_count += len(entries_by_type.get("analysis", []))
    with open(CONTENT_PATH / "players" / "_index.md", "w") as f:
        f.write(f"""---
title: "The Players"
layout: list
contractor_count: {contractor_count}
people_count: {people_count}
money_count: {money_count}
---
""")
    with open(CONTENT_PATH / "players" / "contractors" / "_index.md", "w") as f:
        f.write(f"""---
title: "Contractors"
layout: list
count: {contractor_count}
---
""")
    with open(CONTENT_PATH / "players" / "people" / "_index.md", "w") as f:
        f.write(f"""---
title: "People"
layout: list
count: {people_count}
---
""")
    with open(CONTENT_PATH / "players" / "money" / "_index.md", "w") as f:
        f.write(f"""---
title: "Financial Flows"
layout: list
count: {money_count}
---
""")

    # Facilities index
    facility_count = len(entries_by_type.get("igsa", [])) + len(entries_by_type.get("facility", []))
    with open(CONTENT_PATH / "facilities" / "_index.md", "w") as f:
        f.write(f"""---
title: "Detention Facilities"
layout: list
facility_count: {facility_count}
---
""")

    # Signals index
    with open(CONTENT_PATH / "signals" / "_index.md", "w") as f:
        f.write("""---
title: "Signal Types"
type: signals_list
layout: list
---
""")

    # Signal type pages
    for stype, meta in SIGNAL_META.items():
        page_path = CONTENT_PATH / "signals" / f"{stype}.md"
        with open(page_path, "w") as f:
            f.write(f"""---
title: "{meta['label']}"
type: signal
layout: single
signal_type: "{stype}"
signal_color: "{meta['color']}"
weight_score: {meta.get('weight', 0)}
---
""")

    # Map page
    with open(CONTENT_PATH / "map" / "_index.md", "w") as f:
        f.write(f"""---
title: "Heat Map"
type: map_page
layout: single
total_counties: {len(heat_data)}
max_score: {heat_data[0]['score'] if heat_data else 0}
---
""")

    # County list index
    with open(CONTENT_PATH / "county" / "_index.md", "w") as f:
        f.write(f"""---
title: "All Tracked Counties"
type: county_list
layout: list
---
""")

    # Entry list index
    with open(CONTENT_PATH / "entry" / "_index.md", "w") as f:
        f.write("""---
title: "All Entries"
type: entry
layout: list
---
""")


def generate_state_pages(entries_by_state, heat_data):
    """Generate state pages."""
    state_dir = CONTENT_PATH / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    for state_abbr, entries in entries_by_state.items():
        state_name = STATE_NAMES.get(state_abbr, state_abbr)
        state_counties = [d for d in heat_data if d["state"] == state_abbr]
        with open(state_dir / f"{state_abbr.lower()}.md", "w") as f:
            f.write(f"""---
title: "{state_name}"
type: state
layout: single
state_abbr: "{state_abbr}"
county_count: {len(state_counties)}
entry_count: {len(entries)}
---
""")
    with open(state_dir / "_index.md", "w") as f:
        f.write("""---
title: "States"
type: state_list
layout: list
---
""")


def generate_static_pages():
    """Generate methodology and contribute pages."""
    for name, layout in [("methodology", "methodology"), ("contribute", "contribute"), ("foia", "foia")]:
        with open(CONTENT_PATH / f"{name}.md", "w") as f:
            f.write(f"""---
title: "{name.title()}"
type: page
layout: {layout}
---
""")


def copy_static_assets(heat_data):
    """Copy map assets and write data files."""
    DATA_PATH.mkdir(parents=True, exist_ok=True)
    with open(DATA_PATH / "heat.json", "w") as f:
        json.dump(heat_data, f)
    with open(DATA_PATH / "signals.json", "w") as f:
        json.dump(SIGNAL_META, f, indent=2)
    with open(DATA_PATH / "entry_types.json", "w") as f:
        json.dump(ENTRY_TYPE_META, f, indent=2)

    STATIC_PATH.mkdir(parents=True, exist_ok=True)
    docs = Path("../docs")
    for fname in ["counties-albers-10m.json", "fips_names.json", "heat_data.json"]:
        src = docs / fname
        if src.exists():
            dst = STATIC_PATH / fname
            if not dst.exists() or dst.read_bytes() != src.read_bytes():
                dst.write_bytes(src.read_bytes())


def main():
    load_fips()

    heat_path = Path("../docs/heat_data.json")
    heat_data = json.load(open(heat_path)) if heat_path.exists() else []

    # Clean and recreate content
    if CONTENT_PATH.exists():
        shutil.rmtree(CONTENT_PATH)
    CONTENT_PATH.mkdir(parents=True)

    print("Scanning KB entries...")
    parsed = scan_all_entries()
    print(f"  {len(parsed)} entries found")

    print("Generating pages...")
    entries_by_fips, entries_by_state, entries_by_type, all_entries = generate_all_pages(parsed, heat_data)
    print(f"  entry types: {sorted(entries_by_type.keys())}")
    for etype, elist in sorted(entries_by_type.items(), key=lambda x: -len(x[1])):
        print(f"    {etype}: {len(elist)}")

    print("Generating county pages...")
    generate_county_pages(entries_by_fips, heat_data)
    print(f"  {len(entries_by_fips)} counties")

    print("Generating section indexes...")
    generate_section_indexes(entries_by_type, all_entries, heat_data)

    print("Generating state pages...")
    generate_state_pages(entries_by_state, heat_data)
    print(f"  {len(entries_by_state)} states")

    print("Generating static pages...")
    generate_static_pages()

    print("Copying static assets...")
    copy_static_assets(heat_data)

    # Featured data for homepage
    fights = [e for e in all_entries if e["type"] == "county-fight"]
    contractors = [e for e in all_entries if e["type"] == "contractor"]

    with open(CONTENT_PATH / "_index.md", "w") as f:
        f.write(f"""---
title: "Detention Pipeline"
type: home
total_counties: {len(entries_by_fips)}
total_entries: {len(all_entries)}
total_states: {len(entries_by_state)}
total_facilities: {len(entries_by_type.get('igsa', [])) + len(entries_by_type.get('facility', []))}
total_fights: {len(fights)}
total_contractors: {len(contractors)}
max_score: {heat_data[0]['score'] if heat_data else 0}
---
""")

    print(f"\nDone. {len(all_entries)} entries → Hugo content.")
    print("Run 'hugo' to build.")


if __name__ == "__main__":
    main()
