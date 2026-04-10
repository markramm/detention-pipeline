#!/usr/bin/env python3
"""
Generate Hugo content pages from the detention-pipeline KB.

Reads KB entries and heat score data, generates:
- /county/{fips}/ pages (one per scored county)
- /entry/{id}/ pages (one per KB entry)
- /signals/{type}/ taxonomy pages
- /state/{abbr}/ taxonomy pages

Run from the hugo/ directory:
    python3 generate_content.py
"""

import csv
import json
import os
import re
import sys
from pathlib import Path

KB_PATH = Path("../kb")
CONTENT_PATH = Path("content")
STATIC_PATH = Path("static")
DATA_PATH = Path("data")

# Signal type metadata
SIGNAL_META = {
    "287g-agreement":     {"label": "287(g) Agreement",    "weight": 7,  "color": "#d46a2f"},
    "anc-contract":       {"label": "ANC Contract",        "weight": 8,  "color": "#c49025"},
    "budget-distress":    {"label": "Budget Distress",     "weight": 5,  "color": "#9a4fb5"},
    "commission-activity":{"label": "Commission Activity", "weight": 7,  "color": "#8a9f2a"},
    "comms-discipline":   {"label": "Comms Discipline",    "weight": 6,  "color": "#6a5fb5"},
    "job-posting":        {"label": "Job Posting",         "weight": 7,  "color": "#2a9f6f"},
    "legislative-trace":  {"label": "Legislative Trace",   "weight": 1,  "color": "#5f6a7a"},
    "real-estate-trace":  {"label": "Real Estate Trace",   "weight": 2,  "color": "#b54f8a"},
    "sheriff-network":    {"label": "Sheriff Network",     "weight": 6,  "color": "#2a7fb5"},
}

# FIPS lookup
FIPS_TO_COUNTY = {}
STATE_FIPS_TO_ABBR = {}


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
    for line in content[3:end].split("\n"):
        line = line.strip()
        if not line or line.startswith("-"):
            continue
        if ":" in line:
            key, val = line.split(":", 1)
            key = key.strip()
            val = val.strip().strip("'\"")
            if key == "tags":
                continue  # handle multi-line tags separately
            fields[key] = val

    # Parse tags
    tags = []
    in_tags = False
    for line in content[3:end].split("\n"):
        if line.strip() == "tags:":
            in_tags = True
            continue
        if in_tags:
            if line.strip().startswith("- "):
                tags.append(line.strip()[2:])
            else:
                in_tags = False
    fields["tags"] = tags

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


def generate_entry_pages():
    """Generate Hugo content for each KB entry."""
    entries_by_fips = {}
    entries_by_state = {}
    entries_by_type = {}
    all_entries = []

    entry_dir = CONTENT_PATH / "entry"
    entry_dir.mkdir(parents=True, exist_ok=True)

    skip_dirs = {"scripts", ".pyrite", "__pycache__"}
    for md_file in sorted(KB_PATH.rglob("*.md")):
        # Skip non-entry files
        if any(part in skip_dirs for part in md_file.parts):
            continue
        if md_file.name == "kb.yaml" or md_file.name.startswith("_"):
            continue

        fields, body = parse_entry(md_file)
        if not fields:
            continue

        entry_id = fields.get("id", md_file.stem)
        entry_type = fields.get("type", "unknown")
        title = fields.get("title", entry_id)
        fips = fields.get("fips", "")
        state = fields.get("state", "")
        county = fields.get("county", "")

        # Track by FIPS
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

        all_entries.append({
            "id": entry_id, "type": entry_type, "title": title,
            "state": state, "county": county, "fips": fips,
        })

        # Signal metadata
        sig_meta = SIGNAL_META.get(entry_type, {})
        signal_label = sig_meta.get("label", entry_type)
        signal_color = sig_meta.get("color", "#666")

        # Relative path in the repo for "Edit on GitHub"
        try:
            rel_path = str(md_file.relative_to(KB_PATH.parent))
        except ValueError:
            rel_path = str(md_file)

        # Write Hugo content page
        page_path = entry_dir / f"{entry_id}.md"
        with open(page_path, "w") as f:
            f.write(f"""---
title: "{title.replace('"', '\\"')}"
type: entry
layout: single
entry_type: "{entry_type}"
signal_label: "{signal_label}"
signal_color: "{signal_color}"
fips: "{fips}"
state: "{state}"
county: "{county}"
repo_path: "{rel_path}"
signals: ["{entry_type}"]
states: ["{state}"]
---

{body}
""")

    return entries_by_fips, entries_by_state, entries_by_type, all_entries


def generate_county_pages(entries_by_fips, heat_data):
    """Generate a page for each county with signals."""
    county_dir = CONTENT_PATH / "county"
    county_dir.mkdir(parents=True, exist_ok=True)

    # Index heat data
    heat_by_fips = {d["fips"]: d for d in heat_data}

    for fips, entries in entries_by_fips.items():
        heat = heat_by_fips.get(fips, {})
        county_name = heat.get("county", FIPS_TO_COUNTY.get(fips, f"County {fips}"))
        score = heat.get("score", 0)
        signal_types = heat.get("signal_types", 0)
        state = entries[0].get("state", "")
        signals = heat.get("signals", {})

        # Group entries by type
        by_type = {}
        for e in entries:
            by_type.setdefault(e["type"], []).append(e)

        # Build signal summary for template
        signal_summary = []
        for stype, sdata in signals.items():
            meta = SIGNAL_META.get(stype, {})
            signal_summary.append({
                "type": stype,
                "label": meta.get("label", stype),
                "color": meta.get("color", "#666"),
                "count": sdata.get("count", 0),
            })

        # Determine rank
        rank = 0
        for i, d in enumerate(heat_data):
            if d["fips"] == fips:
                rank = i + 1
                break
        total = len(heat_data)
        percentile = round((1 - rank / total) * 100) if total > 0 and rank > 0 else 0

        page_path = county_dir / f"{fips}.md"
        with open(page_path, "w") as f:
            f.write(f"""---
title: "{county_name.replace('"', '\\"')}"
type: county
layout: single
fips: "{fips}"
state: "{state}"
score: {score}
signal_types: {signal_types}
rank: {rank}
total_counties: {total}
percentile: {percentile}
states: ["{state}"]
---
""")

    return


def generate_state_pages(entries_by_state, heat_data):
    """Generate state index pages."""
    state_dir = CONTENT_PATH / "state"
    state_dir.mkdir(parents=True, exist_ok=True)

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

    heat_by_fips = {d["fips"]: d for d in heat_data}

    for state_abbr, entries in entries_by_state.items():
        state_name = STATE_NAMES.get(state_abbr, state_abbr)

        # Counties in this state from heat data
        state_counties = [d for d in heat_data if d["state"] == state_abbr]
        state_counties.sort(key=lambda x: -x["score"])
        total_entries = len(entries)

        page_path = state_dir / f"{state_abbr.lower()}.md"
        with open(page_path, "w") as f:
            f.write(f"""---
title: "{state_name}"
type: state
layout: single
state_abbr: "{state_abbr}"
county_count: {len(state_counties)}
entry_count: {total_entries}
---
""")


def generate_signal_pages():
    """Generate signal type index pages."""
    sig_dir = CONTENT_PATH / "signals"
    sig_dir.mkdir(parents=True, exist_ok=True)

    for stype, meta in SIGNAL_META.items():
        page_path = sig_dir / f"{stype}.md"
        with open(page_path, "w") as f:
            f.write(f"""---
title: "{meta['label']}"
type: signal
layout: single
signal_type: "{stype}"
signal_color: "{meta['color']}"
weight_score: {meta['weight']}
---
""")


def generate_static_pages():
    """Generate methodology and contribute pages."""
    page_dir = CONTENT_PATH
    page_dir.mkdir(parents=True, exist_ok=True)

    # Methodology
    with open(page_dir / "methodology.md", "w") as f:
        f.write("""---
title: "Methodology"
type: page
layout: methodology
---
""")

    # Contribute
    with open(page_dir / "contribute.md", "w") as f:
        f.write("""---
title: "Contribute"
type: page
layout: contribute
---
""")


def copy_static_assets(heat_data):
    """Copy heat map assets and write data files."""
    # Heat data as Hugo data file
    DATA_PATH.mkdir(parents=True, exist_ok=True)
    with open(DATA_PATH / "heat.json", "w") as f:
        json.dump(heat_data, f)

    # Signal metadata
    with open(DATA_PATH / "signals.json", "w") as f:
        json.dump(SIGNAL_META, f, indent=2)

    # Copy static assets from docs/ to static/
    docs = Path("../docs")
    static = STATIC_PATH
    static.mkdir(parents=True, exist_ok=True)
    for fname in ["counties-albers-10m.json", "fips_names.json", "heat_data.json"]:
        src = docs / fname
        if src.exists():
            dst = static / fname
            if not dst.exists() or dst.read_bytes() != src.read_bytes():
                dst.write_bytes(src.read_bytes())


def main():
    load_fips()

    # Load heat data
    heat_path = Path("../docs/heat_data.json")
    if heat_path.exists():
        with open(heat_path) as f:
            heat_data = json.load(f)
    else:
        heat_data = []

    # Clean content dir
    import shutil
    if CONTENT_PATH.exists():
        shutil.rmtree(CONTENT_PATH)
    CONTENT_PATH.mkdir(parents=True)

    print("Generating entry pages...")
    entries_by_fips, entries_by_state, entries_by_type, all_entries = generate_entry_pages()
    print(f"  {len(all_entries)} entries")

    print("Generating county pages...")
    generate_county_pages(entries_by_fips, heat_data)
    print(f"  {len(entries_by_fips)} counties")

    print("Generating state pages...")
    generate_state_pages(entries_by_state, heat_data)
    print(f"  {len(entries_by_state)} states")

    print("Generating signal type pages...")
    generate_signal_pages()
    print(f"  {len(SIGNAL_META)} signal types")

    print("Generating static pages...")
    generate_static_pages()

    print("Copying static assets...")
    copy_static_assets(heat_data)

    # Write a homepage
    with open(CONTENT_PATH / "_index.md", "w") as f:
        f.write(f"""---
title: "Detention Pipeline"
type: home
total_counties: {len(entries_by_fips)}
total_entries: {len(all_entries)}
total_states: {len(entries_by_state)}
max_score: {heat_data[0]['score'] if heat_data else 0}
---
""")

    print("Done. Run 'hugo' to build.")


if __name__ == "__main__":
    main()
