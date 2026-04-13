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
from datetime import datetime
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
    "organization":        {"label": "Organization",        "color": "#4a7ab5", "section": "organizations"},
    "personnel-flow":      {"label": "Personnel Flow",      "color": "#7a4ab5", "section": "players"},
    "county-fight":        {"label": "County Fight",        "color": "#2a8a5a", "section": "fights"},
    "event":               {"label": "Event",               "color": "#5a7a6a", "section": "players"},
    "note":                {"label": "Research Note",       "color": "#6a6a6a", "section": "entry"},
    # Facilities
    "igsa":                {"label": "IGSA Facility",       "color": "#c93b3b", "weight": 10, "section": "facilities"},
    "facility":            {"label": "Facility",            "color": "#c93b3b", "section": "facilities"},
}

# Signal types only (for signal index pages and heat score)
SIGNAL_META = {k: v for k, v in ENTRY_TYPE_META.items() if v.get("weight")}

# Coverage depth classification
AUTOMATED_TYPES = {"287g-agreement", "anc-contract", "igsa", "budget-distress"}
HUMAN_TYPES = {"commission-activity", "comms-discipline", "sheriff-network",
               "real-estate-trace", "job-posting", "county-fight", "legislative-trace",
               "contract", "analysis", "event", "person", "organization"}

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

# Reverse lookup: full state name → abbreviation
_STATE_ABBR = {v.lower(): k for k, v in STATE_NAMES.items()}

_STATE_ABBR_EXTRA = {"w. va.": "WV", "w.va.": "WV", "d.c.": "DC"}

def normalize_state(s):
    """Normalize state to 2-letter abbreviation."""
    s = s.strip().strip('"').strip("'")
    if len(s) == 2 and s.upper() in STATE_NAMES:
        return s.upper()
    low = s.lower()
    return _STATE_ABBR.get(low, _STATE_ABBR_EXTRA.get(low, s))


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
                    fields["state"] = normalize_state(v)
            elif bline.startswith("County:") and not fields.get("county"):
                v = bline.split(":", 1)[1].strip()
                if v:
                    fields["county"] = v

    return fields, body


def esc(s):
    """Escape string for YAML frontmatter."""
    return s.replace('"', '\\"').replace('\n', ' ')


def normalize_title(title):
    """Fix screaming-case titles and unreadable contract IDs."""
    # If more than half the alpha chars are uppercase and title > 20 chars, it's screaming
    alpha = [c for c in title if c.isalpha()]
    if len(alpha) > 20 and sum(1 for c in alpha if c.isupper()) > len(alpha) * 0.6:
        title = title.title()
        # Restore common acronyms that .title() breaks
        for acronym in ["Llc", "Inc", "Ice", "Dhs", "Cbp", "Nj", "Ny", "Tx", "Fl",
                        "Ca", "Az", "Ga", "Va", "Pa", "Nc", "Sc", "Md", "Tn", "Mn",
                        "Wi", "Mi", "Oh", "Il", "La", "Mo", "Ks", "Ok", "Ar", "Ms",
                        "Al", "Ky", "Wv", "Nm", "Nd", "Sd", "Mt", "Wy", "Id", "Ut",
                        "Nv", "Co", "Ne", "Ia", "In", "Ct", "Ri", "Nh", "Vt", "Me",
                        "De", "Hi", "Ak", "Or", "Wa", "Dc"]:
            upper = acronym.upper()
            # Only replace as whole word (with word boundary)
            title = re.sub(r'\b' + acronym + r'\b', upper, title)
    # Clean up contract titles that are just dollar amounts or IDs
    title = re.sub(r'\s+\d{6,}$', '', title)  # strip trailing numeric IDs
    return title


# Wikilink slug -> canonical URL mapping (built during page generation)
_wikilink_urls = {}

def build_wikilink_map(parsed_entries):
    """Build a map of entry slugs to their canonical URLs."""
    for parsed in parsed_entries:
        fields = parsed["fields"]
        entry_id = fields.get("id", parsed["md_file"].stem)
        entry_type = fields.get("type", "unknown")
        if entry_type == "county-fight":
            _wikilink_urls[entry_id] = f"/fights/{entry_id}/"
        elif entry_type == "contractor":
            _wikilink_urls[entry_id] = f"/players/contractors/{entry_id}/"
        elif entry_type == "person":
            _wikilink_urls[entry_id] = f"/players/people/{entry_id}/"
        elif entry_type in ("financial-flow", "analysis"):
            _wikilink_urls[entry_id] = f"/players/money/{entry_id}/"
        elif entry_type == "organization":
            _wikilink_urls[entry_id] = f"/organizations/{entry_id}/"
        elif entry_type in ("igsa", "facility"):
            _wikilink_urls[entry_id] = f"/facilities/{entry_id}/"
        else:
            _wikilink_urls[entry_id] = f"/entry/{entry_id}/"


def resolve_wikilinks(body):
    """Convert [[slug|Display Text]] and [[slug]] to markdown links."""
    def resolve_slug(slug):
        return _wikilink_urls.get(slug, f"/entry/{slug}/")

    # [[slug|Display Text]] -> [Display Text](canonical_url)
    body = re.sub(
        r'\[\[([^\]|]+)\|([^\]]+)\]\]',
        lambda m: f'[{m.group(2)}]({resolve_slug(m.group(1))})',
        body
    )
    # [[slug]] -> [slug](canonical_url)
    body = re.sub(
        r'\[\[([^\]]+)\]\]',
        lambda m: f'[{m.group(1)}]({resolve_slug(m.group(1))})',
        body
    )
    return body


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
        mod_time = datetime.fromtimestamp(md_file.stat().st_mtime).strftime("%Y-%m-%d")
        entries.append({
            "fields": fields,
            "body": body,
            "rel_path": rel_path,
            "md_file": md_file,
            "last_updated": mod_time,
        })
    return entries


def generate_all_pages(parsed_entries, heat_data):
    """Generate all Hugo content from parsed entries."""
    entries_by_fips = {}
    entries_by_state = {}
    entries_by_type = {}
    all_entries_meta = []

    # Load LLM-generated summaries if available
    llm_summaries = {}
    summary_path = DATA_PATH / "summaries.json"
    if summary_path.exists():
        with open(summary_path) as f:
            llm_summaries = json.load(f)
        print(f"  Loaded {len(llm_summaries)} LLM summaries")

    # Section directories
    for d in ["entry", "fights", "players/contractors", "players/people",
              "players/money", "organizations", "facilities", "signals",
              "county", "state", "map"]:
        (CONTENT_PATH / d).mkdir(parents=True, exist_ok=True)

    for parsed in parsed_entries:
        fields = parsed["fields"]
        body = parsed["body"]
        rel_path = parsed["rel_path"]

        entry_id = fields.get("id", parsed["md_file"].stem)
        entry_type = fields.get("type", "unknown")
        title = normalize_title(fields.get("title", entry_id))
        fips = fields.get("fips", "")
        state = normalize_state(fields.get("state", ""))
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

        # Generate or look up summary
        summary = ""
        # Check for LLM-generated summary in sidecar file
        if entry_id in llm_summaries:
            summary = llm_summaries[entry_id]
        # Template summaries for automated entry types
        elif entry_type == "287g-agreement":
            # Extract model from body if not in fields
            model = fields.get("model", "")
            if not model:
                for bline in body.split("\n"):
                    if bline.strip().startswith("Model:"):
                        model = bline.split(":", 1)[1].strip()
                        break
            signed = fields.get("signed_date", "")
            if not signed:
                for bline in body.split("\n"):
                    if bline.strip().startswith("Signed:"):
                        signed = bline.split(":", 1)[1].strip()
                        break
            agency = ""
            for bline in body.split("\n"):
                if "agreement between ICE and" in bline:
                    agency = bline.split("agreement between ICE and", 1)[1].strip().rstrip(".")
                    break
            if agency:
                summary = f"287(g) {model} agreement between ICE and {agency}."
            else:
                summary = f"287(g) {model} agreement in {county + ', ' if county else ''}{state}."
            if signed:
                summary = summary[:-1] + f", signed {signed}."
        elif entry_type == "igsa":
            fname = fields.get("facility_name", "")
            op = fields.get("operator", "")
            summary = f"IGSA detention facility in {county + ', ' if county else ''}{state}."
            if op:
                summary = summary[:-1] + f", operated by {op}."
        elif entry_type == "anc-contract":
            contractor = fields.get("contractor", "")
            val = fields.get("contract_value", "")
            summary = f"Federal contract awarded to {contractor or 'ANC subsidiary'} in {state}."
            if val:
                summary = summary[:-1] + f" ({val})."
        elif entry_type == "budget-distress":
            summary = f"Budget distress indicators for {county + ', ' if county else ''}{state}."
        elif not summary:
            # Fallback: first substantive sentence from body, stripped of markdown
            for line in body.split("\n"):
                line = line.strip()
                if line and not line.startswith("#") and not line.startswith("-") and not line.startswith("*") and not line.startswith("|") and not line.startswith("{{") and len(line) > 30:
                    # Strip markdown formatting
                    clean = re.sub(r'\*\*([^*]+)\*\*', r'\1', line)  # bold
                    clean = re.sub(r'\*([^*]+)\*', r'\1', clean)  # italic
                    clean = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', clean)  # links
                    clean = clean.strip()
                    summary = clean[:200]
                    if len(clean) > 200:
                        summary = summary.rsplit(" ", 1)[0] + "..."
                    break

        # Count sources (URLs, citation patterns) in body
        source_count = len(re.findall(r'https?://', body))
        source_count += len(re.findall(r'\bSource[s]?:', body))
        # Cap at reasonable number — some entries have many inline URLs
        source_count = min(source_count, 20)

        # Cascade cross-link
        cascade_url = fields.get("cascade_url", "")

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
            "lastmod": parsed["last_updated"],
            "summary": esc(summary),
            "source_count": source_count,
        }
        if cascade_url:
            fm["cascade_url"] = cascade_url
        source_url = fields.get("source_url", "")
        if source_url:
            fm["source_url"] = source_url

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
            fm["layout"] = "single"
            fm["player_type"] = "contractor"
            fm["contractor_type"] = fields.get("contractor_type", "")
            fm["headquarters"] = esc(fields.get("headquarters", ""))
            fm["founded"] = fields.get("founded", "")
            fm["status"] = fields.get("status", "")
            # Key facilities for cross-linking
            key_facs = fields.get("_list_fields", {}).get("key_facilities", [])
            if key_facs:
                fm["key_facilities"] = key_facs
        elif entry_type == "person":
            page_path = CONTENT_PATH / "players" / "people" / f"{entry_id}.md"
            fm["layout"] = "single"
            fm["player_type"] = "person"
            fm["role"] = fields.get("role", "")
            fm["government_service"] = esc(fields.get("government_service", ""))
            fm["private_role"] = esc(fields.get("private_role", ""))
        elif entry_type in ("financial-flow", "analysis"):
            page_path = CONTENT_PATH / "players" / "money" / f"{entry_id}.md"
            fm["layout"] = "single"
            fm["player_type"] = "money"
        elif entry_type == "organization":
            page_path = CONTENT_PATH / "organizations" / f"{entry_id}.md"
            fm["type"] = "organizations"
            fm["layout"] = "single"
            fm["org_type"] = fields.get("org_type", "")
            fm["headquarters"] = esc(fields.get("headquarters", ""))
            fm["status"] = fields.get("status", "")
            fm["ticker"] = esc(fields.get("ticker", ""))
            fm["aum"] = esc(fields.get("aum", ""))
        elif entry_type in ("igsa", "facility"):
            page_path = CONTENT_PATH / "facilities" / f"{entry_id}.md"
            fm["layout"] = "single"
            fm["facility_name"] = esc(fields.get("facility_name", ""))
            fm["operator"] = esc(fields.get("operator", ""))
            fm["status"] = fields.get("status", "")
            fm["bed_count"] = fields.get("bed_count", fields.get("capacity", ""))
            fm["facility_type"] = fields.get("facility_type", "")
            fm["city"] = esc(fields.get("city", ""))
            fm["address"] = esc(fields.get("address", ""))
            fm["aor"] = esc(fields.get("aor", ""))
            fm["avg_daily_pop"] = fields.get("avg_daily_pop", "")
        else:
            # Default: entry page
            page_path = CONTENT_PATH / "entry" / f"{entry_id}.md"
            fm["type"] = "entry"
            fm["layout"] = "single"

        # Clean body: strip ALL H1 lines (title is rendered by template) and resolve wikilinks
        clean_body = body
        lines = clean_body.split("\n")
        # Remove all lines that are H1 headings (# Title)
        lines = [l for l in lines if not (l.startswith("# ") and not l.startswith("## "))]
        # Also strip leading blank lines
        while lines and lines[0].strip() == "":
            lines.pop(0)
        clean_body = "\n".join(lines)
        resolved_body = resolve_wikilinks(clean_body)

        # Write the page
        fm_lines = "\n".join(f'{k}: "{v}"' if isinstance(v, str) else f'{k}: {v}'
                             for k, v in fm.items())
        with open(page_path, "w") as f:
            f.write(f"---\n{fm_lines}\n---\n\n{resolved_body}\n")

        # No duplicate entry/ page — each entry lives in one canonical section only

    # Build cross-reference indexes for contractor ↔ county linking
    # Index: which contractors are referenced in entries for each FIPS
    contractor_ids = {e["id"] for e in all_entries_meta if e["type"] == "contractor"}
    facility_ids = {e["id"] for e in all_entries_meta if e["type"] in ("igsa", "facility")}

    return entries_by_fips, entries_by_state, entries_by_type, all_entries_meta


def generate_county_pages(entries_by_fips, heat_data):
    """Generate county pages with coverage depth."""
    county_dir = CONTENT_PATH / "county"
    county_dir.mkdir(parents=True, exist_ok=True)
    heat_by_fips = {d["fips"]: d for d in heat_data}
    rank_by_fips = {d["fips"]: i + 1 for i, d in enumerate(heat_data)}
    total = len(heat_data)

    for fips, entries in entries_by_fips.items():
        heat = heat_by_fips.get(fips, {})
        county_name = heat.get("county", FIPS_TO_COUNTY.get(fips, f"County {fips}"))
        score = heat.get("score", 0)
        signal_types = heat.get("signal_types", 0)
        state = entries[0].get("state", "")

        rank = rank_by_fips.get(fips, 0)
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

        # Generate auto-summary for county narrative (#4)
        signal_descriptions = []
        type_counts = {}
        for e in entries:
            type_counts[e["type"]] = type_counts.get(e["type"], 0) + 1

        summary_parts = []
        if "igsa" in type_counts:
            summary_parts.append(f"{type_counts['igsa']} existing IGSA facilit{'y' if type_counts['igsa'] == 1 else 'ies'}")
        if "287g-agreement" in type_counts:
            summary_parts.append(f"{type_counts['287g-agreement']} 287(g) agreement{'s' if type_counts['287g-agreement'] > 1 else ''}")
        if "anc-contract" in type_counts:
            summary_parts.append(f"{type_counts['anc-contract']} ANC contract{'s' if type_counts['anc-contract'] > 1 else ''}")
        if "commission-activity" in type_counts:
            summary_parts.append("commission activity")
        if "job-posting" in type_counts:
            summary_parts.append("detention consultant job posting")
        if "budget-distress" in type_counts:
            summary_parts.append("budget distress")
        if "comms-discipline" in type_counts:
            summary_parts.append("communications discipline pattern")
        if "sheriff-network" in type_counts:
            summary_parts.append("sheriff network activity")
        if "real-estate-trace" in type_counts:
            summary_parts.append(f"{type_counts['real-estate-trace']} real estate trace{'s' if type_counts['real-estate-trace'] > 1 else ''}")
        if "county-fight" in type_counts:
            summary_parts.append("active community resistance")

        if summary_parts:
            auto_summary = f"{county_name} shows {', '.join(summary_parts[:-1])}"
            if len(summary_parts) > 1:
                auto_summary += f", and {summary_parts[-1]}"
            elif summary_parts:
                auto_summary = f"{county_name} shows {summary_parts[0]}"
            auto_summary += "."
        else:
            auto_summary = ""

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
auto_summary: "{esc(auto_summary)}"
states: ["{state}"]
---
""")

        # Write JSON endpoint
        json_dir = STATIC_PATH / "county"
        json_dir.mkdir(parents=True, exist_ok=True)
        json_path = json_dir / f"{fips}.json"
        county_json = {
            "fips": fips,
            "county": county_name,
            "state": state,
            "score": score,
            "signal_types": signal_types,
            "rank": rank,
            "total_counties": total,
            "percentile": percentile,
            "coverage": coverage,
            "auto_summary": auto_summary,
            "entry_types": dict(type_counts),
        }
        with open(json_path, "w") as jf:
            json.dump(county_json, jf, indent=2)


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
    org_count_for_players = len(entries_by_type.get("organization", []))
    with open(CONTENT_PATH / "players" / "_index.md", "w") as f:
        f.write(f"""---
title: "The Players"
layout: list
contractor_count: {contractor_count}
people_count: {people_count}
money_count: {money_count}
org_count: {org_count_for_players}
---
""")
    with open(CONTENT_PATH / "players" / "contractors" / "_index.md", "w") as f:
        f.write(f"""---
title: "Contractors"
type: contractors
layout: list
count: {contractor_count}
---
""")
    with open(CONTENT_PATH / "players" / "people" / "_index.md", "w") as f:
        f.write(f"""---
title: "People"
type: people
layout: list
count: {people_count}
---
""")
    with open(CONTENT_PATH / "players" / "money" / "_index.md", "w") as f:
        f.write(f"""---
title: "Financial Flows"
type: money
layout: list
count: {money_count}
---
""")

    # Organizations index
    org_count = len(entries_by_type.get("organization", []))
    with open(CONTENT_PATH / "organizations" / "_index.md", "w") as f:
        f.write(f"""---
title: "Organizations"
type: organizations
layout: list
count: {org_count}
---
""")

    # Facilities index
    facility_count = len(entries_by_type.get("igsa", [])) + len(entries_by_type.get("facility", []))
    with open(CONTENT_PATH / "facilities" / "_index.md", "w") as f:
        f.write(f"""---
title: "Detention Facilities"
type: facilities
layout: list
facility_count: {facility_count}
---
""")

    # Signals index
    with open(CONTENT_PATH / "signals" / "_index.md", "w") as f:
        f.write("""---
title: "Signal Types"
type: signals
layout: list
---
""")

    # Signal type pages
    for stype, meta in SIGNAL_META.items():
        page_path = CONTENT_PATH / "signals" / f"{stype}.md"
        with open(page_path, "w") as f:
            f.write(f"""---
title: "{meta['label']}"
type: signals
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

    # Network page
    (CONTENT_PATH / "network").mkdir(parents=True, exist_ok=True)
    with open(CONTENT_PATH / "network" / "_index.md", "w") as f:
        f.write("""---
title: "The Network"
type: network
layout: single
---
""")

    # County list index — no type override so Hugo uses county/ templates
    with open(CONTENT_PATH / "county" / "_index.md", "w") as f:
        f.write(f"""---
title: "All Tracked Counties"
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
type: state
layout: list
---
""")


def generate_static_pages():
    """Generate methodology and contribute pages."""
    STATIC_TITLES = {
        "tactics": "Playbook & Counter-Playbook",
    }
    for name, layout in [("methodology", "methodology"), ("contribute", "contribute"), ("foia", "foia"), ("tactics", "tactics"), ("coverage", "coverage"), ("pipeline", "pipeline"), ("sources", "sources"), ("develop", "develop")]:
        title = STATIC_TITLES.get(name, name.title())
        with open(CONTENT_PATH / f"{name}.md", "w") as f:
            f.write(f"""---
title: "{title}"
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
    if heat_path.exists():
        with open(heat_path) as f:
            heat_data = json.load(f)
    else:
        heat_data = []

    # Clean and recreate content, preserving manually-authored directories
    PRESERVE_DIRS = {"blog"}
    if CONTENT_PATH.exists():
        for child in list(CONTENT_PATH.iterdir()):
            if child.name in PRESERVE_DIRS:
                continue
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
    CONTENT_PATH.mkdir(parents=True, exist_ok=True)

    print("Scanning KB entries...")
    parsed = scan_all_entries()
    print(f"  {len(parsed)} entries found")

    print("Building wikilink map...")
    build_wikilink_map(parsed)
    print(f"  {len(_wikilink_urls)} slugs mapped")

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

    # Generate facilities map data for overlay
    facility_types = entries_by_type.get("igsa", []) + entries_by_type.get("facility", [])
    fac_map_data = []
    seen_fips = {}
    for fac in facility_types:
        fips = fac.get("fips", "")
        if not fips:
            continue
        fac_map_data.append({
            "id": fac["id"],
            "fips": fips,
            "name": fac.get("title", fac["id"]),
            "state": fac.get("state", ""),
        })
        seen_fips.setdefault(fips, []).append(fac["id"])
    with open(STATIC_PATH / "facilities_map.json", "w") as f:
        json.dump(fac_map_data, f)
    print(f"  {len(fac_map_data)} facilities mapped for overlay ({len(seen_fips)} unique counties)")

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
