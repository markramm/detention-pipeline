#!/usr/bin/env python3
"""
Generate network.json for the revolving door / conflict visualization.
Reads people, contractor, organization, and financial-flow entries
from the KB and builds a structured graph.

Run from hugo/ directory:
    python3 generate_network.py
"""

import json
import re
from pathlib import Path

KB_PATH = Path("../kb")
DATA_PATH = Path("data")

def parse_frontmatter(filepath):
    """Parse YAML frontmatter from a markdown file."""
    with open(filepath) as f:
        content = f.read()
    if not content.startswith("---"):
        return {}, content
    try:
        end = content.index("---", 3)
    except ValueError:
        return {}, content
    fields = {}
    list_fields = {}
    current_list = None
    for line in content[3:end].split("\n"):
        s = line.strip()
        if not s:
            current_list = None
            continue
        if s.startswith("- ") and current_list:
            list_fields.setdefault(current_list, []).append(s[2:])
            continue
        if ":" in s and not s.startswith("-"):
            k, v = s.split(":", 1)
            k = k.strip()
            v = v.strip().strip("'\"")
            if not v:
                current_list = k
            else:
                current_list = None
                fields[k] = v
    fields["_lists"] = list_fields
    body = content[end+3:].strip()
    return fields, body


def build_revolving_door():
    """Build the revolving door Sankey data."""
    nodes = []
    links = []
    node_index = {}

    def add_node(id, label, type, **extra):
        if id not in node_index:
            node_index[id] = len(nodes)
            node = {"id": id, "label": label, "type": type}
            node.update(extra)
            nodes.append(node)
        return node_index[id]

    # Government agencies
    add_node("ice", "ICE", "government", color="#4a7ab5")
    add_node("dhs", "DHS", "government", color="#4a7ab5")
    add_node("gsa", "GSA", "government", color="#4a7ab5")
    add_node("navy", "U.S. Navy", "government", color="#4a7ab5")
    add_node("omb", "OMB", "government", color="#4a7ab5")

    # Read people entries
    people_dir = KB_PATH / "industry" / "people"
    if people_dir.exists():
        for f in sorted(people_dir.glob("*.md")):
            fields, body = parse_frontmatter(f)
            pid = fields.get("id", f.stem)
            title = fields.get("title", pid)
            name = title.split("—")[0].strip() if "—" in title else title
            gov = fields.get("government_service", "")
            priv = fields.get("private_role", "")
            role = fields.get("role", "")
            affs = fields.get("_lists", {}).get("affiliations", [])

            person_idx = add_node(pid, name, "person", role=role,
                                   url=f"/players/people/{pid}/")

            # Parse government service for agency
            gov_lower = gov.lower()
            if "ice" in gov_lower or "immigration" in gov_lower:
                gov_agency = "ice"
            elif "gsa" in gov_lower:
                gov_agency = "gsa"
            elif "dhs" in gov_lower:
                gov_agency = "dhs"
            elif "omb" in gov_lower:
                gov_agency = "omb"
            elif "navy" in gov_lower:
                gov_agency = "navy"
            else:
                gov_agency = "dhs"  # default

            # Extract gov title
            gov_title = gov[:80] if gov else ""

            links.append({
                "source": node_index[gov_agency],
                "target": person_idx,
                "type": "government-service",
                "label": gov_title,
                "value": 3,
            })

            # Map known company names to node IDs
            company_map = {
                "geo group": ("geo-group", "GEO Group"),
                "geo care": ("geo-group", "GEO Group"),
                "corecivic": ("corecivic", "CoreCivic"),
                "sabot consulting": ("sabot-consulting", "Sabot Consulting"),
                "sabot": ("sabot-consulting", "Sabot Consulting"),
                "blue owl": ("blue-owl-capital", "Blue Owl Capital"),
                "anduril": ("anduril", "Anduril Industries"),
                "goldman sachs": ("goldman-sachs", "Goldman Sachs"),
                "deutsche bank": ("deutsche-bank", "Deutsche Bank"),
                "palantir": ("palantir", "Palantir Technologies"),
                "newmark": ("newmark", "Newmark"),
                "cbre": ("cbre", "CBRE"),
            }

            # Check affiliations list
            all_sources = list(affs) if affs else []
            # Also check private_role and body text for company references
            check_text = (priv + " " + body[:2000]).lower()
            for key, (node_id, node_label) in company_map.items():
                if key in check_text or any(key in a.lower() for a in all_sources):
                    if node_id not in node_index:
                        add_node(node_id, node_label, "contractor",
                                color="#d46a2f",
                                url=f"/players/contractors/{node_id}/")
                    # Avoid duplicate links
                    exists = any(l["source"] == person_idx and l["target"] == node_index[node_id] for l in links)
                    if not exists:
                        links.append({
                            "source": person_idx,
                            "target": node_index[node_id],
                            "type": "revolving-door",
                            "label": priv[:100] if priv else node_label,
                            "value": 3,
                        })

    # Read contractor entries to ensure key ones exist as nodes
    contractors_dir = KB_PATH / "industry" / "contractors"
    if contractors_dir.exists():
        for f in sorted(contractors_dir.glob("*.md")):
            fields, body = parse_frontmatter(f)
            cid = fields.get("id", f.stem)
            title = fields.get("title", cid)
            ctype = fields.get("contractor_type", "")
            if cid not in node_index:
                add_node(cid, title, "contractor", color="#d46a2f",
                        contractor_type=ctype,
                        url=f"/players/contractors/{cid}/")

    # Read organizations
    org_dir = KB_PATH / "industry" / "organizations"
    if org_dir.exists():
        for f in sorted(org_dir.glob("*.md")):
            fields, body = parse_frontmatter(f)
            oid = fields.get("id", f.stem)
            title = fields.get("title", oid)
            if oid not in node_index:
                add_node(oid, title, "organization", color="#8a2a5a",
                        url=f"/players/money/{oid}/")

    return {"nodes": nodes, "links": links}


def build_conflict_matrix():
    """Build the official-to-company financial conflict matrix."""
    # Hardcoded from the comprehensive conflict-of-interest map
    # This is the data that's most impactful as a matrix view
    officials = [
        {"id": "trump", "name": "Donald Trump", "role": "President"},
        {"id": "miller", "name": "Stephen Miller", "role": "Homeland Security Advisor"},
        {"id": "homan", "name": "Thomas Homan", "role": "Border Czar, DHS"},
        {"id": "edgar", "name": "Troy Edgar", "role": "Deputy Secretary, DHS"},
        {"id": "forst", "name": "Edward Forst", "role": "GSA Administrator"},
        {"id": "phelan", "name": "John Phelan", "role": "Secretary of the Navy"},
        {"id": "rhodes", "name": "Kevin Rhodes", "role": "Federal Procurement Policy, OMB"},
        {"id": "barbaccia", "name": "Gregory Barbaccia", "role": "Federal CIO, OMB"},
        {"id": "patel", "name": "Kash Patel", "role": "Director, DOJ"},
        {"id": "venturella", "name": "David Venturella", "role": "Senior Adviser, DHS"},
        {"id": "mccord", "name": "Antoine McCord", "role": "CIO, DHS"},
        {"id": "danley", "name": "Christopher Danley", "role": "Senior Advisor, Interior"},
        {"id": "minor", "name": "Clark Minor", "role": "CTO/CIO, HHS"},
        {"id": "mcgranahan", "name": "John McGranahan", "role": "Former GC, GSA"},
        {"id": "dalelio", "name": "Edward D'Alelio", "role": "Director, Blue Owl"},
    ]

    companies = [
        {"id": "palantir", "name": "Palantir", "type": "surveillance"},
        {"id": "blue-owl", "name": "Blue Owl Capital", "type": "warehouse-seller"},
        {"id": "goldman", "name": "Goldman Sachs", "type": "warehouse-seller"},
        {"id": "geo", "name": "GEO Group", "type": "private-prison"},
        {"id": "anduril", "name": "Anduril", "type": "surveillance"},
        {"id": "cbre", "name": "CBRE", "type": "real-estate"},
        {"id": "deutsche", "name": "Deutsche Bank", "type": "warehouse-seller"},
        {"id": "relx", "name": "RELX/LexisNexis", "type": "surveillance"},
    ]

    # Conflict ties: [official_id, company_id, type, detail]
    # Sources: OGE financial disclosures via trump-appointee KB
    ties = [
        # Trump
        ["trump", "blue-owl", "stock", "$5M+"],
        ["trump", "goldman", "stock", "Significant holdings"],
        # Miller
        ["miller", "palantir", "stock", "$100K-$250K"],
        # Homan
        ["homan", "geo", "compensation", "GEO Care consulting fees"],
        ["homan", "cbre", "stock", "Personal holdings"],
        ["homan", "relx", "stock", "LexisNexis parent, personal IRA"],
        # Edgar
        ["edgar", "palantir", "stock", "$250K-$500K"],
        # Forst
        ["forst", "goldman", "stock/PE", "$1.8M-$6.1M+"],
        ["forst", "deutsche", "pension", "Legacy Bankers Trust"],
        ["forst", "palantir", "stock", "$15K-$50K"],
        ["forst", "cbre", "stock", "Brokered Williamsport sale"],
        # Phelan
        ["phelan", "blue-owl", "stock", "Multiple holdings; administers WEXMAC-TITUS"],
        # Rhodes
        ["rhodes", "palantir", "stock", "$1K-$15K; controls all federal procurement"],
        # Barbaccia
        ["barbaccia", "anduril", "investment", "$100K-$250K MWSI VC"],
        # Patel
        ["patel", "palantir", "stock", "$50K-$100K"],
        ["patel", "goldman", "stock", "Holdings"],
        # Venturella
        ["venturella", "geo", "compensation", "$6M+ over 12 years"],
        # McCord
        ["mccord", "anduril", "equity", "Active RSUs, 401k, ESPP ($250K-$500K)"],
        ["mccord", "palantir", "procurement", "Oversees ICE tech contracts"],
        # Danley
        ["danley", "geo", "stock", "Purchased $15K-$50K June 2025"],
        ["danley", "palantir", "stock", "Multiple sales documented"],
        # Minor
        ["minor", "palantir", "stock", "$1M-$5M"],
        # McGranahan
        ["mcgranahan", "blue-owl", "investment", "Personal holdings"],
        # D'Alelio
        ["dalelio", "blue-owl", "employment", "Director"],
    ]

    return {"officials": officials, "companies": companies, "ties": ties}


def main():
    DATA_PATH.mkdir(parents=True, exist_ok=True)

    print("Building revolving door data...")
    revolving = build_revolving_door()
    print(f"  {len(revolving['nodes'])} nodes, {len(revolving['links'])} links")

    print("Building conflict matrix data...")
    conflicts = build_conflict_matrix()
    print(f"  {len(conflicts['officials'])} officials, {len(conflicts['companies'])} companies, {len(conflicts['ties'])} ties")

    network = {
        "revolving_door": revolving,
        "conflict_matrix": conflicts,
    }

    with open(DATA_PATH / "network.json", "w") as f:
        json.dump(network, f, indent=2)

    # Also copy to static for client-side access
    static = Path("static")
    static.mkdir(parents=True, exist_ok=True)
    with open(static / "network.json", "w") as f:
        json.dump(network, f)

    print(f"Saved to {DATA_PATH / 'network.json'} and {static / 'network.json'}")


if __name__ == "__main__":
    main()
