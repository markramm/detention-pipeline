#!/usr/bin/env python3
"""
Extract dates from detention pipeline entries and generate timeline.json.

Parses date information from entry body text and frontmatter to build
a chronological dataset for the D3 timeline visualization.

Date sources by entry type:
- 287g-agreement: "Signed: {date}" in body text
- anc-contract: "Period: {start} to {end}" in body text
- county-fight: vote_tally dates, outcome text dates
- facility: "opened" field or dates in body
- budget-distress: lastmod as fallback
- real-estate-trace: dates in body or summary
- contract: "Period:" in body text
- All others: lastmod as fallback

Usage:
    python generate_timeline.py
"""

import json
import re
import sys
from datetime import datetime
from pathlib import Path

CONTENT_DIR = Path(__file__).parent.parent / "content"
DATA_DIR = Path(__file__).parent.parent / "data"

# Month name to number
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "jun": 6, "jul": 7, "aug": 8, "sep": 9,
    "oct": 10, "nov": 11, "dec": 12,
}


def parse_date_text(text):
    """Extract a YYYY-MM-DD date from various text formats."""
    if not text:
        return None

    # ISO format: 2025-09-30
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})', text)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # Long format: February 26, 2025 or Feb 26, 2025
    m = re.search(r'(\w+)\s+(\d{1,2}),?\s+(\d{4})', text)
    if m:
        month_name = m.group(1).lower()
        if month_name in MONTHS:
            return f"{m.group(3)}-{MONTHS[month_name]:02d}-{int(m.group(2)):02d}"

    # Short format: 02/26/2025
    m = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', text)
    if m:
        return f"{m.group(3)}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"

    return None


def parse_frontmatter(content):
    """Parse YAML frontmatter from markdown content."""
    if not content.startswith("---"):
        return {}, content

    parts = content.split("---", 2)
    if len(parts) < 3:
        return {}, content

    fm = {}
    for line in parts[1].strip().split("\n"):
        if ":" in line:
            key, _, val = line.partition(":")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            fm[key] = val

    return fm, parts[2]


def extract_date_from_entry(fm, body, entry_type):
    """Extract the most relevant date from an entry."""

    # 287(g) — look for "Signed: {date}"
    if entry_type == "287g-agreement":
        m = re.search(r'Signed:\s*(.+)', body)
        if m:
            d = parse_date_text(m.group(1))
            if d:
                return d, "signed"

    # ANC contracts — look for "Period: {start} to {end}" or "Award ID" context
    if entry_type == "anc-contract":
        m = re.search(r'Period:\s*(\S+)', body)
        if m:
            d = parse_date_text(m.group(1))
            if d:
                return d, "contract_start"

    # Contracts — same pattern
    if entry_type == "contract":
        m = re.search(r'Period:\s*(\S+)', body)
        if m:
            d = parse_date_text(m.group(1))
            if d:
                return d, "contract_start"

    # County fights — extract from vote_tally or outcome
    if entry_type == "county-fight":
        for field in ["vote_tally", "outcome"]:
            val = fm.get(field, "")
            d = parse_date_text(val)
            if d:
                return d, "vote"

    # Facilities — opened field
    if entry_type in ("facility", "igsa"):
        opened = fm.get("opened", "")
        if opened:
            d = parse_date_text(opened)
            if d:
                return d, "opened"

    # Job postings — posting_date
    if entry_type == "job-posting":
        pd = fm.get("posting_date", "")
        if pd:
            d = parse_date_text(pd)
            if d:
                return d, "posted"

    # Try summary for any date
    summary = fm.get("summary", "")
    d = parse_date_text(summary)
    if d:
        return d, "from_summary"

    # Try body for any date (first occurrence)
    d = parse_date_text(body[:500])
    if d:
        return d, "from_body"

    # Fallback: lastmod
    lastmod = fm.get("lastmod", "")
    if lastmod:
        d = parse_date_text(lastmod)
        if d:
            return d, "lastmod"

    return None, None


def main():
    events = []
    no_date_count = 0

    # Process all content directories
    for content_type in ["entry", "fights", "facilities", "players", "organizations"]:
        content_path = CONTENT_DIR / content_type
        if not content_path.exists():
            continue

        for md_file in content_path.rglob("*.md"):
            if md_file.name == "_index.md":
                continue

            content = md_file.read_text(errors="replace")
            fm, body = parse_frontmatter(content)

            entry_type = fm.get("entry_type", "")
            if not entry_type:
                continue

            date, date_source = extract_date_from_entry(fm, body, entry_type)

            if not date:
                no_date_count += 1
                continue

            # Filter out obviously wrong dates
            try:
                dt = datetime.strptime(date, "%Y-%m-%d")
                if dt.year < 2015 or dt.year > 2027:
                    continue
            except ValueError:
                continue

            event = {
                "date": date,
                "title": fm.get("title", md_file.stem),
                "type": entry_type,
                "signal_label": fm.get("signal_label", entry_type),
                "signal_color": fm.get("signal_color", "#666"),
                "state": fm.get("state", ""),
                "county": fm.get("county", ""),
                "fips": fm.get("fips", ""),
                "url": f"/{content_type}/{md_file.stem}/",
                "date_source": date_source,
            }

            # Fix URLs for nested content
            if content_type == "players":
                rel = md_file.relative_to(content_path)
                event["url"] = f"/players/{'/'.join(rel.parts[:-1])}/{md_file.stem}/"
            elif content_type == "facilities":
                event["url"] = f"/facilities/{md_file.stem}/"
            elif content_type == "fights":
                event["url"] = f"/fights/{md_file.stem}/"

            events.append(event)

    # Sort by date
    events.sort(key=lambda e: e["date"])

    # Generate monthly aggregates for the density bars
    monthly = {}
    for e in events:
        month_key = e["date"][:7]  # YYYY-MM
        if month_key not in monthly:
            monthly[month_key] = {"month": month_key, "total": 0, "by_type": {}}
        monthly[month_key]["total"] += 1
        t = e["type"]
        monthly[month_key]["by_type"][t] = monthly[month_key]["by_type"].get(t, 0) + 1

    monthly_list = sorted(monthly.values(), key=lambda m: m["month"])

    # Build output
    output = {
        "metadata": {
            "generated": datetime.now().strftime("%Y-%m-%d"),
            "total_events": len(events),
            "date_range": {
                "start": events[0]["date"] if events else None,
                "end": events[-1]["date"] if events else None,
            },
            "skipped_no_date": no_date_count,
        },
        "events": events,
        "monthly": monthly_list,
    }

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_path = DATA_DIR / "timeline.json"
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Generated {len(events)} timeline events ({no_date_count} entries had no extractable date)")
    print(f"Date range: {output['metadata']['date_range']['start']} to {output['metadata']['date_range']['end']}")
    print(f"Output: {output_path}")

    # Show type breakdown
    type_counts = {}
    for e in events:
        type_counts[e["type"]] = type_counts.get(e["type"], 0) + 1
    print("\nBy type:")
    for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"  {t}: {c}")


if __name__ == "__main__":
    main()
