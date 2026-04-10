#!/usr/bin/env python3
"""
Scan Legistar-powered county commission portals for detention-related agenda items.

Legistar (by Granicus) powers meeting management for hundreds of counties.
Their API is public and returns structured agenda data.

Usage:
    python ingest_legistar.py                       # scan all configured counties
    python ingest_legistar.py --county baker-fl      # scan one county
    python ingest_legistar.py --days 90              # look back 90 days
    python ingest_legistar.py --dry-run              # preview only

The script searches for keywords in agenda item titles and body text:
  ICE, IGSA, detention, intergovernmental service agreement, bed capacity,
  Sabot, federal partnership, revenue opportunity, real estate (closed session)
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen
from urllib.error import HTTPError

# Legistar API base — each client has a subdomain
LEGISTAR_API = "https://webapi.legistar.com/v1"

# Keywords that signal detention pipeline activity
STRONG_KEYWORDS = [
    r"\bIGSA\b", r"intergovernmental service agreement",
    r"\bICE\b detention", r"immigration detention",
    r"bed capacity", r"bed space",
    r"\bSabot\b", r"Sabot Consulting",
    r"detention facility", r"detention center",
    r"NDS 2025", r"national detention standards",
    r"\b287\s*\(?g\)?\b", r"deportation",
    r"GEO Group", r"CoreCivic", r"GardaWorld",
]

MODERATE_KEYWORDS = [
    r"\bICE\b", r"immigration and customs enforcement",
    r"federal partnership", r"federal contract",
    r"revenue opportunity", r"revenue enhancement",
    r"per diem rate", r"per bed",
    r"sheriff.*detention", r"corrections.*revenue",
    r"\bAkima\b", r"Nana Regional",
    r"Immigration Centers of America",
    r"detainee", r"undocumented",
]

# Closed session + real estate = possible early signal
CLOSED_SESSION_KEYWORDS = [
    r"real estate.*(?:acquisition|purchase|federal)",
    r"economic development.*(?:facility|warehouse|federal)",
    r"property acquisition",
    r"closed session.*real estate",
    r"executive session.*economic",
    r"warehouse.*(?:conversion|lease|purchase)",
]

# Counties to monitor — VALIDATED against Legistar API (2026-04-10)
# Format: (legistar_client_id, county_name, state, fips)
# Find client IDs at: https://webapi.legistar.com/Home/Examples
# Validation: curl -s -o /dev/null -w "%{http_code}" "https://webapi.legistar.com/v1/{client}/Bodies"
MONITORED_COUNTIES = [
    # Validated — return 200 from Legistar API
    ("broward", "Broward", "FL", "12011"),           # Heat score 83
    ("miamidade", "Miami-Dade", "FL", "12086"),      # Heat score 65
    ("hillsboroughcounty", "Hillsborough", "FL", "12057"),  # Heat score 43
    ("martin", "Martin", "FL", "12085"),             # Heat score 35
    ("pinellas", "Pinellas", "FL", "12103"),          # Nearby FL county
    ("maricopa", "Maricopa", "AZ", "04013"),         # Heat score 48, Surprise warehouse
    ("fulton", "Fulton", "GA", "13121"),             # Metro Atlanta
    ("lacounty", "Los Angeles", "CA", "06037"),      # Heat score 64
    ("sacramento", "Sacramento", "CA", "06067"),     # Heat score 44

    # NOT on Legistar (as of 2026-04-10) — need alternative monitoring:
    # Palm Beach FL, Pinal AZ, Webb TX, Charlton GA, Bradford FL,
    # Chatham GA, Gwinnett GA, Cobb GA, DeKalb GA, San Diego CA,
    # Orange CA, Kern CA, Essex NJ, Bernalillo NM
]


def fetch_legistar(client, endpoint, params=None):
    """Fetch from Legistar API for a given client."""
    url = f"{LEGISTAR_API}/{client}/{endpoint}"
    if params:
        query = "&".join(f"{k}={quote(str(v))}" for k, v in params.items())
        url += f"?{query}"

    req = Request(url)
    req.add_header("Accept", "application/json")

    try:
        with urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except HTTPError as e:
        if e.code == 404:
            return []  # Client doesn't exist or no data
        print(f"  Legistar API error for {client}: {e}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"  Error fetching {client}/{endpoint}: {e}", file=sys.stderr)
        return []


def check_keywords(text):
    """Check text against keyword lists. Returns (signal_strength, matched_keywords)."""
    if not text:
        return None, []

    matched = []

    for kw in STRONG_KEYWORDS:
        if re.search(kw, text, re.IGNORECASE):
            matched.append(kw)
    if matched:
        return "strong", matched

    for kw in MODERATE_KEYWORDS:
        if re.search(kw, text, re.IGNORECASE):
            matched.append(kw)
    if matched:
        return "moderate", matched

    for kw in CLOSED_SESSION_KEYWORDS:
        if re.search(kw, text, re.IGNORECASE):
            matched.append(kw)
    if matched:
        return "weak", matched

    return None, []


def scan_county(client, county, state, fips, since_date, dry_run=False):
    """Scan a county's Legistar portal for detention-related agenda items."""
    entries = []

    # Fetch recent events (meetings)
    events = fetch_legistar(client, "Events", {
        "$filter": f"EventDate ge datetime'{since_date}T00:00:00'",
        "$orderby": "EventDate desc",
    })

    if not events:
        return entries

    for event in events:
        event_date = event.get("EventDate", "")[:10]
        event_name = event.get("EventBodyName", "")
        event_id = event.get("EventId")

        if not event_id:
            continue

        # Fetch agenda items for this event
        items = fetch_legistar(client, f"Events/{event_id}/EventItems")
        if not items:
            continue

        for item in items:
            item_title = item.get("EventItemTitle", "") or ""
            item_text = item.get("EventItemMatterText", "") or ""
            combined = f"{item_title} {item_text}"

            signal, keywords = check_keywords(combined)
            if not signal:
                continue

            # Determine meeting type
            meeting_type = "regular-session"
            if any(kw in combined.upper() for kw in ["CLOSED", "EXECUTIVE SESSION", "EXEMPT"]):
                meeting_type = "closed-session"
            elif "SPECIAL" in event_name.upper():
                meeting_type = "special-session"
            elif "PUBLIC HEARING" in event_name.upper():
                meeting_type = "public-hearing"

            title = f"{county} County {state} — {event_name} {event_date}: {item_title[:80]}"

            entry = {
                "entry_type": "commission-activity",
                "title": title,
                "body": f"County commission agenda item matching detention pipeline keywords.\n\nMeeting: {event_name}\nDate: {event_date}\nAgenda Item: {item_title}\n\nMatched keywords: {', '.join(keywords)}\n\nFull text: {item_text[:500]}",
                "county": county,
                "state": state,
                "fips": fips,
                "date": event_date,
                "agenda_item": item_title,
                "meeting_type": meeting_type,
                "source": f"Legistar ({client}), Event {event_id}",
                "signal_strength": signal,
                "notes": f"Matched: {', '.join(keywords)}",
                "tags": ["commission-activity", state.lower(), signal],
            }

            if dry_run:
                print(f"  [{signal.upper()}] {title}")
                print(f"    Keywords: {', '.join(keywords)}")
                print(f"    Meeting type: {meeting_type}")
            entries.append(entry)

        time.sleep(0.3)  # Rate limiting between event item fetches

    return entries


def main():
    parser = argparse.ArgumentParser(description="Scan Legistar county portals for detention signals")
    parser.add_argument("--county", type=str, help="Scan only this county (e.g. baker-fl)")
    parser.add_argument("--days", type=int, default=180, help="Look back N days (default: 180)")
    parser.add_argument("--since", type=str, help="Start date (YYYY-MM-DD), overrides --days")
    parser.add_argument("--dry-run", action="store_true", help="Preview without creating entries")
    parser.add_argument("--output", type=str, default="/tmp/commission_items.json", help="Output JSON file")
    args = parser.parse_args()

    if args.since:
        since_date = args.since
    else:
        since_date = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")

    # Filter counties if specified
    counties = MONITORED_COUNTIES
    if args.county:
        target = args.county.lower().replace(" ", "")
        counties = [
            c for c in counties
            if target in c[0].lower() or target in f"{c[1]}-{c[2]}".lower().replace(" ", "")
        ]
        if not counties:
            print(f"No county matching '{args.county}' in monitored list")
            sys.exit(1)

    print(f"Scanning {len(counties)} counties since {since_date}")

    all_entries = []
    for client, county, state, fips in counties:
        print(f"\n  {county}, {state} ({client})...")
        entries = scan_county(client, county, state, fips, since_date, dry_run=args.dry_run)
        all_entries.extend(entries)
        if entries:
            print(f"    Found {len(entries)} matching items")
        time.sleep(0.5)  # Rate limiting between counties

    print(f"\nTotal: {len(all_entries)} matching agenda items across {len(counties)} counties")

    if not args.dry_run and all_entries:
        with open(args.output, "w") as f:
            json.dump(all_entries, f, indent=2)
        print(f"Saved to {args.output}")
        print(f"\nTo import: kb import {args.output} --kb detention-pipeline-research")


if __name__ == "__main__":
    main()
