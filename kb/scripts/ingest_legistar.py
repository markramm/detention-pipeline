#!/usr/bin/env python3
"""
Scan Legistar-powered county commission portals for detention-related agenda items.

Legistar (by Granicus) powers meeting management for hundreds of counties.
Their API is public and returns structured agenda data.

Uses async HTTP to scan all counties in parallel (~5-10x faster than sequential).

Usage:
    python ingest_legistar.py                       # scan all configured counties
    python ingest_legistar.py --county baker-fl      # scan one county
    python ingest_legistar.py --state FL             # scan only Florida portals
    python ingest_legistar.py --days 90              # look back 90 days
    python ingest_legistar.py --dry-run              # preview only

The script searches for keywords in agenda item titles and body text:
  ICE, IGSA, detention, intergovernmental service agreement, bed capacity,
  Sabot, federal partnership, revenue opportunity, real estate (closed session)
"""

import argparse
import asyncio
import json
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

try:
    import aiohttp
except ImportError:
    print("aiohttp required: pip install aiohttp", file=sys.stderr)
    sys.exit(1)

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

# Portals to monitor — VALIDATED against Legistar API (2026-04-10)
# Format: (legistar_client_id, entity_name, state, fips, api_mode)
# api_mode: "events" (default) or "matters" (for portals with broken Events endpoint)
# Validation: curl -s -o /dev/null -w "%{http_code}" "https://webapi.legistar.com/v1/{client}/Bodies"
# Note: FIPS is the COUNTY fips even for city-level portals (for heatmap cross-ref)
MONITORED_COUNTIES = [
    # === COUNTY-LEVEL PORTALS (commission activity) ===

    # Florida (11 portals)
    ("broward", "Broward County", "FL", "12011", "events"),
    ("miamidade", "Miami-Dade County", "FL", "12086", "events"),
    ("hillsboroughcounty", "Hillsborough County", "FL", "12057", "events"),
    ("martin", "Martin County", "FL", "12085", "events"),
    ("pinellas", "Pinellas County", "FL", "12103", "events"),
    ("brevardfl", "Brevard County", "FL", "12009", "events"),
    ("alachua", "Alachua County", "FL", "12001", "events"),
    ("hernandocountyfl", "Hernando County", "FL", "12053", "events"),
    ("occompt", "Orange County", "FL", "12095", "events"),
    ("polkcountyfl", "Polk County", "FL", "12105", "events"),
    ("seminolecountyfl", "Seminole County", "FL", "12117", "events"),

    # California (15 portals)
    ("sdcounty", "San Diego County", "CA", "06073", "events"),
    ("lacounty", "Los Angeles County", "CA", "06037", "events"),
    ("sacramento", "Sacramento County", "CA", "06067", "events"),
    ("sanbernardino", "San Bernardino County", "CA", "06071", "events"),
    ("alameda", "Alameda County", "CA", "06001", "events"),
    ("eldorado", "El Dorado County", "CA", "06017", "events"),
    ("fresno", "Fresno County", "CA", "06019", "events"),
    ("humboldt", "Humboldt County", "CA", "06023", "events"),
    ("lassen", "Lassen County", "CA", "06035", "events"),       # Federal prison county
    ("mendocino", "Mendocino County", "CA", "06045", "events"),
    ("monterey", "Monterey County", "CA", "06053", "events"),
    ("napa", "Napa County", "CA", "06055", "events"),
    ("santabarbara", "Santa Barbara County", "CA", "06083", "events"),
    ("santaclara", "Santa Clara County", "CA", "06085", "events"),
    ("solano", "Solano County", "CA", "06095", "events"),

    # Arizona (3 portals)
    ("maricopa", "Maricopa County", "AZ", "04013", "matters"),  # Events endpoint broken
    ("pima", "Pima County", "AZ", "04019", "events"),
    ("mesa", "Mesa", "AZ", "04013", "events"),

    # Georgia (4 portals — no Columbus GA or Salem VA portal exists)
    ("fulton", "Fulton County", "GA", "13121", "events"),
    ("dekalbcountyga", "DeKalb County", "GA", "13089", "events"),
    ("troup", "Troup County", "GA", "13285", "events"),         # Near Stewart Detention
    ("atlantaga", "Atlanta", "GA", "13121", "events"),
    ("marietta", "Marietta", "GA", "13067", "events"),

    # Texas (8 portals)
    ("harriscountytx", "Harris County", "TX", "48201", "events"),  # Houston metro
    ("brazoriacountytx", "Brazoria County", "TX", "48039", "events"),
    ("galvestoncountytx", "Galveston County", "TX", "48167", "events"),
    ("lubbockcounty", "Lubbock County", "TX", "48303", "events"),
    ("elpasotexas", "El Paso", "TX", "48141", "events"),
    ("sanantonio", "San Antonio", "TX", "48029", "events"),
    ("roundrock", "Round Rock", "TX", "48491", "events"),
    ("carson", "Carson County", "TX", "48065", "events"),

    # Virginia (2 portals — "salem" portal is actually Salem OR, not VA)
    ("richmondva", "Richmond", "VA", "51760", "events"),
    ("albemarle", "Albemarle County", "VA", "51003", "events"),

    # Colorado (2 portals)
    ("arapahoe", "Arapahoe County", "CO", "08005", "events"),
    ("douglascounty", "Douglas County", "CO", "08035", "events"),

    # Illinois (2 portals)
    ("cook-county", "Cook County", "IL", "17031", "events"),    # County-level
    ("chicago", "Chicago", "IL", "17031", "events"),

    # North Carolina (2 portals)
    ("guilford", "Guilford County", "NC", "37081", "events"),
    ("cumberlandcounty", "Cumberland County", "NC", "37051", "events"),  # Fort Liberty area

    # Washington — NEW (4 portals)
    ("kingcounty", "King County", "WA", "53033", "events"),     # Seattle metro, ICE interactions
    ("seattle", "Seattle", "WA", "53033", "events"),
    ("snohomish", "Snohomish County", "WA", "53061", "events"), # NW Detention Center area
    ("bellevue", "Bellevue", "WA", "53033", "events"),

    # Wisconsin — NEW (5 portals)
    ("milwaukeecounty", "Milwaukee County", "WI", "55079", "events"),
    ("milwaukee", "Milwaukee", "WI", "55079", "events"),
    ("dane", "Dane County", "WI", "55025", "events"),
    ("waukesha", "Waukesha", "WI", "55133", "events"),
    ("madison", "Madison", "WI", "55025", "events"),

    # Maryland — NEW (1 portal)
    ("baltimore", "Baltimore", "MD", "24510", "events"),

    # Minnesota — NEW (2 portals)
    ("stpaul", "St. Paul", "MN", "27123", "events"),
    ("dakota", "Dakota County", "MN", "27037", "events"),

    # Kentucky — NEW (1 portal)
    ("louisville", "Louisville Metro", "KY", "21111", "events"),

    # Tennessee — NEW (1 portal)
    ("nashville", "Nashville/Davidson County", "TN", "47037", "events"),

    # Ohio — NEW (1 portal — "columbus" client is Columbus OH, not GA)
    ("columbus", "Columbus", "OH", "39049", "events"),

    # New York — NEW (1 portal)
    ("westchestercountyny", "Westchester County", "NY", "36119", "events"),

    # Oklahoma — NEW (1 portal)
    ("oklahomacounty", "Oklahoma County", "OK", "40109", "events"),

    # Pennsylvania — NEW (1 portal)
    ("pittsburgh", "Pittsburgh", "PA", "42003", "events"),

    # Alabama — NEW (1 portal)
    ("huntsvilleal", "Huntsville", "AL", "01089", "events"),    # Near Etowah detention

    # Oregon — NEW (1 portal — "salem" client is Salem OR)
    ("salem", "Salem", "OR", "41047", "events"),

    # New Hampshire — NEW (1 portal)
    ("concordnh", "Concord", "NH", "33013", "events"),

    # Michigan
    ("detroit", "Detroit", "MI", "26163", "events"),

    # Florida — city-level supplements
    ("pompano", "Pompano Beach", "FL", "12011", "events"),
    ("fortlauderdale", "Fort Lauderdale", "FL", "12011", "events"),
    ("delraybeach", "Delray Beach", "FL", "12099", "events"),
    ("miramar", "Miramar", "FL", "12011", "events"),
    ("ircgov", "Indian River County", "FL", "12061", "events"),

    # New Jersey
    ("newark", "Newark", "NJ", "34013", "events"),

    # Missouri
    ("kansascity", "Kansas City", "MO", "29095", "events"),

    # NOT on Legistar at county OR city level (as of 2026-04-14):
    # HIGH PRIORITY: Pinal AZ (76), Webb TX (68), Charlton GA (79),
    #   Bradford FL (54), Palm Beach FL (63, only Delray Beach city)
    # NO LEGISTAR: LA, NM, SC, IN, UT, NE (need alternative scrapers)
]

# Max concurrent requests per host (Legistar rate limiting)
PER_HOST_DELAY = 0.3  # seconds between requests to the same client
MAX_CONCURRENT_COUNTIES = 10  # scan up to 10 counties at once


# Known false positive patterns — exclude these before keyword matching
FALSE_POSITIVES = [
    r"Easy Ice",           # Ice machine company
    r"Manakin.Sabot",      # Place name in Virginia
    r"ice machine",
    r"snow and ice removal",
    r"de-icing",
]


def check_keywords(text):
    """Check text against keyword lists. Returns (signal_strength, matched_keywords)."""
    if not text:
        return None, []

    # Check for known false positives
    for fp in FALSE_POSITIVES:
        if re.search(fp, text, re.IGNORECASE):
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


async def fetch_legistar(session, client, endpoint, params=None):
    """Fetch from Legistar API for a given client."""
    url = f"{LEGISTAR_API}/{client}/{endpoint}"
    if params:
        from urllib.parse import quote
        query = "&".join(f"{k}={quote(str(v))}" for k, v in params.items())
        url += f"?{query}"

    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as resp:
            if resp.status == 404:
                return []
            if resp.status != 200:
                text = await resp.text()
                print(f"  Legistar API error for {client}/{endpoint}: "
                      f"HTTP {resp.status}: {text[:120]}", file=sys.stderr)
                return []
            return await resp.json()
    except asyncio.TimeoutError:
        print(f"  Timeout fetching {client}/{endpoint}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"  Error fetching {client}/{endpoint}: {e}", file=sys.stderr)
        return []


def make_entry(county, state, fips, event_name, event_date, item_title,
               item_text, signal, keywords, meeting_type):
    """Create a commission-activity entry dict."""
    title = f"{county} County {state} — {event_name} {event_date}: {item_title[:80]}"
    return {
        "entry_type": "commission-activity",
        "title": title,
        "body": (
            f"County commission agenda item matching detention pipeline keywords.\n\n"
            f"Meeting: {event_name}\nDate: {event_date}\n"
            f"Agenda Item: {item_title}\n\n"
            f"Matched keywords: {', '.join(keywords)}\n\n"
            f"Full text: {item_text[:500]}"
        ),
        "county": county,
        "state": state,
        "fips": fips,
        "date": event_date,
        "agenda_item": item_title,
        "meeting_type": meeting_type,
        "source": f"Legistar ({county})",
        "signal_strength": signal,
        "notes": f"Matched: {', '.join(keywords)}",
        "tags": ["commission-activity", state.lower(), signal],
    }


async def scan_county_events(session, client, county, state, fips, since_date, dry_run=False):
    """Scan a county's Legistar portal via the Events endpoint."""
    entries = []

    events = await fetch_legistar(session, client, "Events", {
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

        items = await fetch_legistar(session, client, f"Events/{event_id}/EventItems")
        if not items:
            await asyncio.sleep(PER_HOST_DELAY)
            continue

        for item in items:
            item_title = item.get("EventItemTitle", "") or ""
            item_text = item.get("EventItemMatterText", "") or ""
            combined = f"{item_title} {item_text}"

            signal, keywords = check_keywords(combined)
            if not signal:
                continue

            meeting_type = "regular-session"
            if any(kw in combined.upper() for kw in ["CLOSED", "EXECUTIVE SESSION", "EXEMPT"]):
                meeting_type = "closed-session"
            elif "SPECIAL" in event_name.upper():
                meeting_type = "special-session"
            elif "PUBLIC HEARING" in event_name.upper():
                meeting_type = "public-hearing"

            entry = make_entry(county, state, fips, event_name, event_date,
                               item_title, item_text, signal, keywords, meeting_type)

            if dry_run:
                print(f"  [{signal.upper()}] {entry['title']}")
                print(f"    Keywords: {', '.join(keywords)}")
            entries.append(entry)

        await asyncio.sleep(PER_HOST_DELAY)

    return entries


async def scan_county_matters(session, client, county, state, fips, since_date, dry_run=False):
    """Scan a county's Legistar portal via the Matters endpoint (fallback for broken Events)."""
    entries = []

    matters = await fetch_legistar(session, client, "Matters", {
        "$filter": f"MatterAgendaDate ge datetime'{since_date}T00:00:00'",
        "$orderby": "MatterAgendaDate desc",
        "$top": "500",
    })

    if not matters:
        return entries

    for matter in matters:
        agenda_date = (matter.get("MatterAgendaDate") or "")[:10]
        body_name = matter.get("MatterBodyName", "") or ""
        matter_name = matter.get("MatterName", "") or ""
        matter_title = matter.get("MatterTitle", "") or ""
        matter_text = matter.get("MatterText", "") or ""

        combined = f"{matter_name} {matter_title} {matter_text}"

        signal, keywords = check_keywords(combined)
        if not signal:
            continue

        item_title = matter_title or matter_name
        meeting_type = "regular-session"
        status = (matter.get("MatterStatusName") or "").upper()
        if "SPECIAL" in body_name.upper():
            meeting_type = "special-session"
        elif "HEARING" in body_name.upper():
            meeting_type = "public-hearing"

        entry = make_entry(county, state, fips, body_name, agenda_date,
                           item_title, matter_text, signal, keywords, meeting_type)
        entry["source"] = f"Legistar ({client}, Matters)"

        if dry_run:
            print(f"  [{signal.upper()}] {entry['title']}")
            print(f"    Keywords: {', '.join(keywords)}")
        entries.append(entry)

    return entries


async def scan_one_county(semaphore, session, client, county, state, fips, api_mode,
                          since_date, dry_run, index, total):
    """Scan a single county with concurrency limit."""
    async with semaphore:
        log(f"  [{index}/{total}] {county}, {state} ({client})...")
        t0 = time.monotonic()

        if api_mode == "matters":
            entries = await scan_county_matters(
                session, client, county, state, fips, since_date, dry_run)
        else:
            entries = await scan_county_events(
                session, client, county, state, fips, since_date, dry_run)

        elapsed = time.monotonic() - t0
        if entries:
            log(f"    Found {len(entries)} matching items ({elapsed:.1f}s)")
        return client, entries


def log(msg):
    """Print and flush immediately."""
    print(msg, flush=True)


def load_progress(progress_file):
    """Load scan progress state — which counties have been completed."""
    if Path(progress_file).exists():
        with open(progress_file) as f:
            return json.load(f)
    return {"completed": [], "since_date": None, "started": None}


def save_progress(progress_file, progress):
    """Save scan progress state."""
    with open(progress_file, "w") as f:
        json.dump(progress, f, indent=2)


def load_existing_entries(output_file):
    """Load previously saved entries from output file."""
    if Path(output_file).exists():
        try:
            with open(output_file) as f:
                return json.load(f)
        except (json.JSONDecodeError, ValueError):
            pass
    return []


def save_entries(output_file, entries):
    """Write all entries to the output file."""
    with open(output_file, "w") as f:
        json.dump(entries, f, indent=2)


async def run_scan(counties, since_date, dry_run, output_file, resume,
                   max_concurrent=MAX_CONCURRENT_COUNTIES):
    """Run the async scan across all counties."""
    progress_file = output_file + ".progress"
    progress = load_progress(progress_file)

    if resume and progress["completed"]:
        if progress["since_date"]:
            since_date = progress["since_date"]
        log(f"Resuming — {len(progress['completed'])} counties already done")
    else:
        progress = {
            "completed": [],
            "since_date": since_date,
            "started": datetime.now().isoformat(),
        }
        save_progress(progress_file, progress)

    completed_set = set(progress["completed"])
    remaining = [c for c in counties if c[0] not in completed_set]

    log(f"Scanning {len(remaining)} counties since {since_date}"
        f"{f' (skipping {len(completed_set)} done)' if completed_set else ''}"
        f" [async, max {max_concurrent} concurrent]")

    all_entries = load_existing_entries(output_file) if resume else []
    new_count = 0
    t_start = time.monotonic()

    semaphore = asyncio.Semaphore(max_concurrent)

    async with aiohttp.ClientSession(
        headers={"Accept": "application/json"},
    ) as session:
        tasks = []
        for i, (client, county, state, fips, *rest) in enumerate(remaining, 1):
            api_mode = rest[0] if rest else "events"
            tasks.append(
                scan_one_county(semaphore, session, client, county, state, fips,
                                api_mode, since_date, dry_run, i, len(remaining))
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, Exception):
            log(f"  ERROR: {result}")
            continue
        client, entries = result
        if entries:
            new_count += len(entries)
            all_entries.extend(entries)
        progress["completed"].append(client)

    if not dry_run:
        save_entries(output_file, all_entries)
        save_progress(progress_file, progress)

    elapsed = time.monotonic() - t_start
    log(f"\nDone. {new_count} new items from {len(remaining)} counties"
        f" ({len(all_entries)} total in output) in {elapsed:.1f}s")

    if not dry_run and all_entries:
        log(f"Saved to {output_file}")
        log(f"\nTo import: kb import {output_file} -k detention-pipeline-research")

    # Clean up progress file on successful completion
    if not resume or len(progress["completed"]) >= len(counties):
        if Path(progress_file).exists():
            Path(progress_file).unlink()
        log("Scan complete — progress file cleaned up.")

    return all_entries


def main():
    parser = argparse.ArgumentParser(description="Scan Legistar county portals for detention signals")
    parser.add_argument("--county", type=str, help="Scan only this county (e.g. broward or broward-fl)")
    parser.add_argument("--state", type=str, help="Scan only portals in this state (e.g. FL, TX)")
    parser.add_argument("--days", type=int, default=180, help="Look back N days (default: 180)")
    parser.add_argument("--since", type=str, help="Start date (YYYY-MM-DD), overrides --days")
    parser.add_argument("--dry-run", action="store_true", help="Preview without creating entries")
    parser.add_argument("--output", type=str, default="/tmp/commission_items.json", help="Output JSON file")
    parser.add_argument("--resume", action="store_true", help="Resume from last incomplete run")
    parser.add_argument("--reset", action="store_true", help="Clear progress and start fresh")
    parser.add_argument("--max-concurrent", type=int, default=MAX_CONCURRENT_COUNTIES,
                        help=f"Max concurrent county scans (default: {MAX_CONCURRENT_COUNTIES})")
    args = parser.parse_args()

    max_concurrent = args.max_concurrent

    if args.since:
        since_date = args.since
    else:
        since_date = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")

    if args.reset:
        progress_file = args.output + ".progress"
        if Path(progress_file).exists():
            Path(progress_file).unlink()
        if Path(args.output).exists():
            Path(args.output).unlink()
        log("Progress reset.")

    # Filter counties
    counties = MONITORED_COUNTIES
    if args.county:
        target = args.county.lower().replace(" ", "")
        counties = [
            c for c in counties
            if target in c[0].lower() or target in f"{c[1]}-{c[2]}".lower().replace(" ", "")
        ]
        if not counties:
            log(f"No county matching '{args.county}' in monitored list")
            sys.exit(1)

    if args.state:
        target_state = args.state.upper()
        counties = [c for c in counties if c[2] == target_state]
        if not counties:
            log(f"No portals for state '{args.state}' in monitored list")
            sys.exit(1)

    asyncio.run(run_scan(counties, since_date, args.dry_run, args.output, args.resume,
                         max_concurrent))


if __name__ == "__main__":
    main()
