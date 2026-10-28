#!/usr/bin/env python3
"""
Ingest 287(g) agreement data.

Two sources, chosen with --source:

  ice (default)  ICE's own live participating-agencies roster. The 287(g)
                 page (https://www.ice.gov/identify-and-arrest/287g) links an
                 .xlsx whose /public/<id> link and file name change several
                 times a day, so the link is discovered from the page on every
                 run. The page states "As of <date> <time>"; each entry carries
                 that stamp. The ingest stops if the page or file cannot be
                 read; it never falls back to remembered data.

  ppi            The frozen Prison Policy Initiative appendix table (agency,
                 county, state, per-model signing dates). PPI replaces the
                 table in place; each entry is labelled with the "current as
                 of" date the page gives for Appendix Table 1 (read on every
                 run; the ingest stops if the page has none).

ICE live is the primary source of current agreements. Entries that came from
PPI are kept as history: an ICE row matches an existing entry by (state,
agency name, model) and updates it in place; an existing entry ICE no longer
lists is flagged `status: not-on-ice-roster-as-of <date>`, never deleted.

287(g) agreements indicate local law enforcement willingness to cooperate
with ICE — a signal that the jurisdiction is receptive to detention expansion.

Usage:
    python ingest_287g.py                          # ICE live roster
    python ingest_287g.py --source ppi             # frozen Prison Policy table
    python ingest_287g.py --dry-run                # preview without saving
    python ingest_287g.py --state FL               # Florida only
    python ingest_287g.py --output /tmp/287g.json  # custom output path
"""

import argparse
import csv
import difflib
import json
import re
import sys
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).parent))
from frontmatter import parse as parse_frontmatter_yaml

KB_ROOT = Path(__file__).parent.parent
ENTRY_DIR = KB_ROOT / "287g"

ICE_PAGE_URL = "https://www.ice.gov/identify-and-arrest/287g"
BROWSER_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

SOURCE_URL = "https://www.prisonpolicy.org/blog/2026/02/23/ice_county_collaboration/"

# FIPS lookup
FIPS_MAP = {}

# fips -> original-cased county name (e.g. "St. Mary's", not "Autauga"
# lowercased then re-titled), for backfilling `county` with correct
# capitalization instead of a naive/broken .title() call.
FIPS_TO_COUNTY_NAME = {}

# state -> list of normalized county names (no "county"/"parish" suffix),
# used for typo-tolerant fuzzy matching against misspelled source data.
STATE_COUNTY_NAMES = {}

# City/place name -> county FIPS, for agencies whose PPI source row has no
# county filled in (mostly municipal police departments). Keyed by (state, place).
PLACE_FIPS_MAP = {}

# Suffixes stripped from an agency name to recover a place name for the
# place-name fallback lookup. Order matters: longest/most-specific first.
AGENCY_PLACE_SUFFIXES = [
    " police services department",
    " department of public safety",
    " public safety",
    " police services",
    " police department",
    " sheriff's office",
    " sheriff’s office",
    " airport police department",
]

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
    "W.Va.": "WV", "W. Va.": "WV", "Wis.": "WI", "Wyo.": "WY", "D.C.": "DC",
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
            STATE_COUNTY_NAMES.setdefault(state, []).append(normalized)
            # Preserve Census's original casing (e.g. "St. Mary's County") for
            # backfilling `county` correctly — normalized/lowercased names
            # would need a naive .title() call that breaks on apostrophes.
            FIPS_TO_COUNTY_NAME[fips] = re.sub(
                r"\s+(County|Parish|Borough|Census Area|Municipality|city|City and Borough)$",
                "", county
            ).strip()


def load_place_fips_map():
    """Load city/place -> county FIPS lookup from the Census place-by-county
    crosswalk (bundled in-repo). Used as a fallback when a 287(g) source row
    has no county filled in but the agency name implies a specific city."""
    global PLACE_FIPS_MAP
    crosswalk_path = Path(__file__).parent.parent / "data" / "census_place_by_county_2020.txt"
    if not crosswalk_path.exists():
        return

    with open(crosswalk_path) as f:
        reader = csv.DictReader(f, delimiter="|")
        for r in reader:
            state = r["STATE"]
            fips = r["STATEFP"] + r["COUNTYFP"]
            place = r["PLACENAME"]
            # PLACENAME looks like "Miami Springs city" / "Davie town" / "Hialeah city"
            normalized = re.sub(
                r"\s+(city|town|village|borough|CDP|municipality)$",
                "", place, flags=re.IGNORECASE
            ).strip().lower()
            # Don't overwrite an existing entry — if a place name repeats across
            # counties in the same state, keep the first (most common) match rather
            # than silently picking whichever happens to sort last in the file.
            PLACE_FIPS_MAP.setdefault((state, normalized), fips)


def place_name_from_agency(agency):
    """Recover a likely city/place name from a police-department-style agency
    name, e.g. 'Miami Springs Police Department' -> 'miami springs'."""
    name = agency.replace("\xa0", " ").strip().lower()
    for suffix in AGENCY_PLACE_SUFFIXES:
        if name.endswith(suffix):
            return name[: -len(suffix)].strip()
    return ""


def fuzzy_match_county(state_abbr, county_clean):
    """Typo-tolerant fallback: match a misspelled county name (e.g. 'Pop
    County', 'Berrian County', 'Alleghany County') against the real county
    names for that state. Conservative on purpose — high similarity cutoff,
    single best match only — so it fixes obvious typos without guessing."""
    candidates = STATE_COUNTY_NAMES.get(state_abbr)
    if not candidates:
        return ""
    matches = difflib.get_close_matches(county_clean, candidates, n=1, cutoff=0.82)
    if matches:
        return FIPS_MAP.get((state_abbr, matches[0]), "")
    return ""


def resolve_fips_from_county(state_abbr, county_name, agency=""):
    """Resolve FIPS from state abbreviation + county name, falling back to a
    city/place-name lookup (derived from the agency name) when the source
    row has no county filled in — common for municipal police departments
    in Prison Policy Initiative's table — and finally a typo-tolerant fuzzy
    match for misspelled county names (also common in that source table)."""
    if not state_abbr:
        return ""

    if county_name:
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

        # Typo-tolerant fuzzy match against this state's real county names
        # (e.g. "Pop County" -> "Pope", "Berrian" -> "Berrien"). Also strips
        # near-miss spellings of "county" itself (e.g. "Conty", "Cnty").
        stripped = re.sub(r"\s+(county|conty|cnty|parish|borough)$", "", county_clean).strip()
        fips = fuzzy_match_county(state_abbr, stripped)
        if fips:
            return fips

        # Louisiana parishes
        if state_abbr == "LA":
            fips = FIPS_MAP.get((state_abbr, county_clean + " parish"))
            if fips:
                return fips

    # Fall back to a city/place-name match derived from the agency name.
    # Only applies to municipal-style agencies (police/public-safety depts) —
    # statewide agencies (DOC, State Police, National Guard, AG's office, etc.)
    # have no matching place name and correctly fall through to unresolved,
    # where they're propagated to the whole state at reduced weight instead
    # of being incorrectly pinned to one county.
    if agency and PLACE_FIPS_MAP:
        place = place_name_from_agency(agency)
        if place:
            fips = PLACE_FIPS_MAP.get((state_abbr, place))
            if fips:
                return fips

    return ""


def county_name_from_fips(fips):
    """Reverse lookup: FIPS -> canonical, correctly-cased county name (e.g.
    "St. Mary's", not the broken "St. Mary'S" a naive .title() call would
    produce), for backfilling the `county` field."""
    return FIPS_TO_COUNTY_NAME.get(fips, "")


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


class AsOfDateNotFound(ValueError):
    """The source page gives no usable "current as of" date for Table 1."""


_DATE = r"([A-Z][a-z]+\.? \d{1,2}, \d{4})"


def _to_iso(text):
    text = text.replace(".", "")
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            pass
    raise AsOfDateNotFound(f"unparseable date {text!r}")


def parse_as_of_date(html):
    """Return the ISO date PPI gives for Appendix Table 1 (287(g) agreements).

    The page states it twice: "This information is current as of <date>."
    just before Table 1, and "...a 287(g) agreement as of <date>, see
    Appendix Table 1." in the introduction. Table 2 (jails) has its own,
    later "current as of" line, so only text before the first <table> is
    read. Raise AsOfDateNotFound if neither is there or they disagree;
    never fall back to a remembered date.
    """
    head = html.split("<table", 1)[0]
    text = re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", head))).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    current = re.findall(r"current as of " + _DATE, text, flags=re.IGNORECASE)
    intro = re.findall(_DATE + r",? see Appendix Table 1", text)
    dates = {_to_iso(d) for d in current[-1:] + intro}
    if not dates:
        raise AsOfDateNotFound(
            "no 'current as of <date>' before Appendix Table 1 on the source page; "
            "the page layout may have changed. Not labelling entries with a guessed date."
        )
    if len(dates) > 1:
        raise AsOfDateNotFound(f"source page gives conflicting dates for Table 1: {sorted(dates)}")
    return dates.pop()


def human_date(iso):
    """'2026-09-30' -> 'September 30, 2026'."""
    d = datetime.strptime(iso, "%Y-%m-%d")
    return f"{d:%B} {d.day}, {d.year}"


def fetch_prison_policy_data():
    """Fetch and parse 287(g) data from Prison Policy Initiative.

    Returns (entries, as_of) where as_of is the ISO date the page gives
    for Appendix Table 1."""
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

    as_of = parse_as_of_date(html)
    print(f"  Source data current as of {as_of}")

    parser = TableExtractor()
    parser.feed(html)

    print(f"  Found {len(parser.tables)} tables")
    for i, (headers, rows) in enumerate(parser.tables):
        print(f"    Table {i+1}: {len(rows)} rows, headers: {headers[:3]}...")

    # Table 1 is the 287(g) agreements table
    # Structure: row[0] has sub-headers (Agency, County, State, TFM date, WSO date, JEM date)
    # The actual headers span two rows in the HTML
    if not parser.tables:
        return [], as_of

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

        # Normalize state: try literal, then space-collapsed, then fall back.
        state = (
            STATE_ABBR_NORMALIZE.get(state_raw)
            or STATE_ABBR_NORMALIZE.get(state_raw.replace(" ", ""))
            or state_raw
        )
        if state == state_raw and len(state) > 2:
            # Last-resort warning: something like "W. Va." that we didn't map.
            print(f"  WARN: unrecognized state '{state_raw}'", file=sys.stderr)

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

    return entries, as_of


# ── ICE live roster ─────────────────────────────────────────────────────────

class IceSourceError(RuntimeError):
    """ICE's page or roster file could not be fetched or understood."""


ICE_STATE_NAMES = {
    "ALABAMA": "AL", "ALASKA": "AK", "AMERICAN SAMOA": "AS", "ARIZONA": "AZ",
    "ARKANSAS": "AR", "CALIFORNIA": "CA", "COLORADO": "CO", "CONNECTICUT": "CT",
    "DELAWARE": "DE", "DISTRICT OF COLUMBIA": "DC", "FLORIDA": "FL",
    "GEORGIA": "GA", "GUAM": "GU", "HAWAII": "HI", "IDAHO": "ID",
    "ILLINOIS": "IL", "INDIANA": "IN", "IOWA": "IA", "KANSAS": "KS",
    "KENTUCKY": "KY", "LOUISIANA": "LA", "MAINE": "ME", "MARYLAND": "MD",
    "MASSACHUSETTS": "MA", "MICHIGAN": "MI", "MINNESOTA": "MN",
    "MISSISSIPPI": "MS", "MISSOURI": "MO", "MONTANA": "MT", "NEBRASKA": "NE",
    "NEVADA": "NV", "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ",
    "NEW MEXICO": "NM", "NEW YORK": "NY", "NORTH CAROLINA": "NC",
    "NORTH DAKOTA": "ND", "NORTHERN MARIANA ISLANDS": "MP", "OHIO": "OH",
    "OKLAHOMA": "OK", "OREGON": "OR", "PENNSYLVANIA": "PA",
    "PUERTO RICO": "PR", "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD", "TENNESSEE": "TN", "TEXAS": "TX",
    "U.S. VIRGIN ISLANDS": "VI", "VIRGIN ISLANDS": "VI", "UTAH": "UT",
    "VERMONT": "VT", "VIRGINIA": "VA", "WASHINGTON": "WA",
    "WEST VIRGINIA": "WV", "WISCONSIN": "WI", "WYOMING": "WY",
}

# ICE's SUPPORT TYPE column -> the model codes this KB has always used.
ICE_MODEL_CODES = {
    "task force model": "TFM",
    "warrant service officer": "WSO",
    "jail enforcement model": "JEM",
}

# ICE's TYPE column has typos ("Muncipality", "Municiaplity").
ICE_AGENCY_TYPES = {
    "county": "County",
    "municipality": "Municipality",
    "muncipality": "Municipality",
    "municiaplity": "Municipality",
    "state agency": "State Agency",
}


def _clean(text):
    """Collapse whitespace (incl. NBSP and trailing spaces ICE leaves on
    cells such as 'Jail Enforcement Model ')."""
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def http_get(url, timeout=90):
    req = Request(url)
    req.add_header("User-Agent", BROWSER_UA)
    req.add_header("Accept", "*/*")
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.read(), resp.headers
    except Exception as e:
        raise IceSourceError(
            f"could not fetch {url}: {e}. ice.gov may be blocking this request; "
            "no data was written."
        ) from e


def parse_ice_page(html, base_url=ICE_PAGE_URL):
    """Return (xlsx_url, as_of_iso, as_of_text, claimed_total) from the 287(g) page.

    The roster link is the 'Participating Agencies' button under the
    '287(g) Participating Agencies' heading; its /public/<id> number and file
    name change several times a day, so nothing is hard-coded. The stamp is the
    page's own "As of October 28, 2026 2:04 pm" sentence. Raises IceSourceError
    if either is missing."""
    start = html.find('id="participating"')
    section = html[start:] if start >= 0 else html
    m = re.search(r'<a\s[^>]*href="([^"]+)"[^>]*>\s*View 287\(g\) Participating Agencies',
                  section, re.IGNORECASE)
    if not m:
        m = re.search(r'<a\s[^>]*href="([^"]*file-download[^"]*)"[^>]*>[^<]*Participating Agencies',
                      section, re.IGNORECASE)
    if not m:
        raise IceSourceError("no 'Participating Agencies' download link on the ICE 287(g) page; "
                             "the page layout may have changed")
    xlsx_url = urljoin(base_url, unescape(m.group(1)))

    text = re.sub(r"<[^>]+>", " ", section[: m.start()])
    text = re.sub(r"\s+", " ", unescape(text).replace("​", "").replace("\xa0", " "))
    sm = re.search(r"As of ([A-Z][a-z]+ \d{1,2}, \d{4})(?: (\d{1,2}:\d{2} ?[ap]m))?", text)
    if not sm:
        raise IceSourceError("no 'As of <date>' stamp before the roster link on the ICE 287(g) page; "
                             "not labelling entries with a guessed date")
    as_of_iso = _to_iso(sm.group(1))
    as_of_text = sm.group(1) + (f" {sm.group(2)}" if sm.group(2) else "")
    tm = re.search(r"signed ([\d,]+) Memorandums", text)
    claimed = int(tm.group(1).replace(",", "")) if tm else None
    return xlsx_url, as_of_iso, as_of_text, claimed


def read_xlsx_rows(data):
    """Read the first worksheet of an .xlsx (bytes) into a list of
    {column letter: text} dicts, using only the standard library."""
    import io
    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    try:
        z = zipfile.ZipFile(io.BytesIO(data))
        shared = []
        if "xl/sharedStrings.xml" in z.namelist():
            for si in ET.fromstring(z.read("xl/sharedStrings.xml")).findall(ns + "si"):
                shared.append("".join(t.text or "" for t in si.iter(ns + "t")))
        sheet = ET.fromstring(z.read("xl/worksheets/sheet1.xml"))
    except (zipfile.BadZipFile, KeyError, ET.ParseError) as e:
        raise IceSourceError(f"ICE roster file is not a readable .xlsx: {e}") from e
    rows = []
    for r in sheet.iter(ns + "row"):
        row = {}
        for c in r.findall(ns + "c"):
            v = c.find(ns + "v")
            if v is None or v.text is None:
                continue
            col = re.match(r"[A-Z]+", c.get("r")).group()
            row[col] = shared[int(v.text)] if c.get("t") == "s" else v.text
        rows.append(row)
    return rows


def excel_serial_to_iso(value):
    """Excel date serial ('46225') -> '2026-09-16'. '' if not a serial."""
    try:
        return (datetime(1899, 12, 30) + timedelta(days=int(float(value)))).date().isoformat()
    except (TypeError, ValueError):
        return ""


def parse_ice_rows(rows):
    """Turn roster rows into raw records shaped like the PPI ones.

    Expected header: STATE, LAW ENFORCEMENT AGENCY, TYPE, COUNTY, SUPPORT TYPE,
    SIGNED, MOA, ADDENDUM. Raises IceSourceError if the header changed.
    Returns (records, problems) where problems lists rows skipped and why."""
    if not rows:
        raise IceSourceError("ICE roster file is empty")
    header = [_clean(rows[0].get(c, "")).upper() for c in "ABCDEF"]
    if header != ["STATE", "LAW ENFORCEMENT AGENCY", "TYPE", "COUNTY", "SUPPORT TYPE", "SIGNED"]:
        raise IceSourceError(f"ICE roster header changed: {header}")
    records, problems = [], []
    for i, row in enumerate(rows[1:], start=2):
        state_raw = _clean(row.get("A")).upper()
        agency = _clean(row.get("B"))
        support = _clean(row.get("E")).lower()
        if not (state_raw or agency or support):
            continue
        state = ICE_STATE_NAMES.get(state_raw)
        model = ICE_MODEL_CODES.get(support)
        if not agency or not state or not model:
            problems.append(f"row {i}: unusable (state={state_raw!r}, agency={agency!r}, "
                            f"support type={support!r})")
            continue
        county = _clean(row.get("D"))
        if county.startswith("#"):  # Excel '#N/A' - ICE gives no county
            county = ""
        signed_iso = excel_serial_to_iso(row.get("F"))
        if not signed_iso:
            problems.append(f"row {i}: {agency} ({state}) {model}: unreadable SIGNED {row.get('F')!r}")
        records.append({
            "agency": agency,
            "county": county,
            "state": state,
            "model": model,
            "signed_date": human_date(signed_iso) if signed_iso else "",
            "agency_type": ICE_AGENCY_TYPES.get(_clean(row.get("C")).lower(), _clean(row.get("C"))),
        })
    return records, problems


def fetch_ice_data(xlsx_path=None, page_path=None):
    """Discover and download ICE's current roster.

    Returns (records, meta) where meta has url, file_name, as_of (ISO),
    as_of_text, claimed_total, problems. xlsx_path/page_path read local files
    instead of the network (offline runs and tests)."""
    if page_path:
        html = Path(page_path).read_text(encoding="utf-8")
    else:
        print(f"Fetching {ICE_PAGE_URL}")
        html = http_get(ICE_PAGE_URL)[0].decode("utf-8", errors="replace")
    url, as_of, as_of_text, claimed = parse_ice_page(html)
    file_name = ""
    if xlsx_path:
        data = Path(xlsx_path).read_bytes()
        file_name = Path(xlsx_path).name
    else:
        print(f"Fetching roster {url}")
        data, headers = http_get(url)
        cd = headers.get("Content-Disposition", "")
        fm = re.search(r'filename="?([^";]+)', cd)
        file_name = fm.group(1) if fm else ""
    records, problems = parse_ice_rows(read_xlsx_rows(data))
    print(f"  ICE roster as of {as_of_text} ({file_name or 'unnamed file'}): {len(records)} rows")
    if claimed and claimed != len(records) + len(problems):
        problems.append(f"page says {claimed} agreements but the file has "
                        f"{len(records) + len(problems)} data rows")
    return records, {"url": url, "file_name": file_name, "as_of": as_of,
                     "as_of_text": as_of_text, "claimed_total": claimed,
                     "problems": problems}


# ── Matching ICE rows to existing entries ───────────────────────────────────

def agency_key(name):
    """Normalise an agency name for matching: case, curly quotes, punctuation
    and spacing differences must not split one agency in two."""
    s = _clean(name)
    if "\u201a\u00c4" in s:  # existing entries: curly apostrophe mangled by a mac-roman decode
        s = s.replace("\u201a\u00c4\u00f4", "'").replace("\u201a\u00c4\u00fa", "'").replace("\u201a\u00c4\u00f9", "'")
    s = s.lower().replace("&", " and ")
    s = re.sub(r"[‘’`']", "", s)          # sheriff's -> sheriffs
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"^(the) ", "", s.strip())
    return s.strip()


def load_existing_entries(entry_dir=None):
    """Read every entry in kb/287g/ -> {id: dict(fields, body, path)}."""
    out = {}
    for f in sorted(Path(entry_dir or ENTRY_DIR).glob("*.md")):
        parsed = parse_frontmatter_yaml(f)
        if not parsed or not parsed.fields.get("agency"):
            continue
        out[f.stem] = {"fields": parsed.fields, "body": parsed.body, "path": f}
    return out


def match_entries(records, existing, fuzzy_cutoff=0.96):
    """Match ICE records to existing entries on (state, agency name, model).

    Pass 1: exact on the normalised key. Pass 2: for leftovers, the single
    best difflib match (>= fuzzy_cutoff) among still-unmatched existing
    entries of the same state and model. Returns (matches, fuzzy, new,
    unmatched_existing): matches maps record index -> entry id; fuzzy lists
    (record, entry id, ratio) for pass-2 matches so a human can eyeball them."""
    by_key = {}
    for eid, e in existing.items():
        f = e["fields"]
        by_key.setdefault((f.get("state", ""), agency_key(f["agency"]), f.get("model", "")), []).append(eid)
    matches, claimed = {}, set()
    # Two passes so that when the roster lists one agency name twice in
    # different counties (e.g. "Ward 5 Marshal's Office" in two Louisiana
    # parishes), each row claims the existing entry for its own county first.
    for same_county_only in (True, False):
        for i, r in enumerate(records):
            if i in matches:
                continue
            k = (r["state"], agency_key(r["agency"]), r["model"])
            for eid in by_key.get(k, []):
                if eid in claimed:
                    continue
                if same_county_only and agency_key(existing[eid]["fields"].get("county", "")) != agency_key(r["county"]):
                    continue
                matches[i] = eid
                claimed.add(eid)
                break
    fuzzy = []
    leftovers = {}
    for eid, e in existing.items():
        if eid not in claimed:
            f = e["fields"]
            leftovers.setdefault((f.get("state", ""), f.get("model", "")), []).append(
                (agency_key(f["agency"]), eid))
    for i, r in enumerate(records):
        if i in matches:
            continue
        cands = [(k, eid) for k, eid in leftovers.get((r["state"], r["model"]), []) if eid not in claimed]
        if not cands:
            continue
        rk = agency_key(r["agency"])
        best = difflib.get_close_matches(rk, [k for k, _ in cands], n=2, cutoff=fuzzy_cutoff)
        if len(best) == 1:  # ambiguous (two near-equal candidates) stays unmatched
            eid = next(e for k, e in cands if k == best[0])
            matches[i] = eid
            claimed.add(eid)
            fuzzy.append((r, eid, difflib.SequenceMatcher(None, rk, best[0]).ratio()))
    new = [i for i in range(len(records)) if i not in matches]
    unmatched_existing = [eid for eid in existing if eid not in claimed]
    return matches, fuzzy, new, unmatched_existing


def flag_entry(eid, existing, as_of):
    """The existing entry unchanged except `status: not-on-ice-roster-as-of
    <date>`. Source, as-of and signing date stay as they were: it is history."""
    e = existing[eid]
    entry = dict(e["fields"])
    entry["id"] = eid
    entry["entry_type"] = entry.get("type", "287g-agreement")
    body = re.sub(r"\n+Roster status: .*\Z", "", e["body"].rstrip(), flags=re.S)
    entry["status"] = f"not-on-ice-roster-as-of {as_of}"
    # Say which source and as-of date this history entry carries.
    if not entry.get("source_kind"):
        entry["source_kind"] = "ppi" if "Prison Policy" in str(entry.get("source", "")) else "ice-live"
    if not entry.get("source_as_of"):
        m = re.search(r"as of (\d{4}-\d{2}-\d{2})", str(entry.get("source", "")))
        if m:
            entry["source_as_of"] = m.group(1)
    entry["body"] = (body + f"\n\nRoster status: not on ICE's participating-agencies roster as of "
                     f"{human_date(as_of)}. Kept as history from its original source "
                     f"({entry.get('source', 'unknown')}); an agency missing from the roster "
                     f"may have ended the agreement or be listed under a different name.")
    return entry


def create_entry(raw, as_of, dry_run=False, source_kind="ppi", as_of_text=None, ppi_as_of=None):
    """Create a detention-pipeline-research entry from a 287(g) record.

    as_of is the ISO date the source page gives for its data; source_kind is
    "ppi" or "ice"; as_of_text is ICE's full stamp ("October 28, 2026 2:04 pm");
    ppi_as_of is the PPI date a matched entry carried before ICE took over."""
    agency = raw["agency"]
    county = raw["county"]
    state = raw["state"]
    model = raw["model"]
    signed_date = raw.get("signed_date", "")

    fips = resolve_fips_from_county(state, county, agency=agency)

    # If the source row had no county (place-name fallback) or a misspelled
    # one (fuzzy-match fallback) but fips resolved anyway, backfill county
    # with the canonical name — don't leave a source typo like "Pop County"
    # or an opaque FIPS-only result sitting in the entry.
    if fips:
        resolved_name = county_name_from_fips(fips)
        if resolved_name and resolved_name.lower() not in county.lower():
            county = resolved_name

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

    if source_kind == "ice":
        stamp = as_of_text or human_date(as_of)
        source = f"ICE 287(g) participating agencies roster (ice.gov/identify-and-arrest/287g), as of {as_of}"
        source_line = f"Source: ICE 287(g) participating agencies roster, as of {stamp}."
        if ppi_as_of:
            source_line += (f" Previously carried from the Prison Policy Initiative appendix table "
                            f"(as of {human_date(ppi_as_of)}).")
    else:
        source = f"Prison Policy Initiative (prisonpolicy.org), compiled from ICE data, as of {as_of}"
        source_line = (f"Source: Prison Policy Initiative appendix table, "
                       f"compiled from ICE data as of {human_date(as_of)}.")

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
            f"{source_line}"
        ),
        "county": county,
        "state": state,
        "fips": fips,
        "agency": agency,
        "model": model,
        "signed_date": signed_date,
        "source": source,
        "source_kind": "ice-live" if source_kind == "ice" else "ppi",
        **({"source_url": ICE_PAGE_URL} if source_kind == "ice" else {}),
        "source_as_of": as_of,
        "signal_strength": signal,
        "notes": f"{model} model agreement signed {signed_date}" if signed_date else f"{model} model agreement",
        "tags": ["287g", model.lower(), state.lower() if state else "unknown"],
    }

    if raw.get("agency_type"):
        entry["agency_type"] = raw["agency_type"]
    if ppi_as_of:
        entry["ppi_as_of"] = ppi_as_of

    if dry_run:
        fips_status = f"FIPS: {fips}" if fips else "FIPS: unresolved"
        print(f"  [DRY RUN] {title} | {county} | {fips_status} | {signal}")

    return entry


def slugify_title(text):
    from json_to_entries import slugify
    return slugify(text)


def summarize(all_entries):
    """Print counts by model, state and FIPS resolution for a list of entries."""
    resolved = sum(1 for e in all_entries if e.get("fips"))
    print(f"  FIPS resolved: {resolved}")
    print(f"  FIPS unresolved: {len(all_entries) - resolved}")
    models, states = {}, {}
    for e in all_entries:
        models[e["model"]] = models.get(e["model"], 0) + 1
        states[e["state"]] = states.get(e["state"], 0) + 1
    print(f"  By model: {models}")
    print(f"  Top states: {sorted(states.items(), key=lambda x: -x[1])[:10]}")


def run_ppi(args):
    load_fips_map()
    load_place_fips_map()

    if args.no_cache:
        cache = Path("/tmp/prisonpolicy_287g.html")
        if cache.exists():
            cache.unlink()

    try:
        raw_entries, as_of = fetch_prison_policy_data()
    except AsOfDateNotFound as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    if not raw_entries:
        print("No entries found. Check the URL or try again.", file=sys.stderr)
        sys.exit(1)

    print(f"\nProcessing {len(raw_entries)} raw entries...")

    all_entries = []
    seen = set()
    for raw in raw_entries:
        if args.state and raw["state"] != args.state.upper():
            continue
        entry = create_entry(raw, as_of, dry_run=args.dry_run)
        dedup_key = (entry["agency"], entry["model"])  # agency+model
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        all_entries.append(entry)
    print(f"\n{len(all_entries)} unique entries")
    summarize(all_entries)
    return all_entries


def run_ice(args):
    """ICE live roster -> entries. Matched rows update the existing entry in
    place (same id), unmatched rows become new entries, and existing entries
    ICE no longer lists are re-emitted flagged, never dropped."""
    load_fips_map()
    load_place_fips_map()

    try:
        records, meta = fetch_ice_data(args.xlsx, args.page_html)
    except IceSourceError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    as_of, as_of_text = meta["as_of"], meta["as_of_text"]
    print(f"  Roster link: {meta['url']}")
    for prob in meta["problems"]:
        print(f"  PROBLEM: {prob}", file=sys.stderr)
    if not records:
        print("ICE roster parsed to zero agreements; refusing to flag every entry.", file=sys.stderr)
        sys.exit(1)

    # The roster can list one agency name twice for the same model in
    # different counties (a different agency that shares a name); both rows
    # are kept. A row repeated in the same county is collapsed.
    seen, deduped, dup_rows = set(), [], []
    for r in records:
        k = (r["state"], agency_key(r["agency"]), r["model"], agency_key(r["county"]))
        if k in seen:
            dup_rows.append(r)
            continue
        seen.add(k)
        deduped.append(r)
    records = deduped
    if dup_rows:
        print(f"  {len(dup_rows)} repeated ICE rows collapsed: "
              + "; ".join(f"{r['agency']} ({r['state']}) {r['model']}" for r in dup_rows[:10]))
    same_name = {}
    for r in records:
        same_name.setdefault((r["state"], agency_key(r["agency"]), r["model"]), []).append(r)
    same_name = {k: v for k, v in same_name.items() if len(v) > 1}
    for k, v in same_name.items():
        print(f"  SAME NAME, DIFFERENT COUNTIES: {v[0]['agency']} ({v[0]['state']}) {v[0]['model']}: "
              + ", ".join(r["county"] or "no county" for r in v))

    existing = load_existing_entries()
    # Match against the whole roster even with --state, so a state-limited run
    # does not flag every other state's entries; flagging is then restricted
    # to the requested state.
    matches, fuzzy, new_idx, unmatched = match_entries(records, existing)
    print(f"\n{len(records)} ICE agreements; {len(existing)} existing entries")
    print(f"  matched exactly: {len(matches) - len(fuzzy)}; matched by fuzzy name: {len(fuzzy)}; "
          f"new: {len(new_idx)}; existing not on roster: {len(unmatched)}")
    for r, eid, ratio in fuzzy:
        print(f"  FUZZY {ratio:.2f}: ICE {r['agency']!r} ({r['state']}) {r['model']} ~ "
              f"existing {existing[eid]['fields']['agency']!r}")

    all_entries, report = [], {"new": [], "updated": [], "flagged": [], "date_changed": []}
    for i, raw in enumerate(records):
        if args.state and raw["state"] != args.state.upper():
            continue
        eid = matches.get(i)
        old = existing[eid]["fields"] if eid else None
        # An entry that carried PPI data keeps its PPI date as history.
        ppi_as_of = None
        if old and "Prison Policy" in str(old.get("source", "")):
            m = re.search(r"as of (\d{4}-\d{2}-\d{2})", str(old.get("source", "")))
            ppi_as_of = m.group(1) if m else old.get("ppi_as_of")
        elif old:
            ppi_as_of = old.get("ppi_as_of")
        if old and "\u201a\u00c4" not in str(old.get("agency", "")):
            # Same agency under the name the KB already uses (PPI's curly
            # apostrophes, say): keep it so titles and ids do not churn.
            # Entries with a mangled name (a mac-roman decode of U+2019) take
            # ICE's clean spelling.
            raw = {**raw, "agency": old["agency"]}
        entry = create_entry(raw, as_of, dry_run=args.dry_run, source_kind="ice",
                             as_of_text=as_of_text, ppi_as_of=ppi_as_of)
        if not eid and (raw["state"], agency_key(raw["agency"]), raw["model"]) in same_name:
            # Same-named agency in another county: give it its own id.
            entry["id"] = f"{slugify_title(entry['title'])}-{slugify_title(raw['county'] or 'unknown-county')}"
        if eid:
            entry["id"] = eid
            report["updated"].append(entry)
            if _clean(str(old.get("signed_date", ""))) != entry["signed_date"]:
                report["date_changed"].append((entry, old.get("signed_date", "")))
        else:
            report["new"].append(entry)
        all_entries.append(entry)

    for eid in unmatched:
        f = existing[eid]["fields"]
        if args.state and f.get("state") != args.state.upper():
            continue
        flagged = flag_entry(eid, existing, as_of)
        report["flagged"].append(flagged)
        all_entries.append(flagged)

    current = [e for e in all_entries if not e.get("status")]
    print(f"\n{len(current)} agreements on the ICE roster "
          f"({len(report['new'])} new, {len(report['updated'])} matched to existing entries); "
          f"{len(report['flagged'])} existing entries flagged")
    summarize(current)
    print(f"  signing date differs from the existing entry on {len(report['date_changed'])} matched entries")
    if report["new"]:
        print("  NEW (first 15): " + "; ".join(f"{e['agency']} ({e['state']}) {e['model']}" for e in report["new"][:15]))
    if report["flagged"]:
        print("  FLAGGED (first 15): " + "; ".join(f"{e['agency']} ({e['state']}) {e['model']}" for e in report["flagged"][:15]))

    args.report = {
        "as_of": as_of, "as_of_text": as_of_text, "url": meta["url"], "file_name": meta["file_name"],
        "new": [(e["id"] if "id" in e else None, e["agency"], e["state"], e["model"]) for e in report["new"]],
        "flagged": [(e["id"], e["agency"], e["state"], e["model"]) for e in report["flagged"]],
        "fuzzy": [(r["agency"], r["state"], r["model"], existing[eid]["fields"]["agency"], round(ratio, 3))
                  for r, eid, ratio in fuzzy],
        "date_changed": [(e["agency"], e["state"], e["model"], old, e["signed_date"])
                         for e, old in report["date_changed"]],
        "problems": meta["problems"],
    }
    return all_entries


def main():
    parser = argparse.ArgumentParser(description="Ingest 287(g) agreements (ICE live roster or Prison Policy Initiative)")
    parser.add_argument("--source", choices=["ice", "ppi"], default="ice",
                        help="ice: ICE's live participating-agencies roster (default); "
                             "ppi: the frozen Prison Policy Initiative table")
    parser.add_argument("--dry-run", action="store_true", help="Preview without creating entries")
    parser.add_argument("--state", type=str, help="Filter to one state (e.g. FL, TX)")
    parser.add_argument("--output", type=str, default="/tmp/287g_agreements.json", help="Output JSON file")
    parser.add_argument("--report", type=str, default=None,
                        help="(ice) also write a JSON report of new / flagged / fuzzy-matched entries")
    parser.add_argument("--no-cache", action="store_true", help="(ppi) force re-fetch, ignoring cached HTML")
    parser.add_argument("--xlsx", type=str, default=None, help="(ice) read the roster from a local .xlsx instead of downloading")
    parser.add_argument("--page-html", type=str, default=None, help="(ice) read the 287(g) page from a local file instead of fetching")
    args = parser.parse_args()
    report_path = args.report
    args.report = None

    all_entries = run_ice(args) if args.source == "ice" else run_ppi(args)

    if report_path and args.report is not None and not args.dry_run:
        with open(report_path, "w") as f:
            json.dump(args.report, f, indent=1)
        print(f"Report saved to {report_path}")

    if not args.dry_run and all_entries:
        with open(args.output, "w") as f:
            json.dump(all_entries, f, indent=2)
        print(f"\nSaved to {args.output}")
        print(f"\nTo import: kb import {args.output} --kb detention-pipeline-research"
              f"  (or: python3 kb/scripts/json_to_entries.py {args.output})")

    return all_entries


if __name__ == "__main__":
    main()
