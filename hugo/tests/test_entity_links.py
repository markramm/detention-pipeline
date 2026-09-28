"""Tests for hugo/entity_links.py — the facility -> operator/contractor
and facility -> county link resolver.

Run from hugo/: python3 -m pytest tests/ -q
(or from repo root: python3 -m pytest hugo/tests/ -q)

Covers the two hard requirements from the ticket
(mark-action-emit-the-entity-link-graph-detention-pipeline-generator,
decided 2026-09-28, "narrow: organizations only"):

  - an ambiguous operator yields no link (never guesses)
  - no person link is ever emitted (person entries never enter the
    organization index or the reverse key_facilities hints)
"""

from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from entity_links import (  # noqa: E402
    build_facility_operator_hints,
    build_org_index,
    normalize_org_name,
    resolve_facility_county,
    resolve_facility_links,
    resolve_facility_operator,
    resolve_relative_md_links,
)


def _entry(entry_id, entry_type, title=None, key_facilities=None):
    """Build a fake parsed-entry dict matching generate_content.py's
    scan_all_entries() shape."""
    fields = {
        "id": entry_id,
        "type": entry_type,
        "title": title or entry_id,
        "_list_fields": {},
    }
    if key_facilities is not None:
        fields["_list_fields"]["key_facilities"] = key_facilities
    return {"fields": fields, "md_file": Path(f"{entry_id}.md")}


WIKILINK_URLS = {
    "geo-group": "/players/contractors/geo-group/",
    "corecivic": "/players/contractors/corecivic/",
    "mtc": "/players/contractors/mtc/",
    "acme-holdings": "/organizations/acme-holdings/",
    "acme-detention-partners": "/organizations/acme-detention-partners/",
    "jane-official": "/players/people/jane-official/",
    "some-note": "/entry/some-note/",
    "other-note": "/entry/other-note/",
}


# ---------------------------------------------------------------------------
# normalize_org_name
# ---------------------------------------------------------------------------

def test_normalize_strips_corporate_suffix():
    assert normalize_org_name("The GEO Group, Inc.") == "the geo"


def test_normalize_strips_parenthetical_and_asof_clause():
    assert normalize_org_name(
        'Texas Department of Criminal Justice (TDCJ), as of Feb 2026'
    ) == "texas department of criminal justice"


def test_normalize_empty():
    assert normalize_org_name("") == ""
    assert normalize_org_name(None) == ""


def test_normalize_is_case_and_whitespace_insensitive():
    assert normalize_org_name("  CoreCivic   Inc  ") == normalize_org_name("corecivic")


# ---------------------------------------------------------------------------
# build_org_index — only organization/contractor types, never person
# ---------------------------------------------------------------------------

def test_org_index_includes_organizations_and_contractors():
    entries = [
        _entry("geo-group", "contractor", title="The GEO Group, Inc."),
        _entry("acme-holdings", "organization", title="Acme Holdings LLC"),
    ]
    idx = build_org_index(entries, WIKILINK_URLS)
    assert idx.url_by_id["geo-group"] == "/players/contractors/geo-group/"
    assert idx.url_by_id["acme-holdings"] == "/organizations/acme-holdings/"


def test_org_index_never_includes_a_person():
    entries = [
        _entry("jane-official", "person", title="Jane Official"),
    ]
    idx = build_org_index(entries, WIKILINK_URLS)
    assert idx.url_by_id == {}
    assert idx.by_name == {}


def test_org_index_skips_entries_with_no_known_url():
    # An org/contractor id not present in wikilink_urls (shouldn't happen
    # in practice — build_wikilink_map covers every entry — but the
    # resolver must not emit a link to nowhere if it ever does).
    entries = [_entry("ghost-corp", "organization", title="Ghost Corp")]
    idx = build_org_index(entries, {})
    assert idx.url_by_id == {}


# ---------------------------------------------------------------------------
# resolve_facility_operator — the core "unambiguous or nothing" contract
# ---------------------------------------------------------------------------

def test_exact_single_match_resolves():
    entries = [_entry("geo-group", "contractor", title="The GEO Group, Inc.")]
    idx = build_org_index(entries, WIKILINK_URLS)
    hints = build_facility_operator_hints(entries, facility_ids=set())
    url, entry_id, display, source, reason = resolve_facility_operator(
        "some-facility", "The GEO Group, Inc.", idx, hints
    )
    assert url == "/players/contractors/geo-group/"
    assert entry_id == "geo-group"
    assert source == "operator_field"
    assert reason == ""


def test_ambiguous_operator_name_yields_no_link():
    # Two organizations that normalize to the same name.
    entries = [
        _entry("acme-holdings", "organization", title="Acme Holdings"),
        _entry("acme-detention-partners", "organization", title="ACME HOLDINGS"),
    ]
    idx = build_org_index(entries, WIKILINK_URLS)
    hints = build_facility_operator_hints(entries, facility_ids=set())
    url, entry_id, display, source, reason = resolve_facility_operator(
        "some-facility", "Acme Holdings", idx, hints
    )
    assert url == ""
    assert entry_id == ""
    assert "ambiguous" in reason


def test_no_matching_organization_yields_no_link():
    entries = [_entry("geo-group", "contractor", title="The GEO Group, Inc.")]
    idx = build_org_index(entries, WIKILINK_URLS)
    hints = build_facility_operator_hints(entries, facility_ids=set())
    url, entry_id, display, source, reason = resolve_facility_operator(
        "some-facility", "Some Unlisted Sheriff's Office", idx, hints
    )
    assert url == ""
    assert "does not match" in reason


def test_empty_operator_text_yields_no_link_with_reason():
    idx = build_org_index([], WIKILINK_URLS)
    hints = {}
    url, entry_id, display, source, reason = resolve_facility_operator(
        "some-facility", "", idx, hints
    )
    assert url == ""
    assert reason == "no operator data"


def test_key_facilities_hint_fills_a_blank_operator_field():
    entries = [
        _entry("mtc", "contractor", title="Management & Training Corporation",
               key_facilities=["some-facility"]),
    ]
    idx = build_org_index(entries, WIKILINK_URLS)
    hints = build_facility_operator_hints(entries, facility_ids={"some-facility"})
    url, entry_id, display, source, reason = resolve_facility_operator(
        "some-facility", "", idx, hints
    )
    assert url == "/players/contractors/mtc/"
    assert source == "key_facilities"


def test_key_facilities_hint_ignored_when_facility_id_does_not_exist():
    # A contractor's key_facilities lists a slug that isn't an actual
    # facility entry (data drift) — must not manufacture a link.
    entries = [
        _entry("mtc", "contractor", title="Management & Training Corporation",
               key_facilities=["nonexistent-facility"]),
    ]
    idx = build_org_index(entries, WIKILINK_URLS)
    hints = build_facility_operator_hints(entries, facility_ids={"some-facility"})
    assert hints == {}


def test_conflicting_key_facilities_hints_yield_no_link():
    entries = [
        _entry("mtc", "contractor", title="MTC", key_facilities=["some-facility"]),
        _entry("geo-group", "contractor", title="GEO Group", key_facilities=["some-facility"]),
    ]
    idx = build_org_index(entries, WIKILINK_URLS)
    hints = build_facility_operator_hints(entries, facility_ids={"some-facility"})
    url, entry_id, display, source, reason = resolve_facility_operator(
        "some-facility", "", idx, hints
    )
    assert url == ""
    assert "ambiguous" in reason


def test_person_entry_is_never_used_as_an_operator_hint():
    # Even if a person entry happens to list a facility in a
    # key_facilities-shaped field, only organization/contractor types are
    # scanned — the reverse index must never route a facility to a person.
    entries = [
        _entry("jane-official", "person", title="Jane Official",
               key_facilities=["some-facility"]),
    ]
    idx = build_org_index(entries, WIKILINK_URLS)
    hints = build_facility_operator_hints(entries, facility_ids={"some-facility"})
    assert hints == {}
    url, entry_id, display, source, reason = resolve_facility_operator(
        "some-facility", "Jane Official", idx, hints
    )
    assert url == ""


def test_person_with_matching_name_text_is_never_linked():
    # An operator string that happens to textually match a person's name
    # must not resolve — the org index simply never contains it.
    entries = [_entry("jane-official", "person", title="Jane Official")]
    idx = build_org_index(entries, WIKILINK_URLS)
    hints = build_facility_operator_hints(entries, facility_ids=set())
    url, entry_id, display, source, reason = resolve_facility_operator(
        "some-facility", "Jane Official", idx, hints
    )
    assert url == ""
    assert entry_id == ""


# ---------------------------------------------------------------------------
# resolve_facility_county
# ---------------------------------------------------------------------------

def test_county_link_resolves_from_fips():
    url, reason = resolve_facility_county("48253", {"48253", "06037"})
    assert url == "/county/48253/"
    assert reason == ""


def test_county_link_missing_fips():
    url, reason = resolve_facility_county("", {"48253"})
    assert url == ""
    assert "no fips" in reason


def test_county_link_fips_with_no_generated_page():
    url, reason = resolve_facility_county("99999", {"48253"})
    assert url == ""
    assert "99999" in reason


# ---------------------------------------------------------------------------
# resolve_facility_links — the combined entry point
# ---------------------------------------------------------------------------

def test_resolve_facility_links_combines_both():
    entries = [_entry("geo-group", "contractor", title="The GEO Group, Inc.")]
    idx = build_org_index(entries, WIKILINK_URLS)
    hints = build_facility_operator_hints(entries, facility_ids={"some-facility"})
    result = resolve_facility_links(
        "some-facility", "The GEO Group, Inc.", "48253", idx, hints, {"48253"}
    )
    assert result.operator_url == "/players/contractors/geo-group/"
    assert result.operator_unresolved_reason == ""
    assert result.county_url == "/county/48253/"
    assert result.county_unresolved_reason == ""


def test_resolve_facility_links_reports_both_unresolved():
    idx = build_org_index([], WIKILINK_URLS)
    result = resolve_facility_links(
        "some-facility", "Unknown Sheriff", "", idx, {}, set()
    )
    assert result.operator_url == ""
    assert result.operator_unresolved_reason
    assert result.county_url == ""
    assert result.county_unresolved_reason


# ---------------------------------------------------------------------------
# resolve_relative_md_links — the ~300 broken .md-path links
# ---------------------------------------------------------------------------

def test_relative_md_link_rewritten_to_canonical_url():
    body = "See also: [some note](../notes/some-note.md) for detail."
    new_body, resolved_count, unresolved = resolve_relative_md_links(body, WIKILINK_URLS)
    assert new_body == "See also: [some note](/entry/some-note/) for detail."
    assert resolved_count == 1
    assert unresolved == []


def test_multiple_relative_md_links_rewritten():
    body = (
        "- See also: [some-note](some-note.md)\n"
        "- See also: [other-note](../../fights/notes/other-note.md)\n"
    )
    new_body, resolved_count, unresolved = resolve_relative_md_links(body, WIKILINK_URLS)
    assert "/entry/some-note/" in new_body
    assert "/entry/other-note/" in new_body
    assert ".md" not in new_body
    assert resolved_count == 2
    assert unresolved == []


def test_unresolvable_md_link_is_stripped_to_plain_text_not_left_broken():
    body = "See [missing entry](../notes/does-not-exist.md) for more."
    new_body, resolved_count, unresolved = resolve_relative_md_links(body, WIKILINK_URLS)
    assert "does-not-exist.md" not in new_body
    assert "missing entry" in new_body
    assert resolved_count == 0
    assert unresolved == [("missing entry", "../notes/does-not-exist.md")]


def test_absolute_links_and_wikilinks_are_untouched():
    body = (
        "External: [site](https://example.com/report.md)\n"
        "Wikilink: [[geo-group|GEO Group]]\n"
    )
    new_body, resolved_count, unresolved = resolve_relative_md_links(body, WIKILINK_URLS)
    assert new_body == body
    assert unresolved == []
