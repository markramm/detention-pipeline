#!/usr/bin/env python3
"""
Entity-link resolution for the Hugo site generator.

Scope (Mark, 2026-09-28, ticket
mark-action-emit-the-entity-link-graph-detention-pipeline-generator):
narrow — organizations only. This module resolves two kinds of link on a
facility page:

  1. facility -> its operator/contractor organization page
  2. facility -> its county page

and fixes raw `.md`-relative markdown links left over from KB authoring
so they resolve to the site's canonical URLs instead of 404ing.

Deliberately out of scope: any link to or from a PERSON or official.
Auto-linking people to facilities at scale creates association claims on
a legally sensitive public site, so this module never looks at `type:
person` entries and never emits a link naming one.

The guiding rule throughout: where a fact can't be resolved
unambiguously, emit no link and record why. A wrong association is worse
than a missing one.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

# Entry types this module is allowed to resolve links to. Anything else
# (notably `person`) is never a target of an operator/contractor link.
_ORG_LINK_TYPES = ("organization", "contractor")

# Corporate suffixes / filler stripped before comparing operator strings
# to organization/contractor titles. Order matters only for readability;
# each is applied independently.
_SUFFIX_WORDS = (
    "incorporated", "inc", "corporation", "corp", "company", "co",
    "llc", "l.l.c", "ltd", "limited", "lp", "l.p", "llp", "group",
    "holdings", "holding", "enterprises",
)

_PAREN_RE = re.compile(r"\([^)]*\)")
_TRAILING_ASOF_RE = re.compile(
    r",?\s*as of\s+[a-z0-9 ,./-]+$", re.IGNORECASE
)
_PUNCT_RE = re.compile(r"[^a-z0-9 ]+")
_WS_RE = re.compile(r"\s+")


def normalize_org_name(name: str) -> str:
    """Normalize an organization/operator name for equality comparison.

    Lowercases, drops parenthetical asides, drops a trailing "as of ..."
    clause some operator fields carry, strips punctuation, collapses
    whitespace, and drops common corporate suffix words (Inc, LLC, Group,
    ...). Conservative on purpose: this feeds an unambiguous-match-only
    resolver, so two names that are "close" but not equal after this
    normalization simply don't match — no fuzzy/partial matching.
    """
    if not name:
        return ""
    s = name.lower()
    s = _TRAILING_ASOF_RE.sub("", s)
    s = _PAREN_RE.sub(" ", s)
    s = _PUNCT_RE.sub(" ", s)
    words = [w for w in s.split() if w not in _SUFFIX_WORDS]
    s = " ".join(words)
    s = _WS_RE.sub(" ", s).strip()
    return s


@dataclass
class OrgIndex:
    """normalized name -> ids, and id -> canonical URL / title, for
    organization + contractor entries only."""

    by_name: dict = field(default_factory=dict)
    url_by_id: dict = field(default_factory=dict)
    title_by_id: dict = field(default_factory=dict)

    def lookup(self, raw_name: str):
        """Return (url, entry_id) if `raw_name` matches exactly one
        organization/contractor, else None. Ambiguous or no matches both
        return None — callers that want the reason should inspect
        `self.by_name` themselves."""
        key = normalize_org_name(raw_name)
        if not key:
            return None
        ids = self.by_name.get(key)
        if not ids or len(ids) != 1:
            return None
        entry_id = ids[0]
        url = self.url_by_id.get(entry_id)
        if not url:
            return None
        return url, entry_id


def build_org_index(parsed_entries, wikilink_urls: dict) -> OrgIndex:
    """Build the organization/contractor name index from parsed KB
    entries. `wikilink_urls` is the slug->canonical-URL map generate_content
    already builds (build_wikilink_map); reused here so URL logic lives
    in one place."""
    idx = OrgIndex()
    for parsed in parsed_entries:
        fields = parsed["fields"]
        entry_type = fields.get("type", "")
        if entry_type not in _ORG_LINK_TYPES:
            continue
        entry_id = fields.get("id", parsed["md_file"].stem)
        title = fields.get("title", entry_id)
        url = wikilink_urls.get(entry_id)
        if not url:
            continue
        idx.url_by_id[entry_id] = url
        idx.title_by_id[entry_id] = title
        for name in {title, entry_id}:
            key = normalize_org_name(name)
            if not key:
                continue
            idx.by_name.setdefault(key, [])
            if entry_id not in idx.by_name[key]:
                idx.by_name[key].append(entry_id)
    return idx


def build_facility_operator_hints(parsed_entries, facility_ids: set) -> dict:
    """Reverse index: facility_id -> sorted list of organization/contractor
    ids that list it in their `key_facilities` frontmatter field.

    Only facility ids that actually exist as facility entries are kept —
    a stale slug in a contractor's key_facilities list (data drift) must
    not manufacture a link to nowhere or silently resolve to the wrong
    page."""
    hints: dict = {}
    for parsed in parsed_entries:
        fields = parsed["fields"]
        entry_type = fields.get("type", "")
        if entry_type not in _ORG_LINK_TYPES:
            continue
        entry_id = fields.get("id", parsed["md_file"].stem)
        key_facilities = fields.get("_list_fields", {}).get("key_facilities", [])
        for fac_id in key_facilities:
            fac_id = (fac_id or "").strip()
            if not fac_id or fac_id not in facility_ids:
                continue
            hints.setdefault(fac_id, [])
            if entry_id not in hints[fac_id]:
                hints[fac_id].append(entry_id)
    return hints


@dataclass
class FacilityLinkResult:
    operator_url: str = ""
    operator_id: str = ""
    operator_display: str = ""
    operator_source: str = ""  # "operator_field" | "key_facilities" | ""
    operator_unresolved_reason: str = ""  # set iff no link was emitted

    county_url: str = ""
    county_unresolved_reason: str = ""


def resolve_facility_operator(
    facility_id: str,
    operator_raw: str,
    org_index: OrgIndex,
    operator_hints: dict,
) -> tuple:
    """Resolve a facility's operator/contractor link.

    Returns (url, entry_id, display_title, source, reason). `url` is ""
    when nothing could be resolved unambiguously; `reason` then explains
    why (for the unresolved-facilities report). Never both a url and a
    reason.
    """
    operator_raw = (operator_raw or "").strip()

    if operator_raw:
        key = normalize_org_name(operator_raw)
        candidates = org_index.by_name.get(key, [])
        if len(candidates) == 1:
            entry_id = candidates[0]
            return (
                org_index.url_by_id[entry_id],
                entry_id,
                org_index.title_by_id.get(entry_id, operator_raw),
                "operator_field",
                "",
            )
        if len(candidates) > 1:
            return ("", "", "", "", f"ambiguous operator name '{operator_raw}' matches {len(candidates)} organizations: {', '.join(candidates)}")
        # Non-empty operator text with no organization/contractor entry
        # for it: fall through to the key_facilities hint below rather
        # than failing outright — the reverse mapping is the more
        # authoritative source when it disagrees or fills a gap.

    hint_ids = operator_hints.get(facility_id, [])
    if len(hint_ids) == 1:
        entry_id = hint_ids[0]
        return (
            org_index.url_by_id.get(entry_id, ""),
            entry_id,
            org_index.title_by_id.get(entry_id, entry_id),
            "key_facilities",
            "",
        )
    if len(hint_ids) > 1:
        return ("", "", "", "", f"ambiguous: {len(hint_ids)} organizations claim this facility in key_facilities: {', '.join(hint_ids)}")

    if operator_raw:
        return ("", "", "", "", f"operator text '{operator_raw}' does not match any organization or contractor entry")
    return ("", "", "", "", "no operator data")


def resolve_facility_county(fips: str, county_fips_set: set) -> tuple:
    """Resolve a facility's county link. fips is a direct, already-unique
    identifier (no fuzzy matching needed) — the only failure modes are
    "missing" and "no county page was generated for that fips"."""
    fips = (fips or "").strip()
    if not fips:
        return "", "no fips on facility record"
    if fips not in county_fips_set:
        return "", f"fips {fips} has no generated county page"
    return f"/county/{fips}/", ""


def resolve_facility_links(
    facility_id: str,
    operator_raw: str,
    fips: str,
    org_index: OrgIndex,
    operator_hints: dict,
    county_fips_set: set,
) -> FacilityLinkResult:
    result = FacilityLinkResult()
    url, entry_id, display, source, reason = resolve_facility_operator(
        facility_id, operator_raw, org_index, operator_hints
    )
    if url:
        result.operator_url = url
        result.operator_id = entry_id
        result.operator_display = display
        result.operator_source = source
    else:
        result.operator_unresolved_reason = reason

    county_url, county_reason = resolve_facility_county(fips, county_fips_set)
    if county_url:
        result.county_url = county_url
    else:
        result.county_unresolved_reason = county_reason

    return result


# ---------------------------------------------------------------------------
# Raw `.md`-relative link fix
# ---------------------------------------------------------------------------

# Matches a standard markdown link whose target is a relative path ending
# in `.md`, e.g. [text](../facilities/foo.md) or [text](foo.md). Does not
# touch [[wikilinks]] (handled separately by resolve_wikilinks) or
# absolute http(s) links.
_MD_LINK_RE = re.compile(r"\[([^\]]*)\]\((?!https?://)([^)]*?\.md)\)")


def resolve_relative_md_links(body: str, wikilink_urls: dict) -> tuple:
    """Rewrite `[text](relative/path/slug.md)` links to the slug's
    canonical site URL. Returns (new_body, resolved_count, unresolved)
    where `unresolved` is a list of (link_text, raw_target) pairs for
    targets whose slug isn't a known KB entry — those are left as plain
    text (the link markup is stripped) rather than shipped as a link to
    nowhere.
    """
    unresolved = []
    resolved_count = 0

    def _replace(m):
        nonlocal resolved_count
        text, target = m.group(1), m.group(2)
        slug = target.rsplit("/", 1)[-1]
        if slug.endswith(".md"):
            slug = slug[: -len(".md")]
        url = wikilink_urls.get(slug)
        if url:
            resolved_count += 1
            return f"[{text}]({url})"
        unresolved.append((text, target))
        return text

    new_body = _MD_LINK_RE.sub(_replace, body)
    return new_body, resolved_count, unresolved
