"""The bare ICE keyword must mean the agency, not the frozen-water word.

The 2026-09-30 ingest created ~30 commission entries whose only match was a
case-insensitive \\bICE\\b: "National Ice Cream Day", the Ice Age Trail,
an ice rink contract, "Ice Casino improvements", snow-and-ice maintenance.
They inflated county heat scores (Sacramento, Dane, Westchester).
"""

import pytest

from ingest_legistar import check_keywords


@pytest.mark.parametrize("text", [
    "National Ice Cream Day - Presented by City Clerk",
    "Ice Age Trail Alliance trail crew event map, Indian Lake County Park",
    "Three-year term contract for seasonal ice rink concessions",
    "Bond Act amended RP02A 3248 Ice Casino Improvements II",
    "New dba: Madison's and Good News Cafe & Ice Cream",
])
def test_lowercase_ice_is_not_a_signal(text):
    assert check_keywords(text) == (None, [])


@pytest.mark.parametrize("text,strength", [
    ("Resolution regarding ICE access to county jail", "strong"),
    ("Discussion of ICE presence at courthouse", "moderate"),
    ("Ordinance prohibiting ICE use of city property", "moderate"),
])
def test_agency_ice_still_matches(text, strength):
    assert check_keywords(text)[0] == strength


def test_other_keywords_stay_case_insensitive():
    assert check_keywords("Immigration Detention standards briefing")[0] == "strong"
