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


# --- Weak closed-session/real-estate tier needs real detention context -----
# The 2026-09-30 ingest matched a Newark Municipal Council agenda item
# authorizing a Community Benefits Agreement for a YMCA/health clinic —
# "economic development...federal" fired because the unbounded regex let
# "economic development" (near the top) pair with "federally subsidized
# health care center" (hundreds of characters later), with no ICE or
# detention content anywhere in the item. It pushed Essex County NJ's heat
# score past the 100-point "hot" threshold on a false positive.
NEWARK_COMMUNITY_CENTER_AGENDA_ITEM = (
    "Dept/ Agency: Economic and Housing Development Action: ( ) Ratifying "
    "(X) Authorizing ( ) Amending Type of Service: Execute Community "
    "Benefits Agreement. Purpose: To authorize the execution of a Community "
    "Benefits Agreement between the City of Newark, SWPN 479 Clinton Avenue "
    "LLC, C/O South Ward Alliance, a NJ Nonprofit Corporation, and the New "
    "Jersey Economic Development Authority as a condition of receiving tax "
    "credits incentives under the Aspire Program Act, N.J.S.A. 34:1B-322 et "
    "seq., towards the construction of a 4-story, new construction, "
    "approximately 51,000 sq. ft. building to house community programs "
    "including pre-natal through toddler health services, a full service "
    "federally subsidized health care center, pharmacy, maternity center, "
    "a YMCA wellness center with nutrition counseling, physical therapy and "
    "fitness programs and office space at the properties located at "
    "479-481 Clinton Avenue, Newark, New Jersey."
)


def test_newark_community_benefits_agreement_is_not_a_signal():
    assert check_keywords(NEWARK_COMMUNITY_CENTER_AGENDA_ITEM) == (None, [])


@pytest.mark.parametrize("text", [
    # Context terms below (DHS, removal operations) are deliberately not
    # STRONG/MODERATE/SANCTUARY keywords themselves, so these exercise the
    # CLOSED_SESSION_KEYWORDS tier specifically rather than firing earlier.
    "Closed session to discuss real estate acquisition for a DHS-related facility",
    "Executive session: economic development incentives for a DHS facility",
    "Warehouse conversion lease near a planned removal operations center",
])
def test_closed_session_keywords_still_match_with_detention_context(text):
    assert check_keywords(text)[0] == "weak"


def test_bare_property_acquisition_is_not_a_signal():
    # "property acquisition" alone, the loosest CLOSED_SESSION_KEYWORDS
    # entry, matches almost any routine municipal real-estate item.
    assert check_keywords(
        "Resolution authorizing property acquisition for a new library branch"
    ) == (None, [])


# --- Legistar clerk test/placeholder records are never a signal ------------
# A rolled-back 2026-09-15 ingest run created two Sacramento County entries
# from a clerk's bracketed test item ("[WKJ Test] Ordinance Amending
# Sacramento City Code Title 2 to Establish the City Hall Weekly Morale
# Enhancement and Mandatory Friday Free Ice Cream Initiative..."). It only
# matched because of the lowercase-ICE bug (now fixed), but a test record
# could just as easily contain a real-looking keyword, so it is filtered
# outright regardless of what it says.
@pytest.mark.parametrize("text", [
    "[WKJ Test] Ordinance Amending Sacramento City Code Title 2 to "
    "Establish the City Hall Weekly Morale Enhancement and Mandatory "
    "Friday Free Ice Cream Initiative, Providing Sweet Operational Relief "
    "to Staff and the Public",
    "[TEST] Resolution authorizing an IGSA with ICE for a detention facility",
    "[Agenda Test Entry] Ordinance prohibiting ICE use of city property",
])
def test_bracketed_test_record_is_never_a_signal(text):
    assert check_keywords(text) == (None, [])
