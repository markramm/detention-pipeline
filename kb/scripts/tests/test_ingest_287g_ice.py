"""ICE live-roster path of ingest_287g.py: link discovery, As-of stamp, support
type normalisation, and matching against existing (PPI-derived) entries."""

import pytest

import ingest_287g as m

PAGE = """
<h3 id="participating">287(g) Participating Agencies</h3><p>As of October 28, 2026 2:04 pm ICE has
signed 2,725 Memorandums of Agreement for ​287(g) programs.</p>
<p><a class="usa-button" href="https://www.ice.gov/file-download/download/public/210012"
target="_blank">View 287(g) Participating Agencies</a></p>
<h2>Monthly 287(g) Encounter Reports</h2>
<a href="https://www.ice.gov/file-download/download/public/999">FY 2026</a>
"""

HEADER = {"A": "STATE", "B": "LAW ENFORCEMENT AGENCY", "C": "TYPE", "D": "COUNTY",
          "E": "SUPPORT TYPE", "F": "SIGNED", "G": "MOA"}


def test_link_and_stamp_come_from_the_page():
    url, as_of, text, claimed = m.parse_ice_page(PAGE)
    assert url == "https://www.ice.gov/file-download/download/public/210012"
    assert as_of == "2026-10-28"
    assert text == "October 28, 2026 2:04 pm"
    assert claimed == 2725


def test_changed_link_is_followed_not_hardcoded():
    url, *_ = m.parse_ice_page(PAGE.replace("210012", "210345"))
    assert url.endswith("/public/210345")


def test_page_without_stamp_or_link_fails_loudly():
    with pytest.raises(m.IceSourceError):
        m.parse_ice_page(PAGE.replace("As of October 28, 2026 2:04 pm", "Updated daily"))
    with pytest.raises(m.IceSourceError):
        m.parse_ice_page("<p>nothing here</p>")


def test_rows_normalise_trailing_spaces_state_model_and_date():
    rows = [HEADER,
            {"A": "TEXAS ", "B": "Smith County Sheriff's Office ", "C": "County ", "D": "Smith County ",
             "E": "Jail Enforcement Model ", "F": "46225"},
            {"A": "FLORIDA", "B": "Atlantic Beach Police Department", "C": "Muncipality", "D": "#N/A",
             "E": "Warrant Service Officer ", "F": "45840"},
            {"A": "GUAM", "B": "Guam PD", "C": "State Agency", "D": "Guam", "E": "Task Force Model", "F": "45840"}]
    recs, problems = m.parse_ice_rows(rows)
    assert not problems
    assert (recs[0]["state"], recs[0]["model"], recs[0]["agency"]) == ("TX", "JEM", "Smith County Sheriff's Office")
    assert recs[0]["signed_date"] == "July 22, 2026"
    assert recs[1]["county"] == "" and recs[1]["agency_type"] == "Municipality"
    assert (recs[1]["model"], recs[2]["state"]) == ("WSO", "GU")


def test_changed_header_fails_loudly():
    with pytest.raises(m.IceSourceError):
        m.parse_ice_rows([{**HEADER, "E": "PROGRAM"}])


def test_agency_key_ignores_apostrophe_style_and_mojibake():
    straight = m.agency_key("Smith County Sheriff's Office ")
    assert straight == m.agency_key("Smith County Sheriff’s Office")
    assert straight == m.agency_key("Smith County Sheriff‚Äôs Office")


def _existing(agency, state, model, county=""):
    return {"fields": {"agency": agency, "state": state, "model": model, "county": county}, "body": ""}


def test_match_flag_and_new():
    existing = {"a": _existing("Smith County Sheriff’s Office", "TX", "TFM"),
                "b": _existing("Old PD", "TX", "TFM")}
    recs = [{"agency": "Smith County Sheriff's Office", "state": "TX", "model": "TFM", "county": ""},
            {"agency": "Smith County Sheriff's Office", "state": "TX", "model": "WSO", "county": ""}]
    matches, fuzzy, new, unmatched = m.match_entries(recs, existing)
    assert matches == {0: "a"} and new == [1] and unmatched == ["b"] and not fuzzy


def test_same_name_in_two_counties_each_claims_its_own_entry():
    existing = {"a": _existing("Ward 5 Marshal's Office", "LA", "TFM", "Allen Parish")}
    recs = [{"agency": "Ward 5 Marshal's Office", "state": "LA", "model": "TFM", "county": "Calcasieu Parish"},
            {"agency": "Ward 5 Marshal's Office", "state": "LA", "model": "TFM", "county": "Allen Parish"}]
    matches, *_ = m.match_entries(recs, existing)
    assert matches == {1: "a"}


def test_neighbouring_counties_are_not_fuzzy_matched():
    existing = {"a": _existing("Linn County Sheriff's Office", "MO", "TFM")}
    recs = [{"agency": "Clinton County Sheriff's Office", "state": "MO", "model": "TFM", "county": ""}]
    matches, fuzzy, new, unmatched = m.match_entries(recs, existing)
    assert not matches and new == [0] and unmatched == ["a"]


def test_flag_keeps_history_and_says_which_source():
    ex = {"a": {"fields": {"type": "287g-agreement", "agency": "X PD", "state": "TX", "model": "TFM",
                           "source": "Prison Policy Initiative (prisonpolicy.org), compiled from ICE data, as of 2026-09-30"},
                "body": "Body."}}
    e = m.flag_entry("a", ex, "2026-10-28")
    assert e["status"] == "not-on-ice-roster-as-of 2026-10-28"
    assert (e["source_kind"], e["source_as_of"]) == ("ppi", "2026-09-30")
    assert e["source"].startswith("Prison Policy")
    # flagging twice does not stack the notice
    ex["a"]["body"] = e["body"]
    assert m.flag_entry("a", ex, "2026-10-29")["body"].count("Roster status:") == 1
