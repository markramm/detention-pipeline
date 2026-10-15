"""The 287(g) entries carry the date of the data they hold, read from the
source page, never a hardcoded one.

ingest_287g.py labelled every entry "as of Feb 17, 2026". Prison Policy
Initiative replaced its Appendix Table 1 in October 2026 with data "current
as of September 30, 2026", and the Oct 10 ingest wrote the new rows under
the old label. The page carries two "current as of" dates: one for Appendix
Table 1 (287(g) agreements) and one for Appendix Table 2 (jails). The ingest
must take Table 1's, and stop if it cannot find one.
"""

import pytest

import ingest_287g as m

# Trimmed from the live page (2026-10-15): Table 1's date, then Table 1,
# then Table 2's date and Table 2.
SEPT_PAGE = """
<p>To check whether your county has a 287(g) agreement as of September 30,
2026, see Appendix Table 1. To check whether your local jail is used for U.S.
Marshals detention and/or ICE detention as of February 5, 2026, see Appendix
Table 2.</p>
<p>This information is current as of <strong>September&nbsp;30, 2026</strong>.</p>
<table><tr><th>Law Enforcement Agency</th><th>County</th><th>State</th></tr>
<tr><td>Autauga County Sheriff’s Office</td><td>Autauga County</td><td>Ala.</td>
<td>September 22, 2025</td><td>September 22, 2025</td><td></td></tr></table>
<p>This information is current as of February 5, 2026.</p>
<table><tr><td>Bay Minette</td></tr></table>
"""

FEB_PAGE = SEPT_PAGE.replace("September 30,\n2026", "February 17, 2026").replace(
    "September&nbsp;30, 2026", "February 17, 2026"
)


def test_reads_table_1_date_not_table_2():
    assert m.parse_as_of_date(SEPT_PAGE) == "2026-09-30"


def test_reads_the_february_snapshot_date():
    assert m.parse_as_of_date(FEB_PAGE) == "2026-02-17"


def test_no_date_before_table_1_fails_loudly():
    page = SEPT_PAGE.replace("This information is current as of", "This information is from")
    page = page.replace("as of September 30,\n2026, see", "see")
    with pytest.raises(m.AsOfDateNotFound):
        m.parse_as_of_date(page)


def test_disagreeing_dates_fail_loudly():
    page = SEPT_PAGE.replace("as of September 30,\n2026, see", "as of August 1, 2026, see")
    with pytest.raises(m.AsOfDateNotFound):
        m.parse_as_of_date(page)


def test_entry_carries_the_page_date():
    raw = {"agency": "Autauga County Sheriff’s Office", "county": "Autauga County",
           "state": "AL", "model": "TFM", "signed_date": "September 22, 2025"}
    entry = m.create_entry(raw, as_of="2026-09-30")
    assert entry["source"].endswith("as of 2026-09-30")
    assert "as of September 30, 2026." in entry["body"]
    assert "Feb 17" not in entry["body"] and "2026-02-17" not in entry["source"]


def test_entry_requires_a_date():
    raw = {"agency": "X Police Department", "county": "", "state": "AL",
           "model": "TFM", "signed_date": ""}
    with pytest.raises(TypeError):
        m.create_entry(raw)
