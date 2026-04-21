"""Tests for the stable-slug logic in json_to_entries.py.

These are the slugs that form the site's URLs. If stable_slug regresses,
URLs churn on re-ingest. If it collides, entries stomp each other.
"""

import pytest

from json_to_entries import stable_slug, slugify


class TestBudgetDistress:
    def test_stable_from_county_state(self):
        entry = {
            "entry_type": "budget-distress",
            "county": "Elk County",
            "state": "PA",
            "fips": "42047",
        }
        assert stable_slug(entry) == "elk-county-pa-usda-distress"

    def test_idempotent_when_score_changes(self):
        """If the ingester re-emits with a different score, slug shouldn't change."""
        base = {"entry_type": "budget-distress", "county": "Maricopa", "state": "AZ"}
        s1 = stable_slug({**base, "title": "Maricopa, AZ — Budget Distress (score 3/10)"})
        s2 = stable_slug({**base, "title": "Maricopa, AZ — Budget Distress (score 8/10)"})
        assert s1 == s2 == "maricopa-az-usda-distress"

    def test_returns_none_when_county_missing(self):
        assert stable_slug({"entry_type": "budget-distress", "state": "AZ"}) is None
        assert stable_slug({"entry_type": "budget-distress", "county": "X"}) is None

    def test_independent_cities(self):
        # Virginia independent cities use 'city' suffix, not 'county'.
        entry = {"entry_type": "budget-distress", "county": "Radford city", "state": "VA"}
        assert stable_slug(entry) == "radford-city-va-usda-distress"


class TestContracts:
    def test_anc_uses_award_id(self):
        entry = {
            "entry_type": "anc-contract",
            "contractor": "AKIMA GLOBAL SERVICES, LLC",
            "usaspending_id": "70CDCR23FR0000036",
        }
        assert stable_slug(entry) == "akima-global-services-llc-70cdcr23fr0000036"

    def test_ice_uses_award_id(self):
        entry = {
            "entry_type": "ice-contract",
            "contractor": "CORECIVIC, INC.",
            "usaspending_id": "70CDCR25FR0000011",
        }
        assert stable_slug(entry) == "corecivic-inc-70cdcr25fr0000011"

    def test_idempotent_when_amount_changes(self):
        """Contract amount changes when USAspending posts a modification.
        The slug must stay put so the URL doesn't break."""
        base = {
            "entry_type": "ice-contract",
            "contractor": "GEO GROUP",
            "usaspending_id": "70CDCR25FR0000100",
        }
        assert stable_slug({**base, "contract_value": "$1,000,000"}) == \
               stable_slug({**base, "contract_value": "$2,500,000"})

    def test_returns_none_without_award_id(self):
        assert stable_slug({
            "entry_type": "ice-contract",
            "contractor": "X",
        }) is None

    def test_returns_none_without_contractor(self):
        assert stable_slug({
            "entry_type": "ice-contract",
            "usaspending_id": "70CDCR25FR0000011",
        }) is None


class TestNonStableTypes:
    """Entry types without a stable-slug rule should return None so the
    caller falls back to title-based slugging."""

    @pytest.mark.parametrize("etype", [
        "287g-agreement",
        "commission-activity",
        "job-posting",
        "sheriff-network",
        "comms-discipline",
        "real-estate-trace",
        "legislative-trace",
        "igsa",
        "facility",
        "contractor",
        "person",
        "organization",
        "note",
    ])
    def test_returns_none(self, etype):
        entry = {
            "entry_type": etype,
            "title": "Whatever",
            "county": "X", "state": "Y", "fips": "12345",
            "contractor": "Z", "usaspending_id": "A",
        }
        assert stable_slug(entry) is None


class TestSlugify:
    def test_strips_currency(self):
        assert slugify("$704,997") == "704-997"

    def test_em_dash_to_hyphen(self):
        assert slugify("foo — bar") == "foo-bar"

    def test_curly_apostrophe_dropped_entirely(self):
        """Curly apostrophe is stripped (not replaced) -> the letters close up.
        Straight apostrophe becomes a hyphen. The existing on-disk
        'sheriff-s-office' slug came from Pyrite with straight quotes;
        new ingests with curly quotes yield 'sheriffs-office'. Documenting
        the current behavior so future drift is noticed."""
        assert slugify("Sheriff\u2019s Office") == "sheriffs-office"
        assert slugify("Sheriff's Office") == "sheriff-s-office"

    def test_collapses_separators(self):
        assert slugify("foo___bar---baz") == "foo-bar-baz"

    def test_matches_pyrite_287g_form(self):
        """Must exactly reproduce Pyrite's old slug so CI-created entries
        don't collide with existing entries on disk."""
        title = "287(g) JEM: Aransas County Sheriff's Office (TX)"
        assert slugify(title) == "287-g-jem-aransas-county-sheriff-s-office-tx"

    def test_matches_pyrite_contract_form(self):
        title = "10GFEDSUPPLY, LLC \u2014 70CMSW25FR0000069 (NC) $704,997"
        assert slugify(title) == "10gfedsupply-llc-70cmsw25fr0000069-nc-704-997"
