"""Tests for json_to_entries.render_frontmatter.

The converter is the single choke point that writes every auto-ingested
entry. A regression here quietly corrupts thousands of files.
"""

import json

import pytest

from json_to_entries import render_frontmatter


class TestRequiredShapes:
    def test_287g_produces_expected_id(self):
        entry = {
            "entry_type": "287g-agreement",
            "title": "287(g) JEM: Aransas County Sheriff's Office (TX)",
            "county": "Aransas County",
            "state": "TX",
            "fips": "48007",
            "model": "JEM",
            "tags": ["287g", "jem", "tx"],
        }
        _, entry_id, entry_type = render_frontmatter(entry)
        assert entry_id == "287-g-jem-aransas-county-sheriff-s-office-tx"
        assert entry_type == "287g-agreement"

    def test_anc_contract_uses_stable_slug(self):
        entry = {
            "entry_type": "anc-contract",
            "title": "AKIMA GLOBAL SERVICES, LLC \u2014 70CDCR23FR0000036 (FL) $120,377,303",
            "contractor": "AKIMA GLOBAL SERVICES, LLC",
            "usaspending_id": "70CDCR23FR0000036",
            "county": "MIAMI-DADE",
            "state": "FL",
            "fips": "12086",
            "tags": ["anc-contract", "fl"],
        }
        _, entry_id, _ = render_frontmatter(entry)
        assert entry_id == "akima-global-services-llc-70cdcr23fr0000036"

    def test_budget_uses_stable_slug(self):
        entry = {
            "entry_type": "budget-distress",
            "title": "Elk County, PA \u2014 Budget Distress (score 3/10)",
            "county": "Elk County",
            "state": "PA",
            "fips": "42047",
            "tags": ["budget-distress", "pa"],
        }
        _, entry_id, _ = render_frontmatter(entry)
        assert entry_id == "elk-county-pa-usda-distress"


class TestFrontmatterFields:
    def _render_yaml(self, entry):
        import yaml
        fm_lines, _, _ = render_frontmatter(entry)
        return yaml.safe_load("\n".join(fm_lines))

    def test_emits_required_keys(self):
        """Minimum: id, title, type, tags, importance must always land."""
        entry = {
            "entry_type": "287g-agreement",
            "title": "Test",
            "tags": ["287g"],
        }
        parsed = self._render_yaml(entry)
        for key in ("id", "title", "type", "tags", "importance"):
            assert key in parsed, f"missing {key} in rendered frontmatter"

    def test_preserves_county_state_fips(self):
        entry = {
            "entry_type": "287g-agreement",
            "title": "Test",
            "county": "Cook County",
            "state": "IL",
            "fips": "17031",
        }
        parsed = self._render_yaml(entry)
        assert parsed["county"] == "Cook County"
        assert parsed["state"] == "IL"
        assert parsed["fips"] == "17031"

    def test_skips_empty_fields(self):
        """Empty strings must not land in frontmatter — that's how the
        jobs-seed regression got caught: ingester sent state='', the
        converter correctly skipped, validator correctly reported missing."""
        entry = {
            "entry_type": "287g-agreement",
            "title": "Test",
            "state": "",
            "county": "",
            "fips": "",
        }
        parsed = self._render_yaml(entry)
        assert "state" not in parsed
        assert "county" not in parsed
        assert "fips" not in parsed

    def test_escapes_yaml_special_chars(self):
        """Title with colons, apostrophes must round-trip through YAML."""
        import yaml
        entry = {
            "entry_type": "287g-agreement",
            "title": "287(g) JEM: Sheriff's Office (TX)",
            "tags": ["287g"],
        }
        fm_lines, _, _ = render_frontmatter(entry)
        # Should yaml-parse without error
        parsed = yaml.safe_load("\n".join(fm_lines))
        assert parsed["title"] == "287(g) JEM: Sheriff's Office (TX)"

    def test_national_remote_state_survives(self):
        """US is a valid schema state abbreviation for national-scope entries."""
        entry = {
            "entry_type": "job-posting",
            "title": "Project Manager - Remote",
            "state": "US",
            "tags": ["job-posting"],
        }
        parsed = self._render_yaml(entry)
        assert parsed["state"] == "US"


class TestJobsSeedRegression:
    """Regression test for the specific class of bug atomic-rollback caught
    on the first real CI run: a seed entry with state='' silently produced
    an entry missing the required state field."""

    def test_empty_state_causes_no_state_in_output(self):
        """The converter correctly strips state="".
        This is an assertion of existing behaviour — callers must supply
        a valid state (e.g. 'US') instead of empty string."""
        import yaml
        entry = {
            "entry_type": "job-posting",
            "title": "National Role",
            "state": "",
        }
        fm_lines, _, _ = render_frontmatter(entry)
        parsed = yaml.safe_load("\n".join(fm_lines))
        assert "state" not in parsed

    def test_us_national_marker_survives(self):
        """If the ingester supplies 'US' for a national role, that must
        propagate to frontmatter (the fix we just landed)."""
        import yaml
        entry = {
            "entry_type": "job-posting",
            "title": "National Role",
            "state": "US",
        }
        fm_lines, _, _ = render_frontmatter(entry)
        parsed = yaml.safe_load("\n".join(fm_lines))
        assert parsed["state"] == "US"
