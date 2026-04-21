"""Shape tests for kb/schema.yaml.

Catches regressions like: someone removes a signal type from schema but
leaves references in templates, or a new entry type lacks required_fields.
"""

import pytest

from schema import load_schema


SIGNAL_TYPES_WITH_WEIGHT = {
    "igsa", "anc-contract", "ice-contract", "287g-agreement",
    "commission-activity", "job-posting", "sheriff-network",
    "comms-discipline", "budget-distress", "real-estate-trace",
    "legislative-trace",
}


@pytest.fixture(scope="module")
def schema():
    return load_schema()


def test_schema_loads(schema):
    assert schema.entry_types, "entry_types is empty"
    assert schema.coverage, "coverage classification is empty"


class TestSignalTypes:
    def test_all_weighted_types_present(self, schema):
        weighted = set(schema.signal_types())
        missing = SIGNAL_TYPES_WITH_WEIGHT - weighted
        assert not missing, f"expected weighted signal types missing: {missing}"

    def test_weights_are_positive(self, schema):
        for name, meta in schema.signal_types().items():
            assert meta["weight"] > 0, f"{name} has weight {meta['weight']}"

    def test_max_entries_bounded(self, schema):
        for name, cap in schema.max_entries().items():
            assert 1 <= cap <= 20, f"{name}.max_entries={cap} out of range"


class TestRequiredFields:
    def test_title_always_required(self, schema):
        required = schema.required_fields()
        for name, fields in required.items():
            assert "title" in fields, f"{name} does not require title"

    def test_no_unknown_keys(self, schema):
        """Required-fields lists should reference only known entry fields."""
        allowed = {
            "title", "county", "state", "fips",
            "contractor", "usaspending_id", "employer",
        }
        required = schema.required_fields()
        for name, fields in required.items():
            unknown = set(fields) - allowed
            # "title" is always allowed; others are common entry keys.
            # A brand-new required field will fail here, forcing the author
            # to decide whether it belongs in the common set.
            assert not unknown, f"{name} requires unknown fields: {unknown}"


class TestEntryTypeMetadata:
    def test_every_type_has_label(self, schema):
        for name, meta in schema.entry_types.items():
            assert meta.get("label"), f"{name} missing label"

    def test_every_type_has_section(self, schema):
        for name, meta in schema.entry_types.items():
            assert meta.get("section"), f"{name} missing section"

    def test_every_signal_has_color_and_anchor(self, schema):
        """Signal types are rendered on county pages and need visual identity."""
        for name, meta in schema.signal_types().items():
            assert meta.get("color"), f"signal {name} missing color"
            assert meta.get("anchor"), f"signal {name} missing anchor"
            assert meta.get("css_var"), f"signal {name} missing css_var"

    def test_subdirectory_matches_json_to_entries(self, schema):
        """json_to_entries.py uses schema.subdirectories() to route files."""
        subdirs = schema.subdirectories()
        # At minimum, the big ingester targets must be routable.
        for sig in ("287g-agreement", "commission-activity", "budget-distress",
                    "anc-contract", "ice-contract", "job-posting"):
            assert sig in subdirs, f"{sig} has no subdirectory routing"


class TestCoverage:
    def test_coverage_references_real_types(self, schema):
        known = set(schema.entry_types)
        for bucket in ("automated", "human"):
            for t in schema.coverage.get(bucket, []):
                assert t in known, f"coverage/{bucket} references unknown type {t}"
