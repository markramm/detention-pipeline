"""
Shared loader for kb/schema.yaml — the single source of truth for
entry-type metadata.

Use from any kb/scripts/ tool:
    from schema import load_schema
    schema = load_schema()
    meta = schema.entry_type("commission-activity")   # -> dict with weight, color, etc.
    for t in schema.signal_types():                   # -> types with a weight > 0
        ...
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

try:
    import yaml
except ImportError as e:
    print(
        "ERROR: PyYAML is required (pip install pyyaml)",
        file=sys.stderr,
    )
    raise

SCHEMA_PATH = Path(__file__).parent.parent / "schema.yaml"


@dataclass(frozen=True)
class Schema:
    entry_types: dict
    coverage: dict
    county_page_signal_order: list

    def entry_type(self, name: str) -> dict:
        """Return metadata for one entry type. Raises if unknown."""
        if name not in self.entry_types:
            raise KeyError(f"unknown entry_type: {name!r}")
        return self.entry_types[name]

    def get(self, name: str, default=None) -> dict:
        """Return metadata, or `default` if unknown."""
        return self.entry_types.get(name, default)

    def signal_types(self) -> dict:
        """Entry types that carry a numeric weight (scored by heat map)."""
        return {k: v for k, v in self.entry_types.items() if v.get("weight")}

    def weights(self) -> dict:
        return {k: v["weight"] for k, v in self.entry_types.items() if v.get("weight")}

    def max_entries(self) -> dict:
        return {k: v["max_entries"] for k, v in self.entry_types.items() if v.get("max_entries")}

    def required_fields(self) -> dict:
        return {k: v.get("required_fields", ["title"]) for k, v in self.entry_types.items()}

    def source_url_required(self) -> set:
        return {k for k, v in self.entry_types.items() if v.get("source_url_required")}

    def source_url_defaults(self) -> dict:
        return {
            k: v["source_url_default"]
            for k, v in self.entry_types.items()
            if v.get("source_url_default")
        }

    def subdirectories(self) -> dict:
        return {
            k: v["subdirectory"]
            for k, v in self.entry_types.items()
            if v.get("subdirectory")
        }


_cached: Schema | None = None


def load_schema(path: Path | str | None = None) -> Schema:
    """Load and cache the schema. Pass a path to override for tests."""
    global _cached
    if _cached is not None and path is None:
        return _cached
    p = Path(path) if path else SCHEMA_PATH
    with open(p) as f:
        data = yaml.safe_load(f)
    schema = Schema(
        entry_types=data.get("entry_types", {}),
        coverage=data.get("coverage", {}),
        county_page_signal_order=data.get("county_page_signal_order", []),
    )
    if path is None:
        _cached = schema
    return schema
