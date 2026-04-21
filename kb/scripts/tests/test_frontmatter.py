"""Tests for the shared frontmatter parser."""

from frontmatter import parse


def test_simple_scalars():
    text = """---
id: foo
title: Foo Bar
type: note
---

body
"""
    r = parse(text)
    assert r is not None
    assert r.fields["id"] == "foo"
    assert r.fields["title"] == "Foo Bar"
    assert r.body == "body\n"


def test_list_is_preserved_as_list():
    """The hand-rolled parsers lost lists. yaml.safe_load preserves them."""
    text = """---
id: foo
tags:
- a
- b
- c
---
body
"""
    r = parse(text)
    assert r.fields["tags"] == ["a", "b", "c"]


def test_no_frontmatter_returns_none():
    assert parse("just body text") is None


def test_unclosed_frontmatter_returns_none():
    assert parse("---\nfoo: bar\nno closing") is None


def test_malformed_yaml_returns_empty_fields():
    """A bad frontmatter block shouldn't crash batch scans of 17k entries."""
    r = parse("---\nbad: [unclosed\n---\nbody")
    assert r is not None
    assert r.fields == {}
    assert r.body == "body"


def test_null_values_become_empty_string():
    """Our callers expect str for scalar fields."""
    r = parse("---\nfoo:\n---\n")
    assert r.fields["foo"] == ""


def test_int_values_coerced_to_string():
    """county_heat_score + validate_entries both assume str fields."""
    r = parse("---\nimportance: 5\n---\n")
    assert r.fields["importance"] == "5"


def test_tuple_unpacking_compat():
    r = parse("---\nfoo: bar\n---\nbody\n")
    fields, body = r.fields, r.body
    assert fields["foo"] == "bar"
    # 3-arg unpack also works via __iter__
    fields, body, fm_raw = r
    assert "foo" in fm_raw
