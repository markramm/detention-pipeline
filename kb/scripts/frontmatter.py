"""
Shared YAML frontmatter parser for the detention-pipeline repo.

Every script in this repo used to hand-roll a frontmatter parser by
splitting on ':'. That worked for simple scalar fields but silently
mishandled lists, quoted strings with colons in them, booleans, etc.

This module parses the frontmatter block with yaml.safe_load and exposes
one API covering every caller's needs:

    from frontmatter import parse, ParsedEntry

    result = parse(text)          # or parse(Path)
    result.fields                 # dict of frontmatter fields (list-aware)
    result.body                   # body string below the closing ---
    result.fm_raw                 # original frontmatter text (for edits)
    result.fm_end                 # byte offset of the closing ---

Callers that used to assign tuples still work — ParsedEntry supports
iteration for (fields, body, fm_raw) and (fields, body) unpacking.

If the file has no frontmatter, parse() returns None.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

try:
    import yaml
except ImportError as e:
    raise ImportError("PyYAML is required (pip install pyyaml)") from e


@dataclass(frozen=True)
class ParsedEntry:
    fields: dict
    body: str
    fm_raw: str
    fm_end: int  # index in original text where the closing '---' starts

    def __iter__(self) -> Iterator:
        # Support `fields, body = parse(...)` and `fields, body, _ = parse(...)`.
        yield self.fields
        yield self.body
        yield self.fm_raw


def parse(source: str | Path) -> ParsedEntry | None:
    """Parse a markdown file with YAML frontmatter.

    Returns None if the file does not start with `---` or has no closing
    delimiter. Callers should treat None as "not a frontmatter file."
    """
    if isinstance(source, Path):
        text = source.read_text(encoding="utf-8")
    else:
        text = source

    if not text.startswith("---"):
        return None
    try:
        end = text.index("\n---", 3)
    except ValueError:
        return None

    fm_raw = text[4:end]
    body = text[end + 4:].lstrip("\n")

    try:
        parsed = yaml.safe_load(fm_raw) or {}
    except yaml.YAMLError:
        # Malformed frontmatter — return empty fields so the caller can
        # decide how to handle it rather than crash the whole batch.
        parsed = {}

    if not isinstance(parsed, dict):
        parsed = {}

    # Normalize: coerce all values to strings for the scalar fields that
    # callers expect to be strings (fips, state, etc.). Leave lists alone.
    fields: dict = {}
    for k, v in parsed.items():
        if v is None:
            fields[k] = ""
        elif isinstance(v, (list, dict)):
            fields[k] = v
        else:
            fields[k] = str(v)

    return ParsedEntry(fields=fields, body=body, fm_raw=fm_raw, fm_end=end)
