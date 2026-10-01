"""json_to_entries must not create an entry whose ID already lives in another
signal directory.

Since 2026-07-14 every weekly ingest rolled back: USAspending re-emitted two
award IDs (70CDCR22FR0000045, 70CDCR24FR0000041) that already exist under
kb/anc/, json_to_entries created them again under kb/ice-contracts/, the
validator flagged a duplicate ID, and the run discarded ~950 new entries.
The existing record wins; the new one is counted and skipped.
"""

import json_to_entries as j


def _entry(award="70CDCR22FR0000045"):
    return {
        "entry_type": "ice-contract",
        "title": f"THE GEO GROUP, INC. — {award} (TX) $1",
        "state": "TX",
        "contractor": "THE GEO GROUP, INC.",
        "usaspending_id": award,
        "body": "ICE contract award.",
    }


def _setup(tmp_path, monkeypatch):
    monkeypatch.setattr(j, "KB_ROOT", tmp_path)
    monkeypatch.setattr(j, "_ID_DIRS", None)


def test_skips_id_existing_in_other_dir(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    entry_id = j.render_frontmatter(_entry())[1]
    (tmp_path / "anc").mkdir()
    (tmp_path / "anc" / f"{entry_id}.md").write_text("---\nid: x\n---\n")

    stats = {"created": 0, "updated": 0, "unchanged": 0, "unroutable": 0, "duplicate": 0}
    result = j.write_entry(_entry(), stats=stats)

    assert result is not None  # routed, not unroutable
    assert not (tmp_path / "ice-contracts" / f"{entry_id}.md").exists()
    assert stats["duplicate"] == 1
    assert stats["created"] == 0


def test_creates_when_id_is_new(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    stats = {"created": 0, "updated": 0, "unchanged": 0, "unroutable": 0, "duplicate": 0}
    path = j.write_entry(_entry("70CDCR99FR0000001"), stats=stats)
    assert path.exists()
    assert stats["created"] == 1


def test_updates_in_own_dir_are_not_duplicates(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    stats = {"created": 0, "updated": 0, "unchanged": 0, "unroutable": 0, "duplicate": 0}
    j.write_entry(_entry("70CDCR99FR0000002"), stats=stats)
    changed = _entry("70CDCR99FR0000002")
    changed["body"] = "changed"
    j.write_entry(changed, stats=stats)
    assert stats["updated"] == 1
    assert stats["duplicate"] == 0


# --- Re-ingest must not erase what the source doesn't carry -----------------
# USAspending gives ICE contracts a state but no county. 17 ice-contract
# entries had county/fips resolved earlier; the 2026-09-30 re-ingest wrote
# them back without either, dropping them from their counties' heat scores.
# And a hand-verified entry (Rivers Correctional, Hertford NC: parent IDV,
# ceiling, competition, sourcing notes) was overwritten wholesale, taking
# Hertford's score from 16 to 1.


def _write(path, fm, body="ICE contract award."):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("---\n" + fm.strip() + "\n---\n\n" + body + "\n")


def _stats():
    return {"created": 0, "updated": 0, "unchanged": 0, "unroutable": 0,
            "duplicate": 0, "curated": 0}


def test_update_keeps_county_and_fips_the_source_lacks(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    entry = _entry("70CMSD22C00000001")
    entry_id = j.render_frontmatter(entry)[1]
    path = tmp_path / "ice-contracts" / f"{entry_id}.md"
    _write(path, f"""
id: {entry_id}
title: old
type: ice-contract
state: 'TX'
county: 'LANCASTER'
fips: '31109'
tags:
- ice-contract
importance: 5
""")
    j.write_entry(entry, stats=_stats())
    text = path.read_text()
    assert "fips: '31109'" in text
    assert "county: 'LANCASTER'" in text


def test_hand_curated_entry_is_not_overwritten(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    entry = _entry("70CDCR26FR0000108")
    entry_id = j.render_frontmatter(entry)[1]
    path = tmp_path / "ice-contracts" / f"{entry_id}.md"
    _write(path, f"""
id: {entry_id}
title: curated
type: ice-contract
state: 'NC'
county: 'Hertford'
fips: '37091'
parent_idv: '70CDCR26D00000001'
tags:
- ice-contract
importance: 6
""", body="Hand-verified against USAspending.")
    before = path.read_text()
    stats = _stats()
    j.write_entry(entry, stats=stats)
    assert path.read_text() == before
    assert stats["curated"] == 1
    assert stats["updated"] == 0


def test_preserved_fips_keeps_leading_zero(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    entry = _entry("70CMSD22C00000009")
    entry_id = j.render_frontmatter(entry)[1]
    path = tmp_path / "ice-contracts" / f"{entry_id}.md"
    _write(path, f"""
id: {entry_id}
title: old
type: ice-contract
county: Pulaski
fips: 05119
tags:
- ice-contract
importance: 5
""")
    j.write_entry(entry, stats=_stats())
    assert "fips: '05119'" in path.read_text()
