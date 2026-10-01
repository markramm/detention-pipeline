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


# --- Non-ANC contractors resolve toward ice-contracts/, not anc/ -----------
# The April cleanup's convention: a contractor classified 'anc' (an Alaska
# Native Corporation) lives in kb/anc/; every other contractor (GEO Group =
# private-prison, Valor Network = other, ...) lives in kb/ice-contracts/.
# GEO's Montgomery TX award (70CDCR22FR0000045) and Valor's Ocean NJ award
# (70CDCR24FR0000041) were created in anc/ before that convention existed.
# USAspending re-emits both every week; the cross-directory duplicate-skip
# logic above was keeping them stuck in anc/ forever because "existing
# wins." A non-ANC contractor must instead win the migration to ice-contracts/.


def _ice_entry(award="70CDCR22FR0000045", contractor_type="private-prison"):
    return {
        "entry_type": "ice-contract",
        "title": f"THE GEO GROUP, INC. — {award} (TX) $1",
        "state": "TX",
        "contractor": "THE GEO GROUP, INC.",
        "contractor_type": contractor_type,
        "usaspending_id": award,
        "body": "ICE contract award.",
    }


def test_non_anc_contractor_migrates_from_anc_to_ice_contracts(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    entry_id = j.render_frontmatter(_ice_entry())[1]
    (tmp_path / "anc").mkdir()
    old_path = tmp_path / "anc" / f"{entry_id}.md"
    _write(old_path, f"""
id: {entry_id}
title: old
type: anc-contract
state: 'TX'
county: 'MONTGOMERY'
fips: '48339'
tags:
- ice-contract
importance: 7
""")
    stats = _stats()
    result = j.write_entry(_ice_entry(), stats=stats)

    assert not old_path.exists()
    new_path = tmp_path / "ice-contracts" / f"{entry_id}.md"
    assert result == new_path
    text = new_path.read_text()
    assert "type: ice-contract" in text
    assert "fips: '48339'" in text  # merged from the doomed anc/ copy
    assert "county: 'MONTGOMERY'" in text
    assert stats["migrated"] == 1
    assert stats["duplicate"] == 0


def test_anc_classified_contractor_does_not_migrate(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    entry_id = j.render_frontmatter(_ice_entry(award="70CDCR30FR0000099", contractor_type="anc"))[1]
    (tmp_path / "anc").mkdir()
    old_path = tmp_path / "anc" / f"{entry_id}.md"
    _write(old_path, f"id: {entry_id}\ntitle: old\ntype: anc-contract\ntags:\n- ice-contract\nimportance: 7\n")

    stats = _stats()
    j.write_entry(_ice_entry(award="70CDCR30FR0000099", contractor_type="anc"), stats=stats)

    assert old_path.exists()  # a true ANC stays in anc/
    assert not (tmp_path / "ice-contracts" / f"{entry_id}.md").exists()
    assert stats["duplicate"] == 1
    assert stats.get("migrated", 0) == 0


def test_entry_without_contractor_type_keeps_old_duplicate_behavior(tmp_path, monkeypatch):
    # test_skips_id_existing_in_other_dir's _entry() carries no
    # contractor_type — nothing to classify, so it must not start migrating.
    _setup(tmp_path, monkeypatch)
    entry_id = j.render_frontmatter(_entry())[1]
    (tmp_path / "anc").mkdir()
    (tmp_path / "anc" / f"{entry_id}.md").write_text("---\nid: x\n---\n")

    stats = _stats()
    j.write_entry(_entry(), stats=stats)

    assert stats["duplicate"] == 1
    assert stats.get("migrated", 0) == 0
