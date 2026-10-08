"""The weekly ingest lands a batch's valid entries and quarantines its
invalid ones; it never discards the whole batch for a few bad entries.

On 2026-10-06 (run 37514175429) the ingest wrote 1,197 new entries, four of
them invalid. run_ingest_ci.sh compared the validation error count before
and after (2 -> 6), reverted all of kb/ to HEAD and exited 1, and the
workflow filed "All ingest sources failed and produced no changes" (#66).
The same shape discarded the runs of Sept 8, 22 and 29 (#56, #57, #60).

These tests drive the real run_ingest_ci.sh in a throwaway git repo that
holds a copy of the converter, the validator and the schema, with a staged
batch in place of the network sources and stubs for the heat/Hugo steps.
"""

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
SCRIPTS = REPO / "kb" / "scripts"


def _entry(county, state, fips, score=4):
    return {
        "entry_type": "budget-distress",
        "title": f"{county}, {state} — USDA distress score {score}",
        "county": county,
        "state": state,
        "fips": fips,
        "source_url": "https://www.ers.usda.gov/data-products/county-typology-codes/",
        "body": "Fixture.",
    }


VALID = [
    _entry("Alpha County", "TX", "48001"),
    _entry("Beta County", "GA", "13001"),
    _entry("Gamma County", "LA", "22001"),
]
# The two shapes of bad record the sources emit: a state the validator does
# not know (Prison Policy's "Northern Mariana Isl."), and a FIPS that lost
# its leading zero.
INVALID = [
    _entry("Saipan", "Northern Mariana Isl.", "69110"),
    _entry("Delta County", "AL", "1001"),
]

BASELINE_ERROR_FILE = "budget/700-rural-counties-funding-lapse.md"


def _git(repo, *args):
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


@pytest.fixture
def fixture_repo(tmp_path):
    repo = tmp_path / "repo"
    (repo / "kb" / "scripts").mkdir(parents=True)
    for py in SCRIPTS.glob("*.py"):
        shutil.copy(py, repo / "kb" / "scripts" / py.name)
    for name in ("schema.yaml", "kb.yaml"):
        shutil.copy(REPO / "kb" / name, repo / "kb" / name)
    shutil.copy(REPO / "run_ingest_ci.sh", repo / "run_ingest_ci.sh")

    # The heat/Hugo steps after the gate are not under test here.
    (repo / "build.sh").write_text(
        "#!/bin/bash\nmkdir -p docs && echo '[]' > docs/heat_data.json\n")
    (repo / "build.sh").chmod(0o755)
    (repo / "kb" / "scripts" / "test_heat_contract.py").write_text("import sys; sys.exit(0)\n")
    (repo / "hugo").mkdir()
    (repo / "hugo" / "generate_content.py").write_text("print('stub')\n")

    budget = repo / "kb" / "budget"
    budget.mkdir()
    # Already on main, already invalid (no county/fips): the baseline.
    (repo / "kb" / BASELINE_ERROR_FILE).write_text(
        "---\nid: 700-rural-counties-funding-lapse\ntitle: '700 rural counties'\n"
        "type: budget-distress\nstate: 'US'\nsource_url: 'https://example.org'\n"
        "tags:\n- budget-distress\nimportance: 5\n---\n\nBody.\n")
    # Already on main, valid; the batch will update it.
    (budget / "epsilon-county-ms-usda-distress.md").write_text(
        "---\nid: epsilon-county-ms-usda-distress\ntitle: 'Epsilon County, MS'\n"
        "type: budget-distress\ncounty: 'Epsilon County'\nstate: 'MS'\nfips: '28001'\n"
        "source_url: 'https://example.org'\ntags:\n- budget-distress\nimportance: 5\n---\n\nOld.\n")

    (repo / ".gitignore").write_text("docs/\n")
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "t@example.org")
    _git(repo, "config", "user.name", "t")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "fixture")
    return repo


def _run(repo, tmp_path, batch, name="budget"):
    staged = tmp_path / "staged"
    staged.mkdir(exist_ok=True)
    (staged / f"{name}.json").write_text(json.dumps(batch))
    env = {
        **os.environ,
        "DP_SKIP_UNIT_TESTS": "1",
        "DP_STAGE_DIR": str(tmp_path / "stage"),
        "DP_REPORT_DIR": str(tmp_path / "report"),
    }
    return subprocess.run(
        ["bash", str(repo / "run_ingest_ci.sh"), "--from-staged", str(staged)],
        cwd=repo, env=env, capture_output=True, text=True, timeout=120,
    )


def _ids(repo):
    return sorted(p.stem for p in (repo / "kb" / "budget").glob("*.md"))


def test_valid_entries_land_when_others_fail_validation(fixture_repo, tmp_path):
    result = _run(fixture_repo, tmp_path, VALID + INVALID)
    out = result.stdout + result.stderr

    landed = _ids(fixture_repo)
    for slug in ("alpha-county-tx-usda-distress", "beta-county-ga-usda-distress",
                 "gamma-county-la-usda-distress"):
        assert slug in landed, f"valid entry {slug} was discarded:\n{out}"
    assert "saipan-northern-mariana-isl-usda-distress" not in landed, out
    assert "delta-county-al-usda-distress" not in landed, out
    assert result.returncode == 0, out


def test_quarantined_entries_are_reported_with_error_and_source(fixture_repo, tmp_path):
    _run(fixture_repo, tmp_path, VALID + INVALID)
    report = (tmp_path / "report" / "report.md").read_text()
    status = json.loads((tmp_path / "report" / "status.json").read_text())

    assert status["quarantined"] == 2
    assert status["all_sources_failed"] is False
    assert "Quarantined entries (2)" in report
    assert "budget/saipan-northern-mariana-isl-usda-distress.md" in report
    assert "invalid state" in report
    assert "budget/delta-county-al-usda-distress.md" in report
    assert "invalid FIPS code" in report
    assert "| budget |" in report
    # The quarantined files exist, outside kb/, for a human to fix.
    qfiles = sorted(p.name for p in (tmp_path / "report" / "quarantine").rglob("*.md"))
    assert qfiles == ["delta-county-al-usda-distress.md", "saipan-northern-mariana-isl-usda-distress.md"]


def test_baseline_errors_neither_block_nor_get_blamed(fixture_repo, tmp_path):
    result = _run(fixture_repo, tmp_path, VALID)
    out = result.stdout + result.stderr
    assert result.returncode == 0, out
    assert "alpha-county-tx-usda-distress" in _ids(fixture_repo), out
    report = (tmp_path / "report" / "report.md").read_text()
    assert "Quarantined" not in report
    assert "700-rural-counties-funding-lapse" not in report
    # The pre-existing invalid entry is still there, untouched.
    assert (fixture_repo / "kb" / BASELINE_ERROR_FILE).exists()


def test_invalid_update_restores_head_version(fixture_repo, tmp_path):
    bad_update = _entry("Epsilon County", "MS", "2801")  # FIPS lost a digit
    result = _run(fixture_repo, tmp_path, VALID + [bad_update])
    out = result.stdout + result.stderr
    text = (fixture_repo / "kb" / "budget" / "epsilon-county-ms-usda-distress.md").read_text()
    assert "fips: '28001'" in text and "Old." in text, out
    assert "alpha-county-tx-usda-distress" in _ids(fixture_repo), out


def test_failed_source_is_named_not_reported_as_all_failed(fixture_repo, tmp_path):
    # A source whose JSON is malformed fails; the other source's data lands.
    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "legistar.json").write_text("{not json")
    result = _run(fixture_repo, tmp_path, VALID)
    out = result.stdout + result.stderr
    status = json.loads((tmp_path / "report" / "status.json").read_text())
    assert status["failed_sources"] == ["legistar"], out
    assert status["all_sources_failed"] is False
    assert "alpha-county-tx-usda-distress" in _ids(fixture_repo), out
    assert "legistar" in (tmp_path / "report" / "report.md").read_text()
    assert result.returncode == 1  # a source failed: the run says so


# ── the gate's attribution and the report's wording, without git ──────


def test_duplicate_id_is_blamed_on_the_batch_file_whichever_is_flagged():
    import ingest_gate as g

    # The validator flags the second file it sees; here that is the old one.
    errors = [["ice-contracts/geo-70cdcr.md", "duplicate ID (also in anc/geo-70cdcr.md)"]]
    touched = {"anc/geo-70cdcr.md": {"path": "anc/geo-70cdcr.md", "source": "usaspending"}}
    culprits, unattributed = g.attribute(errors, touched)
    assert list(culprits) == ["anc/geo-70cdcr.md"]
    assert unattributed == []


def test_new_errors_are_a_multiset_difference():
    import ingest_gate as g

    base = [["a.md", "x"], ["b.md", "y"]]
    post = [["a.md", "x"], ["b.md", "y"], ["c.md", "z"]]
    assert g.new_errors(base, post) == [["c.md", "z"]]


def test_partial_source_errors_are_named(tmp_path):
    import ingest_gate as g

    leg = tmp_path / "legistar.log"
    leg.write_text("  [12/79] Chicago, IL (chicago)...\n"
                   "  Legistar API error for chicago/Events: HTTP 500: {...}\n"
                   "Done. 253 new items\n")
    bud = tmp_path / "budget.log"
    bud.write_text("  Skipped (BLS blocks automated access; manually download ...)\n")
    ok = tmp_path / "jobs.log"
    ok.write_text("Total: 3 entries\n")
    tsv = tmp_path / "sources.tsv"
    tsv.write_text(f"legistar\tok\t253\t{leg}\nbudget\tok\t1086\t{bud}\njobs\tok\t3\t{ok}\n")

    sources = g.read_sources(tsv)
    status = g.summarize(sources, {"quarantined": [], "baseline_errors": 2})
    assert status["partial_sources"] == ["legistar", "budget"]
    assert status["all_sources_failed"] is False
    md = g.render(sources, None, status)
    assert "chicago/Events: HTTP 500" in md
    assert "BLS blocks automated access" in md
    assert "All" not in g.headline(status)


def test_all_failed_is_said_only_when_every_source_failed():
    import ingest_gate as g

    two_failed = [{"name": "a", "status": "failed", "entries": 0, "problems": []},
                  {"name": "b", "status": "failed", "entries": 0, "problems": []}]
    one_failed = two_failed[:1] + [{"name": "b", "status": "ok", "entries": 5, "problems": []}]
    assert g.headline(g.summarize(two_failed, None)).startswith("All 2 ingest sources failed")
    assert g.summarize(one_failed, None)["all_sources_failed"] is False
    assert "All" not in g.issue_title(g.summarize(one_failed, None), "2026-10-06")
