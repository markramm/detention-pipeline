#!/bin/bash
# CI ingestion runner — no Pyrite, no local paths.
#
# Gate model: an invalid entry costs only itself. After conversion,
# kb/scripts/ingest_gate.py compares validation errors with the baseline
# taken on HEAD before the batch, pins each new error to the file this
# batch wrote, and quarantines that file: its content goes to
# $REPORT_DIR/quarantine/ (outside kb/, so the site never builds it) and
# kb/ gets back what HEAD had at that path. The rest of the batch lands.
# Errors already on HEAD neither block the batch nor are blamed on it.
#
# Git is still the snapshot for the cases the gate cannot resolve: a new
# error it cannot pin to a file this batch wrote, or a broken heat-score
# contract, reverts the KB data dirs to HEAD (unless --no-rollback).
#
# Every exit writes $REPORT_DIR/report.md (the PR or issue body: per-source
# results, source errors, quarantined entries) and $REPORT_DIR/status.json.
# Exit status: 0 when every source ran and nothing aborted, even if entries
# were quarantined (the PR says so); 1 when a source failed or the run
# stopped early; 2 on bad usage or a dirty tree.
#
# Skip-known: json_to_entries.py writes byte-for-byte identical content
# only when content changes. Re-running on the same source data produces
# zero file writes — the weekly PR diff reflects genuine deltas only.
#
# Usage:
#   ./run_ingest_ci.sh              # run everything
#   ./run_ingest_ci.sh --only legistar
#   ./run_ingest_ci.sh --days 30
#   ./run_ingest_ci.sh --no-rollback
#   ./run_ingest_ci.sh --from-staged DIR   # convert DIR/*.json; fetch nothing
#
# Environment: DP_STAGE_DIR (default /tmp/dp_ingest), DP_REPORT_DIR
# (default /tmp/dp_ingest_report), RUN_URL (linked from the report),
# DP_SKIP_UNIT_TESTS=1 skips the pre-flight pytest run (tests use it).

set -u
cd "$(dirname "$0")"

ONLY=""
DAYS=180
ROLLBACK=true
FROM_STAGED=""
FAIL_COUNT=0

while [[ $# -gt 0 ]]; do
  case $1 in
    --only) ONLY="$2"; shift 2 ;;
    --days) DAYS="$2"; shift 2 ;;
    --no-rollback) ROLLBACK=false; shift ;;
    --from-staged) FROM_STAGED="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 2 ;;
  esac
done

# Safety: require clean KB data dirs so rollback is well-defined. We don't
# care if kb/scripts/ is dirty (those are tool edits, not data).
KB_DATA_DIRS=(287g anc budget commission comms facilities ice-contracts
              industry jobs legislative real-estate sheriff)
KB_DATA_PATHS=()
for d in "${KB_DATA_DIRS[@]}"; do KB_DATA_PATHS+=("kb/$d"); done

if [ "$ROLLBACK" = true ] && ! git diff --quiet HEAD -- "${KB_DATA_PATHS[@]}"; then
  echo "ERROR: KB data dirs have uncommitted changes. Commit, stash, or pass --no-rollback." >&2
  git status --short "${KB_DATA_PATHS[@]}" | head -5 >&2
  exit 2
fi

rollback_kb() {
  if [ "$ROLLBACK" = true ]; then
    echo "  Rolling back KB data dirs to HEAD…"
    git checkout HEAD -- "${KB_DATA_PATHS[@]}" 2>/dev/null || true
    # Remove any untracked entries this run created.
    git clean -fd "${KB_DATA_PATHS[@]}" > /dev/null 2>&1 || true
  fi
}

SCRIPTS_DIR="kb/scripts"
STAGE_DIR="${DP_STAGE_DIR:-/tmp/dp_ingest}"
REPORT_DIR="${DP_REPORT_DIR:-/tmp/dp_ingest_report}"
mkdir -p "$STAGE_DIR"
rm -f "$STAGE_DIR"/*.json  # start each run with clean staging
rm -rf "$REPORT_DIR"
mkdir -p "$REPORT_DIR/logs" "$REPORT_DIR/quarantine"
SOURCES_TSV="$REPORT_DIR/sources.tsv"
: > "$SOURCES_TSV"

# Write the report and exit. finish <code> [<stage it stopped at> <why>]
finish() {
  local code="$1" aborted="${2:-}" reason="${3:-}"
  local gate_arg=()
  [ -f "$REPORT_DIR/gate.json" ] && gate_arg=(--gate "$REPORT_DIR/gate.json")
  python3 "$SCRIPTS_DIR/ingest_gate.py" report \
    --sources "$SOURCES_TSV" ${gate_arg[@]+"${gate_arg[@]}"} \
    --aborted "$aborted" --aborted-reason "$reason" \
    --run-url "${RUN_URL:-}" --date "$(date -u +%Y-%m-%d)" \
    --out-md "$REPORT_DIR/report.md" --out-status "$REPORT_DIR/status.json" \
    || echo "  WARN: could not write the run report" >&2
  exit "$code"
}

if [ "${DP_SKIP_UNIT_TESTS:-}" != 1 ]; then
  echo "── Running unit tests ──"
  if ! python3 -m pytest "$SCRIPTS_DIR/tests/" -q; then
    echo "  FAIL: unit tests failed — aborting before any ingest runs." >&2
    finish 1 "the unit tests" "kb/scripts/tests failed, so no source was fetched. See the run log."
  fi
  echo ""
fi

echo "═══════════════════════════════════════════"
echo "  Detention Pipeline — CI Ingestion"
echo "═══════════════════════════════════════════"
echo "  Date:     $(date -u '+%Y-%m-%d %H:%M UTC')"
echo "  Lookback: ${DAYS} days"
echo "  Only:     ${ONLY:-all}"
echo ""

should_run() {
  [ -z "$ONLY" ] || [ "$ONLY" = "$1" ]
}

# Check a staged file is a JSON list and record the source's result.
# check_staged <name> <log>
check_staged() {
  local name="$1" log="$2" out="$STAGE_DIR/$1.json"
  if ! python3 -c "import json, sys; d = json.load(open('$out')); sys.exit(0 if isinstance(d, list) else 1)" 2>/dev/null; then
    echo "  FAIL: ${name} produced malformed JSON; discarding." >&2
    rm -f "$out"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    printf '%s\tmalformed\t0\t%s\n' "$name" "$log" >> "$SOURCES_TSV"
  else
    local count
    count=$(python3 -c "import json; print(len(json.load(open('$out'))))" 2>/dev/null || echo 0)
    echo "  OK: $out ($count entries)"
    printf '%s\tok\t%s\t%s\n' "$name" "$count" "$log" >> "$SOURCES_TSV"
  fi
}

run_ingest() {
  local name="$1"
  local out="$STAGE_DIR/${name}.json"
  local log="$REPORT_DIR/logs/${name}.log"
  shift
  echo "── ${name} ──"
  # The log feeds the report: a source can exit 0 and still have lost part
  # of its data (one Legistar client's HTTP 500, BLS refusing a download).
  python3 "$@" --output "$out" 2>&1 | tee "$log"
  if [ "${PIPESTATUS[0]}" -eq 0 ]; then
    check_staged "$name" "$log"
  else
    echo "  FAIL: ${name} ingest failed; discarding staged JSON." >&2
    rm -f "$out"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    printf '%s\tfailed\t0\t%s\n' "$name" "$log" >> "$SOURCES_TSV"
  fi
  echo ""
}

if [ -n "$FROM_STAGED" ]; then
  # Re-land a batch fetched earlier (or a test fixture) without fetching.
  echo "── Using staged JSON from $FROM_STAGED (no fetch) ──"
  for f in "$FROM_STAGED"/*.json; do
    [ -e "$f" ] || continue
    name="$(basename "$f" .json)"
    cp "$f" "$STAGE_DIR/$name.json"
    check_staged "$name" ""
  done
  echo ""
  ONLY="__staged__"
fi

# Only legistar and usaspending (ICE contracts) support --days. 287g scrapes
# a full Prison Policy list and has no lookback. Jobs + budget are seed /
# typology data with no time dimension.
should_run legistar    && run_ingest legistar    "$SCRIPTS_DIR/ingest_legistar.py" --days "$DAYS"
should_run 287g        && run_ingest 287g        "$SCRIPTS_DIR/ingest_287g.py"
should_run usaspending && run_ingest usaspending "$SCRIPTS_DIR/ingest_ice_contracts.py" --days "$DAYS"
should_run jobs        && run_ingest jobs        "$SCRIPTS_DIR/ingest_jobs.py" --seed-known
should_run budget      && run_ingest budget      "$SCRIPTS_DIR/ingest_budget_distress.py" --min-score 3

echo "── Pre-ingest validation baseline ──"
python3 "$SCRIPTS_DIR/ingest_gate.py" baseline --out "$REPORT_DIR/baseline.json"
echo ""

echo "── Converting JSON → KB entries ──"
JSON_FILES=("$STAGE_DIR"/*.json)
echo "[]" > "$REPORT_DIR/manifest.json"
if [ -e "${JSON_FILES[0]}" ]; then
  python3 "$SCRIPTS_DIR/json_to_entries.py" --manifest "$REPORT_DIR/manifest.json" "${JSON_FILES[@]}"
else
  echo "  No staged JSON files — nothing to convert."
fi
echo ""

echo "── Validation gate ──"
if ! python3 "$SCRIPTS_DIR/ingest_gate.py" gate \
     --baseline "$REPORT_DIR/baseline.json" \
     --manifest "$REPORT_DIR/manifest.json" \
     --quarantine-dir "$REPORT_DIR/quarantine" \
     --out "$REPORT_DIR/gate.json"; then
  echo "  FAIL: new validation errors the gate could not pin to this batch — rolling back." >&2
  rollback_kb
  finish 1 "the validation gate" "New validation errors appeared that no file in this batch accounts for, so kb/ was reverted to HEAD. They are listed below."
fi
echo ""

echo "── Regenerating heat scores (with strict contract check) ──"
./build.sh
if ! python3 kb/scripts/test_heat_contract.py --strict; then
  echo "  FAIL: heat_data.json contract violated — rolling back." >&2
  rollback_kb
  finish 1 "the heat-score contract check" "heat_data.json built from this batch broke its contract (kb/scripts/test_heat_contract.py --strict), so kb/ was reverted to HEAD. See the run log."
fi
echo ""

echo "── Regenerating Hugo content ──"
(cd hugo && python3 generate_content.py)
echo ""

echo "═══════════════════════════════════════════"
if [ "$FAIL_COUNT" -gt 0 ]; then
  echo "  Done with ${FAIL_COUNT} source failure(s). $(date -u '+%H:%M UTC')"
  echo "═══════════════════════════════════════════"
  finish 1
fi
echo "  Done. $(date -u '+%H:%M UTC')"
echo "═══════════════════════════════════════════"
finish 0
