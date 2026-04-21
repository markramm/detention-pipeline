#!/bin/bash
# CI ingestion runner — no Pyrite, no local paths.
#
# Atomicity model: git is the snapshot. The runner requires a clean
# working tree at the start. On any validation regression the runner
# reverts kb/ to HEAD before exiting. The only two ways this run can
# leave kb/ modified are (a) success, or (b) the caller passes
# --no-rollback (useful for debugging).
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

set -u
cd "$(dirname "$0")"

ONLY=""
DAYS=180
ROLLBACK=true
FAIL_COUNT=0

while [[ $# -gt 0 ]]; do
  case $1 in
    --only) ONLY="$2"; shift 2 ;;
    --days) DAYS="$2"; shift 2 ;;
    --no-rollback) ROLLBACK=false; shift ;;
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
STAGE_DIR="/tmp/dp_ingest"
mkdir -p "$STAGE_DIR"
rm -f "$STAGE_DIR"/*.json  # start each run with clean staging

echo "── Running unit tests ──"
if ! python3 -m pytest "$SCRIPTS_DIR/tests/" -q; then
  echo "  FAIL: unit tests failed — aborting before any ingest runs." >&2
  exit 1
fi
echo ""

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

run_ingest() {
  local name="$1"
  local out="$STAGE_DIR/${name}.json"
  shift
  echo "── ${name} ──"
  if python3 "$@" --output "$out"; then
    # Validate JSON well-formedness before exposing to the converter.
    if ! python3 -c "import json, sys; d = json.load(open('$out')); sys.exit(0 if isinstance(d, list) else 1)" 2>/dev/null; then
      echo "  FAIL: ${name} produced malformed JSON; discarding." >&2
      rm -f "$out"
      FAIL_COUNT=$((FAIL_COUNT + 1))
    else
      local count
      count=$(python3 -c "import json; print(len(json.load(open('$out'))))" 2>/dev/null || echo 0)
      echo "  OK: $out ($count entries)"
    fi
  else
    echo "  FAIL: ${name} ingest failed; discarding staged JSON." >&2
    rm -f "$out"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
  echo ""
}

# Only legistar and usaspending (ICE contracts) support --days. 287g scrapes
# a full Prison Policy list and has no lookback. Jobs + budget are seed /
# typology data with no time dimension.
should_run legistar    && run_ingest legistar    "$SCRIPTS_DIR/ingest_legistar.py" --days "$DAYS"
should_run 287g        && run_ingest 287g        "$SCRIPTS_DIR/ingest_287g.py"
should_run usaspending && run_ingest usaspending "$SCRIPTS_DIR/ingest_ice_contracts.py" --days "$DAYS"
should_run jobs        && run_ingest jobs        "$SCRIPTS_DIR/ingest_jobs.py" --seed-known
should_run budget      && run_ingest budget      "$SCRIPTS_DIR/ingest_budget_distress.py" --min-score 3

echo "── Pre-ingest validation baseline ──"
BASELINE_ERRORS=$(python3 "$SCRIPTS_DIR/validate_entries.py" --quiet 2>&1 | tail -1 | grep -oE '[0-9]+ errors' | awk '{print $1}')
BASELINE_ERRORS=${BASELINE_ERRORS:-0}
echo "  HEAD has $BASELINE_ERRORS validation errors."
echo ""

echo "── Converting JSON → KB entries ──"
JSON_FILES=("$STAGE_DIR"/*.json)
if [ -e "${JSON_FILES[0]}" ]; then
  python3 "$SCRIPTS_DIR/json_to_entries.py" "${JSON_FILES[@]}"
else
  echo "  No staged JSON files — nothing to convert."
fi
echo ""

echo "── Post-ingest validation ──"
POST_ERRORS=$(python3 "$SCRIPTS_DIR/validate_entries.py" --quiet 2>&1 | tail -1 | grep -oE '[0-9]+ errors' | awk '{print $1}')
POST_ERRORS=${POST_ERRORS:-0}
echo "  Now at $POST_ERRORS validation errors (baseline $BASELINE_ERRORS)."
if [ "$POST_ERRORS" -gt "$BASELINE_ERRORS" ]; then
  echo "  REGRESSION: ingest introduced $((POST_ERRORS - BASELINE_ERRORS)) new validation errors." >&2
  rollback_kb
  exit 1
fi
echo ""

echo "── Regenerating heat scores (with strict contract check) ──"
./build.sh
if ! python3 kb/scripts/test_heat_contract.py --strict; then
  echo "  FAIL: heat_data.json contract violated — rolling back." >&2
  rollback_kb
  exit 1
fi
echo ""

echo "── Regenerating Hugo content ──"
(cd hugo && python3 generate_content.py)
echo ""

echo "═══════════════════════════════════════════"
if [ "$FAIL_COUNT" -gt 0 ]; then
  echo "  Done with ${FAIL_COUNT} source failure(s). $(date -u '+%H:%M UTC')"
  exit 1
else
  echo "  Done. $(date -u '+%H:%M UTC')"
fi
echo "═══════════════════════════════════════════"
