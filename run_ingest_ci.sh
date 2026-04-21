#!/bin/bash
# CI ingestion runner — no Pyrite, no local paths.
# Writes JSON to /tmp, converts to kb/<signal>/*.md, rebuilds heat scores.
#
# Usage:
#   ./run_ingest_ci.sh              # run everything
#   ./run_ingest_ci.sh --only legistar
#   ./run_ingest_ci.sh --days 30
#
# Exits non-zero if any individual source fails but still commits what
# succeeded (caller decides what to do with partial success).

set -u
cd "$(dirname "$0")"

ONLY=""
DAYS=180
FAIL_COUNT=0

while [[ $# -gt 0 ]]; do
  case $1 in
    --only) ONLY="$2"; shift 2 ;;
    --days) DAYS="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 2 ;;
  esac
done

SCRIPTS_DIR="kb/scripts"
STAGE_DIR="/tmp/dp_ingest"
mkdir -p "$STAGE_DIR"

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
    echo "  OK: $out"
  else
    echo "  FAIL: ${name} ingest failed (keeping prior staged data if any)" >&2
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
  echo ""
}

should_run legistar    && run_ingest legistar    "$SCRIPTS_DIR/ingest_legistar.py" --days "$DAYS"
should_run 287g        && run_ingest 287g        "$SCRIPTS_DIR/ingest_287g.py" --days "$DAYS"
should_run usaspending && run_ingest usaspending "$SCRIPTS_DIR/ingest_ice_contracts.py" --days "$DAYS"
should_run jobs        && run_ingest jobs        "$SCRIPTS_DIR/ingest_jobs.py" --seed-known
should_run budget      && run_ingest budget      "$SCRIPTS_DIR/ingest_budget_distress.py" --min-score 3

echo "── Converting JSON → KB entries ──"
JSON_FILES=("$STAGE_DIR"/*.json)
if [ -e "${JSON_FILES[0]}" ]; then
  python3 "$SCRIPTS_DIR/json_to_entries.py" "${JSON_FILES[@]}"
else
  echo "  No staged JSON files — nothing to convert."
fi
echo ""

echo "── Validating entries ──"
python3 "$SCRIPTS_DIR/validate_entries.py" --quiet || \
  echo "  (validation reported issues — see above)"
echo ""

echo "── Regenerating heat scores (with strict contract check) ──"
./build.sh
python3 kb/scripts/test_heat_contract.py --strict || {
  echo "  FAIL: heat_data.json contract violated — not deploying." >&2
  exit 1
}
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
