#!/bin/bash
# Central ingestion pipeline runner.
# Runs all data ingestion scripts, imports to Pyrite KB, syncs to local kb/,
# regenerates heat scores, and rebuilds the Hugo site.
#
# Usage:
#   ./run_ingest.sh              # run everything
#   ./run_ingest.sh --skip-287g  # skip 287(g) (already fully ingested)
#   ./run_ingest.sh --only legistar  # run only one source
#   ./run_ingest.sh --dry-run    # preview without importing
#   ./run_ingest.sh --no-build   # ingest only, skip site rebuild

set -e
cd "$(dirname "$0")"

# ── Parse arguments ──
SKIP_287G=false
ONLY=""
DRY_RUN=""
NO_BUILD=false
DAYS=180

while [[ $# -gt 0 ]]; do
  case $1 in
    --skip-287g) SKIP_287G=true; shift ;;
    --only) ONLY="$2"; shift 2 ;;
    --dry-run) DRY_RUN="--dry-run"; shift ;;
    --no-build) NO_BUILD=true; shift ;;
    --days) DAYS="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

SCRIPTS_DIR="kb/scripts"
KB_NAME="detention-pipeline-research"
PYRITE_KB="/Users/markr/tcp-kb-internal/detention-pipeline-research"
LOCAL_KB="kb"

echo "═══════════════════════════════════════════"
echo "  Detention Pipeline — Ingestion Runner"
echo "═══════════════════════════════════════════"
echo "  Date: $(date '+%Y-%m-%d %H:%M')"
echo "  Lookback: ${DAYS} days"
echo "  Dry run: ${DRY_RUN:-no}"
echo ""

should_run() {
  local source="$1"
  if [ -n "$ONLY" ] && [ "$ONLY" != "$source" ]; then
    return 1
  fi
  return 0
}

# ── 1. Legistar (commission activity) ──
if should_run "legistar"; then
  echo "── Legistar (commission activity) ──"
  python3 "$SCRIPTS_DIR/ingest_legistar.py" --days "$DAYS" $DRY_RUN \
    --output /tmp/commission_items.json || echo "  WARNING: Legistar scan failed"

  if [ -z "$DRY_RUN" ] && [ -f /tmp/commission_items.json ]; then
    COUNT=$(python3 -c "import json; print(len(json.load(open('/tmp/commission_items.json'))))" 2>/dev/null || echo 0)
    if [ "$COUNT" -gt 0 ]; then
      echo "  Importing $COUNT commission items..."
      kb import /tmp/commission_items.json -k "$KB_NAME" || echo "  WARNING: Import failed"
    else
      echo "  No new commission items found"
    fi
  fi
  echo ""
fi

# ── 2. 287(g) agreements ──
if should_run "287g" && [ "$SKIP_287G" = false ]; then
  echo "── 287(g) agreements ──"
  python3 "$SCRIPTS_DIR/ingest_287g.py" --days "$DAYS" $DRY_RUN \
    --output /tmp/287g_agreements.json || echo "  WARNING: 287(g) ingest failed"

  if [ -z "$DRY_RUN" ] && [ -f /tmp/287g_agreements.json ]; then
    COUNT=$(python3 -c "import json; print(len(json.load(open('/tmp/287g_agreements.json'))))" 2>/dev/null || echo 0)
    if [ "$COUNT" -gt 0 ]; then
      echo "  Importing $COUNT 287(g) agreements..."
      kb import /tmp/287g_agreements.json -k "$KB_NAME" || echo "  WARNING: Import failed"
    else
      echo "  No new 287(g) agreements found"
    fi
  fi
  echo ""
else
  echo "── 287(g) — skipped (--skip-287g or --only) ──"
  echo ""
fi

# ── 3. All ICE contracts (USAspending) ──
if should_run "usaspending"; then
  echo "── All ICE contracts (USAspending) ──"
  python3 "$SCRIPTS_DIR/ingest_ice_contracts.py" --days "$DAYS" $DRY_RUN \
    --output /tmp/ice_contracts.json || echo "  WARNING: ICE contracts ingest failed"

  if [ -z "$DRY_RUN" ] && [ -f /tmp/ice_contracts.json ]; then
    COUNT=$(python3 -c "import json; print(len(json.load(open('/tmp/ice_contracts.json'))))" 2>/dev/null || echo 0)
    if [ "$COUNT" -gt 0 ]; then
      echo "  Importing $COUNT ICE contracts..."
      kb import /tmp/ice_contracts.json -k "$KB_NAME" || echo "  WARNING: Import failed"
    else
      echo "  No new ICE contracts found"
    fi
  fi
  echo ""
fi

# ── 4. Job postings ──
if should_run "jobs"; then
  echo "── Job postings ──"
  python3 "$SCRIPTS_DIR/ingest_jobs.py" --seed-known $DRY_RUN \
    --output /tmp/job_postings.json || echo "  WARNING: Jobs ingest failed"

  if [ -z "$DRY_RUN" ] && [ -f /tmp/job_postings.json ]; then
    COUNT=$(python3 -c "import json; print(len(json.load(open('/tmp/job_postings.json'))))" 2>/dev/null || echo 0)
    if [ "$COUNT" -gt 0 ]; then
      echo "  Importing $COUNT job postings..."
      kb import /tmp/job_postings.json -k "$KB_NAME" || echo "  WARNING: Import failed"
    else
      echo "  No new job postings found"
    fi
  fi
  echo ""
fi

# ── 5. Budget distress (USDA + BLS) ──
if should_run "budget"; then
  echo "── Budget distress (USDA + BLS) ──"
  python3 "$SCRIPTS_DIR/ingest_budget_distress.py" --min-score 3 $DRY_RUN \
    --output /tmp/budget_distress.json || echo "  WARNING: Budget distress ingest failed"

  if [ -z "$DRY_RUN" ] && [ -f /tmp/budget_distress.json ]; then
    COUNT=$(python3 -c "import json; print(len(json.load(open('/tmp/budget_distress.json'))))" 2>/dev/null || echo 0)
    if [ "$COUNT" -gt 0 ]; then
      echo "  Importing $COUNT budget distress entries..."
      kb import /tmp/budget_distress.json -k "$KB_NAME" || echo "  WARNING: Import failed"
    else
      echo "  No distressed counties found above threshold"
    fi
  fi
  echo ""
fi

# ── 6. Sync Pyrite KB → local kb/ ──
if [ -z "$DRY_RUN" ]; then
  echo "── Syncing Pyrite KB → local kb/ ──"
  # Sync signal-type directories from Pyrite KB to local repo
  for dir in 287g anc ice-contracts budget commission comms jobs legislative real-estate sheriff; do
    if [ -d "$PYRITE_KB/$dir" ]; then
      rsync -a --delete "$PYRITE_KB/$dir/" "$LOCAL_KB/$dir/"
    fi
  done
  # Sync kb.yaml schema
  if [ -f "$PYRITE_KB/kb.yaml" ]; then
    cp "$PYRITE_KB/kb.yaml" "$LOCAL_KB/kb.yaml"
  fi
  echo "  Synced signal directories from Pyrite KB"
  echo ""
fi

# ── 6. Regenerate heat scores ──
if [ -z "$DRY_RUN" ] && [ "$NO_BUILD" = false ]; then
  echo "── Regenerating heat scores ──"
  ./build.sh
  echo ""

  # ── 7. Regenerate Hugo content + build ──
  echo "── Regenerating Hugo site ──"
  cd hugo
  python3 generate_content.py
  hugo
  cd ..
  echo ""
fi

echo "═══════════════════════════════════════════"
echo "  Done. $(date '+%Y-%m-%d %H:%M')"
echo "═══════════════════════════════════════════"
