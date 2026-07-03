---
id: garza-county-tx-heat-score-signal-audit
title: "Garza County TX — heat score 74 largely reflects stale/duplicate/propagated signals, not live ICE detention activity"
type: note
state: TX
importance: 5
tags:
- garza-county
- signal-audit
- stale-signal
- data-quality
- heat-score-methodology
- propagated-signals
research_status: working
last_researched: "2026-07-03"
---

# Garza County TX — Heat Score Audit (Score 74, No Prior Research)

## Overview

Garza County, TX (FIPS 48169) carries a heat score of 74 despite never having been researched by this project. A fresh research pass (2026-07-03) finds that essentially none of the underlying signal reflects live ICE/private-detention activity in the county today. This note documents the audit for future scoring-methodology cleanup.

## Key Details

**igsa:4** — Traced to exactly two KB stub records (`bscc-giles-darby-facility-garza-tx`, `dalby-correctional-institute-garza-tx`), both sourced from the Vera Institute ICE Detention Trends dataset, both listing the identical address (805 N. Avenue F / 805 N. Ave F, Post, TX) and identical coordinates. These are name variants of **one physical facility** — the Giles W. Dalby Correctional Institution — counted twice, and the heat.json signal entry list shows each title appearing twice again (likely a second duplication in the scoring join), producing igsa:4 for what is actually zero *currently active* IGSA facilities. The facility's federal (BOP) contract with operator MTC ended September 2024; the site was vacant through 2025; the State of Texas bought it for $110M and reopened it February 24, 2026 as a **TDCJ state prison** — not an ICE- or BOP-contracted detention facility. Both stub records have been corrected in this pass (status: closed, correction notes added) but not merged/deduplicated — that requires a scoring-pipeline decision, not a content edit.

**287g:1 (WSO)** — Confirmed real (Prison Policy Initiative, signed July 7, 2025). However, Texas SB 8 (89th legislature, 2025) made 287(g) agreements **mandatory** for nearly all Texas counties. This signal is very likely state-mandate compliance rather than the discretionary "cooperative sheriff actively courting ICE" pattern this KB's 287g signal is designed to detect elsewhere. Worth a scoring note/flag for all post-SB8 Texas WSO agreements, not just Garza.

**budget-distress:1 (score 3/10)** — Real but weak (USDA ERS typology, "moderate" signal strength per the KB record itself). A 3/10 distress score is on the low end of what this project treats as a meaningful fiscal-desperation indicator.

**ice-contract:31 / anc-contract:16** — heat.json marks both as `"propagated"` (31 and 16 respectively, matching the raw counts exactly — i.e., the entire signal is propagated, not county-specific). Web search found **no evidence** connecting the listed contractors (GEO Group, ASRC Federal entities, Akima, Aleut Technical Services, Prime Masonry, Coho Construction) to any Garza County site or activity. These read as statewide-TX federal contract records attributed to the county by the scoring pipeline's propagation logic rather than county-specific procurement. This is very likely a **methodology artifact inflating the headline score**, not a real signal of detention-industry interest in Garza County specifically.

**Net assessment**: Of the five signal types making up Garza's 74, only 287g and budget-distress reflect anything real and county-specific, and both are weak/structural (state mandate; low distress score) rather than indicative of active targeting. The igsa signal is a duplicate-counted stale record for a facility that is now a state prison. The ice-contract and anc-contract signals are explicitly flagged as propagated with no county-specific corroboration found. **Garza County does not currently show credible signs of being pitched for new ICE/private detention capacity** — its main carceral facility went the opposite direction (state DOC acquisition foreclosing a CoreCivic bid).

## Why This Matters

This is a useful test case for the scoring pipeline: a high score (74, above many actively-contested counties) generated almost entirely by duplicate/stale/propagated signals rather than ground truth. Recommend: (1) deduplicate the two Dalby/Giles-Darby igsa records, (2) review how `ice-contract`/`anc-contract` propagation is computed and whether it should carry full weight in the headline score when marked `propagated`, (3) consider a "post-SB8 mandatory 287(g)" flag for Texas counties so that signal isn't weighted the same as a discretionary agreement elsewhere.

## Sources

- [heat.json signal payload for FIPS 48169 (this repo, hugo/data/heat.json)] — internal
- [KCBD: TDCJ celebrates reopening of Giles W. Dalby Unit in Garza County (Feb 25, 2026)](https://www.kcbd.com/2026/02/25/tdcj-celebrates-reopening-giles-w-dalby-unit-garza-county/)
- [Texas Immigration Law Council: Understanding the 2025 ICE-Texas AG 287(g) Agreement / SB 8 mandate](https://txilc.org/resource/understanding-the-2025-ice-texas-attorney-general-287g-agreement/)
- See also `kb/industry/county-fights/garza-county-tx-dalby-state-preemption.md` for the underlying facility history
