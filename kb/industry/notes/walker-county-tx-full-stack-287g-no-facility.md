---
id: walker-county-tx-full-stack-287g-no-facility
title: "Walker County, TX — full-stack 287(g) sheriff's office, no confirmed active ICE-dedicated facility"
type: note
state: TX
importance: 5
tags:
  - walker-county
  - texas
  - 287g
  - full-stack-sheriff
  - jail-bed-lease
  - heatmap-methodology
  - state-propagated-signal
  - false-positive-check
research_status: working
last_researched: "2026-07-03"
---

# Walker County, TX: Full-Stack 287(g) Sheriff, No Confirmed Active ICE Facility

## Overview

Walker County (Huntsville, FIPS 48471) carries a heat score of 73 with 5 signal types on file, but fresh 2025-2026 web research finds no evidence of a currently operating ICE-dedicated detention facility physically sited in the county, and no news coverage of a live buildout fight there. The score is driven almost entirely by (1) a genuinely notable full-stack 287(g) sheriff's office and (2) two large contractor-award signal categories that turn out, on inspection, to be Texas-wide state-propagated noise rather than Walker-specific activity. This entry documents both the real signal and the false-positive risk for future researchers and for heatmap-methodology review.

## Key Details

**Full-stack 287(g) (real, direct signal):** Walker County has all three 287(g) models active on the same sheriff's office — JEM (Walker County Sheriff's Department, signed June 9, 2020), WSO (Walker County Sheriff's Office, signed April 2, 2025), and TFM (Walker County Constable Pct 3, signed September 9, 2025) — per Prison Policy Initiative's ICE-sourced appendix table (as of 2026-02-17; already on file as `kb/287g/287-g-jem-walker-county-sheriff-s-department-tx.md`, `287-g-wso-walker-county-sheriff-s-office-tx.md`, `287-g-tfm-walker-county-constable-pct-3-tx.md`). This predates and now overlaps with Texas SB 8 (effective January 1, 2026), which requires counties over 100,000 population to hold a 287(g) agreement by December 2026 — Walker County (pop. ~72,000 in 2020) was already ahead of the state mandate curve, consistent with the county's documented history of actively soliciting ICE business (see below).

**Historical ICE jail-bed lease (2017, unconfirmed current status):** Walker County Sheriff Clint McRae and Capt. Steve Fisher applied to house ICE detainees at the Walker County Jail (655 FM 2821 W, Huntsville; ~162-268 bed capacity depending on source), proposing to lease roughly 20 beds to DHS/ICE, structured so the county would be paid whether or not the beds were occupied (Item, itemonline.com, 2017). This is the likely origin of the "ice-contract" heat signal in spirit, though no USAspending record tying an active per-diem detainee contract to Walker County specifically was found in this pass — see false-positive note below. **Current 2025-2026 status of this bed lease is unconfirmed; treat as historical unless a fresh contract record surfaces.**

**"Huntsville State P." IGSA entry is not an ICE facility — it's the Walls Unit:** The `igsa:2` heat signal and `kb/facilities/huntsville-state-p-walker-tx.md` (source: Vera Institute ICE Detention Trends dataset) list "Huntsville State P." in Walker County as an active IGSA facility, but this is almost certainly TDCJ's Huntsville Unit — the "Walls Unit," Texas's oldest state prison (opened 1849) and the state's execution site. It is a Texas Department of Criminal Justice facility, not an ICE-operated or ICE-contracted immigration detention facility. No search result from this pass shows ICE detention operations at the Walls Unit. **This IGSA entry appears to be a Vera dataset mismatch/artifact and should not be read as current ICE detention activity in Walker County.**

**"ice-contract:31" and "anc-contract:16" are state-propagated, not Walker-specific:** Direct inspection of `hugo/data/heat.json` for FIPS 48471 shows both signal blocks report identical `count` and `propagated` values (ice-contract: 31/31; anc-contract: 16/16). Per `kb/scripts/county_heat_score.py`, a signal is only counted as "propagated" when it has **no county-specific place-of-performance** and is instead spread to every county in the state at reduced weight. Confirmed by direct grep: zero files under `kb/ice-contracts/` or `kb/anc/` contain "48471" or "walker" as county/fips text. The specific contractors cited (GEO Group, ASRC Federal, Akima, Aleut Technical Services, etc.) are Texas-wide federal award recipients with no demonstrated nexus to Walker County itself — they inflate the score for every TX county via state-level propagation. **This is a heatmap-methodology finding, not a Walker County finding**: any county with a high `ice-contract`/`anc-contract` count where `count == propagated` should be treated as carrying no county-specific contractor signal until direct entries are found.

**No 2025-2026 opposition/commission fight found:** Targeted searches for Walker County ICE facility protests, commission votes, or community opposition returned no county-specific results — all hits were from other TX counties (San Antonio/Bexar, Hutchins/Dallas, El Paso, McAllen). Walker County does not appear to be a site in the ICE Detention Reengineering Initiative warehouse buildout (Hutchins, El Paso, San Antonio, Los Fresnos are the named 2026 mega-facility sites per Bloomberg/El Paso Matters reporting).

## Why It Matters

Walker County illustrates a heat-score failure mode worth flagging for the broader project: a county can score 73 (high) almost entirely from (a) one real but modest signal — full-stack 287(g) — combined with (b) an outdated single-source facility misattribution (Vera's Huntsville State P./Walls Unit conflation) and (c) two large-looking contractor-count signals that are actually diffuse state-level propagation with zero county-specific grounding. Researchers pulling Walker County from the heatmap should not expect an active warehouse fight or a large ICE-dedicated facility on the ground — what's actually there is a sheriff's office fully bought into every tier of 287(g) partnership, a stale historical 20-bed jail lease application, and a TDCJ execution unit misfiled as an ICE facility. This is a "high score, thin ground truth" case, distinct from counties like Lubbock where the high score reflects real but indirect harm exposure (see `kb/industry/notes/lubbock-county-tx-local-enforcement-pattern.md`).

## Sources

- [287(g) JEM/WSO/TFM entries — Prison Policy Initiative, compiled from ICE data as of 2026-02-17](https://www.prisonpolicy.org/) (already on file: `kb/287g/287-g-jem-walker-county-sheriff-s-department-tx.md`, `287-g-wso-walker-county-sheriff-s-office-tx.md`, `287-g-tfm-walker-county-constable-pct-3-tx.md`)
- [Walker County applies to house inmates with immigration detainers for feds — The Item (2017)](https://www.itemonline.com/news/walker-county-applies-to-house-inmates-with-immigration-detainers-for/article_b79c6c7e-175c-11e7-a08c-db9efeec2c0c.html)
- [Sheriffs would be required to cooperate with immigration agents under bill approved by Senate (SB 8) — Texas Tribune (2025-04-01)](https://www.texastribune.org/2025/04/01/texas-senate-bill-8-vote-287g-agreements-sheriffs-ice/)
- [Immigration advocates worry as new law requiring Texas sheriffs to work with ICE goes into effect — KERA/TPR (2025-12-29)](https://www.keranews.org/news/2025-12-29/immigration-advocates-new-law-texas-sheriffs-ice-287g-senate-bill-8)
- [Here's Where ICE Is Locating Its Massive Warehouse Jails — Bloomberg (2026-02-26)](https://www.bloomberg.com/news/features/2026-02-26/where-ice-s-38-billion-plan-is-turning-warehouses-into-immigration-mega-jails)
- [ICE Has Considered Adding 20,000 Detention Center Beds in Secretive Expansion Across Texas — The Barbed Wire (2026-02-19)](https://thebarbedwire.com/2026/02/19/ice-expansions-texas/)
- Direct inspection of `hugo/data/heat.json` (FIPS 48471) and `kb/scripts/county_heat_score.py` propagation logic (2026-07-03)
