---
id: texas-287g-statewide-attribution-la-salle-county
title: "TX — statewide 287(g) agreements (AG/DPS/National Guard) are misattributed to individual counties, not county-specific deals"
type: note
state: TX
importance: 6
tags: [287g, texas, sb8, statewide-agreement, data-quality, heat-score-methodology]
research_status: working
last_researched: "2026-07-03"
---

# Texas: statewide 287(g) agreements getting attributed to individual counties (verification note for La Salle County FIPS 48283 heat signal)

## Overview

La Salle County's heat-score carries a 287g:8 signal that includes the Texas Attorney General's Office, Texas DPS Criminal Investigations, Texas National Guard, and small-town PDs (Marlin PD, Nixon PD) as if these were La Salle County-specific 287(g) partners. Research confirms these are **statewide or multi-jurisdictional Texas agreements, not agreements specific to La Salle County** — the heat-score ingestion is very likely attributing statewide-scope agreements to every county in the dataset rather than only to counties with a genuine standalone local agreement. This is a data-quality/methodology finding, not a new La Salle County signal.

## Key Details

- **TX Attorney General 287(g) agreement**: signed January 2025 — an unprecedented statewide-scope agreement between ICE and the Texas AG's office, per Texas Immigration Law Council (TXILC) analysis. Not county-specific.
- **Texas DPS Task Force 287(g)**: Texas DPS troopers statewide have been deputized under the Task Force model ("Texas Turns Its Sprawling State Police Force Into Immigration Agents," Texas Observer) — a state-level rollout, not a La Salle County sheriff action.
- **Senate Bill 8** (effective Jan 1, 2026): requires nearly every Texas county operating a jail to enter a 287(g) agreement (Warrant Service Officer, Jail Enforcement, or Task Force model) by end of 2026 or face fines. This is why the signal count is high statewide — it's a legal mandate hitting ~200+ counties simultaneously, not evidence of unusual La Salle County-specific enforcement intensity.
- **La Salle County Sheriff's Office** (Sheriff Hector C. Ramirez): no confirmed, dated reporting found of a La Salle County-specific 287(g) agreement signature, model selection, or commissioners court vote in this research pass. County has until end of 2026 to comply under SB8 like all others.
- **Marlin PD / Nixon PD**: these are small municipal police departments in Falls County and Gonzales/Wilson County respectively — geographically unrelated to La Salle County. Their presence in the La Salle County signal bundle strongly suggests a name-matching or geocoding error in the ingestion pipeline rather than a real La Salle County connection.

## Why It Matters

If the county_heat_score.py ingestion is folding statewide/multi-county-agency 287(g) records into every county's local signal count, La Salle County's true "8" signal count for 287(g) is likely inflated by state-level noise — the same distortion likely affects most or all of the ~200 Texas counties post-SB8. This is worth flagging to whoever maintains `kb/scripts/county_heat_score.py` for a scope check (agency jurisdiction vs. county FIPS) before treating 287g counts as county-level differentiators across Texas.

## Sources

- [Understanding the 2025 ICE-Texas Attorney General 287(G) Agreement — Texas Immigration Law Council](https://txilc.org/resource/understanding-the-2025-ice-texas-attorney-general-287g-agreement/)
- [Immigration advocates worry as new law requiring Texas sheriffs to work with ICE goes into effect — KERA News (Dec 29, 2025)](https://www.keranews.org/news/2025-12-29/immigration-advocates-new-law-texas-sheriffs-ice-287g-senate-bill-8)
- [Sheriffs would be required to cooperate with immigration agents under bill approved by Senate — Texas Tribune (Apr 1, 2025)](https://www.texastribune.org/2025/04/01/texas-senate-bill-8-vote-287g-agreements-sheriffs-ice/)
- [Texas Turns Its Sprawling State Police Force Into Immigration Agents for Trump — Texas Observer](https://www.texasobserver.org/texas-dps-287g-ice-trump-abbott/)
- [La Salle County Sheriff's Office — official county page](https://www.co.la-salle.tx.us/index.php/offices/2016-03-01-15-29-59)
