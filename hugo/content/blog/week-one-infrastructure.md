---
title: "Week One: Building the Machine in Public"
type: blog
layout: single
date: 2026-04-14
summary: "109 commits. 17,500 KB entries. 92 county fights across 40 states. 2,430 ICE contracts tracked. 79 county commission portals monitored in real time. The site launched six days ago. Here's what I've built since then — and what's coming next."
author: "Mark Ramm"
---

I launched the Detention Pipeline Sunday night, April 6, with a heat map, 1,644 knowledge base entries, and 13 documented county fights. Six days later, the infrastructure underneath it has been rebuilt three times over.

This is not a static report. It's a living early-warning system, and the first week has been about making it faster, broader, and harder to outrun. Here's what changed.

---

## The Numbers

| Metric | Launch night | Today |
|--------|-------------|-------|
| KB entries | 1,644 | 17,500 |
| County fights documented | 13 | 92 |
| States with fight coverage | 7 | 40 |
| Facilities indexed | 459 | 1,294 |
| Legistar portals monitored | 17 | 79 |
| States with commission monitoring | 6 | 25 |
| ICE contracts tracked | 244 (ANC only) | 2,430 (all ICE) |
| Counties scored | 1,644 | 1,998 |

---

## New Data Sources

The biggest infrastructure change this week: **I now track every ICE contract on USAspending.gov**, not just the Alaska Native Corporation sole-source contracts I started with. That's 2,430 contracts across 922 contractors, totaling $13.7 billion in the last year. The system classifies each as detention-related or other, and only detention-related contracts feed the heat map — but the full universe is in the knowledge base for reference.

This is how I found **GardaWorld Federal Services** — a Canadian private security firm with no prior ICE detention experience that landed a $313 million contract for a 1,500-person facility in Surprise, Arizona — and **KVG LLC**, which got $113 million for a new facility in Hagerstown, Maryland. Neither would have appeared in the ANC-only search. The full contractor list also surfaces the supporting infrastructure: $1.6 billion in deportation charter flights (CSI Aviation, Classic Air), $641 million in detention medical staffing (STG International), $381 million in ankle monitoring (B.I. Incorporated), and $150 million in Palantir's case management system.

---

## Commission Monitoring: From 30 Portals to 79

County commissions are where detention deals get approved. The Legistar API — run by Granicus — powers meeting management for hundreds of local governments, and I've been scraping it for detention-related keywords since launch.

This week I rewrote the scanner from scratch. The old version ran sequentially — one county at a time, 15 minutes for a full scan. The new version runs all counties in parallel using async HTTP. The same 180-day scan now finishes in under three minutes.

More importantly, I nearly tripled coverage. Thirty portals across 12 states became **79 portals across 25 states**. The new additions immediately produced signals:

- **Oklahoma County, OK** — 17 hits in 30 days. Active detention center construction, Criminal Justice Authority meetings, bond oversight board presentations, furlough planning. This county is building.
- **Baltimore, MD** — "Private Detention Centers – Citywide Ban" in the Land Use & Transportation Committee. A city trying to preempt what it sees coming.
- **Brazoria County, TX** — "Preliminary Engineering Services — Water Supply for Detention Facility." Infrastructure procurement for a facility that hasn't been announced.
- **Harris County, TX** — ICE discussion in Commissioners Court. Seven hits across multiple meeting types.
- **Galveston County, TX** — 287(g) certification proceeding.
- **Pima County, AZ** — Detention center contract amendment.
- **Westchester County, NY** — "Discussion on ICE in Westchester County" in the Public Safety committee.
- **Madison, WI** — "Emergent Immigration, Deportation and Housing Issues" before the Equal Opportunities Commission.

I also fixed a Maricopa County bug — their Legistar Events endpoint is broken server-side, so I built a fallback that queries their Matters endpoint instead. And I caught a data quality issue: the "Columbus" Legistar portal is Columbus, Ohio, not Columbus, Georgia. The old data had been attributing Ohio city council activity to a Georgia county. That's corrected.

The states still not on Legistar at all — Louisiana, New Mexico, South Carolina, Indiana, Utah, Nebraska — will need custom scrapers. That's [issue #2](https://github.com/markramm/detention-pipeline/issues/2) on GitHub if you want to help.

---

## 92 County Fights

The research has been relentless. I went from 13 documented fights to 92 — stories of communities that fought detention proposals, won or lost, and left a record of what worked.

Some highlights from this week:

**Tennessee's Wilson County** killed a 16,000-bed mega-center — what would have been the largest ICE detention facility in the country — when 24 of 25 all-Republican county commissioners signed a resolution against it. **The Choctaw Nation** in Oklahoma preempted ICE by purchasing the warehouse they were targeting. **Bristol County, Massachusetts** went from the most aggressive ICE collaborator in the country to zero transfers after a single election changed the sheriff.

Every fight is sourced and documented. Every one teaches the next community something about what works.

---

## Site Infrastructure

Beyond the data pipeline, the site itself was rebuilt substantially:

- **County pages now have four specific calls to action**: submit a link to meeting minutes, share local news coverage, report a closed-session agenda item, flag building or warehouse activity. Each pre-fills a GitHub issue template with the county name and FIPS code. Sensitive tips go to an encrypted email.
- **FOIA generator** — enter your county, get ready-to-send public records requests tailored to the signals appearing there.
- **Facilities overlay** — 1,294 IGSA facilities mapped with operators, contract types, and conditions records. Sourced from the Vera Institute dataset.
- **Network visualization** — the revolving door between government and the private detention industry, with financial disclosure data.
- **Timeline** — dynamic bucket resolution that zooms from monthly to weekly to daily. The 287(g) explosion after January 2025 is unmistakable.
- **State pages** with D3 choropleth maps showing county-level heat within each state.
- **OG social cards** generated for all 1,998 counties — share a county page and the card shows its heat score, signal breakdown, and heatmap overlay.

---

## What's Next

The commission scanner now covers 25 states. I want all 50. The [coverage gaps page](https://detention-pipeline.transparencycascade.org/coverage/) shows which counties have only automated signals — local knowledge from people on the ground is what converts a blinking dot on a map into a documented story.

I'm also building toward real-time monitoring: commercial real estate alerts for warehouse purchases in high-heat counties, job board scanning for detention consultant postings, and state legislature tracking through OpenStates.

---

## How to Help

**Look up your county.** If the score is low, that might mean low coverage, not low activity. Check the [coverage gaps](https://detention-pipeline.transparencycascade.org/coverage/) first.

**Submit what you know.** Every county page now has specific tip buttons — meeting minutes URLs, local news links, closed-session reports, building activity. One good URL from a local resident gives me a permanent monitoring target for that county. If you need to share something sensitive, there's an encrypted email option on every county page.

**If you're technical**, the whole thing is [on GitHub](https://github.com/markramm/detention-pipeline) under CC-BY-SA. The ingestion pipeline, the heat scoring algorithm, the Legistar scanner — all open source, all documented. Fork it, run discovery against your state, contribute upstream.

**Follow the reporting.** This site is the data layer for an ongoing investigation at [The RAMM](https://theramm.substack.com). Subscribing is the most direct way to support this work and ensure it continues. The infrastructure costs money to run, the research takes time, and reader support is what keeps it independent.

---

109 commits in six days. The system gets smarter every time someone looks at it. The detention buildout was engineered to outrun public attention. I'm building something faster.

---

*Data current as of April 14, 2026. The Detention Pipeline is at [detention-pipeline.transparencycascade.org](https://detention-pipeline.transparencycascade.org/). The investigation is at [The RAMM](https://theramm.substack.com).*
