---
id: tarrant-county-tx-silver-creek-false-lead
title: "Tarrant County TX — 9449 Silver Creek Rd is a Google data-center lease, not an ICE warehouse (heatmap correction)"
type: note
state: TX
importance: 4
tags:
- texas
- tarrant-county
- data-hygiene
- false-lead
- real-estate-trace
- majestic-realty
- hutchins-confusion
research_status: working
last_researched: "2026-07-03"
---

# Tarrant County TX: 9449 Silver Creek Rd Is Not an ICE Signal — Correction Note

## Overview

The existing KB `real-estate-trace` entry for Tarrant County (`9449-silver-creek-rd-tarrant-tx-1100000-sf`) flags a 1.1M sq ft Majestic Realty warehouse in the Silver Creek Business Park, Fort Worth, as "worth checking" given its warehouse scale. Fresh web research confirms this property has **no connection to ICE detention**. It is confirmed leased by Google for data-center/computing use, reported by the Fort Worth Report and The Real Deal in October 2024 (Google's 1.1M sq ft lease in Roski/Majestic's Fort Worth Silver Creek development). Nothing in current reporting (searched July 2026) connects this specific address to DHS, ICE, or any detention-related transaction.

## Key Details — Source of Likely Confusion

There IS a real, well-documented Majestic Realty warehouse-to-ICE-detention story from early 2026 — but it is a **different property in a different county**: the **PointSouth Logistics & Commerce Centre Building 1** at **950 N. IH-45, Hutchins, TX (Dallas County, FIPS 48113)**, ~12 miles southeast of downtown Dallas and adjacent to the Hutchins State Jail. DHS reportedly moved to purchase this ~1M sq ft ex-Amazon warehouse (built 2022-2023 for ~$42M) to convert into a 9,500-bed detention facility. Following weeks of resident organizing and political opposition (Dallas County Commissioner Elba Garcia among the objectors), Majestic Realty publicly stated in February 2026 that it "has not and will not enter into any agreement for the purchase or lease of any building to the Department of Homeland Security for use as a detention facility" — effectively killing the deal. Hutchins residents celebrated the win (WFAA, Feb 2026) while noting "it could happen anywhere."

Both properties are Majestic Realty-developed logistics buildings of similar (~1.1M sq ft) size, which is almost certainly why the Silver Creek property triggered a real-estate-trace signal — the Hutchins story got wide North Texas coverage in Jan-Feb 2026 and Tarrant/Dallas county lines are easy to conflate in the DFW metroplex. They are not the same building, the same owner action, or the same county.

## Recommendation

The `9449-silver-creek-rd-tarrant-tx-1100000-sf` entry's `importance: 5` and "worth checking" framing should be reconsidered at the next heat-score/data-quality pass — this signal is not corroborating Tarrant County's detention-pipeline risk and should not be weighted as if it were an active or credible real estate trace. The genuine Majestic Realty ICE-warehouse story belongs to Dallas County (Hutchins), not Tarrant County, and should be checked for its own KB entry there if one doesn't already exist.

## Sources

- [Google leases 1.1M SF warehouse space in Fort Worth — Fort Worth Report (Oct 8, 2024)](https://fortworthreport.org/2024/10/08/google-leases-1-1m-sf-warehouse-space-in-fort-worth/)
- [Google Leases 1.1M sf in Roski's Fort Worth Development — The Real Deal (Oct 8, 2024)](https://therealdeal.com/texas/fort-worth/2024/10/08/google-leases-1-1m-sf-in-roskis-fort-worth-development/)
- [Majestic Denies Sale Of Hutchins Warehouse For ICE Detention Use — Bisnow (Feb 2026)](https://www.bisnow.com/dallas-ft-worth/news/industrial/hutchins-ice-detention-facility-133239)
- [Massive Dallas-area warehouse will not be used as an ICE detention center, developer says — CBS Texas (Feb 2026)](https://www.cbsnews.com/texas/news/ice-detention-center-hutchins-dallas-texas-warehouse/)
- [Hutchins residents celebrate victory over ICE detention center — WFAA (Feb 2026)](https://www.wfaa.com/article/news/local/dallas-county/hutchins-residents-celebrate-victory-ice-detention-center-tell-north-texans-could-happen-anywhere/287-d2d7a79c-2084-42a9-a28a-c3a32349d6d3)
- [Possible ICE detention center in Hutchins runs into local opposition — Axios Dallas (Feb 3, 2026)](https://www.axios.com/local/dallas/2026/02/03/hutchins-ice-detention-center-dallas-county-texas)
