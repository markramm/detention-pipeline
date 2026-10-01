---
title: "Pipeline Update: September 30, 2026"
type: blog
layout: single
date: 2026-09-30
summary: "All five counties where DHS signed $7.3 billion in detention construction contracts on September 20 were already near the top of this map. The first data refresh since July adds 755 ICE contract records, including a $100 million GardaWorld contract to transport people arrested under 287(g) anywhere in Texas."
author: "Mark Ramm"
draft: true
---

## What Changed

This is the first new data since the [August 11 update](/blog/287g-fips-fix-update/). The weekly ingest had been failing and rolling itself back every week since mid-July: two contract IDs it pulled from USAspending were already filed elsewhere in the knowledge base, and the validator stopped the run each time. That is fixed. This refresh adds **946 entries**: **755 ICE contract awards** from USAspending, 128 of them classed as detention-related, and **191 county and city agenda items** from the commission scanner. 168 county heat scores went up. None went down.

We also fixed the scanner before running it. It had been reading "ICE" case-insensitively, so National Ice Cream Day, the Ice Age Trail and an ice-rink concession contract were being counted as immigration enforcement activity. They aren't anymore.

## Notable Signals

**The construction awards went to counties already on the map.** On September 20, DHS signed five construction IDIQs with ceilings totaling $7.32 billion: [El Paso, TX](https://www.usaspending.gov/award/362519502) and [Batavia, NY](https://www.usaspending.gov/award/362519501) to Rapid Deployment Inc.; [Florence, AZ](https://www.usaspending.gov/award/362519503) and [Miami, FL](https://www.usaspending.gov/award/362519504) to GardaWorld Federal Services; [El Centro, CA](https://www.usaspending.gov/award/362519505) to Rudiarius Holdings Corp. [Project Saltbox](https://www.projectsaltbox.com/p/ice-signs-contracts-worth-up-to-73b) identified the winners. It also traced Rudiarius, registered in Wyoming in 2025, to a Boston developer whose related company, SK2, already held an ICE contract to help acquire warehouses. Here is where those counties stood among the map's 2,032 scored counties the day the contracts were signed: [Miami-Dade](/county/12086/) 2nd, [El Paso](/county/48141/) tied for 3rd, [Pinal](/county/04021/) (Florence) tied for 11th, [Genesee](/county/36037/) (Batavia) tied for 36th, [Imperial](/county/06025/) (El Centro) tied for 42nd. USAspending shows $0 obligated on all five so far, so they don't move any scores yet. The task orders will.

**GardaWorld's other new contract.** One of the records this ingest brought in is a [$100.5 million ICE contract](https://www.usaspending.gov/award/361906644) to GardaWorld Federal Services, running from September 1, 2026 to August 31, 2027, for "24/7/365 armed ground transportation for individuals placed into ICE custody under the 287(g) program across all 254 Texas counties." Texas accounts for 277 of the 1,311 287(g) agreements in our dataset. USAspending codes the contract to Virginia, where the contractor is headquartered, so it shows up on no Texas county page. That is one way a county-level map undercounts.

**GAO on the first wave of spending.** [GAO-26-108663](https://www.gao.gov/products/gao-26-108663) (September 24): ICE bought 11 warehouses for about $1.07 billion and now plans to sell 7 of them, after more than $20 million in costs it can't recover. Florida was reimbursed through a $608 million FEMA grant at $249 per detainee per day, against ICE's $92 median. ICE says its strategic plan for the expansion will be done on August 31, 2027.

**Moshannon.** On September 22, [Clearfield County](/fights/pa-clearfield-moshannon-valley-fight/) commissioners voted 2-1 to extend the Moshannon Valley IGSA for six months ([Spotlight PA](https://www.spotlightpa.org/statecollege/2026/09/moshannon-valley-clearfield-county-pennsylvania-ice-immigration-detention/)). ICE's turn-key detention solicitation for the Philadelphia area ([RFP 70CDCR26R00000026](https://sam.gov/opp/06d1f5210673483ab27eb9b276111055/view)) includes an attachment titled ["Moshannon Enhanced Transportation"](https://sam.gov/api/prod/opps/v3/opportunities/resources/files/12b4e66b122e4ac98c302bcf57c37fbe/download). It asks for GEO Transport teams in Philadelphia, York, Pittsburgh, Williamsport, Pike County and Dover, Delaware, staged near arrest teams to act as a "mobile detention facility" until the vans are full. [NPR](https://www.npr.org/2026/09/23/nx-s1-5976875/ice-mobile-detention-facilities-immigration) first reported the van plan. As of September 29, nothing had been awarded.

## Coverage Updates

Local agenda items the scanner picked up:

- **Santa Barbara County, CA** (July 7): supervisors took a report on land-use rules for immigration enforcement facilities and detention centers. They told planning staff to watch for private developers and come back about a possible moratorium.
- **Baltimore, MD** (June 22): the City Council took up a bill making private detention centers a prohibited use anywhere in the city.
- **Galveston County, TX** (September 14): commissioners certified $1,125,000 in revenue for the sheriff's DHS ICE 287(g) Task Force.

Outside the scanner's portals: [Pierce County, WA](/fights/pierce-county-wa-detention-moratorium/) extended its detention moratorium by six months on August 25 ([Spokesman-Review](https://www.spokesman.com/stories/2026/aug/30/no-more-ice-facilities-will-be-built-in-pierce-cou/)). [Hall County, GA](/county/13139/) paused rezonings for data centers and detention centers in a single vote ([Gainesville Times](https://www.gainesvilletimes.com/news/government/hall-county-imposes-new-moratoriums-on-data-centers-detention-centers-here-are-the-details/)).

## Numbers

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| Knowledge-base entries | 18,192 | 19,138 | +946 |
| ICE contract records | 2,553 | 3,308 | +755 |
| Commission agenda items | 621 | 812 | +191 |
| 287(g) agreements | 1,311 | 1,311 | — (source snapshot dated Feb. 17) |
| Counties scored | 2,032 | 2,032 | — |
| Highest heat score | 203 | 210 | +7 ([Broward](/county/12011/), FL) |

---

*Data current as of September 30, 2026. [Contribute what you know](https://detention-pipeline.transparencycascade.org/contribute/). Local knowledge is what turns signals into stories.*
