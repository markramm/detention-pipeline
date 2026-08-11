---
title: "Pipeline Update: August 11, 2026"
type: blog
layout: single
date: 2026-08-11
summary: "A silent 287(g) FIPS-matching bug was dropping signals from some of the highest-heat counties in the country. Fixed it — and Broward, Orange (FL), Wayne (MI), and several Texas border counties just gained their missing signal types."
author: "Mark Ramm"
---

## What Changed

This update ships a fix, not new data: a bug in how 287(g) agreement records were matched to county FIPS codes was silently dropping signal from some of the counties we most need to see clearly. Place-name mismatches and typos in source data meant a subset of 287(g) agreements never resolved to a county at all — they just vanished from the heat model instead of erroring loudly.

The fix adds place-name and typo-tolerant fallback matching, and we've now applied it against the live knowledge base and rebuilt every heat score.

## Notable Signals

The recompute moved several already-hot counties further up the board, because they now show their true signal count:

- **Maricopa County, AZ**: 124 → 139 (+15)
- **Wayne County, MI**: 147 → 161 (+14)
- **Orange County, FL**: 157 → 169 (+12), also gained a 10th signal type
- **Hidalgo County, TX**: 122 → 134 (+12), now spans 8 signal types
- **Allegheny County, PA**: 143 → 150 (+7)

Broward County, FL remains the highest-heat county in the country at 202, now with all 10 signal types represented — it was previously showing 9.

A handful of Texas border and interior counties (Williamson, Harris, Frio, El Paso, Bexar, Webb, Cameron) each picked up their previously-missing 287(g) signal, nudging their scores up by 1 point apiece — small individually, but it confirms the fix is working as intended across the board rather than just on the highest-profile counties.

## Coverage Updates

No new counties entered the dataset this cycle — this was a correction pass, not new ingestion. 2,032 counties remain scored, same as before. The timeline (`timeline.json`) was regenerated alongside the heat scores and is back in sync — it had been stale since early July.

## Numbers

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| Counties tracked | 2,032 | 2,032 | — |
| Highest heat score | 202 | 202 | — |
| Broward Co. signal types | 9 | 10 | +1 |
| Counties with score changes (top 50) | — | 13 | — |

---

*Data current as of August 11, 2026. [Contribute what you know](https://detention-pipeline.transparencycascade.org/contribute/) — local knowledge is what converts signals into stories.*
