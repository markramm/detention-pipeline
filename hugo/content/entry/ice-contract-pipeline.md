---
title: "The ICE Contract Pipeline — From Targeting to Deportation"
entry_type: "note"
signal_label: "Research Note"
signal_color: "#6a6a6a"
fips: ""
state: ""
county: ""
repo_path: "kb/industry/notes/ice-contract-pipeline.md"
type: "entry"
layout: "single"
---

# The ICE Contract Pipeline

The immigration enforcement system is not one contract or one company. It is a **pipeline** in which each stage is served by different contractors, each taking their cut. A person can move through the entire system — identified, tracked, located, arrested, transported, detained, monitored, and deported — without the federal government operating any stage directly.

## Stage 1: Identification and Targeting

**What happens:** Data brokers and surveillance platforms identify enforcement targets — people with deportation orders, people who missed court dates, people who match enforcement priorities.

**Key contractors:**
- [[palantir-technologies]] — ImmigrationOS (formerly FALCON), ICM, EAGLE. The backbone. Integrates data from multiple sources into targeting lists.
- [[thomson-reuters]] — Utility records, property records, phone records. Lets ICE find people by their utility connections.
- [[lexisnexis]] — Similar data brokering. Address histories, associates, phone numbers.
- Babel Street — Location data from mobile devices, social media monitoring.
- Clearview AI — Facial recognition matching.
- Vigilant Solutions (Motorola) — License plate reader network. Vehicle tracking.

**Revenue model:** Subscription licenses, per-query fees, platform access fees.

## Stage 2: Location and Skip-Tracing

**What happens:** Once a target is identified, skip-tracing contractors locate them physically.

**Key contractors:**
- [[bi-incorporated]] (GEO subsidiary) — $121M skip-tracing contract. Also runs the ISAP ankle monitor program.
- Various bounty hunting and investigation firms — contracted to find people with outstanding deportation orders.

**Revenue model:** Per-case bounties, bulk contracts.

## Stage 3: Arrest and Transport

**What happens:** ICE ERO officers (or in some cases local law enforcement under 287(g) agreements) make arrests. Private contractors transport detainees to processing facilities.

**Key contractors:**
- MVM Inc — Long-standing ICE transportation contractor.
- G4S / Allied Universal — Security and transport.
- Local sheriff's offices under 287(g) agreements.

**Revenue model:** Per-trip, per-mile, per-detainee transport contracts.

## Stage 4: Processing

**What happens:** Detainees are processed through intake at regional processing centers (3-7 day stays). Biometrics collected, case files opened in ICM/Palantir, initial custody determinations made.

**Key facilities:** 16 planned processing centers (1,000-1,500 beds each) under the Detention Reengineering Initiative.

## Stage 5: Detention

**What happens:** Detainees held pending removal proceedings, appeals, or deportation logistics. This is where the per-bed-day revenue model operates.

**Key contractors:**
- [[geo-group]] — Largest operator. $2.6B revenue, $254M profit (2025).
- [[corecivic]] — Second largest. $2.2B revenue (2025).
- [[mtc]] — Third largest (privately held).
- [[sabot-consulting]] — Advises sheriffs on county-led IGSA facilities.
- County sheriff's offices — Operate IGSA facilities directly.

**Revenue model:** Per-bed-day ($90-$269 depending on facility type and location). Empty beds = lost revenue.

## Stage 6: Monitoring (Alternative to Detention)

**What happens:** Some detainees released under supervision with ankle monitors, GPS tracking, or app-based check-ins.

**Key contractors:**
- [[bi-incorporated]] (GEO subsidiary) — ISAP program. SmartLINK app collects biometrics, geolocation, contact data. Target: 450K people under surveillance by 2026.

**Revenue model:** Per-participant monitoring fees. Revenue scales with population under surveillance.

## Stage 7: Deportation

**What happens:** Detainees moved to staging facilities, put on charter flights, deported.

**Key contractors:**
- Charter airlines (GlobalX, Swift Air/iAero, World Atlantic, CSI Aviation)
- [[alexandria-la-staging-facility]] — GEO-operated staging hub on former military airstrip.
- ICE Air Operations — Five hubs: San Antonio, Brownsville, Alexandria LA, Miami, Mesa AZ.

**Revenue model:** Per-flight charter costs. 2,253 deportation flights in Year 1 (46% increase). 9,066 domestic transfer flights (132% increase).

## The Structural Point

Each stage is a profit center. Each contractor has a financial interest in the volume of people flowing through the pipeline. The data brokers profit from more queries. The skip-tracers profit from more cases. The transporters profit from more trips. The detention operators profit from more beds filled. The airlines profit from more flights.

**But one company sees the whole system.** [[geo-group|GEO Group]] doesn't just operate one stage — through its subsidiary [[bi-incorporated|BI Incorporated]], it operates the **closed loop**: surveillance (SmartLINK app, 182,000 people monitored via GPS and facial recognition), location ($121M skip-tracing contract — bounty hunting using the same surveillance data), and detention (95 facilities, 75,000 beds). The same company that tracks you finds you when you miss a check-in and profits from holding you when you're caught. See [The Closed Loop](https://theramm.substack.com/p/the-closed-loop) for the full investigation.

The more people surveilled, the more flagged for non-compliance. The more flagged, the more hunted. The more hunted, the more detained. The more detained, the more revenue. Growth is built into the model. GEO told investors their subsidiary could "scale up its monitoring contract to serve **millions** of people for ICE."

The other contractors — Palantir (targeting), the charter airlines (deportation), the data brokers (identification) — operate specific stages. But GEO's vertical integration means it captures revenue at every point: $4.12/person/day for monitoring, bounty incentives for finding, $150-200/person/day for detaining. One company. All three functions. $2.63 billion in 2025 revenue.

This is why the KB tracks the full pipeline, not just detention. The facility is where the person ends up. The contracts are how they got there. And the closed loop is what happens when one company controls the entire journey.
