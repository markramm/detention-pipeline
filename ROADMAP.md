# Detention Pipeline — Roadmap

Last updated: April 2026

## What's built

- **4,500+ page static site** at detention-pipeline.transparencycascade.org
- **Interactive heat map** — 986 counties scored by signal convergence, click-to-navigate, toggleable IGSA facility overlay (yellow dots)
- **Coverage gaps dashboard** — D3 choropleth showing unresearched counties, state breakdown, priority table with "Adopt a County" framing
- **Pipeline narrative** — 7-section scrollytelling page tracing the detention pipeline from 287(g) demand generation through the IGSA pivot
- **Network visualization** — revolving door diagram + conflict matrix (15 officials × 8 companies), with filter toggles
- **Timeline visualization** — chronological view of signal accumulation with type/state filtering
- **County dossiers** — auto-summaries, coverage depth badges, per-county research guides showing which signal types are missing
- **State pages** — D3 choropleth maps per state showing county heat scores, data-rich state cards
- **13 county fights** with tactics playbook
- **25 contractor profiles**, 19 people profiles, 6 organization profiles, 11 financial flow analyses
- **495 IGSA facility directory** with search/filter
- **Resources section** — external watchdog feeds, legal resources
- **Data sources page** — automation status per signal type, coverage gaps
- **Developer guide** — architecture, entry format, highest-impact contributions
- **FOIA request generator**
- **RAMM integration** — homepage, methodology, fight pages, footer
- **5 ingestion scripts** — 287(g), ANC contracts, Legistar commission scanning (30 portals), budget distress (USDA ERS), job postings
- **Central pipeline runner** (`run_ingest.sh`) orchestrating all ingestion + scoring + site rebuild

## Backlog

Tracked as GitHub issues: https://github.com/markramm/detention-pipeline/issues

### High-impact data sources (help wanted)
- [#1](https://github.com/markramm/detention-pipeline/issues/1) Enrich facility data with bed counts from TRAC/Vera
- [#2](https://github.com/markramm/detention-pipeline/issues/2) County commission scrapers for non-Legistar counties
- [#3](https://github.com/markramm/detention-pipeline/issues/3) State legislature bill tracker (OpenStates/LegiScan)
- [#4](https://github.com/markramm/detention-pipeline/issues/4) Job board monitoring for detention consultants
- [#5](https://github.com/markramm/detention-pipeline/issues/5) Commercial real estate monitor
- [#6](https://github.com/markramm/detention-pipeline/issues/6) Local news monitoring

### Infrastructure
- [#8](https://github.com/markramm/detention-pipeline/issues/8) Expand Legistar coverage
- [#12](https://github.com/markramm/detention-pipeline/issues/12) Async Legistar scanning

### Content & editorial
- [#7](https://github.com/markramm/detention-pipeline/issues/7) Improve timeline data coverage
- [#9](https://github.com/markramm/detention-pipeline/issues/9) Reporter/outlet tracking
- [#10](https://github.com/markramm/detention-pipeline/issues/10) Capture Cascade cross-links
- [#11](https://github.com/markramm/detention-pipeline/issues/11) Editorial review of /pipeline/ narrative

### Launch
- [#13](https://github.com/markramm/detention-pipeline/issues/13) HN/Reddit announcement and contributor recruitment
