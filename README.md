# Detention Pipeline

An early-warning system for ICE detention facility expansion. Tracks the convergence of multiple independent signals — 287(g) agreements, IGSA facilities, ANC contracts, ICE contracts, county commission activity, budget distress, and more — to identify counties likely being pitched for new detention infrastructure.

**Live site**: [detention-pipeline.transparencycascade.org](https://detention-pipeline.transparencycascade.org/)

## What this is

Detention consultants target counties with budget distress, cooperative sheriffs, and existing infrastructure. The pitch follows a playbook: sheriff conference recruitment, closed-session presentations, NDA-protected negotiations, then a commission vote framed as fiscal salvation. By the time the public knows, the deal is nearly done.

This project surfaces the signals early — when multiple independent indicators converge on the same county, that county is likely being pitched. Early detection enables democratic response.

The site serves four audiences:
- **Street-level activists** — "My county is being pitched. What do I do right now?"
- **Researcher-activists** — systematic investigation, data contribution, methodology
- **Journalists** — leads, sources, verified data for reporting
- **Lawmakers/policy staff** — aggregate data, system-level analysis

## How scoring works

Each county receives a **heat score** based on weighted signals:

| Signal | Weight | Cap | What it means |
|--------|--------|-----|---------------|
| IGSA Facility | 10 | 5 | Existing federal detention agreement |
| ANC Contract | 8 | 3 | Alaska Native Corp sole-source contract in detention |
| 287(g) Agreement | 7 | 3 | Local law enforcement deputized for immigration enforcement |
| Commission Activity | 7 | 5 | County commission agenda, vote, or hearing on detention |
| Job Posting | 7 | 3 | Detention consultant actively recruiting for this area |
| Sheriff Network | 6 | 3 | Sheriff recruited at conference, pitching commissioners |
| Comms Discipline | 6 | 3 | Polished messaging, NDA use, or opposition framing |
| Budget Distress | 5 | 2 | County budget shortfall — vulnerability to the pitch |
| Real Estate | 2 | 2 | Warehouse or county building that could be converted |
| Legislative | 1 | 2 | State legislation to block or enable detention agreements |

A **convergence bonus** rewards signal diversity: +10 per signal type beyond 2, and +15 more for 5+ types. Three different signal types in one county is far more predictive than ten entries of the same type.

## Data sources

| Source | Signal type | Count | Notes |
|--------|------------|-------|-------|
| [Vera Institute of Justice](https://www.vera.org) | IGSA Facilities | 1,189 | Full facility dataset with operators, bed counts, status |
| [Prison Policy Initiative](https://www.prisonpolicy.org/blog/2026/02/23/ice_county_collaboration/) | 287(g) Agreements | 1,311 | All three models: TFM, JEM, WSO |
| [USAspending.gov](https://www.usaspending.gov) | ANC Contracts | 244 | Alaska Native Corp sole-source contracts |
| [USAspending.gov](https://www.usaspending.gov) | ICE Contracts | 2,430 | All ICE/ERO contract awards |
| [USDA ERS](https://www.ers.usda.gov) | Budget Distress | 1,090 | County typology codes + specific shortfall data |
| [Legistar API](https://webapi.legistar.com) | Commission Activity | 61 portals | County commission agendas and minutes (61 validated portals) |
| Public career pages, LinkedIn | Job Postings | ongoing | Sabot Consulting and other detention consultants |
| Local news, public records | Other signals | ongoing | Real estate, sheriff network, comms, legislative |

## Site features

- **Interactive heat map** — 1,988 counties scored, zoomable, with facility overlay
- **Timeline visualization** — D3 stacked bar chart with multi-select filters, state filter, dynamic daily/weekly/monthly resolution
- **Network visualization** — revolving door diagram + conflict-of-interest matrix with clickable node filtering
- **Playbook & Counter-Playbook** — 10 consultant tactics + 9 counter-tactics from 13 documented fights
- **FOIA request generator** — ready-to-send public records requests
- **Coverage gaps dashboard** — where investigation is most needed
- **Resources directory** — 39 external resources across 8 categories
- **County dossiers** — per-county signal breakdown, research guide, corroboration CTAs
- **Start Here** — 5-step action guide for communities facing a detention pitch

## Repository structure

```
detention-pipeline/
├── .github/
│   └── workflows/
│       └── deploy.yml         # GitHub Actions: Hugo build + GitHub Pages deploy
├── hugo/                      # Hugo site source
│   ├── hugo.toml              # Hugo configuration
│   ├── content/               # 6,000+ content pages (counties, entries, fights, etc.)
│   ├── layouts/               # Hugo templates (30+ HTML files)
│   ├── data/                  # Site data (heat scores, network, timeline, signals)
│   ├── static/                # Static assets (JSON data files, CNAME)
│   └── scripts/               # Site build scripts (timeline generation)
├── kb/                        # Knowledge base source (markdown + YAML frontmatter)
│   ├── kb.yaml                # Schema: entry types, fields, validation rules
│   ├── county_heat_scores.csv # Pre-computed heat scores with Legistar client IDs
│   ├── 287g/                  # 287(g) agreements (1,311 entries)
│   ├── anc/                   # Alaska Native Corporation contracts (244 entries)
│   ├── budget/                # County budget distress indicators (1,090 entries)
│   ├── commission/            # County commission activity
│   ├── comms/                 # Communications discipline patterns
│   ├── data/                  # Reference data (census FIPS, Legistar map)
│   ├── facilities/            # IGSA facilities (1,189 entries from Vera)
│   ├── ice-contracts/         # ICE contract awards (2,430 entries)
│   ├── industry/              # Detention industry profiles
│   │   ├── contractors/       # 25 contractor profiles
│   │   ├── contracts/         # 16 specific contract analyses
│   │   ├── county-fights/     # 13 documented community fights
│   │   ├── facilities/        # 36 researched facility deep-dives
│   │   ├── financial-flows/   # 11 financial flow analyses
│   │   ├── notes/             # 10 research notes and analyses
│   │   ├── organizations/     # 6 financial institution profiles
│   │   └── people/            # 19 revolving-door personnel profiles
│   ├── jobs/                  # Detention consultant job postings
│   ├── legislative/           # State legislation on detention
│   ├── real-estate/           # Properties that could be converted
│   ├── sheriff/               # Sheriff network and conference activity
│   └── scripts/               # Ingestion and validation scripts
├── build.sh                   # Full build pipeline
├── run_ingest.sh              # Data ingestion runner
├── .pre-commit-config.yaml    # Entry validation hooks
├── ROADMAP.md                 # Project roadmap
├── LICENSE                    # CC-BY-SA 4.0
└── README.md
```

## Building and contributing

### Full build

The site is built with [Hugo](https://gohugo.io/) and deployed via GitHub Actions. Pushing to `main` triggers an automatic build and deploy to GitHub Pages.

```bash
# Full build pipeline (ingestion + Hugo build)
./build.sh

# Or just rebuild the Hugo site locally
cd hugo && hugo server
```

### Run ingestion scripts

```bash
# Run all ingestion scripts
./run_ingest.sh

# Or run individual scripts:
cd kb/scripts

# 287(g) agreements from Prison Policy Initiative
python3 ingest_287g.py

# ANC contracts from USAspending.gov
python3 ingest_usaspending.py

# ICE contract awards from USAspending.gov
python3 ingest_ice_contracts.py

# IGSA facilities from Vera Institute
python3 ingest_vera_facilities.py

# Enrich facility data with bed counts, operators
python3 enrich_facilities.py

# Budget distress indicators from USDA ERS
python3 ingest_budget_distress.py

# County commission agendas via Legistar API
python3 ingest_legistar.py

# Discover new Legistar portals (brute-force + web search)
python3 discover_legistar.py --discover --census-file ../data/census_counties.csv

# Job postings from detention consultant career pages
python3 ingest_jobs.py

# Validate all KB entries against schema
python3 validate_entries.py
```

### Pre-commit validation

A pre-commit hook validates entry frontmatter on every commit:

```bash
# Install the hook
pip install pre-commit
pre-commit install

# Runs automatically on git commit — validates YAML frontmatter,
# required fields, FIPS codes, and signal type consistency
```

### Submit a tip

[Open an issue](../../issues/new/choose) with what you've found. Good tips include:

- County commission agenda items mentioning ICE, detention, or IGSA
- Job postings from detention consultants or contractors
- Real estate transactions involving warehouses near existing jails
- Sheriff conference presentations or "federal partnership" pitches
- Local news coverage of detention facility proposals

Please include the **county and state**, a **source URL** where possible, and any relevant **dates**.

### Add data directly

KB entries are markdown files with YAML frontmatter. See `kb/kb.yaml` for the schema.

Example entry (`kb/287g/287-g-tfm-henry-county-al.md`):
```yaml
---
id: 287-g-tfm-henry-county-al
title: "287(g) TFM: Henry County Sheriff's Office (AL)"
type: 287g-agreement
fips: "01067"
state: "AL"
county: "Henry County"
signal_strength: "strong"
source: "ICE 287(g) MOA database"
tags:
- 287g
- al
---

287(g) agreement between ICE and Henry County Sheriff's Office.

Model: TFM
Signed: December 11, 2025
```

Fork the repo, add entries, and submit a PR. The pre-commit hook will validate your entries automatically.

## Limitations

This is an early-warning system, not a confirmed list. A high score means multiple independent signals converge — it does not mean a facility deal is confirmed or inevitable. False positives are expected. Some counties with active pipeline work may score low if their signals haven't been captured yet.

## Related projects

- [The RAMM](https://theramm.substack.com) — the investigative journalism that underpins this data
- [Project Salt Box](https://www.projectsaltbox.com) — citizen OSINT tracking ICE warehouse acquisitions
- [ICE Warehouse Resistance Network (IWRNN)](https://iwrnn.org) — community organizing against detention expansion
- [Freedom for Immigrants](https://www.freedomforimmigrants.org) — detention monitoring and bond fund network
- [TRAC Immigration](https://trac.syr.edu/immigration/) — government-sourced detention and enforcement data

## License

CC-BY-SA 4.0. See [LICENSE](LICENSE).

Built by [The RAMM](https://theramm.substack.com) · [detention-pipeline.transparencycascade.org](https://detention-pipeline.transparencycascade.org/)
