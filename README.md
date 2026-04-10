# Detention Pipeline Heat Map

An early-warning system for ICE detention facility expansion. Tracks the convergence of multiple independent signals — 287(g) agreements, IGSA facilities, ANC contracts, county commission activity, budget distress, and more — to identify counties likely being pitched for new detention infrastructure.

**Live map**: [View the heat map](https://markramm.github.io/detention-pipeline/) (GitHub Pages)

## What this is

Detention consultants target counties with budget distress, cooperative sheriffs, and existing infrastructure. The pitch follows a playbook: sheriff conference recruitment, closed-session presentations, NDA-protected negotiations, then a commission vote framed as fiscal salvation. By the time the public knows, the deal is nearly done.

This project aims to surface the signals early — when multiple independent indicators converge on the same county, that county is likely being pitched. Early detection enables democratic response.

## How scoring works

Each county receives a **heat score** based on:

| Signal | Weight | What it means |
|--------|--------|---------------|
| IGSA Facility | 10 | Existing federal detention agreement |
| ANC Contract | 8 | Alaska Native Corp contract in detention/security |
| 287(g) Agreement | 7 | Local law enforcement deputized for immigration enforcement |
| Commission Activity | 7 | County commission agenda, vote, or hearing on detention |
| Job Posting | 7 | Detention consultant actively recruiting for this area |
| Sheriff Network | 6 | Sheriff recruited at conference, pitching commissioners |
| Comms Discipline | 6 | Polished messaging, NDA use, or opposition framing |
| Budget Distress | 5 | County budget shortfall — vulnerability to the pitch |
| Real Estate | 2 | Warehouse or county building that could be converted |
| Legislative | 1 | State legislation to block or enable detention agreements |

A **convergence bonus** rewards signal diversity: +10 per signal type beyond 2, and +15 more for 5+ types. Three different signal types in one county is far more predictive than ten entries of the same type.

## Data sources

- **IGSA facilities**: Vera Institute of Justice (438 facilities, FIPS-coded)
- **287(g) agreements**: [Prison Policy Initiative](https://www.prisonpolicy.org/blog/2026/02/23/ice_county_collaboration/) (1,200+ agreements, compiled from ICE data Feb 2026)
- **ANC contracts**: [USAspending.gov](https://www.usaspending.gov) API
- **Commission activity**: Legistar API (county commission agendas and minutes)
- **Job postings**: Public career pages and LinkedIn
- **Other signals**: Local news, public records, Census economic data

## Repository structure

```
detention-pipeline/
├── kb/                    # Knowledge base entries (markdown + YAML frontmatter)
│   ├── kb.yaml            # Schema: entry types, fields, validation rules
│   ├── 287g/              # 287(g) agreements with local law enforcement
│   ├── anc/               # Alaska Native Corporation contract signals
│   ├── budget/            # County budget distress indicators
│   ├── commission/        # County commission activity
│   ├── comms/             # Communications discipline patterns
│   ├── jobs/              # Detention consultant job postings
│   ├── legislative/       # State legislation on detention
│   ├── real-estate/       # Properties that could be converted
│   ├── sheriff/           # Sheriff network and conference activity
│   └── scripts/           # Ingestion scripts (USAspending, 287g, Legistar)
├── docs/                  # GitHub Pages site
│   ├── index.html         # Interactive heat map viewer
│   ├── heat_data.json     # Generated scores (rebuild with scripts)
│   ├── fips_names.json    # County FIPS-to-name lookup
│   └── counties-albers-10m.json  # US county boundaries (TopoJSON)
├── LICENSE                # CC-BY-SA 4.0
└── README.md
```

## Contributing

### Submit a tip

See something? [Open an issue](../../issues/new/choose) with what you've found. Good tips include:

- County commission agenda items mentioning ICE, detention, or IGSA
- Job postings from detention consultants or contractors
- Real estate transactions involving county-owned buildings near existing jails
- Sheriff conference presentations or "federal partnership" pitches
- Local news coverage of detention facility proposals

Please include the **county and state**, a **source URL** where possible, and any relevant **dates**.

### Add data directly

KB entries are markdown files with YAML frontmatter. See `kb/kb.yaml` for the schema. Every entry needs at minimum a county, state, and FIPS code. Fork, add entries, and submit a PR.

### Rebuild the heat map

```bash
cd kb/scripts
python3 county_heat_score.py --output json --top 5000 --min-score 0 > ../../docs/heat_data.json
```

### Run ingestion scripts

```bash
# 287(g) agreements from Prison Policy Initiative
python3 kb/scripts/ingest_287g.py --output /tmp/287g.json

# ANC contracts from USAspending.gov
python3 kb/scripts/ingest_usaspending.py --output /tmp/anc.json

# County commission agendas via Legistar
python3 kb/scripts/ingest_legistar.py --output /tmp/legistar.json
```

## Limitations

This is an early-warning system, not a confirmed list. A high score means multiple independent signals converge — it does not mean a facility deal is confirmed or inevitable. False positives are expected. Some counties with active pipeline work may score low if their signals haven't been captured yet.

## License

CC-BY-SA 4.0. See [LICENSE](LICENSE).

Built by [The RAMM](https://theramm.substack.com).
