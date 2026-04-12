# Detention Pipeline

Early-warning system for ICE detention facility expansion. Tracks signal convergence across US counties.

## Architecture

- **Hugo static site** at `hugo/` — builds and deploys via GitHub Actions on push to main
- **Knowledge base** at `kb/` — markdown entries with YAML frontmatter, organized by signal type
- **Ingestion scripts** at `kb/scripts/` — pull data from USAspending, Vera, Prison Policy Initiative, Legistar, etc.
- **Build pipeline** — `run_ingest.sh` (data) → `county_heat_score.py` (scoring) → `hugo` (site)
- **Site URL**: https://detention-pipeline.transparencycascade.org/

## Key Paths

- `hugo/hugo.toml` — Hugo config (baseURL, taxonomies)
- `hugo/layouts/` — all templates (baseof.html has the CSS and nav)
- `hugo/data/heat.json` — county heat scores (generated)
- `hugo/data/timeline.json` — timeline events (generated)
- `hugo/static/timeline.json` — copy for client-side fetch
- `kb/kb.yaml` — KB schema
- `kb/scripts/county_heat_score.py` — the scoring engine
- `kb/data/legistar_map.json` — validated Legistar portal mapping (CC0 public data)

## Conventions

- CSS is embedded in `baseof.html`, not external stylesheets
- D3.js visualizations load JSON from `/static/` via client-side fetch
- Signal colors are defined as CSS variables `--signal-*` in baseof.html
- Entry frontmatter must include `entry_type`, `fips`, `state`, `county`
- Pre-commit hooks validate entries via `kb/scripts/validate_entries.py`
- Blog posts go in `hugo/content/blog/` with type `blog`
- `docs/` is gitignored — site is built by GitHub Actions, not committed

## Data Update Workflow

Use the `/pipeline-update` skill for the full ingest → diff → blog post → deploy cycle.
Quick update: `./run_ingest.sh && cd kb/scripts && python3 county_heat_score.py && cd ../../hugo && hugo --quiet`

## Related Projects

- **tcp-kb-internal** — private research KBs (detention-industrial, cascade-timeline, etc.)
- **Pyrite** at `/Users/markr/pyrite` — KB management CLI
- **The RAMM** — the investigative journalism at theramm.substack.com
