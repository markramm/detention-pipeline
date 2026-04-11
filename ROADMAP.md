# Detention Pipeline — Roadmap

Last updated: April 2026

## What's done

- 3,450+ page static site at detention-pipeline.transparencycascade.org
- Interactive heat map (986 counties scored, click-to-navigate)
- Network visualization (revolving door + conflict matrix, 15 officials × 8 companies)
- County dossiers with auto-summaries, coverage depth badges, research toolkit
- 13 county fights with tactics playbook
- 25 contractor profiles, 19 people profiles, 11 financial flow analyses
- 495 IGSA facility directory
- FOIA request generator
- Mobile nav, schema.org, OpenGraph, JSON API endpoints
- 194 LLM-generated summaries
- GitHub issue templates for tips, corrections, methodology

## Phase 3: Remaining features

### 3.1 Organizations section (/organizations/)

Organizations (Blue Owl Capital, Goldman Sachs, Deutsche Bank, Carlyle Group, Rockefeller Group, PNK Group) currently fall through to /entry/ with no dedicated section or navigation. These are the warehouse sellers and financial institutions whose profits drive detention expansion.

**Work:**
- New Hugo templates: organizations/list.html, organizations/single.html
- Update generate_content.py to route organization type to /organizations/
- Update wikilink resolver for organization slugs
- Add to nav (or as sub-nav under Players)
- Update network generator to link from conflict matrix cells to org pages

### 3.2 Facility map overlay

IGSA facilities have FIPS codes. Overlay facility locations on the heat map as a toggleable layer — red dots for active IGSA facilities on top of the county choropleth. Users can toggle between "heat score" and "existing facilities" views, or show both.

**Work:**
- Generate facilities GeoJSON or coordinate data from FIPS codes (county centroids)
- Add toggle control to map page
- D3 circle markers for facilities, sized by bed count if available
- Click facility marker → /facilities/{id}/

### 3.3 Coverage gaps dashboard

A dedicated page showing where investigation is needed most. High-heat counties with only automated signals are the highest priority for volunteer investigators.

**Work:**
- New page at /coverage/ or enhance /contribute/
- Table: counties sorted by (heat score × inverse coverage depth) — high score + unresearched = highest priority
- Map view: toggle from "heat" to "coverage gaps" coloring (green = well-researched, yellow = partial, red = unresearched)
- Per-state breakdown: "Florida has 52 unresearched counties with signals"
- Link to FOIA generator and tip form from each row
- "Adopt a county" framing for organizers

### 3.4 Timeline visualization (/timeline/)

Chronological view showing signal accumulation over time. Shows the acceleration — 287(g) agreements exploding in mid-2025, warehouse purchase pause in early 2026, congressional investigations.

**Work:**
- Extract dates from all entries with date fields → data/timeline.json
- D3 visualization: horizontal time axis, signal-type colored dots
- Density bars showing signal volume per month
- Filter by signal type, state
- Click event → entry page
- Dark theme consistent with map and network views

### 3.5 Pipeline narrative (/pipeline/)

Scrollytelling page explaining the 7-stage playbook with real county examples.

**Content to write (not auto-generated):**
1. Targeting — 287(g), budget distress, who gets pitched
2. Recruitment — sheriff conferences, Sabot pitch deck
3. Pitch — closed sessions, NDA-protected presentations
4. Negotiation — consultant contracts, ANC sole-source
5. Vote — commission votes, community response
6. Construction — warehouse purchases, real estate traces
7. Operation — IGSA activation, facility conditions

Each stage cites real entries from the KB as examples. Bradford FL is the case study that went through every stage.

**Work:**
- CSS sticky positioning + Intersection Observer for scroll effects
- 7 content sections with stage diagrams
- Links to relevant entries at each stage

### 3.6 Resources section (/resources/)

External watchdog feeds, legal resources, FOIA templates, reporting guides.

**Work:**
- New KB entry type: resource-link (url, resource_type, geographic_scope)
- Seed with 15-20 entries: EyesOnICE, Freedom for Immigrants, NIJC, ACLU state affiliates, bond funds, know-your-rights guides, TRAC Syracuse, The Markup tracker
- New Hugo templates for resource listing
- Wire resources into county pages (national resources on all pages, state-level on state pages)

### 3.7 The RAMM integration

Prominent link to the investigative reporting that underpins this data. The RAMM's detention series provides the narrative context the site's data supports.

**Work:**
- Add "The Investigation" or "Read the reporting" section to homepage linking to https://theramm.substack.com/p/the-detention-architecture-an-investigation
- Add RAMM link to methodology page data sources
- Add RAMM link to Bradford county fight page (the story broke there)
- Consider a sidebar or banner on fight/player pages: "This research is published at The RAMM"
- Footer already links to The RAMM — make it more prominent

## Phase 4: Polish and infrastructure

### 4.1 Revolving door filter toggles

Toggle buttons on left (government agencies) and right (companies) of the revolving door diagram. Click to dim/highlight specific nodes and their connections. Animate the center column repositioning.

### 4.2 Facilities search/filter

The 495-row facility table needs client-side search and state filter. Simple JS: input field filters table rows by text match.

### 4.3 Reporter/outlet tracking

Add optional reporter, outlet fields to human-sourced entry types. Build an index of who covers detention where. Show on state and county pages.

### 4.4 Structured timeline events in fights

Convert flat markdown timelines in fight pages to structured data (date, actor, action, source). Enables cross-fight timeline, "what happened this week" digests, and auto-surfacing next meeting dates.

### 4.5 Single-source flagging

Source count badge already exists on entry pages. Extend: entries with source_count ≤ 1 get a "needs corroboration" call-to-action linking to the tip form.

### 4.6 Capture Cascade cross-links

Enrich entries that reference dateable events with cascade_url field pointing to capturecascade.org timeline events. Template support already built — just needs editorial enrichment of individual entries.

## Priority order

1. **3.7 RAMM integration** — quick, high impact for credibility
2. **3.1 Organizations section** — fixes 404s for organization wikilinks
3. **3.3 Coverage gaps dashboard** — highest activation value for organizers
4. **3.2 Facility map overlay** — extends the prestige visualization
5. **4.2 Facilities search** — usability fix
6. **3.4 Timeline** — new prestige visualization
7. **3.6 Resources** — content work
8. **4.1 Revolving door filters** — polish
9. **3.5 Pipeline narrative** — requires significant writing
10. Everything else
