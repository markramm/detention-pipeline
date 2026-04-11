---
title: "SEC Filing Analysis — Fundrise East Coast REIT and RREEF Core Plus Industrial Fund"
entry_type: "analysis"
signal_label: "Analysis"
signal_color: "#5a6a8a"
fips: ""
state: ""
county: ""
repo_path: "kb/industry/financial-flows/sec-filings-fundrise-rreef.md"
lastmod: "2026-04-08"
summary: "SEC filing forensics on Fundrise (Williamsport MD) and RREEF/Deutsche Bank (SLC UT) warehouse ownership chains. Fundrise investors lost 33% while property was sold to DHS."
type: "players"
layout: "single"
player_type: "money"
---

## Fundrise East Coast Opportunistic REIT, LLC (CIK 1660918)

### The Williamsport/Hagerstown Acquisition (June 2022)

From Form 1-U filed June 17, 2022:
- Acquired through subsidiary **FRIND-Hopewell, LLC**
- Property: ~825,620 sq ft on 54 acres at **10900 Hopewell Road, Williamsport, MD**
- **Purchase price: ~$104,854,000** ($127/sq ft)
- **100% VACANT at acquisition** — received Temporary Certificate of Occupancy June 2022
- Fundrise equity: ~$53.3M
- PIMCO bridge loan: ~$56.4M allocated (part of $95M two-property facility)
- Budgeted $9.05M for leasing commissions/tenant improvements
- Underwriting: 10-year projected hold, 6% rent growth, 4.50% exit cap rate

### The Refinancing (June 2025)

From 1-SA filed September 18, 2025:
- On **June 26, 2025**, Fundrise refinanced with a **syndicated warehouse loan of up to $352.7M**
- **The SEC filing does NOT name Goldman Sachs or TPG as lenders** — only "syndicated warehouse loan"
- Goldman/TPG identity comes from JLL press release and Commercial Observer, not SEC filings
- Prior PIMCO facility ($95M) repaid in full
- New terms: SOFR + 3.15%, interest-only, matures June 26, 2027
- East Coast share: $87.03M (Hagerstown + E66 Springfield VA combined)

### The DHS Sale Signal (Q1 2026)

From quarterly NAV filings:

| Date | Investments at FV | Total Assets | NAV/Share |
|------|-------------------|-------------|-----------|
| Dec 31, 2025 | $130.0M | $140.2M | $10.74 |
| **Apr 1, 2026** | **$80.5M** | **$99.8M** | **$10.21** |

**The $49.5M drop in investment value** between Dec 31 and April 1 is consistent with the Williamsport property sale to DHS (January 2026, $102.4M). The related-party note payable also dropped from $49M to $22.2M.

**No filing explicitly mentions DHS, ICE, or a government buyer.** The FY2025 1-K annual report (expected April-May 2026) should disclose the sale.

### NAV Decline — Investor Losses

| Date | NAV/Share |
|------|-----------|
| Jun 2022 (peak, post-acquisition) | $15.33 |
| Dec 2022 | $14.74 |
| Dec 2023 | $12.72 |
| Dec 2024 | $11.09 |
| Apr 2026 | **$10.21** |

**33% decline from peak.** Fundrise investors lost a third of their value — the "liability" language in earlier filings was accurate.

### Proposed Merger and Suspended Redemptions

From October 10, 2025 1-U: Fundrise closed its Regulation A offering and suspended redemptions "in advance of a proposed merger." No merger party or terms disclosed.

---

## RREEF Core Plus Industrial Fund L.P. (CIK 1707229)

### The SLC Warehouse Parent Fund

Corporate structure confirmed from SEC filings:
```
DWS Group (Deutsche Bank 79.49%)
  → RREEF America L.L.C. (Investment Manager, CRD 109596)
     → RREEF Core Plus Industrial Fund L.P. (CIK 1707229)
        → RREEF Core Plus Industrial GP L.L.C.
           → RREEF Core Plus Industrial Lower Fund II L.P.
              → RREEF Core Plus Industrial REIT L.L.C. (CIK 1726248)
                 → RREEF CPIF 6020 W 300 S LLC (property-level, SLC)
```

### Fund Size (Form D amendments):

| Year | Total Sold | Investors |
|------|-----------|-----------|
| 2018 | $463.6M | 35 |
| 2019 | $763.1M | 41 |
| 2020 | $986.7M | 44 |
| 2021 | $1.535B | 50 |
| 2022 | $2.274B | 59 |
| 2023 | $2.329B | 61 |
| 2024 | $2.351B | 61 |
| **2025** | **$2.371B** | **61** |

**Fund is NOT liquidated** as of May 2025 — still filing D/A amendments. Growth stopped after 2022. Investor count plateaued at 61 since 2023. Fund appears to be in wind-down/harvest mode. PitchBook "liquidated" status may be incorrect.

### Key Personnel (from filings)
- W. Todd Henderson (Manager/Director)
- Timothy K. Gonzalez
- Lenore M. Sullivan
- Ryan Schroeder
- Kevin Howlely (San Francisco — Portfolio Manager)
- Darrell Campos (San Francisco — Portfolio Manager)

### What the SEC Filings Don't Show
- No property-level transactions disclosed (Form D filings are offering notices only)
- No mention of SLC warehouse sale to DHS
- The DWS Group 20-F (foreign private issuer annual report) would be the place to find property-level disclosures

---

## Key Filing URLs for Manual Review

### Fundrise
- All filings: sec.gov/cgi-bin/browse-edgar?CIK=0001660918
- 2022 acquisition 1-U: sec.gov/Archives/edgar/data/1660918/000110465922074423/tm2219441d1_1u.htm
- 2025 1-SA (refinancing): sec.gov/Archives/edgar/data/1660918/000110465925091175/tm2525946d1_1sa.htm
- April 2026 1-U (most recent NAV): sec.gov/Archives/edgar/data/1660918/000110465926038630/tm2610961d1_1u.htm

### RREEF
- RREEF Core Plus Industrial Fund LP: sec.gov Form D filings under CIK 1707229
- RREEF Core Plus Industrial REIT LLC: sec.gov Form D under CIK 1726248
- RREEF Property Trust (separate vehicle): rreefpropertytrust.com/Investor-Relations/SEC-Filings/
