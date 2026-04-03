# Data Sources

This file is the working registry for all source families used by CoordWatch.

## Source families

| Family | Public source | Frequency | Main fields | Script | Current status |
|---|---|---:|---|---|---|
| Treasury refunding statements | Treasury quarterly refunding pages, archives | Quarterly | issuance guidance, financing estimates, TGA assumptions, buyback guidance, debt-limit notes | `02`, `03`, `07`, `08` | downloaded; text extraction partially manual |
| TBAC materials | Treasury refunding archives / TBAC | Quarterly | recommended financing tables, buyback discussions, refinancing assumptions | `03` | downloaded |
| Buybacks | TreasuryDirect buyback results, FAQ, rules | Daily / quarterly | accepted par, operation type, bucket, tentative schedule | `04` | downloaded |
| H.4.1 via FRED | FRED API | Weekly / daily | SOMA Treasuries, reserves, TGA, ON RRP | `01`, `06` | all 12 series downloaded |
| H.15 and repo rates | FRED and NY Fed | Daily | TGCR, IORB, Treasury yields | `01`, `06` | downloaded |
| NY Fed primary dealer statistics | NY Fed markets API | Weekly | dealer Treasury positions, financing activity | `05` | downloaded; 677 weekly observations (2013–2026) via JSON history endpoint |
| NY Fed term premia (optional) | NY Fed research data page | Daily | ACM term premia and expected short rates | — | not yet automated |
| Daily cash and debt operations (optional) | Daily Treasury Statement, Debt to the Penny | Daily | operating cash balance, debt changes | — | appendix target |
| Bank and sector holdings (optional) | H.8 and Z.1 | Weekly / quarterly | securities, deposits, sectoral holdings | — | appendix target |
| Fed balance-sheet policy regime notes | Federal Reserve normalization page, NY Fed speeches | Episodic | QT start/slow/stop dates | — | reference only |

## Important practical notes

### Treasury refunding layer

Treasury's quarterly refunding pages expose the quarterly refunding process, most recent documents, archives, TBAC reports and minutes, and financing estimates by calendar year.

The index builder (`07`) captures candidate HTML and PDF documents first, then the text-extractor (`08`) and manual override files narrow the analysis to the statement layer actually used in the quarter panel.

### Primary dealer layer

Confirmed public endpoints:

- Series catalog: `https://markets.newyorkfed.org/api/pd/list/timeseries.json`
- Latest release: `https://markets.newyorkfed.org/api/pd/latest/{seriesbreak}.json`
- Full history: `https://markets.newyorkfed.org/api/pd/get/{keyid}.json`

The downloader pulls the catalog, fetches full history via the JSON endpoint, and stores results locally. 677 weekly observations from 2013 onward.

### FRED layer

The pipeline uses the FRED API JSON endpoint with a `FRED_API_KEY` (in `.env`) for reliable downloads. A public CSV fallback exists but is rate-limited.

### Optional appendix sources

- Daily Treasury Statement / Debt to the Penny for TGA validation.
- H.8 and Z.1 for bank-fragility or sector-holdings appendix.
- NY Fed term premia for a pricing appendix.

## Manual review files

- `data/manual/refunding_manual_overrides.csv` — 68 quarters populated (2009Q1–2025Q4)
- `data/manual/episode_registry_seed.csv` — episode window definitions
- `data/manual/source_verification_checklist.csv` — source verification log
