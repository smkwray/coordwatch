# CoordWatch

**[Live site](https://smkwray.github.io/coordwatch/)**

Tracking Fed balance-sheet runoff, Treasury issuance, reserves, and market intermediation.

## Question

**How do Fed balance-sheet runoff, Treasury issuance, reserves, and market intermediation move together?**

CoordWatch follows Fed runoff, Treasury issuance composition, TGA management, ON RRP drainage, dealer balance sheets, and repo spreads using free public data.

## What it measures

1. When SOMA holdings decline, how much additional duration must the private sector absorb?
2. Does Treasury's bill-vs-coupon choice ease or tighten private absorption?
3. How do TGA movements affect reserve conditions?
4. Do dealer inventories and repo conditions respond to combined Fed + Treasury effects?
5. Do buybacks act as a release valve for duration pressure?
6. How does ON RRP drainage change money-market sensitivity?
7. How do lower-buffer states differ from higher-buffer states?
8. What do official refunding statements say about bills, cash management, buybacks, and market function?
9. Do actual refunding weeks look different from quarter-specific placebo weeks?

## Data

All data from free public sources. No proprietary data required.

| Source | Provider |
|--------|----------|
| H.4.1 balance-sheet items | FRED |
| H.15 rates and repo | FRED / NY Fed |
| Z.1 sectoral Treasury holdings appendix | FRED |
| Primary dealer statistics | NY Fed Markets API |
| Quarterly refunding | Treasury |
| Refunding statement text and signal appendix | Treasury |
| Auction results and realized mix appendix | Treasury Fiscal Data |
| Daily cash and debt cross-check appendix | Treasury Fiscal Data |
| Buyback operations | TreasuryDirect |

## Quickstart

```bash
pip install -e ".[dev]"

# Synthetic demo (no network)
make offline-demo

# Real data
make mvp
```

## Pipeline

```
make download      Download from public sources
make extract       Build refunding statement index + text signals
make panel         Build quarterly + weekly panels
make episodes      Classify quarters by alignment state
make descriptive   Build regime/episode summary tables
make reaction      Treasury reaction function (OLS)
make lp            Weekly local projections
make publish       JSON artifacts for the site
make site          Build site manifest
make verify        Check all artifacts present
```

## Public structure

1. Fed runoff and liquidity buffers.
2. Debt-ceiling cash mechanics and the DTS cross-check.
3. Duration burden, dealer balance sheets, and repo spreads.
4. Treasury holders, realized auction mix, refunding-statement signals, and refunding-vs-placebo timing appendices.
5. Baseline regressions plus appendix checks for continuous liquidity, alternative repo spread, coupon-vs-bills mechanism, and refunding timing.

## Current Scope

CoordWatch is strongest as an operational-interdependence project, not a legal or constitutional independence project.

- The core repo shows how Fed runoff, Treasury issuance mix, cash management, and funding-market conditions interact in the same public-data bundle.
- The statement-signal appendix adds a coarse institutional layer from official refunding statements.
- The refunding-timing appendix compares actual refunding weeks with quarter-specific placebo weeks, but it should be read as timing evidence, not a definitive causal design on its own.

## Conventions

- **Positive Fed pressure** = more duration burden on private sector.
- **Positive buyback offset** = Treasury removes duration from private hands.
