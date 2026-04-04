# CoordWatch

**[Live site](https://smkwray.github.io/coordwatch/)**

Tracking how Fed runoff, Treasury issuance, the TGA, ON RRP, and private intermediation interact under ample reserves.

## Question

**How do Fed runoff, Treasury debt management, and liquidity conditions combine under an ample-reserves regime?**

The effective stance of monetary conditions depends jointly on Fed balance-sheet runoff, Treasury issuance composition, TGA management, ON RRP drainage, and private intermediation capacity. CoordWatch follows the balance sheets using free public data.

## What it measures

1. When SOMA holdings decline, how much additional duration must the private sector absorb?
2. Does Treasury's bill-vs-coupon choice ease or tighten private absorption?
3. How do TGA movements affect reserve conditions?
4. Do dealer inventories and repo conditions respond to combined Fed + Treasury effects?
5. Do buybacks act as a release valve for duration pressure?
6. How does ON RRP drainage change money-market sensitivity?
7. How do lower-buffer states differ from higher-buffer states?

Descriptive evidence is the core analysis. The site is organized around reserve drainage, buffers, cash mechanics, duration burden, intermediation, sectoral absorption, and realized issuance. Light econometrics appear only as supplementary checks.

## Data

All data from free public sources. No proprietary data required.

| Source | Provider |
|--------|----------|
| H.4.1 balance-sheet items | FRED |
| H.15 rates and repo | FRED / NY Fed |
| Z.1 sectoral Treasury holdings appendix | FRED |
| Primary dealer statistics | NY Fed Markets API |
| Quarterly refunding | Treasury |
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

1. Drainage and buffer conditions.
2. Debt-ceiling cash mechanics and the DTS cross-check.
3. Duration burden, dealer absorption, and repo intermediation.
4. Sectoral absorption and realized auction mix appendices.
5. Supplementary econometric checks.

## Conventions

- **Positive Fed pressure** = more duration burden on private sector.
- **Positive buyback offset** = Treasury removes duration from private hands.
- **Motive-neutral language.** Measures balance-sheet interactions and market effects; does not infer intent.
