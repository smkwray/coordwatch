# CoordWatch

Measuring operational interdependence between Federal Reserve balance-sheet policy and Treasury debt management.

## Question

**Under an ample-reserves regime, does Treasury debt management partly offset or amplify the effects of Fed QT?**

The effective stance of monetary conditions depends jointly on Fed balance-sheet runoff, Treasury issuance composition, TGA management, and private intermediation capacity. CoordWatch follows the balance sheets using free public data.

## What it measures

1. When SOMA holdings decline, how much additional duration must the private sector absorb?
2. Does Treasury's bill-vs-coupon choice ease or tighten private absorption?
3. How do TGA movements affect reserve conditions?
4. Do dealer inventories and repo conditions respond to combined Fed + Treasury effects?
5. Do buybacks act as a release valve for duration pressure?
6. How does ON RRP drainage change money-market sensitivity?
7. Does the Fed-Treasury interaction tighten when liquidity is low?

Descriptive evidence is the core analysis. Light econometrics (reaction function, local projections) corroborate.

## Data

All data from free public sources. No proprietary data required.

| Source | Provider |
|--------|----------|
| H.4.1 balance-sheet items | FRED |
| H.15 rates and repo | FRED / NY Fed |
| Primary dealer statistics | NY Fed Markets API |
| Quarterly refunding | Treasury |
| Auction results | Treasury Fiscal Data |
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

## Conventions

- **Positive Fed pressure** = more duration burden on private sector.
- **Positive buyback offset** = Treasury removes duration from private hands.
- **Motive-neutral language.** Measures operational alignment; does not claim coordination.
