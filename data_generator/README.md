# Data Generator

## What problem does this solve?

Before we can build a pipeline, we need data. In a real FMCG company, a Point-of-Sale system generates thousands of sales transactions every day. We don't have that system here — so `generate.py` simulates it.

Every day this script produces four CSV files that represent what a real retailer would export from their POS:

| File | What it represents |
|---|---|
| `products_YYYYMMDD.csv` | Product catalogue (50 products across 6 FMCG categories) |
| `stores_YYYYMMDD.csv` | Store directory (10 stores across Australian states) |
| `customers_YYYYMMDD.csv` | Customer registry (200 customers with loyalty tiers) |
| `fact_sales_YYYYMMDD.csv` | Daily sales transactions (500 rows, one row per sale) |

These files land in `data_generator/output/` locally. In production, Lambda 1 calls this script and uploads the files directly to S3.

---

## How the data is structured (star schema)

The four files map directly to the star schema in RDS:

```
fact_sales
  ├── product_id  →  dim_product
  ├── store_id    →  dim_store
  └── customer_id →  dim_customer
```

`fact_sales` is the centre — every row is **one transaction: one customer buying one product at one store**. The dimension files describe *who*, *where*, and *what*.

---

## The two types of data: master data vs daily data

This is the most important concept in the script.

**Master data (dimensions)** — generated with `random.Random(42)` (a dedicated seeded instance) so the output is *identical every time*. Your 50 products, 10 stores, and 200 customers are always the same people and places. This is intentional: dimensions represent entities that exist in the real world and should be consistent across pipeline runs.

**Daily data (fact sales)** — generated with `random.seed(None)` so the output is *different every time*. Each day's 500 transactions are a fresh slice of activity. This mirrors reality — the store catalogue stays the same, but today's sales are new.

### Why isolated `random.Random` instances matter

Each generator (`generate_products`, `generate_stores`, `generate_customers`) receives its own `random.Random(42)` instance. This means:

- Adding a new field to `generate_customers` will never change which products `generate_products` picks
- SCD changes on Mondays always target the same products/stores regardless of what else changes in the code
- Fact generation is completely isolated — `random.seed(None)` only affects the global `random` module, not the dimension `rng` instances

---

## SCD Type 2: simulating dimension changes

In the real world, things change. A product gets repriced. A store is reclassified from Metro to Regional. When that happens, we don't want to overwrite the history — we want to track *what was true at the time of each sale*.

This is called **Slowly Changing Dimension Type 2 (SCD Type 2)**, and dbt handles it using snapshots.

The generator triggers these changes automatically every Monday:
- **2 random products** get a new `unit_price` and `unit_cost`
- **1 random store** gets a new `region`

When dbt runs on Monday and sees those changed values, it closes the old record (sets `dbt_valid_to`) and inserts a new one. Sales before Monday reference the old price. Sales after Monday reference the new price. Full history preserved.

```
Monday pipeline run:
  generate.py  →  product PRD-007 price: $3.50 → $4.20
  dbt snapshot →  old row expires, new row created
  fact_sales   →  future sales reference the $4.20 row
```

---

## Idempotency and backfill

`main()` resolves `run_date` in this priority order:

1. Explicit parameter passed by Lambda: `main(run_date="2025-12-01")`
2. `RUN_DATE` environment variable
3. Today's date as fallback

No global variables are mutated — `run_date` and `run_timestamp` are resolved once at the top of `main()` and passed explicitly into every function that needs them. This means calling `main()` twice in the same process (e.g. Lambda warm start) with different dates is safe.

Date format is validated immediately at startup via `validate_run_date()`. A bad value like `"2025/01/01"` raises a clear error before anything else runs.

---

## The `created_at` timestamp

Every row in `fact_sales` has a `created_at` column set to the moment the script ran. This is the **incremental load watermark**.

When dbt builds `fact_sales`, it only processes rows where `created_at > last_run_max`. This means:
- Day 1 load: process all rows
- Day 2 load: only process yesterday's new rows
- Much faster and cheaper at scale

```
Without watermark: scan entire fact table every day
With watermark:    only scan new rows since last run  ← what we do
```

---

## Data distributions (making it realistic)

Random doesn't mean uniform. Real retail data has patterns:

| What | Distribution | Why |
|---|---|---|
| Quantity per transaction | Weighted toward 1–2 items | Most shoppers buy 1–2 of a product at a time |
| Discounts | 0% most common (70% of rows) | Promotions are not every transaction |
| State allocation | NSW 35%, VIC 25%, QLD 20%... | Matches Australian population distribution |
| Age groups | Peak at 25–44 | Core FMCG shopper demographic |
| Loyalty tiers | Bronze 40%, Platinum 10% | Pyramid — most customers are entry-level |

---

## Error handling

`save_csv()` raises a `RuntimeError` if the file write fails — it never returns a filepath implying success when the write did not complete. This matters in Lambda: if a CSV fails to write, the S3 upload step won't silently receive a bad path.

---

## Running locally

```bash
# Default: generate today's data
python data_generator/generate.py

# Backfill a specific date
RUN_DATE=2025-12-01 python data_generator/generate.py

# Control number of transactions
NUM_TRANSACTIONS=1000 python data_generator/generate.py
```

Output files appear in `data_generator/output/`.

---

## How this fits in the pipeline

```
EventBridge (daily cron)
  → Step Functions
    → Lambda 1
        calls generate.py (this folder)
        uploads 4 CSVs to S3
    → Lambda 2
        loads S3 CSVs into RDS staging
    → EC2 via SSM
        runs dbt (transforms staging → gold schema)
```

`generate.py` is the very first step — without it, the rest of the pipeline has nothing to process.
