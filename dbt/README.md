# dbt — FMCG Pipeline Transformation Layer

This dbt project transforms raw staging data loaded by Lambda 2 into a clean star schema in the `gold` schema, served to Power BI via RDS PostgreSQL.

## Project: `fmcg_pipeline`

### How it fits in the pipeline

```
Lambda 2 (S3 → staging.raw_*)
    → dbt staging  (raw_* → stg_* views)
    → dbt snapshot (stg_* → SCD Type 2 in gold)
    → dbt gold     (stg_* → fact and dim tables in gold)
```

---

## Folder structure

```
fmcg_pipeline/
├── models/
│   ├── staging/
│   │   ├── sources.yml          # declares raw_* tables as dbt sources
│   │   ├── schema.yml           # tests for staging models
│   │   ├── stg_products.sql
│   │   ├── stg_stores.sql
│   │   ├── stg_customers.sql
│   │   └── stg_sales.sql
│   └── gold/
│       ├── dim_customer.sql
│       ├── dim_date.sql
│       └── fact_sales.sql
├── snapshots/
│   ├── dim_product_snapshot.sql
│   └── dim_store_snapshot.sql
├── macros/
│   └── generate_schema_name.sql # overrides dbt default schema naming
└── dbt_project.yml
```

---

## Schemas

| Schema | Owner | Purpose |
|---|---|---|
| `staging` | Lambda 2 + dbt | `raw_*` physical tables (Lambda loads); `stg_*` views (dbt cleans) |
| `gold` | dbt | Star schema served to Power BI |

---

## Layers

### Staging — views (`models/staging/`)

Thin views that sit on top of the raw tables loaded by Lambda 2. They apply type casting and column renaming only — no business logic.

| dbt model | reads from | output |
|---|---|---|
| `stg_products` | `staging.raw_products` | `staging.stg_products` (view) |
| `stg_stores` | `staging.raw_stores` | `staging.stg_stores` (view) |
| `stg_customers` | `staging.raw_customers` | `staging.stg_customers` (view) |
| `stg_sales` | `staging.raw_sales` | `staging.stg_sales` (view) |

> **Why views?** The raw tables are physical tables owned by Lambda 2. dbt staging adds a typed, named layer without duplicating data.

### Snapshots — SCD Type 2 (`snapshots/`)

Tracks slowly changing dimensions for products and stores. When a tracked column changes (e.g. price, region), dbt closes the old record and inserts a new one with updated `dbt_valid_from` / `dbt_valid_to` timestamps.

| Snapshot | Source | Tracked columns |
|---|---|---|
| `dim_product_snapshot` | `staging.raw_products` (via `source()`) | `unit_price`, `unit_cost`, `category` |
| `dim_store_snapshot` | `staging.raw_stores` (via `source()`) | `region`, `store_type`, `state` |

> Snapshots read directly from the raw source tables, not the dbt staging views. This removes any ordering dependency — snapshots can run before or after staging models.

### Gold — star schema (`models/gold/`)

| Model | Materialization | Description |
|---|---|---|
| `dim_customer` | table | SCD Type 1 — latest customer record, deduplicated |
| `dim_date` | table | Static date spine, 730 days |
| `fact_sales` | incremental | Daily sales transactions, loaded via `created_at` watermark |

---

## Incremental load — `fact_sales`

`fact_sales` uses an incremental strategy with `created_at` as the watermark:

```sql
where created_at > (
    select coalesce(max(created_at), '1900-01-01'::timestamp)
    from {{ this }}
)
```

`COALESCE` handles the edge case where `fact_sales` exists but is empty — without it, `max(created_at)` returns `NULL` and `anything > NULL` evaluates to `NULL` (not TRUE), so zero rows would load. The fallback `'1900-01-01'` ensures all rows load when the table is empty.

Each daily pipeline run appends only new rows. To do a full refresh:

```bash
dbt run --select fact_sales --full-refresh --profiles-dir /home/ec2-user/.dbt
```

---

## Custom schema macro

`macros/generate_schema_name.sql` overrides dbt's default behaviour, which would prefix schema names with the target (e.g. `dev_gold`). This macro uses the custom schema name directly so models land in exactly `staging` and `gold` regardless of target.

---

## Run order

```bash
dbt run --select staging --profiles-dir /home/ec2-user/.dbt   # build stg_* views
dbt snapshot               --profiles-dir /home/ec2-user/.dbt   # SCD Type 2 on products and stores
dbt run --select gold      --profiles-dir /home/ec2-user/.dbt   # build fact and dim tables
dbt test                   --profiles-dir /home/ec2-user/.dbt   # validate data quality
```

> Snapshots now read directly from `raw_*` source tables so they have no dependency on staging views. The order above matches the Step Functions pipeline.

---

## Tests

Tests are defined in `models/staging/schema.yml` and cover:

- `not_null` on all primary and foreign keys
- `unique` on all primary keys
- `not_null` on financial columns (`total_amount`, `unit_price`)

Run tests:

```bash
dbt test
dbt test --select staging   # staging only
dbt test --select gold      # gold only
```

---

## Syncing to EC2

dbt runs on EC2 (not locally). After making changes on your laptop, sync with:

```powershell
# from project root on Windows
.\sync_dbt.ps1
```

This copies `models/`, `snapshots/`, `macros/`, and `dbt_project.yml` to the EC2 instance via SCP.

---

## Common issues

**`relation "staging.raw_*" does not exist`**
Lambda 2 hasn't run yet for this date, or `setup_rds.py` hasn't been run. Run `python infrastructure/setup_rds.py` first, then trigger the pipeline.

**`relation "staging.stg_*" does not exist` (in gold)**
`dbt run --select staging` hasn't run yet. Always run staging before gold.

**`fact_sales` not picking up new rows**
Check that Lambda 2 loaded a new `created_at` timestamp. The incremental filter uses `created_at` as the watermark — if Lambda 2 failed, there are no new rows to load. If `fact_sales` exists but is empty, the `COALESCE` fallback ensures all rows load on the next run.

**`unique` test failures on `stg_customers`, `stg_products`, `stg_stores`**
The raw dimension tables have accumulated duplicate rows from multiple pipeline runs. Truncate them in RDS (`TRUNCATE TABLE staging.raw_products` etc.) and re-run. Dimension tables are wiped and reloaded on each run — duplicates only appear if rows were manually inserted or a previous bug caused accumulation.
