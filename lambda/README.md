# Lambda Functions

Two Python Lambda functions form the ingestion layer of the pipeline. They are triggered sequentially by Step Functions — Lambda 1 generates and uploads data, then passes its output directly to Lambda 2 as input.

## How they fit in the pipeline

```
EventBridge (daily cron)
  → Step Functions
      → Lambda 1: generate synthetic data → upload CSV to S3
      → Lambda 2: read CSV from S3 → load into RDS staging tables
      → EC2 via SSM: run dbt
```

---

## Lambda 1 — Data Generator

**Folder:** `lambda_1_generator/`
**Function name:** `retail-fmcg-lambda-1-generator`
**Handler:** `handler.handler`

### What it does

1. Accepts a `run_date` from Step Functions (defaults to today if not provided)
2. Calls `data_generator/generate.py` to produce synthetic FMCG retail data using Faker
3. Writes 4 CSV files to `/tmp/output/` inside the Lambda container
4. Uploads each CSV to S3 under a partitioned path

### S3 path pattern

```
s3://<bucket>/<table>/year=YYYY/month=MM/day=DD/<table>_YYYYMMDD.csv
```

Example:
```
s3://my-bucket/fact_sales/year=2026/month=04/day=23/fact_sales_20260423.csv
```

### Event input (from Step Functions)

```json
{
    "run_date": "2026-04-23",
    "run_timestamp": "2026-04-23 06:00:00"
}
```

Both fields are optional — Lambda 1 defaults to today's date if omitted.

### Event output (passed to Lambda 2)

```json
{
    "status": "success",
    "run_date": "2026-04-23",
    "run_timestamp": "2026-04-23 06:00:00",
    "s3_uris": {
        "products":   "s3://bucket/products/year=2026/...",
        "stores":     "s3://bucket/stores/year=2026/...",
        "customers":  "s3://bucket/customers/year=2026/...",
        "fact_sales": "s3://bucket/fact_sales/year=2026/..."
    },
    "rows_generated": 500
}
```

### Dependencies (`requirements.txt`)

| Package | Why |
|---|---|
| `Faker` | Generates realistic synthetic names, IDs, states |
| `pandas` | Builds and writes CSV files |
| `python-dotenv` | Loads `.env` for local testing |

---

## Lambda 2 — Staging Loader

**Folder:** `lambda_2_staging/`
**Function name:** `retail-fmcg-lambda-2-staging`
**Handler:** `handler.handler`

### What it does

1. Receives the S3 URIs output by Lambda 1
2. Reads each CSV file directly from S3 into memory (never touches disk)
3. Loads each file into its corresponding RDS staging table using delete-then-insert (idempotent)
4. Logs the run result to `gold.pipeline_runs`

### RDS staging tables loaded

| CSV file | RDS table |
|---|---|
| `products_YYYYMMDD.csv` | `staging.raw_products` |
| `stores_YYYYMMDD.csv` | `staging.raw_stores` |
| `customers_YYYYMMDD.csv` | `staging.raw_customers` |
| `fact_sales_YYYYMMDD.csv` | `staging.raw_sales` |

### Idempotency

Each table uses a different delete key to make reruns safe:

| Table | Delete key | Why |
|---|---|---|
| `raw_products` | `loaded_at::date` | Full refresh per day |
| `raw_stores` | `loaded_at::date` | Full refresh per day |
| `raw_customers` | `loaded_at::date` | Full refresh per day |
| `raw_sales` | `created_at` | Exact timestamp match — preserves watermark for dbt incremental |

### Event input (from Lambda 1 via Step Functions)

```json
{
    "run_date": "2026-04-23",
    "run_timestamp": "2026-04-23 06:00:00",
    "s3_uris": {
        "products":   "s3://...",
        "stores":     "s3://...",
        "customers":  "s3://...",
        "fact_sales": "s3://..."
    }
}
```

### Event output

```json
{
    "status": "success",
    "run_id": 42,
    "run_date": "2026-04-23",
    "run_timestamp": "2026-04-23 06:00:00",
    "rows_loaded": 1250
}
```

### Dependencies (`requirements.txt`)

| Package | Why |
|---|---|
| `psycopg2-binary` | PostgreSQL connection to RDS |
| `python-dotenv` | Loads `.env` for local testing |

---

## Deploying

Both Lambdas are deployed from your laptop via:

```bash
python infrastructure/deploy_lambdas.py
```

This script:
1. Installs each Lambda's `requirements.txt` into a temp folder using the `manylinux2014_x86_64` platform flag (ensures compiled packages like psycopg2 work on Amazon Linux, not just your local OS)
2. Zips the handler, utils/, and dependencies together
3. Creates or updates the Lambda function in AWS

Zip files are written to `infrastructure/packages/` (git-ignored).

---

## Environment variables

Both Lambdas receive these from the deploy script (sourced from your local `.env`):

| Variable | Used by | Description |
|---|---|---|
| `S3_BUCKET` | Lambda 1 + 2 | S3 bucket name for CSV files |
| `DB_HOST` | Lambda 2 | RDS endpoint |
| `DB_NAME` | Lambda 2 | Database name (`fmcg_db`) |
| `DB_USER` | Lambda 2 | RDS username |
| `DB_PASSWORD` | Lambda 2 | RDS password |
| `DB_PORT` | Lambda 2 | RDS port (`5432`) |
| `SNS_TOPIC_ARN` | Lambda 2 | SNS topic for pipeline alerts |
| `NUM_TRANSACTIONS` | Lambda 1 | Rows to generate per run |
| `OUTPUT_DIR` | Lambda 1 | Temp write path (`/tmp/output`) |
| `PIPELINE_ENV` | Lambda 1 + 2 | Environment tag (`dev`) |

> `AWS_REGION` is **not** set manually — Lambda injects it automatically from the deployment region.

---

## Backfill

Both Lambdas accept a `run_date` parameter, making backfill straightforward. To reprocess a past date, invoke Lambda 1 with:

```json
{ "run_date": "2026-04-01", "run_timestamp": "2026-04-01 06:00:00" }
```

Step Functions will chain the output to Lambda 2 automatically.
