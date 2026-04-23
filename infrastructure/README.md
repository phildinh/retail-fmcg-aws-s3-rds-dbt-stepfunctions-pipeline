# Infrastructure

Scripts for setting up and deploying the AWS pipeline. Run these from the project root on your laptop.

## Files

```
infrastructure/
├── setup_rds.py        # Creates RDS schemas and tables
├── deploy_lambdas.py   # Packages and deploys both Lambda functions
├── setup_notes.md      # Manual AWS setup steps
└── packages/           # Generated zip files (git-ignored)
    ├── lambda_1.zip
    └── lambda_2.zip
```

---

## Setup order

Run these once when setting up the pipeline from scratch:

```
1. setup_rds.py       — create schemas and tables in RDS
2. deploy_lambdas.py  — package and push Lambda code to AWS
3. (manual) Step Functions, EventBridge, SNS, EC2, SSM — see below
```

---

## setup_rds.py

Creates the two schemas and all physical tables in RDS PostgreSQL.

```bash
python infrastructure/setup_rds.py
```

### What it creates

**`staging` schema** — raw landing tables, owned by Lambda 2:

| Table | Description |
|---|---|
| `staging.raw_products` | Product catalogue per daily run |
| `staging.raw_stores` | Store master per daily run |
| `staging.raw_customers` | Customer attributes per daily run |
| `staging.raw_sales` | Transaction rows per daily run |

**`gold` schema** — pipeline metadata table (dbt builds the rest):

| Table | Description |
|---|---|
| `gold.pipeline_runs` | Logs every run — status, rows loaded, errors, timing |

> dbt creates all other gold tables (`fact_sales`, `dim_*`) automatically on first run. Only `pipeline_runs` needs to exist before Lambda 2 runs.

### Re-running safely

All `CREATE TABLE` statements use `IF NOT EXISTS` — safe to re-run on an existing database without losing data.

---

## deploy_lambdas.py

Packages both Lambda functions and deploys them to AWS. Creates or updates — safe to re-run after any code change.

```bash
python infrastructure/deploy_lambdas.py
```

### What it does per Lambda

1. Reads `lambda/<function>/requirements.txt`
2. Installs dependencies into a temp folder using the `manylinux2014_x86_64` platform — this ensures compiled packages (e.g. psycopg2) work on Amazon Linux, not just your local OS
3. Zips dependencies + source code + `utils/` together
4. Calls `UpdateFunctionCode` if the function exists, or `CreateFunction` if not
5. Waits for the code update to finish before updating environment variables (avoids `ResourceConflictException`)

### Lambda packages

| Function | Includes | Dependencies |
|---|---|---|
| `retail-fmcg-lambda-1-generator` | `lambda_1_generator/`, `data_generator/`, `utils/` | Faker, pandas, python-dotenv |
| `retail-fmcg-lambda-2-staging` | `lambda_2_staging/`, `utils/` | psycopg2-binary, python-dotenv |

### Requirements

- `.env` file must be present and populated (script reads credentials via `utils/config.py`)
- IAM role `retail-fmcg-lambda-role` must already exist in AWS

---

## Step Functions — `step_functions/state_machine.json`

The state machine orchestrates the full daily pipeline. It is deployed manually via the AWS Console or CLI.

### States

```
GenerateAndUpload  →  LoadToStaging  →  RunDbt  →  NotifySuccess
        ↓                   ↓              ↓
   NotifyFailure       NotifyFailure  NotifyFailure
```

| State | Type | What it does |
|---|---|---|
| `GenerateAndUpload` | Lambda | Invokes Lambda 1 — generates CSV and uploads to S3 |
| `LoadToStaging` | Lambda | Invokes Lambda 2 — loads S3 CSV into `staging.raw_*` tables |
| `RunDbt` | SSM SendCommand | SSHes into EC2 and runs dbt staging → snapshot → gold → test |
| `NotifySuccess` | SNS | Publishes success email with run date and row count |
| `NotifyFailure` | SNS | Publishes failure email with error details |
| `PipelineComplete` | Succeed | Terminal success state |
| `PipelineFailed` | Fail | Terminal failure state |

### Data flow between states

Lambda 1's full output is stored at `$.lambda1_result.Payload` and passed directly as input to Lambda 2 — this is how Lambda 2 receives the S3 URIs without any manual wiring.

### Retry config

| State | Retries | Initial wait | Backoff |
|---|---|---|---|
| Lambda 1 | 2 | 30s | 2× |
| Lambda 2 | 2 | 30s | 2× |
| dbt (SSM) | 1 | 60s | 2× |

### dbt run order inside SSM

The `RunDbt` state sends these commands to EC2 in sequence:

```bash
dbt snapshot --profiles-dir /home/ec2-user/.dbt   # SCD Type 2
dbt run --select staging                           # build stg_* views
dbt run --select gold                              # build fact + dim tables
dbt test                                           # validate data quality
```

> Note: `dbt snapshot` runs before staging because snapshots read from `stg_*` views via `ref()` — if staging views don't exist yet, snapshot fails. The correct order is: snapshot first, then staging, then gold.

### EC2 instance

The SSM command targets instance `i-0d5a310331e078521`. If the EC2 instance is replaced, update `InstanceIds` in `state_machine.json` and redeploy the state machine.

### DB credentials in SSM Parameter Store

The `RunDbt` state pulls RDS credentials from SSM Parameter Store at runtime — they are never hardcoded in the state machine:

| Parameter | Used for |
|---|---|
| `/fmcg/db_host` | RDS endpoint |
| `/fmcg/db_user` | RDS username |
| `/fmcg/db_password` | RDS password |

---

## SNS alerts

Both success and failure states publish to:
```
arn:aws:sns:ap-southeast-2:819404925252:retail-fmcg-pipeline-alerts
```

Subscribe your email to this topic in the AWS Console to receive daily pipeline notifications.

---

## EventBridge — daily trigger

The Step Functions state machine is triggered daily by an EventBridge rule (configured manually in AWS Console):

- **Schedule:** `cron(0 20 * * ? *)` — runs at 20:00 UTC (06:00 AEST)
- **Target:** the Step Functions state machine ARN
- **Input:** `{}` — Lambda 1 defaults to today's date when no `run_date` is provided

To trigger a manual run or backfill, go to **Step Functions → Start Execution** and pass:

```json
{ "run_date": "2026-04-01", "run_timestamp": "2026-04-01 06:00:00" }
```
