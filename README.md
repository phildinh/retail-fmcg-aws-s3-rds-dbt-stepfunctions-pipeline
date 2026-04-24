# 🛒 Retail FMCG AWS Data Pipeline

> A production-grade, fully automated cloud data pipeline built on AWS — migrating a local FMCG retail pipeline to a scalable, serverless architecture with daily automated runs, SCD Type 2 history tracking, and Power BI reporting.

---

## 📋 Table of Contents

- [Business Problem](#business-problem)
- [Architecture](#architecture)
- [AWS Services](#aws-services)
- [Tech Stack](#tech-stack)
- [Data Pipeline Flow](#data-pipeline-flow)
- [Database Design](#database-design)
- [Data Engineering Patterns](#data-engineering-patterns)
- [Synthetic Dataset](#synthetic-dataset)
- [Project Structure](#project-structure)
- [Setup Guide](#setup-guide)
- [Pipeline Execution](#pipeline-execution)
- [Monitoring and Alerting](#monitoring-and-alerting)
- [Cost Estimate](#cost-estimate)
- [Key Learnings](#key-learnings)

---

## 🎯 Business Problem

A small FMCG distributor runs their entire data pipeline on a local machine. When the data engineer leaves, the pipeline stops. Stakeholders lose access to daily sales reports. The Power BI dashboard goes stale.

**The solution:** Migrate the entire pipeline to AWS so it:
- Runs automatically every day at 6am AEST — no manual intervention
- Lives in the cloud — accessible by the whole team, not just one laptop
- Stores data in a managed cloud database — team connects from anywhere
- Sends email alerts on success or failure — full observability
- Tracks historical dimension changes — proper SCD Type 2 audit trail

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        AWS Cloud                                │
│                                                                 │
│  ┌─────────────┐    ┌──────────────────────────────────────┐   │
│  │ EventBridge │───▶│         Step Functions               │   │
│  │ 6am AEST    │    │                                      │   │
│  └─────────────┘    │  ┌──────────┐   ┌──────────┐        │   │
│                     │  │Lambda 1  │──▶│Lambda 2  │        │   │
│                     │  │Generate  │   │Load to   │        │   │
│                     │  │+ Upload  │   │Staging   │        │   │
│                     │  └──────────┘   └──────────┘        │   │
│                     │       │               │              │   │
│                     │       ▼               ▼              │   │
│                     │  ┌────────┐    ┌────────────┐        │   │
│                     │  │  S3    │    │    RDS     │        │   │
│                     │  │Bronze  │    │  Staging   │        │   │
│                     │  └────────┘    └────────────┘        │   │
│                     │                      │               │   │
│                     │  ┌───────────────────▼─────────┐     │   │
│                     │  │    EC2 (dbt Core)            │     │   │
│                     │  │  dbt snapshot (SCD Type 2)  │     │   │
│                     │  │  dbt run (incremental)      │     │   │
│                     │  │  dbt test (data quality)    │     │   │
│                     │  └───────────────────┬─────────┘     │   │
│                     │                      │               │   │
│                     │  ┌───────────────────▼─────────┐     │   │
│                     │  │    RDS PostgreSQL (Gold)     │     │   │
│                     │  │    Star Schema               │     │   │
│                     │  └───────────────────┬─────────┘     │   │
│                     │                      │               │   │
│                     │  ┌───────┐    ┌──────▼──────┐        │   │
│                     │  │  SNS  │    │  Power BI   │        │   │
│                     │  │Email  │    │  Dashboard  │        │   │
│                     │  └───────┘    └─────────────┘        │   │
│                     └──────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## ☁️ AWS Services

| Service | Purpose | Why Chosen |
|---|---|---|
| **EventBridge** | Daily cron trigger at 6am AEST | Serverless scheduler, near-zero cost |
| **Step Functions** | Pipeline orchestrator — sequences all steps | Native AWS, built-in retry, visual monitoring |
| **Lambda (×2)** | Serverless Python compute | Pay per invocation, no server management |
| **S3** | Raw CSV landing zone (Bronze layer) | Durable, cheap, partitioned by date |
| **RDS PostgreSQL** | Cloud data warehouse | Managed, always-on, team accessible |
| **EC2 (t2.micro)** | Runs dbt Core | Persistent environment needed for dbt |
| **SSM** | Remote command execution on EC2 | Secure, no open ports needed |
| **SNS** | Email alerts on success/failure | Instant observability |
| **CloudWatch** | Centralised logging | Automatic, zero config |
| **IAM** | Roles and permissions | Least privilege per service |

---

## 🛠️ Tech Stack

| Tool | Version | Purpose |
|---|---|---|
| Python | 3.11 | Lambda functions and data generator |
| boto3 | Latest | AWS SDK for S3, Lambda, SNS |
| Faker | 24.x | Synthetic FMCG data generation |
| psycopg2-binary | 2.9.x | PostgreSQL connection from Python |
| pandas | 2.x | Data manipulation in generator |
| dbt Core | 1.10.x | SQL transformations and snapshots |
| dbt-postgres | 1.9.x | dbt adapter for RDS PostgreSQL |
| Git + GitHub | — | Version control |

---

## 🔄 Data Pipeline Flow

### Daily Execution (6am AEST)

```
Step 1 — EventBridge fires cron(0 20 * * ? *)
         ↓
Step 2 — Step Functions state machine starts
         ↓
Step 3 — Lambda 1 executes
         • Python + Faker generates 500 daily transactions
         • Produces 4 CSV files (products, stores, customers, fact_sales)
         • Uploads to S3: s3://retail-fmcg-raw-data/{table}/year=/month=/day=/
         ↓
Step 4 — Lambda 2 executes (receives S3 URIs from Lambda 1)
         • Reads each CSV from S3 into memory
         • Deletes existing rows for this run (idempotency)
         • Bulk inserts into RDS staging schema (raw_* tables)
         • Logs run to pipeline_runs metadata table
         ↓
Step 5 — EC2 receives SSM command, runs dbt
         • dbt snapshot → SCD Type 2 on dim_product, dim_store
         • dbt run staging → creates stg_* views on raw_* tables
         • dbt run gold → builds fact_sales (incremental) + dim tables
         • dbt test → validates data quality (18 tests)
         ↓
Step 6 — SNS publishes success email
         "Pipeline completed successfully for 2026-04-23. Rows loaded: 760"
```

### On Failure

```
Any step fails → Step Functions catches error
               → SNS publishes failure email with error details
               → CloudWatch logs capture full stack trace
               → pipeline_runs table records failure + error message
```

---

## 🗄️ Database Design

### RDS PostgreSQL — Two Schemas

```
fmcg_db
├── staging schema (Silver — owned by Lambda 2)
│   ├── raw_products    ← raw CSV data, loaded daily
│   ├── raw_stores      ← raw CSV data, loaded daily
│   ├── raw_customers   ← raw CSV data, loaded daily
│   └── raw_sales       ← raw CSV data, loaded daily
│
└── gold schema (Gold — owned by dbt)
    ├── stg_products*         ← dbt view, typed + cleaned
    ├── stg_stores*           ← dbt view, typed + cleaned
    ├── stg_customers*        ← dbt view, typed + cleaned
    ├── stg_sales*            ← dbt view, typed + cleaned
    ├── fact_sales            ← incremental fact table
    ├── dim_product_snapshot  ← SCD Type 2 history
    ├── dim_store_snapshot    ← SCD Type 2 history
    ├── dim_customer          ← SCD Type 1 (overwrite)
    ├── dim_date              ← static date spine (2026-2027)
    └── pipeline_runs         ← metadata logging

* dbt staging views
```

### Star Schema (Gold Layer)

```
                    ┌─────────────────┐
                    │   dim_date      │
                    │  date_id (PK)   │
                    │  full_date      │
                    │  year/month/day │
                    │  week/quarter   │
                    │  is_weekend     │
                    └────────┬────────┘
                             │
┌──────────────────┐         │         ┌──────────────────┐
│ dim_product      │         │         │ dim_store        │
│ (SCD Type 2)     │         │         │ (SCD Type 2)     │
│ product_id (PK)  │         │         │ store_id (PK)    │
│ product_name     │         │         │ store_name       │
│ category         ├────┐    │    ┌────┤ state/region     │
│ brand/supplier   │    │    │    │    │ store_type       │
│ unit_cost/price  │    │    │    │    │ dbt_valid_from   │
│ dbt_valid_from   │    │    │    │    │ dbt_valid_to     │
│ dbt_valid_to     │    │    │    │    │ dbt_scd_id       │
└──────────────────┘    │    │    │    └──────────────────┘
                        │    │    │
                        ▼    ▼    ▼
              ┌──────────────────────────┐
              │       fact_sales         │
              │  transaction_id (PK)     │
              │  transaction_date        │
              │  product_id (FK)         │
              │  store_id (FK)           │
              │  customer_id (FK)        │
              │  quantity                │
              │  unit_price              │
              │  discount_pct            │
              │  total_amount            │
              │  created_at (watermark)  │
              └──────────────────────────┘
                        │    │
                        │    │
┌──────────────────┐    │    │
│ dim_customer     │    │    │
│ (SCD Type 1)     │◀───┘    │
│ customer_id (PK) │         │
│ age_group        │         │
│ loyalty_tier     │         │
│ state            │         │
└──────────────────┘         │
                             │
                    (dim_date joined
                     on transaction_date)
```

---

## ⚙️ Data Engineering Patterns

| Pattern | Implementation | Where |
|---|---|---|
| **Idempotency** | Delete-then-insert on every staging load | Lambda 2 |
| **Incremental load** | `created_at` watermark — only new rows | dbt fact_sales model |
| **Backfill** | `RUN_DATE` parameter accepted by Lambda 1 + 2 | Both Lambdas |
| **SCD Type 2** | dbt snapshots — expire old rows, insert new | dim_product, dim_store |
| **SCD Type 1** | `DISTINCT ON` deduplication | dim_customer |
| **S3 partitioning** | `/year=/month=/day/` folder structure | Lambda 1 |
| **Watermarking** | `pipeline_runs` table tracks last loaded timestamp | RDS gold schema |
| **Metadata logging** | Every run logged — status, rows, duration, errors | Lambda 2 + logger.py |
| **Data quality** | 18 dbt tests — unique, not_null, accepted_values | dbt test suite |
| **Retry logic** | Step Functions built-in retry with exponential backoff | State machine |
| **Alerting** | SNS email on success and failure | Step Functions |
| **Observability** | CloudWatch logs for all Lambda and dbt runs | Automatic |

---

## 📊 Synthetic Dataset

Generated using Python Faker (Australian locale) with `random.seed(42)` for reproducible dimension data.

### Dataset Size

| Table | Records | Refresh |
|---|---|---|
| Products | 50 | Stable (SCD changes on Mondays) |
| Stores | 10 | Stable (SCD changes on Mondays) |
| Customers | 200 | Stable (SCD Type 1) |
| Daily transactions | 500 | New rows every day |

### FMCG Categories

| Category | Brands | Price Range |
|---|---|---|
| Beverages | Coca Cola, Pepsi, Schweppes, Red Bull, Bundaberg | $1.50 – $5.00 |
| Snacks | Smiths, Doritos, Pringles, Shapes, Grain Waves | $2.00 – $6.00 |
| Dairy | Pauls, Dairy Farmers, Devondale, Bega, Mainland | $2.50 – $8.00 |
| Bakery | Tip Top, Wonder White, Helgas, Bakers Delight | $3.00 – $7.00 |
| Personal Care | Dove, Palmolive, Head & Shoulders, Colgate | $4.00 – $12.00 |
| Cleaning | Ajax, Domestos, Morning Fresh, Finish, Vanish | $3.50 – $14.00 |

### State Distribution (weighted by population)

| State | Weight |
|---|---|
| NSW | 35% |
| VIC | 25% |
| QLD | 20% |
| WA | 10% |
| SA | 10% |

### SCD Change Simulation

Every Monday the generator simulates realistic dimension changes:
- **2 products** receive price/cost updates
- **1 store** changes region classification

This triggers dbt snapshots to record history — demonstrating real SCD Type 2 behaviour across weeks of pipeline runs.

---

## 📁 Project Structure

```
retail-fmcg-aws-s3-rds-dbt-stepfunctions-pipeline/
│
├── data_generator/
│   ├── generate.py              # Synthetic data generator
│   └── output/                  # Local CSV output (gitignored)
│
├── lambda/
│   ├── lambda_1_generator/
│   │   ├── handler.py           # Lambda 1 — generate + upload to S3
│   │   └── requirements.txt     # Faker, pandas, python-dotenv
│   └── lambda_2_staging/
│       ├── handler.py           # Lambda 2 — S3 to RDS staging
│       └── requirements.txt     # psycopg2-binary, python-dotenv
│
├── dbt/
│   └── fmcg_pipeline/
│       ├── dbt_project.yml
│       ├── macros/
│       │   └── generate_schema_name.sql
│       ├── models/
│       │   ├── staging/
│       │   │   ├── sources.yml
│       │   │   ├── schema.yml
│       │   │   ├── stg_products.sql
│       │   │   ├── stg_stores.sql
│       │   │   ├── stg_customers.sql
│       │   │   └── stg_sales.sql
│       │   └── gold/
│       │       ├── dim_customer.sql
│       │       ├── dim_date.sql
│       │       └── fact_sales.sql
│       └── snapshots/
│           ├── dim_product_snapshot.sql
│           └── dim_store_snapshot.sql
│
├── step_functions/
│   └── state_machine.json       # Step Functions state machine
│
├── infrastructure/
│   ├── setup_rds.py             # Creates RDS schemas and tables
│   ├── deploy_lambdas.py        # Packages and deploys Lambdas to AWS
│   └── setup_notes.md           # AWS resource setup documentation
│
├── utils/
│   ├── config.py                # Environment variable loader
│   ├── db.py                    # RDS connection and query helpers
│   ├── s3.py                    # S3 upload/download helpers
│   └── logger.py                # pipeline_runs metadata logging
│
├── tests/
│   ├── test_lambda_1.py         # Local test for Lambda 1
│   └── test_lambda_2.py         # Local test for Lambda 2
│
├── .claude/
│   └── claude.md                # Project context for Claude AI
│
├── sync_dbt.ps1                 # Sync local dbt files to EC2
├── load_env.ps1                 # Load .env variables into PowerShell
├── requirements.txt             # Python dependencies
├── .env.example                 # Environment variable template
└── README.md
```

---

## 🚀 Setup Guide

### Prerequisites

- Python 3.10+
- AWS account with credits
- Power BI Desktop
- VS Code
- Git

### 1. Clone the Repository

```bash
git clone https://github.com/phildinh/retail-fmcg-aws-s3-rds-dbt-stepfunctions-pipeline.git
cd retail-fmcg-aws-s3-rds-dbt-stepfunctions-pipeline
```

### 2. Create Virtual Environment

```bash
python -m venv venv
venv\Scripts\Activate.ps1   # Windows
pip install -r requirements.txt
```

### 3. Configure Environment Variables

```bash
cp .env.example .env
# Fill in your AWS credentials and RDS details
```

### 4. Configure AWS CLI

```bash
aws configure
# Enter: Access Key, Secret Key, Region (ap-southeast-2), Format (json)
```

### 5. Create AWS Infrastructure

```bash
# S3 bucket, RDS, EC2, IAM roles, SNS — see infrastructure/setup_notes.md
python infrastructure/setup_rds.py
```

### 6. Deploy Lambda Functions

```bash
python infrastructure/deploy_lambdas.py
```

### 7. Set Up dbt on EC2

```bash
# SSH into EC2
ssh fmcg-ec2

# Install dbt
pip3 install dbt-core dbt-postgres psycopg2-binary

# Configure profiles.yml with RDS credentials
mkdir -p ~/.dbt
nano ~/.dbt/profiles.yml
```

### 8. Sync dbt Project to EC2

```powershell
.\sync_dbt.ps1
```

### 9. Deploy Step Functions and EventBridge

```bash
aws stepfunctions create-state-machine \
  --name "retail-fmcg-daily-pipeline" \
  --definition file://step_functions/state_machine.json \
  --role-arn "arn:aws:iam::YOUR_ACCOUNT:role/retail-fmcg-stepfunctions-role"

aws events put-rule \
  --name "retail-fmcg-daily-trigger" \
  --schedule-expression "cron(0 20 * * ? *)" \
  --state ENABLED
```

---

## ▶️ Pipeline Execution

### Manual Trigger

```bash
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:ap-southeast-2:ACCOUNT:stateMachine:retail-fmcg-daily-pipeline" \
  --input '{"run_date": "2026-04-23", "run_timestamp": "2026-04-23 06:00:00"}'
```

### Backfill a Specific Date

```bash
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:ap-southeast-2:ACCOUNT:stateMachine:retail-fmcg-daily-pipeline" \
  --input '{"run_date": "2026-04-01", "run_timestamp": "2026-04-01 06:00:00"}'
```

### Local Testing

```bash
# Test data generator
python data_generator/generate.py

# Test Lambda 1 locally
python tests/test_lambda_1.py

# Test Lambda 2 locally
python tests/test_lambda_2.py

# Run dbt on EC2
ssh fmcg-ec2
cd /home/ec2-user/dbt/fmcg_pipeline
dbt snapshot && dbt run && dbt test
```

---

## 📡 Monitoring and Alerting

### SNS Email Alerts

| Event | Subject | Message |
|---|---|---|
| Success | `FMCG Pipeline - Success` | Run date, rows loaded |
| Failure | `FMCG Pipeline - FAILED` | Step that failed, error details |

### CloudWatch Log Groups

```
/aws/lambda/retail-fmcg-lambda-1-generator
/aws/lambda/retail-fmcg-lambda-2-staging
```

### Pipeline Runs Table

```sql
SELECT
    run_id,
    run_date,
    status,
    rows_loaded,
    finished_at - started_at as duration,
    error_message
FROM gold.pipeline_runs
ORDER BY run_id DESC;
```

### dbt Test Results

18 automated data quality tests covering:
- `unique` constraints on all primary keys
- `not_null` constraints on critical columns
- Referential integrity across fact and dimension tables

---

## 💰 Cost Estimate

| Service | Monthly Cost |
|---|---|
| RDS PostgreSQL (db.t4g.micro) | ~$20 |
| EC2 (t2.micro) | ~$10 |
| S3 (< 1GB) | ~$0.02 |
| Lambda (< 1M invocations) | Free tier |
| Step Functions | Free tier |
| EventBridge | Free tier |
| SNS (< 1000 emails) | Free tier |
| **Total** | **~$30/month** |

> Note: Costs covered by AWS credits for this portfolio project.

---

## 🏭 Production Considerations

This project is built for a dev/portfolio environment. Production enhancements would include:

| Area | Dev (this project) | Production |
|---|---|---|
| IAM policies | FullAccess for simplicity | Least privilege custom policies |
| RDS access | Public + 0.0.0.0/0 for Lambda | Private VPC + Lambda inside VPC |
| SSL | Force SSL disabled for Power BI | SSL enforced with certificate rotation |
| Secrets | Environment variables | AWS Secrets Manager |
| File format | CSV (simple, debuggable) | Parquet (compressed, typed) |
| EC2 | Always on | Stop/start schedule to save cost |
| dbt | Single EC2 instance | dbt Cloud or MWAA |
| Monitoring | CloudWatch + SNS email | CloudWatch dashboards + PagerDuty |

---

## 💡 Key Learnings

### Architecture Decisions

**Why Step Functions over Airflow?**
Single daily pipeline with sequential steps — Step Functions is serverless, near-zero cost, and native to AWS. Airflow on EC2 adds infrastructure cost and complexity that isn't justified for one pipeline. At 5+ pipelines with complex dependencies, Airflow becomes the right choice.

**Why EC2 for dbt instead of Lambda?**
dbt needs a persistent file system for its project files, compiled SQL, and profiles. Lambda's ephemeral storage resets after every run. EC2 keeps everything installed and ready.

**Why RDS over Redshift?**
Small dataset (500 rows/day), simple batch processing, direct Power BI connection needed. Redshift is optimised for massive analytical queries — overkill here. RDS PostgreSQL provides all needed SQL capabilities at a fraction of the cost.

**Why CSV over Parquet in S3?**
Simple pipeline, small data, direct load to PostgreSQL. Parquet requires pyarrow in Lambda and conversion before loading to RDS. CSV loads directly with psycopg2. Production would use Parquet for compression and type safety.

---

## 👤 Author

**Phil Dinh**
Data Engineer | Sydney, Australia

- Portfolio: [github.com/phildinh](https://github.com/phildinh)
- LinkedIn: [linkedin.com/in/phildinh](https://linkedin.com/in/phildinh)

---

## 📄 License

MIT License — feel free to use this project as a reference for your own data engineering work.