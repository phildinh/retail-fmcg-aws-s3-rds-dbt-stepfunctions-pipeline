# Project: Retail FMCG AWS Pipeline

## What this project is
A production-grade cloud data pipeline migrating a local FMCG retail 
pipeline to AWS. Synthetic retail sales data is generated daily, 
processed through AWS services, transformed by dbt, and served to 
Power BI via RDS PostgreSQL.

## Architecture
EventBridge (daily cron)
  → Step Functions (orchestrator)
    → Lambda 1 (generate synthetic data → upload CSV to S3)
    → Lambda 2 (load S3 CSV → RDS staging table)
    → EC2 via SSM (run dbt Core)
      → dbt snapshots (SCD Type 2 on dim_product, dim_store)
      → dbt incremental (fact_sales via created_at watermark)
  → SNS (email alert on success/failure)
  → CloudWatch (logs)

RDS PostgreSQL → Power BI dashboard

## AWS services used
- S3: raw CSV landing zone, partitioned by /year/month/day/
- RDS PostgreSQL (t3.micro): staging schema + gold schema
- EC2 (t3.micro): runs dbt Core, triggered via SSM
- Lambda: serverless Python functions
- Step Functions: pipeline orchestration and sequencing
- EventBridge: daily cron trigger
- SSM: sends dbt run command to EC2 remotely
- SNS: pipeline success/failure email alerts
- CloudWatch: automatic logging for all services
- IAM: permissions and roles

## Database structure
RDS has two schemas:
- staging: raw rows loaded by Lambda 2 (stg_sales)
- gold: star schema built by dbt
  - fact_sales (incremental load)
  - dim_product (SCD Type 2)
  - dim_store (SCD Type 2)
  - dim_customer (SCD Type 1 upsert)
  - dim_date (static, loaded once)
  - pipeline_runs (metadata logging)

## Data patterns used
- Idempotency: delete-then-insert on staging load
- Incremental load: created_at watermark on fact table
- Backfill: date parameter on Lambda 1 and Lambda 2
- SCD Type 2: dbt snapshots on dim_product and dim_store
- Upsert: ON CONFLICT for SCD Type 1 dims
- S3 partitioning: /year=/month=/day/ folder structure
- Metadata logging: pipeline_runs table tracks every run
- Data quality: dbt tests (not_null, unique, accepted_values)
- Retry logic: Step Functions built-in retry and backoff
- SNS alerting: email on pipeline success or failure

## File format
CSV in S3 (simple, debuggable, direct load to RDS)
Note: production would use Parquet for compression and type safety

## Tech stack
- Python 3.10+
- boto3 (AWS SDK)
- Faker (synthetic data generation)
- psycopg2-binary (PostgreSQL connection)
- pandas (data manipulation)
- python-dotenv (environment variables)
- dbt Core + dbt-postgres adapter
- Git + GitHub

## Project structure
retail-fmcg-aws-s3-rds-dbt-stepfunctions-pipeline/
├── data_generator/
│   └── generate.py         # Faker synthetic data generator
├── lambda/
│   ├── lambda_1_generator/
│   │   └── handler.py      # Lambda 1 entry point
│   └── lambda_2_staging/
│       └── handler.py      # Lambda 2 entry point
├── dbt/                    # dbt project
├── step_functions/
│   └── state_machine.json  # Step Functions state machine
├── infrastructure/
│   └── setup_notes.md      # AWS setup documentation
├── utils/
│   ├── config.py           # environment variable loader
│   ├── db.py               # RDS connection and queries
│   ├── s3.py               # S3 upload/download helpers
│   └── logger.py           # pipeline_runs table logging
├── tests/                  # unit tests
├── .env                    # credentials (never commit)
├── .gitignore
└── README.md

## Coding conventions
- Always explain the business problem before writing code
- Business logic goes in utils/, handlers stay thin
- Every function has a docstring explaining what and why
- Environment variables always loaded from config.py
- Never hardcode credentials or connection strings
- Use python-dotenv for local development
- Follow the pattern: bad approach → good approach → code

## How to teach me (important)
- I am a junior data engineer transitioning from analyst
- Always explain WHY before HOW
- Show bad approach vs good approach before writing code
- Walk through new code line by line
- Connect technical decisions back to business value
- I work in FMCG/retail domain — use this for analogies
- Never skip the reasoning — I learn concepts not just syntax

## Environment variables needed
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=ap-southeast-2
S3_BUCKET=
DB_HOST=
DB_NAME=fmcg_db
DB_USER=
DB_PASSWORD=
DB_PORT=5432
SNS_TOPIC_ARN=