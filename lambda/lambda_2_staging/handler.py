import sys
import os
import json
import csv
import io

sys.path.insert(0, os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
))

from utils.config import get_config
from utils.db import execute_query, execute_many
from utils.logger import log_run_start, log_run_success, log_run_failure
from utils.s3 import get_s3_client


def read_csv_from_s3(bucket: str, s3_key: str) -> list:
    """
    Read a CSV file from S3 and return list of dicts.
    Never touches local disk — streams directly from S3 into memory.
    """
    s3       = get_s3_client()
    response = s3.get_object(Bucket=bucket, Key=s3_key)
    content  = response["Body"].read().decode("utf-8")
    reader   = csv.DictReader(io.StringIO(content))
    return list(reader)


def get_s3_key(table_name: str, run_date: str,
               bucket: str) -> str:
    """
    Build and verify the S3 key for a given table and run date.
    Raises clearly if the file doesn't exist in S3.
    """
    year, month, day = run_date.split("-")
    date_str = run_date.replace("-", "")
    key = (f"{table_name}/"
           f"year={year}/month={month}/day={day}/"
           f"{table_name}_{date_str}.csv")

    s3 = get_s3_client()
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except Exception:
        raise FileNotFoundError(
            f"S3 file not found: s3://{bucket}/{key}\n"
            f"Has Lambda 1 run successfully for {run_date}?"
        )
    return key


def load_products(rows: list, run_timestamp: str):
    """
    Load products into staging — full replace each run.
    Dimensions are master data: wipe and reload the current snapshot.
    SCD Type 2 history is tracked in gold.dim_product_snapshot, not here.
    """
    execute_query("DELETE FROM staging.raw_products")

    records = [(
        r["product_id"], r["product_name"], r["category"],
        r["brand"], r["supplier"],
        float(r["unit_cost"]), float(r["unit_price"]),
        run_timestamp
    ) for r in rows]

    execute_many("""
        INSERT INTO staging.raw_products
            (product_id, product_name, category, brand,
             supplier, unit_cost, unit_price, loaded_at)
        VALUES %s
    """, records)
    print(f"  raw_products: {len(records)} rows loaded")
    return len(records)


def load_stores(rows: list, run_timestamp: str):
    """
    Load stores into staging — full replace each run.
    Dimensions are master data: wipe and reload the current snapshot.
    SCD Type 2 history is tracked in gold.dim_store_snapshot, not here.
    """
    execute_query("DELETE FROM staging.raw_stores")

    records = [(
        r["store_id"], r["store_name"], r["state"],
        r["region"], r["store_type"], r["city"],
        run_timestamp
    ) for r in rows]

    execute_many("""
        INSERT INTO staging.raw_stores
            (store_id, store_name, state, region,
             store_type, city, loaded_at)
        VALUES %s
    """, records)
    print(f"  raw_stores: {len(records)} rows loaded")
    return len(records)


def load_customers(rows: list, run_timestamp: str):
    """
    Load customers into staging — full replace each run.
    Customers use SCD Type 1 (latest state wins), so wiping is correct.
    """
    execute_query("DELETE FROM staging.raw_customers")

    records = [(
        r["customer_id"], r["age_group"],
        r["loyalty_tier"], r["state"],
        run_timestamp
    ) for r in rows]

    execute_many("""
        INSERT INTO staging.raw_customers
            (customer_id, age_group, loyalty_tier,
             state, loaded_at)
        VALUES %s
    """, records)
    print(f"  raw_customers: {len(records)} rows loaded")
    return len(records)


def load_fact_sales(rows: list, run_timestamp: str):
    """
    Load fact sales into staging — idempotent.
    Delete by created_at (the watermark column) so dbt
    incremental model can correctly identify new rows.
    """
    execute_query(
        "DELETE FROM staging.raw_sales WHERE created_at = %s",
        (run_timestamp,)
    )

    records = [(
        r["transaction_id"],
        r["transaction_date"],
        r["product_id"],
        r["store_id"],
        r["customer_id"],
        int(r["quantity"]),
        float(r["unit_price"]),
        float(r["discount_pct"]),
        float(r["total_amount"]),
        r["created_at"]
    ) for r in rows]

    execute_many("""
        INSERT INTO staging.raw_sales
            (transaction_id, transaction_date, product_id,
             store_id, customer_id, quantity, unit_price,
             discount_pct, total_amount, created_at)
        VALUES %s
    """, records)
    print(f"  raw_sales: {len(records)} rows loaded")
    return len(records)


def handler(event: dict, context) -> dict:
    """
    Lambda 2 — S3 to RDS Staging Loader.

    Triggered by Step Functions after Lambda 1 completes.
    Reads CSV files from S3 and loads into RDS staging tables.

    Event payload (passed from Lambda 1 via Step Functions):
    {
        "run_date":      "2026-04-18",
        "run_timestamp": "2026-04-18 06:00:00",
        "s3_uris": {
            "products":  "s3://...",
            "stores":    "s3://...",
            "customers": "s3://...",
            "fact_sales": "s3://..."
        }
    }

    Techniques applied:
    - Idempotency: delete-then-insert on every table
    - Metadata logging: pipeline_runs table records every run
    - Backfill safe: run_date parameter controls which day loads
    - Error handling: logs failure to pipeline_runs before raising
    """
    print(f"Lambda 2 started — event: {json.dumps(event)}")

    config        = get_config()
    bucket        = config["s3_bucket"]
    run_date      = event.get("run_date")
    run_timestamp = event.get("run_timestamp")

    if not run_date or not run_timestamp:
        raise ValueError(
            "Event must contain run_date and run_timestamp. "
            "Has Lambda 1 run successfully?"
        )

    # ── Start metadata log ────────────────────────────────
    run_id = log_run_start(run_date, run_timestamp)
    print(f"Pipeline run {run_id} started")

    try:
        total_rows = 0

        # ── Load products ─────────────────────────────────
        print("\n[1/4] Loading products...")
        key  = get_s3_key("products", run_date, bucket)
        rows = read_csv_from_s3(bucket, key)
        total_rows += load_products(rows, run_timestamp)

        # ── Load stores ───────────────────────────────────
        print("\n[2/4] Loading stores...")
        key  = get_s3_key("stores", run_date, bucket)
        rows = read_csv_from_s3(bucket, key)
        total_rows += load_stores(rows, run_timestamp)

        # ── Load customers ────────────────────────────────
        print("\n[3/4] Loading customers...")
        key  = get_s3_key("customers", run_date, bucket)
        rows = read_csv_from_s3(bucket, key)
        total_rows += load_customers(rows, run_timestamp)

        # ── Load fact sales ───────────────────────────────
        print("\n[4/4] Loading fact sales...")
        key  = get_s3_key("fact_sales", run_date, bucket)
        rows = read_csv_from_s3(bucket, key)
        total_rows += load_fact_sales(rows, run_timestamp)

        # ── Log success ───────────────────────────────────
        log_run_success(run_id, total_rows)

        response = {
            "status":        "success",
            "run_id":        run_id,
            "run_date":      run_date,
            "run_timestamp": run_timestamp,
            "rows_loaded":   total_rows
        }

        print(f"\nLambda 2 complete: {json.dumps(response, indent=2)}")
        return response

    except Exception as e:
        # ── Log failure ───────────────────────────────────
        log_run_failure(run_id, str(e))
        print(f"Lambda 2 failed: {e}")
        raise e
