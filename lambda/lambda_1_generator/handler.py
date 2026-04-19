import sys
import os
import json
from datetime import date, datetime

# Allow Lambda to import from project root
sys.path.insert(0, os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
))

from data_generator.generate import main as generate_data
from utils.s3 import upload_file_to_s3
from utils.config import get_config


def handler(event: dict, context) -> dict:
    """
    Lambda 1 — Data Generator and S3 Uploader.

    Triggered by Step Functions. Generates synthetic FMCG retail
    data for the given run_date and uploads all CSV files to S3.

    Event payload (from Step Functions):
    {
        "run_date": "2026-04-18",       # optional — defaults to today
        "run_timestamp": "2026-04-18 06:00:00"  # optional
    }

    Returns:
    {
        "status": "success",
        "run_date": "2026-04-18",
        "run_timestamp": "2026-04-18 06:00:00",
        "s3_uris": {
            "products":  "s3://bucket/products/...",
            "stores":    "s3://bucket/stores/...",
            "customers": "s3://bucket/customers/...",
            "fact_sales": "s3://bucket/fact_sales/..."
        },
        "rows_generated": 500
    }
    """
    print(f"Lambda 1 started — event: {json.dumps(event)}")

    # ── Resolve run identifiers ───────────────────────────
    run_date = (event.get("run_date") or
                date.today().strftime("%Y-%m-%d"))

    run_timestamp = (event.get("run_timestamp") or
                     datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    print(f"Run date:      {run_date}")
    print(f"Run timestamp: {run_timestamp}")

    # ── Step 1: Generate data ─────────────────────────────
    print("\nGenerating synthetic data...")
    local_files = generate_data(
        run_date=run_date,
        run_timestamp=run_timestamp
    )

    # ── Step 2: Upload to S3 ──────────────────────────────
    print("\nUploading files to S3...")

    table_map = {
        "products":   "products",
        "stores":     "stores",
        "customers":  "customers",
        "fact_sales": "fact_sales"
    }

    s3_uris = {}
    for table_name, local_path in local_files.items():
        s3_uri = upload_file_to_s3(
            local_path = local_path,
            table_name = table_map[table_name],
            run_date   = run_date
        )
        s3_uris[table_name] = s3_uri

    # ── Step 3: Build response ────────────────────────────
    response = {
        "status":        "success",
        "run_date":      run_date,
        "run_timestamp": run_timestamp,
        "s3_uris":       s3_uris,
        "rows_generated": get_config()["num_transactions"]
    }

    print(f"\nLambda 1 complete: {json.dumps(response, indent=2)}")
    return response