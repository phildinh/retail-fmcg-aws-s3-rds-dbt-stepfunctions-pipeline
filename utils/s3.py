import boto3
import os
from utils.config import get_config


def get_s3_client():
    """
    Return a boto3 S3 client using config region.
    """
    config = get_config()
    return boto3.client("s3", region_name=config["aws_region"])


def build_s3_key(table_name: str, run_date: str,
                 filename: str) -> str:
    """
    Build S3 partition key for a given table and run date.

    Example:
        table_name = "fact_sales"
        run_date   = "2026-04-18"
        filename   = "fact_sales_20260418.csv"

        returns → fact_sales/year=2026/month=04/day=18/
                              fact_sales_20260418.csv
    """
    year, month, day = run_date.split("-")
    return (f"{table_name}/"
            f"year={year}/month={month}/day={day}/"
            f"{filename}")


def upload_file_to_s3(local_path: str, table_name: str,
                      run_date: str) -> str:
    """
    Upload a local CSV file to S3 with correct partition path.
    Returns the full S3 URI for logging.

    Idempotent — uploading the same key twice overwrites the
    previous file. Safe to retry.
    """
    config   = get_config()
    s3       = get_s3_client()
    filename = os.path.basename(local_path)
    s3_key   = build_s3_key(table_name, run_date, filename)

    s3.upload_file(
        Filename = local_path,
        Bucket   = config["s3_bucket"],
        Key      = s3_key
    )

    s3_uri = f"s3://{config['s3_bucket']}/{s3_key}"
    print(f"  Uploaded -> {s3_uri}")
    return s3_uri


def list_s3_files(table_name: str, run_date: str) -> list:
    """
    List all files in S3 for a given table and date partition.
    Useful for verifying uploads and Lambda 2 file discovery.
    """
    config     = get_config()
    s3         = get_s3_client()
    year, month, day = run_date.split("-")
    prefix     = (f"{table_name}/"
                  f"year={year}/month={month}/day={day}/")

    response = s3.list_objects_v2(
        Bucket = config["s3_bucket"],
        Prefix = prefix
    )

    return [obj["Key"] for obj in
            response.get("Contents", [])]
