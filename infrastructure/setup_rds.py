import sys
import os
sys.path.insert(0, os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
))

from utils.db import execute_query


def create_schemas():
    print("Creating schemas...")
    execute_query("CREATE SCHEMA IF NOT EXISTS staging;")
    execute_query("CREATE SCHEMA IF NOT EXISTS gold;")
    print("  staging and gold schemas created")


def create_staging_tables():
    print("Creating staging tables...")

    execute_query("""
        CREATE TABLE IF NOT EXISTS staging.raw_products (
            product_id    VARCHAR(20),
            product_name  VARCHAR(100),
            category      VARCHAR(50),
            brand         VARCHAR(50),
            supplier      VARCHAR(100),
            unit_cost     NUMERIC(10,2),
            unit_price    NUMERIC(10,2),
            loaded_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    execute_query("""
        CREATE TABLE IF NOT EXISTS staging.raw_stores (
            store_id    VARCHAR(20),
            store_name  VARCHAR(100),
            state       VARCHAR(10),
            region      VARCHAR(20),
            store_type  VARCHAR(30),
            city        VARCHAR(50),
            loaded_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    execute_query("""
        CREATE TABLE IF NOT EXISTS staging.raw_customers (
            customer_id   VARCHAR(20),
            age_group     VARCHAR(10),
            loyalty_tier  VARCHAR(20),
            state         VARCHAR(10),
            loaded_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    execute_query("""
        CREATE TABLE IF NOT EXISTS staging.raw_sales (
            transaction_id    VARCHAR(30),
            transaction_date  DATE,
            product_id        VARCHAR(20),
            store_id          VARCHAR(20),
            customer_id       VARCHAR(20),
            quantity          INTEGER,
            unit_price        NUMERIC(10,2),
            discount_pct      NUMERIC(5,2),
            total_amount      NUMERIC(10,2),
            created_at        TIMESTAMP,
            loaded_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    print("  All staging tables created")


def create_gold_tables():
    print("Creating gold tables...")

    execute_query("""
        CREATE TABLE IF NOT EXISTS gold.pipeline_runs (
            run_id        SERIAL PRIMARY KEY,
            run_date      DATE,
            run_timestamp TIMESTAMP,
            table_name    VARCHAR(50),
            status        VARCHAR(20),
            rows_loaded   INTEGER,
            error_message VARCHAR(500),
            started_at    TIMESTAMP,
            finished_at   TIMESTAMP
        );
    """)

    print("  Gold tables created")


def main():
    print("Setting up RDS schemas and tables...")
    print("=" * 50)
    create_schemas()
    create_staging_tables()
    create_gold_tables()
    print("=" * 50)
    print("RDS setup complete!")


if __name__ == "__main__":
    main()