import psycopg2
import psycopg2.extras
from utils.config import get_config


def get_connection():
    """
    Return a psycopg2 connection to RDS PostgreSQL.
    Every Lambda and script imports this — no scattered
    connection strings across the codebase.
    """
    config = get_config()
    return psycopg2.connect(
        host     = config["db_host"],
        dbname   = config["db_name"],
        user     = config["db_user"],
        password = config["db_password"],
        port     = config["db_port"],
        connect_timeout = 10
    )


def execute_query(query: str, params: tuple = None):
    """
    Execute a single query that doesn't return rows.
    Used for CREATE, INSERT, DELETE, UPDATE statements.
    Auto-commits and closes connection when done.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def execute_many(query: str, records: list):
    """
    Execute a query for multiple rows efficiently.
    Uses psycopg2 execute_values for bulk inserts —
    much faster than looping execute() one row at a time.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur, query, records,
                page_size=1000
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def fetch_one(query: str, params: tuple = None):
    """
    Execute a query and return a single row.
    Used for metadata lookups and watermark checks.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchone()
    finally:
        conn.close()


def fetch_all(query: str, params: tuple = None):
    """
    Execute a query and return all rows.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()
    finally:
        conn.close()
