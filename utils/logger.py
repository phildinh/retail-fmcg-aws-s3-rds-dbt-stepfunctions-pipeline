from datetime import datetime
from utils.db import execute_query, fetch_one, get_connection


def log_run_start(run_date: str, run_timestamp: str) -> int:
    """
    Insert a new pipeline run record and return its run_id.
    Called at the start of Lambda 2 before any loading begins.
    """
    query = """
        INSERT INTO gold.pipeline_runs
            (run_date, run_timestamp, status, started_at)
        VALUES
            (%s, %s, 'running', %s)
        RETURNING run_id;
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, (
                run_date,
                run_timestamp,
                datetime.now()
            ))
            run_id = cur.fetchone()[0]
        conn.commit()
        return run_id
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def log_run_success(run_id: int, rows_loaded: int):
    """
    Update pipeline run record with success status and row count.
    Called after all tables are loaded successfully.
    """
    query = """
        UPDATE gold.pipeline_runs
        SET status      = 'success',
            rows_loaded = %s,
            finished_at = %s
        WHERE run_id = %s;
    """
    execute_query(query, (rows_loaded, datetime.now(), run_id))
    print(f"  Pipeline run {run_id} logged as success "
          f"({rows_loaded} rows)")


def log_run_failure(run_id: int, error_message: str):
    """
    Update pipeline run record with failure status and error.
    Called in the except block when any loading step fails.
    """
    query = """
        UPDATE gold.pipeline_runs
        SET status        = 'failed',
            error_message = %s,
            finished_at   = %s
        WHERE run_id = %s;
    """
    execute_query(query, (
        str(error_message)[:500],
        datetime.now(),
        run_id
    ))
    print(f"  Pipeline run {run_id} logged as failed")


def get_last_loaded_timestamp() -> str:
    """
    Get the last successfully loaded run_timestamp across all runs.
    Used by dbt incremental model as the watermark.
    Returns None if no previous successful runs exist.
    """
    query = """
        SELECT MAX(run_timestamp)
        FROM gold.pipeline_runs
        WHERE status = 'success';
    """
    result = fetch_one(query)
    return result[0] if result and result[0] else None
