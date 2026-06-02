"""
MarketRisk Lakehouse — MCP Server
Exposes 9 tools for monitoring and operating the pipeline.
Claude Desktop connects to this server to get a full agentic loop.
"""

from fastmcp import FastMCP
import boto3
import os
import requests
import json

# Initialize FastMCP
mcp = FastMCP()

# ── Config from environment variables (set in .env / docker-compose) ─────────
AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://airflow-webserver:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")   

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "")  # e.g. adb-xxxx.azuredatabricks.net
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
DATABRICK_WAREHOUSE_ID = os.getenv("DATABRICK_WAREHOUSE_ID", "")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "marketrisk-raw")

DAG_ID = "marketrisk_pipeline"  # Airflow DAG to monitor/control

# ── Helpers ───────────────────────────────────────────────────────────────────

def airflow_api(path, method="GET", data=None):
    url = f"{AIRFLOW_URL}/api/v1/{path}"
    auth = (AIRFLOW_USER, AIRFLOW_PASSWORD)
    headers = {"Content-Type": "application/json"}
    response = requests.request(method, url, auth=auth, headers=headers, json=data, timeout=10)
    response.raise_for_status()
    return response.json()

def databricks_sql(statement: str) -> dict:
    """Execute SQL on Databricks SQL Warehouse and wait for result."""
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    resp = requests.post(
        f"https://{DATABRICKS_HOST}/api/2.0/sql/statements",
        headers=headers,
        json={
            "statement": statement,
            "warehouse_id": DATABRICKS_WAREHOUSE_ID,
            "wait_timeout": "30s",
            "on_wait_timeout": "CONTINUE",
        },
        timeout=60,
    ).json()
    return resp

# ── MONITORING TOOLS ──────────────────────────────────────────────────────────

@mcp.tool()
def get_pipeline_status():
    """
    Check the status of the last Airflow pipeline run.
    Returns state (success/failed/running), start time, end time, and run ID.
    """
    try:
        data = airflow_api(f"dags/{DAG_ID}/dagRuns?limit=3&order_by=-start_date")
        runs = data.get("dag_runs", [])
        if not runs:
            return "No pipeline runs found yet. The DAG has not been triggered."
        summary = []
        for r in runs:
            summary.append({
                "run_id": r["dag_run_id"],
                "status": r["state"],
                "started": r.get("start_date", "unknown"),
                "ended": r.get("end_date", "still running"),
            })
        return json.dumps(summary, indent=2)
    except Exception as e:
        return f"Could not reach Airflow. Is it running? Error: {e}"
    
@mcp.tool()
def get_task_statuses(run_id: str) -> str:
    """
    Get the status of each individual task within a specific pipeline run.
    Use get_pipeline_status first to get a valid run_id.
    """
    try:
        data = airflow_get(f"dags/{DAG_ID}/dagRuns/{run_id}/taskInstances")
        tasks = data.get("task_instances", [])
        result = [
            {
                "task": t["task_id"],
                "status": t["state"],
                "duration_secs": t.get("duration"),
                "start": t.get("start_date"),
            }
            for t in tasks
        ]
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error fetching task statuses: {e}"
    
@mcp.tool()
def get_var_report(desk: str = "ALL") -> str:
    """
    Get the latest Value at Risk (VaR) figures from the Gold Delta table.
    Optionally filter by desk name. Returns VaR at 95th and 99th percentile.
    """
    sql = """
        With latest_date AS (
            SELECT MAX(calculation_date) AS max_date
            FROM gold.gold_var_daily
        )
        SELECT desk, asset_class, var_95, var_99, calculation_date
        FROM gold.gold_var_daily g, latest_date l
        WHERE calculation_date = l.max_date
    """
    if desk != "ALL":
        sql += f" AND UPPER(desk) = UPPER('{desk}')"
    sql += " ORDER BY var_99 DESC"
 
    try:
        return json.dumps(databricks_sql(sql), indent=2)
    except Exception as e:
        return f"Could not query Databricks: {e}. Is the Gold layer built yet?"
 
@mcp.tool()
def check_limit_breaches() -> str:
    """
    Check for active limit breaches in the exposure monitor Gold table.
    Returns all desks/counterparties where utilisation exceeds 100% of their limit.
    """
    sql = """
        SELECT desk, counterparty, exposure_usd, limit_usd,
               ROUND(utilisation_pct, 2) AS utilisation_pct,
               breach_flag, breach_since_date
        FROM gold.gold_exposure_monitor
        WHERE breach_flag = true
          AND as_of_date = (SELECT MAX(as_of_date) FROM gold.gold_exposure_monitor)
        ORDER BY utilisation_pct DESC
    """
    try:
        result = databricks_sql(sql)
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Could not query Databricks: {e}"    
    
@mcp.tool()
def get_pnl_summary(desk: str = "ALL") -> str:
    """
    Get today's PnL attribution breakdown — actual vs hypothetical PnL by desk.
    Large unexplained PnL (actual minus hypothetical) can signal a risk issue.
    """
    sql = """
        SELECT desk,
               ROUND(actual_pnl, 2)        AS actual_pnl,
               ROUND(hypothetical_pnl, 2)  AS hypothetical_pnl,
               ROUND(actual_pnl - hypothetical_pnl, 2) AS unexplained_pnl,
               pnl_date
        FROM gold.gold_pnl_attribution
        WHERE pnl_date = (SELECT MAX(pnl_date) FROM gold.gold_pnl_attribution)
    """
    if desk != "ALL":
        sql += f" AND UPPER(desk) = UPPER('{desk}')"
    sql += " ORDER BY ABS(actual_pnl - hypothetical_pnl) DESC"
 
    try:
        return json.dumps(databricks_sql(sql), indent=2)
    except Exception as e:
        return f"Could not query Databricks: {e}"
    
@mcp.tool()
def get_table_health() -> str:
    """
    Check row counts for every Bronze, Silver, and Gold Delta table.
    Use this to detect empty tables, failed loads, or unexpected data drops.
    """
    tables = [
        "bronze.bronze_market_prices",
        "bronze.bronze_positions",
        "bronze.bronze_reference",
        "silver.silver_prices_cleaned",
        "silver.silver_positions_enriched",
        "gold.gold_var_daily",
        "gold.gold_pnl_attribution",
        "gold.gold_exposure_monitor",
    ]
    results = {}
    for table in tables:
        try:
            resp = databricks_sql(f"SELECT COUNT(*) AS row_count FROM {table}")
            results[table] = resp
        except Exception as e:
            results[table] = f"error: {e}"
    return json.dumps(results, indent=2)

# ── OPERATIONAL TOOLS ─────────────────────────────────────────────────────────

@mcp.tool()
def trigger_pipeline_run(reason: str = "agent-triggered") -> str:
    """
    Trigger a full pipeline run via the Airflow REST API.
    Use this when you want to kick off a fresh ingestion → transform → load cycle.
    Always state a reason so the audit log is clear.
    """
    try:
        result = airflow_api(method="POST", path=f"dags/{DAG_ID}/dagRuns", data={
            "conf": {"triggered_by": "mcp_agent", "reason": reason}
        })
        run_id = result.get("dag_run_id", "unknown")
        return f"Pipeline triggered successfully. Run ID: {run_id}. Monitor with get_pipeline_status()."
    except Exception as e:
        return f"Failed to trigger pipeline: {e}"
    
@mcp.tool()
def rerun_failed_tasks(run_id: str) -> str:
    """
    Clear and rerun only the failed tasks in a specific pipeline run.
    Use get_task_statuses first to confirm which tasks failed before calling this.
    """
    try:
        result = airflow_api(
            method="POST", path=f"dags/{DAG_ID}/dagRuns/{run_id}/clear", data={
                "dry_run": False, "reset_dag_runs": False, "only_failed": True},
        )
        return f"Failed tasks cleared and queued for rerun. Response: {json.dumps(result, indent=2)}"
    except Exception as e:
        return f"Error clearing tasks: {e}"
    
@mcp.tool()
def list_minio_files(prefix: str = "") -> str:
    """
    List raw files in the MinIO landing zone bucket.
    Use prefix to filter — e.g. prefix='prices/' shows only price files.
    Useful for verifying ingestion actually wrote files before Databricks runs.
    """
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_USER,
            aws_secret_access_key=MINIO_PASS,
        )
        response = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix)
        files = response.get("Contents", [])
        file_list = [
            {
                "filename": f["Key"],
                "size_kb": round(f["Size"] / 1024, 1),
                "last_modified": f["LastModified"].strftime("%Y-%m-%d %H:%M:%S"),
             } for f in files]
        
        if not file_list:
            return f"No files found in bucket '{MINIO_BUCKET}' with prefix '{prefix}'."
        return json.dumps(file_list, indent=2)
    except Exception as e:
        return f"Could not connect to MinIO: {e}"
    
# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Starting MCP Server on port 8888...")
    mcp.run(host="0.0.0.0", port=8888, transport="sse")
