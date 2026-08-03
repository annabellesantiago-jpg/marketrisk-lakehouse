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
import time
import logging

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("mcp-server")

# Initialize FastMCP
mcp = FastMCP()

# ── Config from environment variables (set in .env / docker-compose) ─────────
AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://airflow-webserver:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")   

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "")  # e.g. adb-xxxx.azuredatabricks.net
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG", "")
if not DATABRICKS_CATALOG:
    logger.warning("DATABRICKS_CATALOG not set. Databricks tools will fail.")

DAG_ID = "marketrisk_pipeline"  # Airflow DAG to monitor/control
VALID_DESKS = {"FX DESK", "EQUITY DESK", "RATES DESK", "CREDIT DESK"}

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
    )
    resp.raise_for_status()
    result = resp.json()

    # Poll if warehouse is cold and query hasn't finished yet
    statement_id = result.get("statement_id")
    poll_count = 0
    while result.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        if poll_count >= 12:  # 2 minutes max (12 × 10s)
            return {"error": "Query timed out after 2 minutes. Warehouse may still be starting."}
        poll_count += 1
        logger.info(f"Warehouse warming up... polling attempt {poll_count}/12")
        time.sleep(10)
        poll_resp = requests.get(
            f"https://{DATABRICKS_HOST}/api/2.0/sql/statements/{statement_id}",
            headers=headers,
            timeout=30,
        )
        poll_resp.raise_for_status()
        result = poll_resp.json()

    if result.get("status", {}).get("state") == "FAILED":
        error_msg = result.get("status", {}).get("error", {}).get("message", "Unknown error")
        return {"error": f"Query failed: {error_msg}"}

    return result


def parse_databricks_result(resp: dict) -> list[dict]:
    """Parse Databricks SQL API response into a clean list of row dicts."""
    if "error" in resp:
        return [resp]
    try:
        columns = [col["name"] for col in resp["manifest"]["schema"]["columns"]]
        rows = resp.get("result", {}).get("data_array", [])
        return [dict(zip(columns, row)) for row in rows]
    except (KeyError, TypeError):
        return [{"error": "Could not parse Databricks response", "raw": str(resp)[:500]}]

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
        data = airflow_api(f"dags/{DAG_ID}/dagRuns/{run_id}/taskInstances")
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

# All queries use {DATABRICKS_CATALOG} catalog — matches dbt, Airflow COPY INTO, and load_desk_limits.py.
# If multi-environment support is needed, make this env-driven via DATABRICKS_CATALOG.
@mcp.tool()
def get_var_report(desk: str = "ALL") -> str:
    """
    Get the latest Value at Risk (VaR) figures from the Gold Delta table.
    Optionally filter by desk name. Returns VaR at 95th and 99th percentile.
    """
    sql = f"""
        With latest_date AS (
            SELECT MAX(calculation_date) AS max_date
            FROM {DATABRICKS_CATALOG}.gold.var_daily
        )
        SELECT desk, asset_class, var_95_usd, var_99_usd, calculation_date
        FROM {DATABRICKS_CATALOG}.gold.var_daily g, latest_date l
        WHERE calculation_date = l.max_date
    """
    if desk != "ALL":
        if desk.upper() not in VALID_DESKS:
            return f"Invalid desk: {desk}. Valid desks: {', '.join(sorted(VALID_DESKS))}"
        sql += f" AND UPPER(desk) = UPPER('{desk}')"
    sql += " ORDER BY var_99_usd DESC"
 
    try:
        return json.dumps(parse_databricks_result(databricks_sql(sql)), indent=2)
    except Exception as e:
        return f"Could not query Databricks: {e}. Is the Gold layer built yet?"
 
@mcp.tool()
def check_limit_breaches() -> str:
    """
    Check for active limit breaches in the exposure monitor Gold table.
    Returns all desks/counterparties where utilisation exceeds 100% of their limit.
    """
    sql = f"""
        SELECT desk, gross_exposure_usd, limit_usd,
               ROUND(utilisation_pct, 4) AS utilisation_pct,
               limit_status, breach_flag
        FROM  {DATABRICKS_CATALOG}.gold.exposure_monitor
        WHERE breach_flag = true
          AND as_of_date = (SELECT MAX(as_of_date) FROM {DATABRICKS_CATALOG}.gold.exposure_monitor)
        ORDER BY utilisation_pct DESC
    """
    try:
        result = parse_databricks_result(databricks_sql(sql))
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Could not query Databricks: {e}"    
    
@mcp.tool()
def get_pnl_summary(desk: str = "ALL") -> str:
    """
    Get today's PnL attribution breakdown — actual vs hypothetical PnL by desk.
    Large unexplained PnL (actual minus hypothetical) can signal a risk issue.
    """
    sql = f"""
        SELECT desk,
            ROUND(actual_pnl_usd, 2)        AS actual_pnl_usd,
            ROUND(hypothetical_pnl_usd, 2)  AS hypothetical_pnl_usd,
            ROUND(unexplained_pnl_usd, 2) AS unexplained_pnl_usd,
            pnl_date
        FROM {DATABRICKS_CATALOG}.gold.pnl_attribution
        WHERE pnl_date = (SELECT MAX(pnl_date) FROM {DATABRICKS_CATALOG}.gold.pnl_attribution)
    """
    if desk != "ALL":
        if desk.upper() not in VALID_DESKS:
            return f"Invalid desk: {desk}. Valid desks: {', '.join(sorted(VALID_DESKS))}"
        sql += f" AND UPPER(desk) = UPPER('{desk}')"
    sql += " ORDER BY ABS(actual_pnl_usd - hypothetical_pnl_usd) DESC"
 
    try:
        return json.dumps(parse_databricks_result(databricks_sql(sql)), indent=2)
    except Exception as e:
        return f"Could not query Databricks: {e}"
    
@mcp.tool()
def get_table_health() -> str:
    """
    Check row counts for every Bronze, Silver, and Gold Delta table.
    Use this to detect empty tables, failed loads, or unexpected data drops.
    """
    tables = [
        f"{DATABRICKS_CATALOG}.bronze.market_prices",
        f"{DATABRICKS_CATALOG}.bronze.positions",
        f"{DATABRICKS_CATALOG}.bronze.fx_rates",
        f"{DATABRICKS_CATALOG}.bronze.desk_limits",
        f"{DATABRICKS_CATALOG}.silver.prices_cleaned",
        f"{DATABRICKS_CATALOG}.silver.positions_enriched",
        f"{DATABRICKS_CATALOG}.gold.var_daily",
        f"{DATABRICKS_CATALOG}.gold.pnl_attribution",
        f"{DATABRICKS_CATALOG}.gold.exposure_monitor",
        f"{DATABRICKS_CATALOG}.gold.risk_summary",
        f"{DATABRICKS_CATALOG}.gold.var_backtest",
        f"{DATABRICKS_CATALOG}.gold.stress_testing",
        f"{DATABRICKS_CATALOG}.gold.concentration_risk",
        f"{DATABRICKS_CATALOG}.gold.equity_sensitivity",
        f"{DATABRICKS_CATALOG}.gold.fx_sensitivity",
        f"{DATABRICKS_CATALOG}.gold.rates_credit_sensitivity",
        f"{DATABRICKS_CATALOG}.gold.limit_breach_log",
        f"{DATABRICKS_CATALOG}.gold.risk_adjusted_performance",
    ]
    results = {}
    for table in tables:
        try:
            resp = databricks_sql(f"SELECT COUNT(*) AS row_count FROM {table}")
            parsed = parse_databricks_result(resp)
            results[table] = parsed[0].get("row_count", "error") if parsed else "error"
        except Exception as e:
            results[table] = f"error: {e}"
    return json.dumps(results, indent=2)

# ── OPERATIONAL TOOLS ─────────────────────────────────────────────────────────

@mcp.tool()
def trigger_pipeline_run(reason: str="agent-triggered", run_date: str="") -> str:
    """
    Trigger a full pipeline run via the Airflow REST API.
    Use this when you want to kick off a fresh ingestion → transform → load cycle.
    Always state a reason so the audit log is clear.
    """
    try:
        data = {
            "conf": {"triggered_by": "mcp_agent", "reason": reason}
        }
        if run_date:
            data["logical_date"] = f"{run_date}T00:00:00+00:00"
        result = airflow_api(method="POST", path=f"dags/{DAG_ID}/dagRuns", data=data)
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
def list_s3_files(prefix: str = "") -> str:
    """
    List raw files in the S3 landing zone bucket.
    Use prefix to filter — e.g. prefix='raw/prices/' shows only price files.
    Useful for verifying ingestion actually wrote files before Databricks runs.
    """
    try:
        s3 = boto3.client("s3",
            region_name=os.getenv("AWS_REGION", "ap-southeast-2"),
        )
        bucket = os.getenv("S3_BUCKET", "")
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = response.get("Contents", [])
        file_list = [
            {
                "filename": f["Key"],
                "size_kb": round(f["Size"] / 1024, 1),
                "last_modified": f["LastModified"].strftime("%Y-%m-%d %H:%M:%S"),
            } for f in files]

        if not file_list:
            return f"No files found in bucket '{bucket}' with prefix '{prefix}'."
        return json.dumps(file_list, indent=2)
    except Exception as e:
        return f"Could not connect to S3: {e}"
    
# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logger.info("Starting MCP Server on port 8888...")
    mcp.run(host="0.0.0.0", port=8888, transport="sse")
