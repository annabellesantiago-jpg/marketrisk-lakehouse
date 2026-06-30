"""
config.py
─────────────────────────────────────────────────────────────────────────────
Central configuration for all ingestion scripts.

Sensitive values (AWS credentials, API keys) are NEVER hardcoded here.
They are read from environment variables or the .env file.
.env is listed in .gitignore and must never be committed to Git.

This file contains only:
  - Non-sensitive shared constants (bucket names, prefixes, tickers)
  - Sensitive values read from environment (credentials, tokens)
  - Business reference data (desk limits, counterparties, FX rates)
─────────────────────────────────────────────────────────────────────────────
"""

import os
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load .env file if present (local development only).
# In production (Airflow on EC2), environment variables are set directly
# on the host and .env is not used.
load_dotenv()

# -- sysdate to be used for partitioning of the file in S3 : year/month/day
_run_dt    = datetime.strptime(os.getenv("RUN_DATE", datetime.now(timezone.utc).strftime("%Y-%m-%d")), "%Y-%m-%d")
RUN_DATE   = _run_dt.strftime("%Y-%m-%d")   # kept for logging/display
RUN_YEAR   = _run_dt.strftime("%Y")
RUN_MONTH  = _run_dt.strftime("%m")
RUN_DAY    = _run_dt.strftime("%d")


# ── AWS S3 ────────────────────────────────────────────────────────────────
# Single source of truth for the S3 bucket and region.
# All ingestion scripts import these — change here to update everywhere.
S3_BUCKET  = "market-risk-dev-raw-593153124201-ap-southeast-2-an"
AWS_REGION = "ap-southeast-2"

# S3 prefix (folder) for each data type.
# Matches the Databricks External Location path structure.
S3_PREFIX_PRICES    = "prices/"
S3_PREFIX_POSITIONS = "positions/"
S3_PREFIX_REFERENCE = "reference/"

# AWS credentials — read from environment, never hardcoded.
# Set these in your .env file for local development:
#   AWS_ACCESS_KEY_ID=your_key
#   AWS_SECRET_ACCESS_KEY=your_secret
# boto3 also reads from ~/.aws/credentials automatically if set via aws configure.
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")

# ── Yahoo Finance — market data tickers ──────────────────────────────────
# Raw Yahoo Finance ticker symbols — used by fetch_market_data.py as-is.
# Bronze stores these raw symbols; Silver normalizes them to match positions:
#   EURUSD=X  →  EURUSD    (remove =X suffix)
#   HSBA.L    →  HSBA_L    (replace . with _)
FX_TICKERS = [
    "EURUSD=X",
    "GBPUSD=X",
    "JPYUSD=X",
    "SGDUSD=X",
    "AUDUSD=X",
]

EQUITY_TICKERS = [
    "AAPL",
    "MSFT",
    "JPM",
    "GS",
    "HSBA.L",
    "SAN.MC",
]

# Combined list used by fetch_market_data.py
ALL_TICKERS = FX_TICKERS + EQUITY_TICKERS

# ── Price history ─────────────────────────────────────────────────────────
# Calendar days of price history to fetch from Yahoo Finance.
# 400 days guarantees at least 260 trading days after weekends and
# holidays are removed — the minimum required for VaR historical simulation.
PRICE_HISTORY_DAYS = 400

# ── Position generation ───────────────────────────────────────────────────
TOTAL_POSITIONS = 300   # 75 positions per desk × 4 desks
RANDOM_SEED     = 42    # Fixed seed — ensures reproducible synthetic data

# Trading desks — must match desk names used in dbt Gold models and Apache Superset
DESKS = [
    "FX Desk",
    "Equity Desk",
    "Rates Desk",
    "Credit Desk",
]

# Counterparties — real bank names used as synthetic trade counterparties
COUNTERPARTIES = [
    "Goldman Sachs",
    "JP Morgan",
    "Deutsche Bank",
    "BNP Paribas",
    "Citigroup",
    "HSBC",
    "Barclays",
    "UBS",
]

# Trader IDs per desk — each desk has 5 traders
# Format: T{desk_code}{sequence}
TRADER_IDS = {
    "FX Desk":     ["T-FX-001", "T-FX-002", "T-FX-003", "T-FX-004", "T-FX-005"],
    "Equity Desk": ["T-EQ-001", "T-EQ-002", "T-EQ-003", "T-EQ-004", "T-EQ-005"],
    "Rates Desk":  ["T-RT-001", "T-RT-002", "T-RT-003", "T-RT-004", "T-RT-005"],
    "Credit Desk": ["T-CR-001", "T-CR-002", "T-CR-003", "T-CR-004", "T-CR-005"],
}

# ── Desk risk limits (USD) ────────────────────────────────────────────────
# Risk limits approved by the board risk committee.
# Used by: fetch_reference_data.py (generates desk_limits.csv)
#          Gold model exposure_monitor reads from bronze.desk_limits
# Change here if limits are revised — only one place to update.
DESK_LIMITS = {
    "FX Desk":     10_000_000,   # USD 10M
    "Equity Desk": 15_000_000,   # USD 15M
    "Rates Desk":  20_000_000,   # USD 20M
    "Credit Desk":  8_000_000,   # USD  8M
}

# ── Databricks ────────────────────────────────────────────────────────────
# Connection details read from environment — never hardcoded.
# dbt connection lives in ~/.dbt/profiles.yml, not here.
DATABRICKS_CATALOG = "market_risk_dev"
DATABRICKS_HOST    = os.getenv("DATABRICKS_HOST", "")
DATABRICKS_TOKEN   = os.getenv("DATABRICKS_TOKEN", "")

# ── API keys ──────────────────────────────────────────────────────────────
ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY", "")