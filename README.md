# MarketRisk Lakehouse Pipeline

A production-grade market risk analytics platform built on a Medallion architecture (Bronze / Silver / Gold), designed to mirror how a Tier-1 bank's risk technology team would deliver regulatory-compliant risk reporting under BCBS 239 and FRTB.

The pipeline ingests live market data, generates synthetic trading positions, transforms them through a three-layer Delta Lakehouse, and surfaces risk metrics via interactive dashboards and an Agentic AI layer powered by Claude Desktop.

---

## Table of Contents

1. [Business Context](#business-context)
2. [Architecture Overview](#architecture-overview)
3. [Tech Stack](#tech-stack)
4. [Medallion Architecture](#medallion-architecture)
5. [Project Structure](#project-structure)
6. [Prerequisites](#prerequisites)
7. [Environment Setup](#environment-setup)
8. [Running the Pipeline](#running-the-pipeline)
9. [Airflow Orchestration](#airflow-orchestration)
10. [dbt Transformations](#dbt-transformations)
11. [Testing](#testing)
12. [MCP Server (Agentic AI)](#mcp-server-agentic-ai)
13. [Dashboards (Apache Superset)](#dashboards-apache-superset)
14. [Monitoring (Grafana + Prometheus)](#monitoring-grafana--prometheus)
15. [CI/CD (GitHub Actions)](#cicd-github-actions)
16. [Key Design Decisions](#key-design-decisions)
17. [Skills Demonstrated](#skills-demonstrated)

---

## Business Context

Market risk teams at investment banks must report daily Value-at-Risk (VaR), stress-test results, P&L attribution, and limit utilisation to regulators and senior management. This project replicates that workflow end-to-end: from raw market data ingestion through to CRO-level executive dashboards, with full audit trails and data quality gates at every layer.

Regulatory frameworks addressed: BCBS 239 (risk data aggregation and reporting), FRTB (Fundamental Review of the Trading Book), and Basel III capital adequacy.

---

## Architecture Overview

The system follows a six-stage pipeline:

**Ingest** (Python) &rarr; **Land** (AWS S3) &rarr; **Bronze** (COPY INTO Delta) &rarr; **Silver** (dbt table models) &rarr; **Gold** (dbt incremental MERGE) &rarr; **Serve** (Superset + MCP)

Data flows from Yahoo Finance and synthetic generators, through S3 with Hive-style date partitions, into Databricks Unity Catalog as Delta tables. dbt Core handles all Silver and Gold transformations. Apache Airflow orchestrates the daily run, and downstream consumers (Superset dashboards, Claude Desktop via MCP) query the Gold layer directly.

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Ingestion | Python 3.12, yfinance, boto3 | Fetch live OHLCV data, generate positions, upload to S3 |
| Storage | AWS S3 | Landing zone with Hive date partitions (`raw/prices/year=/month=/day=/`) |
| Lakehouse | Databricks (Unity Catalog), Delta Lake | ACID transactions, time travel, schema enforcement |
| Transform | dbt Core 1.11, dbt-databricks | 14 SQL models + 2 seeds across Silver and Gold |
| Orchestration | Apache Airflow 2.x | Daily DAG with 9 tasks, SLA monitoring, Slack alerts |
| Dashboards | Apache Superset | 7 risk dashboards with RBAC and row-level security |
| Agentic AI | FastMCP, Claude Desktop | 9 MCP tools for natural-language risk queries |
| Monitoring | Grafana, Prometheus, StatsD Exporter | Pipeline health dashboards, metrics scraping |
| Testing | pytest, dbt unit tests | 65 Python tests + 7 dbt unit tests |
| CI/CD | GitHub Actions | Automated testing on push/PR + desk_limits loading on CSV change |
| Infrastructure | Docker Compose (10 services) | Single-command local deployment |

---

## Medallion Architecture

### Bronze (Raw)

Four raw Delta tables loaded via `COPY INTO` with no transformations. Schema is enforced on read.

| Table | Source | Rows (approx) |
|-------|--------|---------------|
| `market_prices` | Yahoo Finance (11 tickers) | ~2,800 |
| `positions` | Synthetic generator (4 desks) | 300 |
| `fx_rates` | Reference data script | 6 |
| `desk_limits` | CSV (annual board review) | 4 |

Plus 2 dbt seeds loaded into Bronze:

| Seed | Rows |
|------|------|
| `scenario_definitions` (stress_scenarios.csv) | 24 |
| `ticker_classifications` | 11 |

### Silver (Cleaned & Enriched)

Two dbt `table` models that always rebuild from full Bronze history:

| Model | Key Transformations |
|-------|-------------------|
| `prices_cleaned` | Ticker normalisation via `REGEXP_REPLACE`, NULL removal, deduplication |
| `positions_enriched` | FX-to-USD conversion, price join, `direction_multiplier`, 5 derived columns |

### Gold (Business Analytics)

Twelve dbt `incremental` models using `MERGE` strategy. History accumulates over time; `on_schema_change: fail` guards against unintended drift.

| Model | Risk Metric |
|-------|------------|
| `var_daily` | VaR at 95%, 97.5%, 99% confidence + Expected Shortfall |
| `var_backtest` | Basel traffic-light backtesting |
| `pnl_attribution` | Actual vs hypothetical P&L decomposition |
| `exposure_monitor` | Limit utilisation by desk |
| `fx_sensitivity` | FX Delta and Net Open Position |
| `equity_sensitivity` | Equity Delta |
| `rates_credit_sensitivity` | PV01 and CS01 |
| `stress_testing` | 6 macro scenarios + stressed VaR proxy |
| `concentration_risk` | Herfindahl-Hirschman Index across 6 dimensions |
| `risk_summary` | CRO executive dashboard aggregate |
| `limit_breach_log` | Audit trail of all limit breaches |
| `risk_adjusted_performance` | Risk-Adjusted Return on Capital (RAROC) |

---

## Project Structure

```
marketrisk-lakehouse/
├── .github/workflows/          # CI/CD pipelines
│   ├── ci.yml                  # Unit tests on push/PR + dbt tests on merge
│   ├── load_desk_limits.yml    # Auto-load limits on CSV change
│   └── setup_environment.yml   # Manual environment validation
├── airflow/                    # Orchestration
│   ├── dags/marketrisk_pipeline.py  # 9-task DAG
│   ├── Dockerfile
│   └── requirements.txt
├── data/raw/reference/         # Reference data (desk_limits.csv)
├── databricks/notebooks/       # Databricks setup & Bronze ingest notebooks
├── dbt/marketrisk/             # dbt project
│   ├── models/
│   │   ├── sources.yml         # Bronze source definitions
│   │   ├── _unit_tests.yml     # dbt unit tests (7 tests)
│   │   ├── silver/             # 2 cleaning/enrichment models + _silver.yml
│   │   └── gold/               # 12 business models + _gold.yml
│   ├── macros/                 # cast_to_double, cast_to_int, cast_to_string
│   ├── seeds/                  # stress_scenarios.csv, ticker_classifications.csv
│   └── dbt_project.yml
├── docs/drawio/                # Architecture diagrams (8 draw.io files)
├── ingestion/                  # Python ingestion scripts
│   ├── config.py               # Centralised config (env vars, constants)
│   ├── fetch_market_data.py    # Yahoo Finance OHLCV fetcher with retry
│   ├── fetch_reference_data.py # FX rates from latest price files
│   ├── generate_positions.py   # Synthetic trading book (4 desks, 300 positions)
│   ├── load_desk_limits.py     # Desk limits loader (CI/CD triggered)
│   └── s3_utils.py             # S3 upload/read helpers
├── mcp-server/                 # Agentic AI layer
│   ├── server.py               # FastMCP server (9 tools)
│   ├── Dockerfile
│   └── requirements.txt
├── monitoring/                 # Observability
│   ├── grafana/                # Dashboard JSON + provisioning
│   └── prometheus/prometheus.yml
├── superset/                   # Dashboard layer
│   ├── dashboards/             # Exported dashboard JSON configs
│   ├── Dockerfile
│   ├── superset_config.py
│   └── superset-init.sh
├── tests/                      # Python unit tests (pytest)
│   ├── conftest.py             # Path configuration
│   ├── test_generate_positions.py   # 28 tests — book shape, IDs, ISIN rules
│   ├── test_fetch_market_data.py    # 16 tests — ticker normalisation, retry
│   ├── test_fetch_reference_data.py # 9 tests — FX rates, USD inclusion
│   └── test_server.py              # 12 tests — Databricks parser, Airflow API
├── docker-compose.yml          # 10-service stack
├── pyproject.toml              # pytest configuration
├── requirements.txt            # Python dependencies
└── README.md
```

---

## Prerequisites

- Python 3.12+
- Docker & Docker Compose
- Node.js 18+ (for MCP bridge: `npx mcp-remote`)
- A Databricks workspace with Unity Catalog enabled
- An AWS account with an S3 bucket
- Claude Desktop (for the Agentic AI layer)

---

## Environment Setup

1. **Clone the repository:**

   ```bash
   git clone https://github.com/<your-username>/marketrisk-lakehouse.git
   cd marketrisk-lakehouse
   ```

2. **Create your `.env` file:**

   ```bash
   cp .env.example .env
   ```

   Fill in all values. Required credentials: AWS (access key, secret, region, bucket), Databricks (host, token, warehouse ID, HTTP path, catalog), Grafana admin creds, Airflow Fernet/secret keys, and Slack webhook URL. **Never commit `.env` to git.**

3. **Create a Python virtual environment:**

   ```bash
   python -m venv .venv
   source .venv/bin/activate    # Linux/macOS
   .venv\Scripts\activate       # Windows
   pip install -r requirements.txt
   ```

4. **Set up Databricks:**

   Run `databricks/notebooks/00_setup.sql` in your Databricks workspace to create the catalog, schemas (bronze, silver, gold), and external storage credential.

5. **Install dbt dependencies:**

   ```bash
   cd dbt/marketrisk
   dbt deps
   cd ../..
   ```

6. **Start the Docker stack:**

   ```bash
   docker-compose up -d
   ```

   This brings up 10 services: Postgres, Airflow (init + webserver + scheduler), MCP Server, Redis, Superset (init + app), StatsD Exporter, Prometheus, and Grafana.

---

## Running the Pipeline

### Manual Execution (step-by-step)

```bash
# 1. Ingest data to S3
python ingestion/fetch_market_data.py
python ingestion/fetch_reference_data.py
python ingestion/generate_positions.py

# 2. Bronze load (run in Databricks or via Airflow)
#    Uses COPY INTO from S3 into Delta tables

# 3. dbt seed (load reference CSVs)
cd dbt/marketrisk
dbt seed --profiles-dir .

# 4. dbt run (Silver + Gold)
dbt run --profiles-dir .

# 5. dbt test (schema + unit tests)
dbt test --profiles-dir .
```

### Full Refresh

To rebuild all tables from scratch (e.g., after fixing a model):

```bash
cd dbt/marketrisk
dbt run --full-refresh --profiles-dir .
```

### Via Airflow

Trigger the `marketrisk_pipeline` DAG from the Airflow UI at `http://localhost:8080`. The DAG runs the full pipeline end-to-end.

---

## Airflow Orchestration

**DAG:** `marketrisk_pipeline`
**Schedule:** `0 18 * * 1-5` (Monday-Friday, 6 PM SGT — after market close)

### Task Graph

```
ingest_prices
├── bronze_ingest_prices
├── bronze_ingest_positions
└── bronze_ingest_fx_rates
        └── dbt_seed
              └── dbt_silver
                    └── dbt_test_silver
                          └── dbt_gold
                                └── dbt_test_gold
```

Features: SLA monitoring with configurable thresholds, Slack alerts on failure or SLA miss, retry logic with exponential backoff.

---

## dbt Transformations

- **14 SQL models**: 2 Silver (table materialisation) + 12 Gold (incremental with MERGE)
- **2 seeds**: `stress_scenarios.csv` (24 rows, 6 macro scenarios) and `ticker_classifications.csv` (11 tickers)
- **Schema tests**: Defined in `_silver.yml` and `_gold.yml` covering not_null, unique, accepted_values, and relationships
- **Unit tests**: 7 dbt unit tests in `_unit_tests.yml` validating transformation logic (ticker normalisation, deduplication, FX conversion, direction multiplier, traffic-light breach logic)
- **Incremental strategy**: Gold models use `merge` on declared `unique_key`; `on_schema_change: fail` prevents silent column drift
- **`run_date` variable**: Passed via `--vars '{"run_date": "2025-07-15"}'` for point-in-time runs; defaults to `1900-01-01` (processes all dates)

---

## Testing

### Python Unit Tests (pytest)

65 tests across 4 test files, run via `python -m pytest tests/ -v`:

| File | Tests | What it covers |
|------|-------|---------------|
| `test_generate_positions.py` | 28 | Book shape, trade IDs, desk-asset class integrity, ISIN/CUSIP rules, notional ranges, direction, counterparties, traders, dates, determinism, `validate()` edge cases |
| `test_fetch_market_data.py` | 16 | Ticker-to-filename normalisation, `fetch_ticker` with mocked yfinance (retry logic, FX volume NULL), `file_exists_in_s3` with mocked boto3 |
| `test_fetch_reference_data.py` | 9 | `get_live_fx_rates` with mocked S3, `generate_fx_rates` output (USD at 1.0, correct structure) |
| `test_server.py` | 12 | `parse_databricks_result` parsing, `airflow_api` with mocked requests, `VALID_DESKS` constant, `databricks_sql` polling logic |

All external calls (yfinance, boto3, requests) are mocked — no credentials or network access needed.

### dbt Unit Tests

7 unit tests in `models/_unit_tests.yml`, run via `dbt test --select "test_type:unit"`:

| Test | Model | What it validates |
|------|-------|------------------|
| `test_prices_cleaned_null_filter_and_ticker_normalization` | prices_cleaned | NULL Close filtered, EURUSD=X &rarr; EURUSD, HSBA.L &rarr; HSBA_L |
| `test_prices_cleaned_deduplication` | prices_cleaned | Keeps latest `_ingested_at` per ticker+date |
| `test_positions_enriched_long_usd` | positions_enriched | LONG direction_multiplier=1, notional_usd = notional &times; fx_rate |
| `test_positions_enriched_short_eur` | positions_enriched | SHORT direction_multiplier=-1, negative net_exposure_usd |
| `test_exposure_monitor_green` | exposure_monitor | Utilisation &lt; 80% &rarr; GREEN, no flags |
| `test_exposure_monitor_breach` | exposure_monitor | Utilisation &ge; 100% &rarr; RED, breach_flag=true |
| `test_exposure_monitor_amber_warning` | exposure_monitor | Utilisation 80-99% &rarr; AMBER, warning_flag=true |

dbt unit tests use mock inputs (Given/Expect pattern) and execute against Databricks.

### dbt Schema Tests

Column-level constraints defined in `_silver.yml` and `_gold.yml` covering not_null, unique, accepted_values, and relationships. These run against actual warehouse data during `dbt test`.

---

## MCP Server (Agentic AI)

The MCP server enables Claude Desktop to query risk data and operate the pipeline using natural language.

**Transport:** streamable-http on port 8888
**Framework:** FastMCP

### Tools

| Tool | Description |
|------|------------|
| `get_pipeline_status` | Latest Airflow DAG run status |
| `get_task_statuses` | Individual task statuses for a given run |
| `get_var_report` | VaR report by desk (95/97.5/99% + ES) |
| `check_limit_breaches` | Current limit breach alerts |
| `get_pnl_summary` | P&L attribution summary by desk |
| `get_table_health` | Row counts and freshness across all layers |
| `trigger_pipeline_run` | Trigger a new DAG run with optional run_date |
| `rerun_failed_tasks` | Rerun failed tasks in a specific run |
| `list_s3_files` | List files in the S3 landing zone |

### Claude Desktop Configuration

Add to your Claude Desktop MCP config (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "marketrisk": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "http://localhost:8888/mcp"]
    }
  }
}
```

Cold warehouse handling: the server polls up to 12 retries with 10-second intervals when the Databricks SQL warehouse is starting up.

---

## Dashboards (Apache Superset)

Seven risk dashboards accessible at `http://localhost:8088`:

| Dashboard | Content |
|-----------|---------|
| CRO Executive Dashboard | Firm-wide risk overview for senior management |
| VaR Analytics | Daily VaR trends, confidence-level comparison, backtesting |
| Limit Monitoring | Real-time limit utilisation and breach history |
| Profit and Loss Attribution | Actual vs hypothetical P&L decomposition |
| Sensitivity Report | FX Delta, Equity Delta, PV01, CS01 |
| Stress Testing | Scenario analysis results and stressed VaR |
| Concentration Risk | HHI scores across 6 dimensions |

RBAC and row-level security are configured so that desk-level users only see their own desk's data, while CRO/management roles see everything.

---

## Monitoring (Grafana + Prometheus)

- **Grafana** (`http://localhost:3000`): Pipeline health dashboard showing task durations, success/failure rates, and data freshness
- **Prometheus** (`http://localhost:9090`): Metrics scraping via StatsD Exporter, collecting Airflow task metrics
- **StatsD Exporter**: Bridges Airflow's StatsD metrics to Prometheus format

---

## CI/CD (GitHub Actions)

### `ci.yml` — Unit Tests

Triggers on every push and PR to `main`. Two jobs:

**Python tests** — runs pytest with mocked external calls (no secrets needed). Fires on every push and PR.

**dbt unit tests** — runs `dbt test --select "test_type:unit"` against Databricks. Fires only on merge to `main` (requires secrets).

### `load_desk_limits.yml` — Data Reload

Triggers when `data/raw/reference/desk_limits.csv` changes on push to `main`. Loads the updated limits into S3 and Databricks Bronze.

### `setup_environment.yml` — Environment Validation

Manual trigger for validating environment setup and dependency installation.

**Secrets required** (8): `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `S3_BUCKET`, `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_CATALOG`

---

## Key Design Decisions

**Why Medallion (Bronze/Silver/Gold)?** Industry-standard pattern for lakehouse analytics. Bronze preserves raw data for auditability (BCBS 239 lineage), Silver handles cleaning once, and Gold models are purpose-built for specific risk reports.

**Why incremental MERGE for Gold?** Risk metrics accumulate history. Daily VaR and P&L records must persist for backtesting and regulatory lookback windows. `on_schema_change: fail` prevents silent column drift that could break downstream reports.

**Why FastMCP over REST API?** MCP is the emerging standard for AI-tool integration. Claude Desktop connects natively, enabling risk managers to query complex analytics in plain English rather than writing SQL.

**Why Apache Superset over Power BI?** Open-source, embeddable, supports Databricks natively via SQL Alchemy, and allows RBAC with row-level security — all without per-seat licensing costs.

**Why S3 as landing zone (not direct Databricks ingestion)?** Decouples ingestion from compute. If Databricks is down, data still lands safely in S3. Hive-style partitions enable efficient `COPY INTO` and support backfill/replay scenarios.

---

## Skills Demonstrated

- **Data Engineering**: End-to-end pipeline design, Medallion architecture, Delta Lake, COPY INTO, incremental models
- **SQL & dbt**: Complex analytical SQL (window functions, CTEs, MERGE), dbt project structure, schema tests, unit tests, seeds, incremental strategy
- **Cloud & Infrastructure**: AWS S3, Databricks Unity Catalog, Docker Compose (10 services), environment configuration
- **Orchestration**: Airflow DAG design, task dependencies, SLA monitoring, Slack alerting, retry logic
- **Testing**: pytest with mocking (65 tests), dbt unit tests with Given/Expect pattern (7 tests), dbt schema tests
- **BI & Visualisation**: Apache Superset dashboards, RBAC, row-level security
- **AI/ML Integration**: MCP server development, Claude Desktop integration, natural-language data access
- **DevOps**: GitHub Actions CI/CD (automated testing on push/PR), Docker containerisation, Prometheus + Grafana observability
- **Domain Knowledge**: Market risk (VaR, Greeks, stress testing, BCBS 239, FRTB), banking regulatory requirements
