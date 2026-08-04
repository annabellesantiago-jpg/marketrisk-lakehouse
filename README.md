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
11. [Data Quality](#data-quality)
12. [MCP Server (Agentic AI)](#mcp-server-agentic-ai)
13. [Dashboards (Apache Superset)](#dashboards-apache-superset)
14. [Monitoring (Grafana + Prometheus)](#monitoring-grafana--prometheus)
15. [CI/CD (GitHub Actions)](#cicd-github-actions)
16. [Documentation](#documentation)
17. [Key Design Decisions](#key-design-decisions)
18. [Skills Demonstrated](#skills-demonstrated)

---

## Business Context

Market risk teams at investment banks must report daily Value-at-Risk (VaR), stress-test results, P&L attribution, and limit utilisation to regulators and senior management. This project replicates that workflow end-to-end: from raw market data ingestion through to CRO-level executive dashboards, with full audit trails and data quality gates at every layer.

Regulatory frameworks addressed: BCBS 239 (risk data aggregation and reporting), FRTB (Fundamental Review of the Trading Book), and Basel III capital adequacy.

---

## Architecture Overview

The system follows a six-stage pipeline:

**Ingest** (Python) &rarr; **Land** (AWS S3) &rarr; **Bronze** (COPY INTO Delta) &rarr; **Silver** (dbt table models) &rarr; **Gold** (dbt incremental MERGE) &rarr; **Serve** (Superset + MCP)

Data flows from Yahoo Finance and synthetic generators, through S3 with Hive-style date partitions, into Databricks Unity Catalog as Delta tables. dbt Core handles all Silver and Gold transformations. Apache Airflow orchestrates the daily run, and downstream consumers (Superset dashboards, Claude Desktop via MCP) query the Gold layer directly.

Refer to the Technical Specification (`MarketRisk_TechnicalSpec_v1.x.docx`) and the draw.io diagrams in `docs/drawio/` for detailed architecture visuals.

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
| Agentic AI | FastMCP 3.4.5, Claude Desktop | 9 MCP tools for natural-language risk queries |
| Monitoring | Grafana, Prometheus, StatsD Exporter | Pipeline health dashboards, metrics scraping |
| CI/CD | GitHub Actions | Automated desk_limits loading on CSV change |
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
├── ingestion/                  # Python ingestion scripts
│   ├── config.py               # Centralised config (env vars, paths)
│   ├── fetch_market_data.py    # Yahoo Finance OHLCV fetcher
│   ├── fetch_reference_data.py # FX rates generator
│   ├── generate_positions.py   # Synthetic positions (4 desks)
│   ├── load_desk_limits.py     # Desk limits loader (CI/CD triggered)
│   └── s3_utils.py             # S3 upload with Hive partitioning
├── dbt/marketrisk/             # dbt project
│   ├── models/
│   │   ├── silver/             # 2 cleaning/enrichment models + _silver.yml
│   │   └── gold/               # 12 business models + _gold.yml
│   ├── seeds/                  # stress_scenarios.csv, ticker_classifications.csv
│   ├── models/sources.yml      # Bronze source definitions
│   └── dbt_project.yml
├── airflow/                    # Orchestration
│   ├── dags/marketrisk_pipeline.py  # 9-task DAG
│   ├── Dockerfile
│   └── requirements.txt
├── mcp-server/                 # Agentic AI layer
│   ├── server.py               # FastMCP server (9 tools)
│   ├── Dockerfile
│   └── requirements.txt
├── superset/                   # Dashboard layer
│   ├── dashboards/             # Exported dashboard JSON configs
│   ├── Dockerfile
│   ├── superset_config.py
│   └── superset-init.sh
├── monitoring/                 # Observability
│   ├── grafana/                # Dashboard JSON + provisioning
│   └── prometheus/prometheus.yml
├── databricks/notebooks/       # Databricks setup & exploration notebooks
├── .github/workflows/          # CI/CD pipelines
│   ├── load_desk_limits.yml
│   └── setup_environment.yml
├── dq_checks.py                # Standalone DQ validation (7 categories)
├── docker-compose.yml          # 10-service stack
├── requirements.txt            # Python dependencies
├── .env.example                # Template for secrets
└── docs/                       # Architecture diagrams (draw.io + PNG)
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

# 5. dbt test
dbt test --profiles-dir .

# 6. Standalone DQ checks
cd ../..
python dq_checks.py
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
- **Incremental strategy**: Gold models use `merge` on declared `unique_key`; `on_schema_change: fail` prevents silent column drift
- **`run_date` variable**: Passed via `--vars '{"run_date": "2025-07-15"}'` for point-in-time runs; defaults to `1900-01-01` (processes all dates)

---

## Data Quality

### dbt Tests

Schema-level tests defined in `_gold.yml` and `_silver.yml` cover column-level constraints (not_null, unique, accepted_values, relationships).

### Standalone DQ Checks (`dq_checks.py`)

Seven categories of validation run against the Gold layer:

1. **Row counts** across all layers (Bronze, Silver, Gold)
2. **Null and referential integrity** checks
3. **Date consistency** validation
4. **Business logic** validation (VaR ordering, limit calculations)
5. **Cross-model consistency** (positions count matches across models)
6. **Completeness** checks (all desks represented, all tickers covered)
7. **Timeliness** checks (data freshness)

---

## MCP Server (Agentic AI)

The MCP server enables Claude Desktop to query risk data and operate the pipeline using natural language.

**Transport:** streamable-http on port 8888
**Framework:** FastMCP 3.4.5

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

### `load_desk_limits.yml`

Triggers when `data/raw/reference/desk_limits.csv` changes on push. Loads the updated limits into Databricks Bronze via the ingestion script.

**Secrets required** (8): `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `S3_BUCKET`, `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_WAREHOUSE_ID`, `DATABRICKS_HTTP_PATH`

### `setup_environment.yml`

Validates environment setup and dependency installation.

---

## Documentation

| Document | Description |
|----------|------------|
| `MarketRisk_BRD_v1.x.docx` | Business Requirements Document — scope, user stories, acceptance criteria, BCBS 239 compliance matrix |
| `MarketRisk_TechnicalSpec_v1.x.docx` | Technical Specification — architecture, data models, API contracts, deployment guide |
| `docs/drawio/` | draw.io architecture diagrams (E2E, ingestion flow, DAG, medallion, gold lineage, serving layer, observability, governance) |
| `docs/generated_diagrams/` | Exported PNG versions of all diagrams |

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
- **SQL & dbt**: Complex analytical SQL (window functions, CTEs, MERGE), dbt project structure, schema tests, seeds, incremental strategy
- **Cloud & Infrastructure**: AWS S3, Databricks Unity Catalog, Docker Compose (10 services), environment configuration
- **Orchestration**: Airflow DAG design, task dependencies, SLA monitoring, Slack alerting, retry logic
- **Data Quality**: Multi-layer validation, business rule checks, referential integrity, freshness monitoring
- **BI & Visualisation**: Apache Superset dashboards, RBAC, row-level security
- **AI/ML Integration**: MCP server development, Claude Desktop integration, natural-language data access
- **DevOps**: GitHub Actions CI/CD, Docker containerisation, Prometheus + Grafana observability
- **Domain Knowledge**: Market risk (VaR, Greeks, stress testing, BCBS 239, FRTB), banking regulatory requirements
- **Documentation**: BRD and Tech Spec authoring to enterprise standards
