-- ─────────────────────────────────────────────────────────────────────────────
-- 00_setup.sql
-- One-time environment setup — run manually when initialising a new environment.
-- Creates the catalog, schema, and all Bronze tables.
-- DO NOT run this in the daily pipeline — it will drop and recreate all tables.
-- ─────────────────────────────────────────────────────────────────────────────


CREATE CATALOG IF NOT EXISTS market_risk_dev;
CREATE SCHEMA IF NOT EXISTS market_risk_dev.bronze;
CREATE SCHEMA IF NOT EXISTS market_risk_dev.silver;
CREATE SCHEMA IF NOT EXISTS market_risk_dev.gold;


-- Bronze: market prices
CREATE OR REPLACE TABLE market_risk_dev.bronze.market_prices 
(
  Date          DATE          COMMENT 'Trading date',
  Open          DOUBLE        COMMENT 'Opening price',
  High          DOUBLE        COMMENT 'Intraday high price',
  Low           DOUBLE        COMMENT 'Intraday low price',
  Close         DOUBLE        COMMENT 'Closing price',
  Volume        DOUBLE        COMMENT 'Number of shares or contracts traded',
  ticker        STRING        COMMENT 'Yahoo Finance ticker symbol',
  fetched_at    STRING        COMMENT 'Timestamp when data was fetched from API',
  _source_file  STRING        COMMENT 'S3 file this row was loaded from',
  _ingested_at  TIMESTAMP     COMMENT 'Timestamp when row was loaded into Bronze'
)
USING DELTA
COMMENT 'Raw OHLCV market prices from Yahoo Finance — loaded as-is, no transformations'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true'
);


-- Bronze: positions
CREATE OR REPLACE TABLE market_risk_dev.bronze.positions (
  trade_id      STRING    COMMENT 'Unique trade identifier',
  desk          STRING    COMMENT 'Trading desk that owns the position',
  book_id       STRING    COMMENT 'Sub-desk level aggregation - a desk can have multiple books',
  trader_id     STRING    COMMENT 'Individual trader ID',  
  counterparty  STRING    COMMENT 'Counterparty name',
  asset_class   STRING    COMMENT 'Asset class: FX, Equity, Rates, Credit',
  instrument_type  STRING    COMMENT 'Distinguished product within an asset class',
  ticker        STRING    COMMENT 'Instrument ticker',
  isin          STRING    COMMENT 'International Securities Identification Number',
  cusip         STRING    COMMENT 'US CUSIP number',
  direction     STRING    COMMENT 'LONG or SHORT',
  notional      DOUBLE    COMMENT 'Notional value in local currency',
  currency      STRING    COMMENT 'Local currency of the notional',
  trade_date    DATE      COMMENT 'Date trade was booked',
  maturity_date DATE      COMMENT 'Date trade matures',
  generated_at  STRING    COMMENT 'Timestamp when synthetic data was generated',
  _source_file  STRING    COMMENT 'S3 file this row was loaded from',
  _ingested_at  TIMESTAMP COMMENT 'Timestamp when row was loaded into Bronze'
)
USING DELTA
COMMENT 'Synthetic trading book — 300 positions across 4 desks'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true'
);


-- Bronze: desk limits
CREATE OR REPLACE TABLE market_risk_dev.bronze.desk_limits (
  desk             STRING    COMMENT 'Trading desk name',
  limit_usd        DOUBLE    COMMENT 'Risk limit in USD',
  limit_currency   STRING    COMMENT 'Limit denomination currency',
  effective_date   DATE      COMMENT 'Date limit became effective',
  review_date      DATE      COMMENT 'Date limit is next reviewed',
  approved_by      STRING    COMMENT 'Name of the limit approver',
  uploaded_by      STRING    COMMENT 'Name of the limit uploader',
  approved_date    DATE      COMMENT 'Date when the limit was approved',
  comments         STRING    COMMENT 'Comment from the approver of the limit',
  _source_file     STRING    COMMENT 'S3 file this row was loaded from',
  _ingested_at     TIMESTAMP COMMENT 'Timestamp when row was loaded into Bronze'
)
USING DELTA
COMMENT 'Risk limits per trading desk in USD'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true'
);


-- Bronze: fx rates
CREATE OR REPLACE TABLE market_risk_dev.bronze.fx_rates (
  currency      STRING    COMMENT 'Currency code e.g. EUR, GBP, JPY',
  rate_vs_usd   DOUBLE    COMMENT 'Exchange rate vs USD — multiply to convert to USD',
  as_of_date    DATE      COMMENT 'Date the rate is valid for',
  generated_at  STRING    COMMENT 'Timestamp when reference data was generated',
  _source_file  STRING    COMMENT 'S3 file this row was loaded from',
  _ingested_at  TIMESTAMP COMMENT 'Timestamp when row was loaded into Bronze'
)
USING DELTA
COMMENT 'FX conversion rates vs USD for reference data'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Silver and Gold tables are managed by dbt.
-- Run: dbt run --profiles-dir ~/.dbt
-- to create and populate Silver and Gold layers.