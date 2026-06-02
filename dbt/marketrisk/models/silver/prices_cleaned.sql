{{
    config(
        materialized='table',
        description="Cleaned market prices with correct data types and missing values imputed. Source: {{ source('bronze', 'market_prices') }}.",
        tags=['silver', 'prices', 'cleaned']
    )
}}

/*
  silver.prices_cleaned
  ─────────────────────────────────────────────────────────────────────────────
  Purpose : Deduplicated and cleaned daily market prices
  Source  : {{ source('bronze', 'market_prices') }}
  Grain   : One row per ticker per trading day
  Notes   : Removes weekends, holidays and duplicate loads.
             We do not forward-fill nulls because forward-filled prices
             produce misleading VaR calculations.
  ─────────────────────────────────────────────────────────────────────────────
*/

WITH

-- Step 1: Cast all columns explicitly and filter out rows with no usable
--         price data. Yahoo Finance returns null Close for weekends and
--         some holidays. We remove these entirely.
valid_prices AS (
  SELECT
    CAST(Date       AS DATE)      AS price_date,
    {{ cast_to_double('Open') }}    AS open_price,
    {{ cast_to_double('High') }}    AS high_price,
    {{ cast_to_double('Low') }}     AS low_price,
    {{ cast_to_double('Close') }}   AS close_price,
    {{ cast_to_double('Volume') }}  AS volume,
    -- Normalize ticker to match the format used in bronze.positions.
    -- generate_positions.py applies: replace("=X", "").replace(".", "_")
    -- We apply the same transformation here so joins work correctly.
    -- EURUSD=X → EURUSD    HSBA.L → HSBA_L    SAN.MC → SAN_MC
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        {{ cast_to_string('ticker') }},
        '=X$', ''
      ),
      '\\.', '_'
    )                       AS ticker,
    {{ cast_to_string('fetched_at') }}    AS fetched_at,
    _ingested_at
   FROM {{ source('bronze', 'market_prices') }}
  WHERE Date IS NOT NULL
    AND Close IS NOT NULL
    AND Open IS NOT NULL
    AND High IS NOT NULL
    AND Low IS NOT NULL
    AND ticker IS NOT NULL
),

-- Step 2: Deduplicate — keep only the most recently ingested record
--         for each ticker + date combination.
--         This handles the case where COPY INTO is rerun and loads
--         the same file twice, which would otherwise create duplicate rows.
deduplicated_prices AS (
  SELECT
    price_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    ticker,
    fetched_at,
    _ingested_at,
    ROW_NUMBER() OVER (PARTITION BY ticker, price_date ORDER BY _ingested_at DESC) AS row_num
  FROM valid_prices
)
-- Step 3: Keep only the canonical row per ticker per date
--         and add the Silver load timestamp
SELECT
  price_date,
  open_price,
  high_price,
  low_price,
  close_price,
  volume,
  ticker,
  fetched_at,
  current_timestamp() AS _silver_loaded_at
FROM deduplicated_prices
WHERE row_num = 1
