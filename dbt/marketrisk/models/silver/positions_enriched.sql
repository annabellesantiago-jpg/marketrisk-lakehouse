{{
  config(
    materialized = 'table',
    tags         = ['silver', 'positions']
  )
}}

/*
  silver.positions_enriched
  ─────────────────────────────────────────────────────────────────────────────
  Purpose : Trading positions enriched with FX rates and latest market prices.
            All monetary values normalised to USD.
  Source  : bronze.positions + bronze.fx_rates + silver.prices_cleaned
  Grain   : One row per trade_id
  Notes   : INNER JOIN to fx_rates ensures every position has a valid FX rate.
             A missing FX rate is a data quality problem that should surface
             as a missing row rather than a null notional_usd.
             LEFT JOIN to prices_cleaned because not all synthetic tickers
             have price history.
  ─────────────────────────────────────────────────────────────────────────────
*/

WITH

-- Step 1: Extend the FX rates table to include USD explicitly.
--         USD is the base currency so it does not appear in bronze.fx_rates.
--         Adding it here means USD positions join cleanly without needing
--         a COALESCE fallback — cleaner logic, no silent defaults.
fx_with_usd AS (
  SELECT
    UPPER({{ cast_to_string('currency') }}) AS currency,
    {{ cast_to_double('rate_vs_usd') }}  AS rate_vs_usd
  FROM {{ source('bronze', 'fx_rates') }}
),

-- Step 1.5: Perform Deduplication: Bronze holds all historical position loads.
-- Positions is a point-in-time snapshot — keep only the latest load per trade_id.
positions_deduped AS (
  SELECT *
  FROM   {{ source('bronze', 'positions') }}  
  WHERE  trade_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY trade_id ORDER BY generated_at DESC) = 1
),

-- Step 2: Find the most recent price date available for each ticker.
--         We use MAX(price_date) rather than CURRENT_DATE() because
--         the latest data may be 1-2 days old due to market close timing.
latest_price_dates AS (
  SELECT
    ticker,
    MAX(price_date) AS latest_date
  FROM {{ ref('prices_cleaned') }}
  GROUP BY ticker
),

-- Step 3: Retrieve the closing price on that latest date.
--         INNER JOIN on both ticker AND date ensures exactly one row
--         per ticker — the join condition itself is the dedup mechanism.
latest_prices AS (
  SELECT
    pc.ticker,
    {{ cast_to_double('pc.close_price') }} AS current_price,
    pc.price_date                  AS latest_price_date
  FROM {{ ref('prices_cleaned') }}  pc
  INNER JOIN latest_price_dates     lpd
    ON  pc.ticker     = lpd.ticker
    AND pc.price_date = lpd.latest_date
),

-- Step 4: Resolve direction multiplier.
--         Centralising this logic avoids repeating the CASE expression
--         in multiple downstream columns.
--         Explicit CAST on every column — no SELECT * in production code.
positions_with_direction AS (
  SELECT
    {{ cast_to_string('trade_id') }}      AS trade_id,
    {{ cast_to_string('desk') }}          AS desk,
    {{ cast_to_string('trader_id') }}     AS trader_id,
    {{ cast_to_string('book_id') }}       AS book_id,
    {{ cast_to_string('isin') }}          AS isin,
    {{ cast_to_string('cusip') }}         AS cusip,
    {{ cast_to_string('instrument_type') }}  AS instrument_type,
    {{ cast_to_string('counterparty') }}  AS counterparty,
    {{ cast_to_string('asset_class') }}   AS asset_class,
    {{ cast_to_string('ticker') }}        AS ticker,
    {{ cast_to_string('direction') }}     AS direction,
    {{ cast_to_double('notional') }}      AS notional,
    UPPER({{ cast_to_string('currency') }}) AS currency,
    CAST(trade_date    AS DATE)           AS trade_date,
    CAST(maturity_date AS DATE)           AS maturity_date,
    CASE direction
      WHEN 'LONG'  THEN  1
      WHEN 'SHORT' THEN -1
      ELSE 0
    END                                   AS direction_multiplier
  FROM positions_deduped
  WHERE trade_id IS NOT NULL
),

-- Step 5: Final enrichment — join all CTEs together.
--         INNER JOIN to fx_with_usd: every position must have a currency.
--         LEFT JOIN to latest_prices: not all tickers have price history.
enriched AS (
  SELECT
    p.trade_id,
    p.desk,
    p.book_id,
    p.trader_id,
    p.counterparty,
    p.asset_class,
    p.instrument_type,    
    p.ticker,
    p.isin,
    p.cusip,    
    p.direction,
    p.notional,
    p.currency,
    p.trade_date,
    p.maturity_date,
    fx.rate_vs_usd                                            AS fx_rate_vs_usd,
    p.notional * fx.rate_vs_usd                               AS notional_usd,
    lp.current_price,
    lp.latest_price_date,
    -- Market value: use current price if available, else fall back to notional
    COALESCE(lp.current_price, p.notional) * fx.rate_vs_usd   AS market_value_usd,
    -- Days remaining from today to maturity — not total trade life
    DATEDIFF(p.maturity_date, CURRENT_DATE())                 AS days_to_maturity,
    p.direction_multiplier,
    -- Net exposure: positive for LONG, negative for SHORT
    p.notional * fx.rate_vs_usd * p.direction_multiplier      AS net_exposure_usd,
    current_timestamp()                                       AS _silver_loaded_at
  FROM positions_with_direction  p
  INNER JOIN fx_with_usd         fx ON p.currency = fx.currency
  LEFT JOIN  latest_prices       lp ON p.ticker   = lp.ticker
)

SELECT
  trade_id,
  desk,
  book_id,
  trader_id,
  counterparty,
  asset_class,
  instrument_type, 
  ticker,
  isin,
  cusip,    
  direction,
  notional,
  currency,
  trade_date,
  maturity_date,
  fx_rate_vs_usd,
  notional_usd,
  current_price,
  latest_price_date,
  market_value_usd,
  days_to_maturity,
  direction_multiplier,
  net_exposure_usd,
  _silver_loaded_at
FROM enriched