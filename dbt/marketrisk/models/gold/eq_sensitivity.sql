{{
  config(
    materialized         = 'incremental',
    schema               = 'gold',
    unique_key           = ['calculation_date', 'desk', 'ticker'],
    incremental_strategy = 'merge',
    merge_update_columns = [
      'sector',
      'geography',
      'position_count',
      'long_position_count',
      'short_position_count',
      'gross_notional_usd',
      'long_notional_usd',
      'short_notional_usd',
      'net_notional_usd',
      'current_price',
      'latest_price_date',
      'equity_delta_1pct_usd',
      'equity_delta_up_1pct_usd',
      'equity_delta_down_1pct_usd',
      'long_delta_usd',
      'short_delta_usd',
      '_gold_loaded_at'
    ],
    on_schema_change     = 'fail',
    tags                 = ['gold', 'sensitivity', 'equity']
  )
}}

/*
  gold.equity_sensitivity
  ─────────────────────────────────────────────────────────────────────────────
  Purpose : Equity Delta — P&L impact of a 1% move in each equity price
            by desk and ticker. Includes sector and geography breakdown
            for concentration analysis.
  Source  : silver.positions_enriched
  Filter  : asset_class = 'Equity' only
  Grain   : One row per desk per ticker per calculation_date
  Unique key: [calculation_date, desk, ticker]
  Regulatory: Internal equity risk management. Feeds sector and geographic
              concentration reporting.
  Limitation: First-order linear sensitivity only. Does not capture gamma
              (convexity) or vega (volatility sensitivity). These require
              options pricing data not available in this project.
  ─────────────────────────────────────────────────────────────────────────────
*/

WITH 

-- Step 1: Define sector and geography classification for each ticker.
--         This mapping is hardcoded because our equity universe is fixed.
--         In production this would come from a reference data system
--         (Bloomberg, Refinitiv) that classifies every instrument.
ticker_classification AS (
    SELECT ticker, sector, geography
      FROM ( 
            VALUES 
            ("AAPL", "Technology", "US"),
            ("MSFT", "Technology", "US"),
            ("GOOGL", "Communication Services", "US"),
            ("AMZN", "Consumer Discretionary", "US"),
            ("TSLA", "Consumer Discretionary", "US"),
            ("BABA", "Consumer Discretionary", "China"),
            ("JPM", "Financial Services", "US"),
            ("GS", "Financial Services", "US"),
            ("HSBA.L", "Financial Services", "UK"),
            ("SAN.MC", "Financial Services", "Europe")
            )
      ) AS t(ticker, sector, geography)

-- STEP 2: filter equity positions only and cast all columns explicitly to ensure consistent data types for calculations and downstream use.
--         We are only calculating equity sensitivity for equity positions.
equity_positions 

AS (
    SELECT 
            {{ cast_to_string('p.desk') }} AS desk, 
            {{ cast_to_string('p.ticker') }} AS ticker, 
            {{ cast_to_string('p.trade_id') }} AS trade_id,
            {{ cast_to_string('p.direction') }} AS direction,
            {{ cast_to_date('p.latest_price_date') }} AS latest_price_date,
            {{ cast_to_double('p.current_price') }} AS current_price,            
            {{ cast_to_double('p.notional_usd') }} AS notional_usd,
            {{ cast_to_double('p.direction_multiplier') }} AS direction_multiplier,
            COALESCE(t.sector, 'Unknown') AS sector,
            COALESCE(t.geography, 'Unknown') AS geography
        FROM {{ ref('positions_enriched') }} p LEFT OUTER JOIN ticker_classification t 
        ON UPPER(p.ticker) = UPPER(t.ticker)
      WHERE UPPER(p.asset_class) = 'EQUITY'
        AND p.trade_id IS NOT NULL
),

-- Step 3: Aggregate to desk + ticker level.
--         Long and short notionals are separated to support:
--         a) Long delta vs short delta reporting
--         b) Net delta for P&L sensitivity
--         c) Gross delta for total exposure measurement
desk_ticker_sensitivity AS (
  SELECT
    desk,
    ticker,
    MAX(sector)                                           AS sector,
    MAX(geography)                                        AS geography,
    COUNT(DISTINCT trade_id)                              AS position_count,
    COUNT(DISTINCT CASE WHEN direction = 'LONG'
                        THEN trade_id END)                AS long_position_count,
    COUNT(DISTINCT CASE WHEN direction = 'SHORT'
                        THEN trade_id END)                AS short_position_count,
    SUM(ABS(notional_usd))                                AS gross_notional_usd,
    SUM(CASE WHEN direction = 'LONG'
               THEN notional_usd ELSE 0 END)                AS long_notional_usd,
    SUM(CASE WHEN direction = 'SHORT'
               THEN notional_usd ELSE 0 END)                AS short_notional_usd,
    SUM(notional_usd * direction_multiplier)                AS net_notional_usd,
    MAX(current_price)                                    AS current_price,
    MAX(latest_price_date)                                AS latest_price_date
  FROM equity_positions
  GROUP BY
    desk,
    ticker
),

-- Step 4: Calculate Equity Delta metrics.
--         equity_delta_1pct = ABS(net_notional) × 0.01
--         This is always positive — the magnitude of P&L from a 1% price move.
--         Up/down variants are signed — positive means the desk GAINS.
--         long_delta and short_delta show the gross sensitivity
--         of each side of the book independently — useful for hedging analysis.
sensitivity_calculated AS (
  SELECT
    desk,
    ticker,
    sector,
    geography,
    position_count,
    long_position_count,
    short_position_count,
    gross_notional_usd,
    long_notional_usd,
    short_notional_usd,
    net_notional_usd,
    current_price,
    latest_price_date,
    -- Absolute equity delta: magnitude of P&L from 1% price move
    ABS(net_notional_usd) * 0.01 AS equity_delta_1pct_usd,
    -- Directional delta up: P&L if equity price RISES 1%
    -- Positive for net long positions (price up = gain for long)
    net_notional_usd * 0.01                   AS equity_delta_up_1pct_usd,
    -- Directional delta down: P&L if equity price FALLS 1%
    -- Negative for net long positions (price down = loss for long)
    net_notional_usd * (-0.01)                AS equity_delta_down_1pct_usd,
    -- Long-only delta: sensitivity from LONG book only
    long_notional_usd * 0.01                  AS long_delta_usd,
    -- Short-only delta: sensitivity from SHORT book only (always positive)
    ABS(short_notional_usd) * 0.01            AS short_delta_usd
  FROM desk_ticker_sensitivity
)

SELECT
  CURRENT_DATE()                      AS calculation_date,
  desk,
  ticker,
  sector,
  geography,
  {{ cast_to_int('position_count') }}       AS position_count,
  {{ cast_to_int('long_position_count') }}  AS long_position_count,
  {{ cast_to_int('short_position_count') }} AS short_position_count,
  gross_notional_usd,
  long_notional_usd,
  short_notional_usd,
  net_notional_usd,
  current_price,
  latest_price_date,
  equity_delta_1pct_usd,
  equity_delta_up_1pct_usd,
  equity_delta_down_1pct_usd,
  long_delta_usd,
  short_delta_usd,
  current_timestamp()                 AS _gold_loaded_at
FROM sensitivity_calculated