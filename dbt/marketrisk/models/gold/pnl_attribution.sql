{{ 
    config(
        materialized        ='incremental',
        schema              ='gold',
        unique_key          =['pnl_date', 'desk'],
        incremental_strategy='merge',
        merge_update_columns=[
            'hypothetical_pnl_usd',
            'actual_pnl_usd',
            'unexplained_pnl_usd',
            'position_count',
            'long_pnl_usd',
            'short_pnl_usd',
            '_gold_loaded_at'
        ],
        on_schema_change    ='fail',
        tags                =['gold', 'pnl_attribution']
    ) 
}}

/*
    gold.pnl_attribution
    ─────────────────────────────────────────────────────────────────────────────
    Purpose : Attribute daily P&L to market moves vs unexplained factors
    Method  : Join enriched positions to price moves, calculate hypothetical
                P&L from market moves alone, compare to actual P&L and
                attribute the difference to unexplained factors.
    Grain   : One row per desk per day (pnl_date)
    Source  : silver.positions_enriched + silver.prices_cleaned
    Unique key: pnl_date + desk
    Notes   : Hypothetical PnL uses only market price moves (clean P&L).
            Unexplained PnL = actual - hypothetical.
            Large unexplained values indicate unmodelled risk or booking errors.
    ─────────────────────────────────────────────────────────────────────────────
*/

WITH

-- Step 1: Get the two most recent trading days per ticker.
--         DENSE_RANK() numbers dates descending per ticker:
--         rank 1 = most recent, rank 2 = previous trading day.
ranked_dates AS (
  SELECT
    ticker,
    price_date,
    close_price,
    DENSE_RANK() OVER (
      PARTITION BY ticker
      ORDER BY price_date DESC
    ) AS date_rank
  FROM {{ ref('prices_cleaned') }}
),

-- Step 2: Pivot the two ranked rows into two columns per ticker.
--         MAX(CASE...) avoids a self-join and produces one clean row
--         per ticker with both current and prior close prices.
current_and_prior_prices AS (
  SELECT
    ticker,
    MAX(CASE WHEN date_rank = 1 THEN close_price END) AS current_close,
    MAX(CASE WHEN date_rank = 2 THEN close_price END) AS prior_close,
    MAX(CASE WHEN date_rank = 1 THEN price_date  END) AS current_date
  FROM ranked_dates
  WHERE date_rank IN (1, 2)
  GROUP BY ticker
),

-- Step 3: Calculate the daily price return per ticker.
--         Null return = only one day of history exists for that ticker.
price_moves AS (
  SELECT
    ticker,
    current_date                                        AS price_date,
    current_close,
    prior_close,
    CASE
      WHEN prior_close IS NULL OR prior_close = 0 THEN NULL
      ELSE (current_close - prior_close) / prior_close
    END                                                 AS daily_return_pct
  FROM current_and_prior_prices
),

-- Step 4: Calculate P&L per position.
--         Hypothetical PnL = pure market move P&L.
--         Actual PnL = market move + small noise simulating unexplained
--         component from new trades, theta decay, and other real-world factors.
--         In production, actual PnL comes from the trade booking system.
position_pnl AS (
  SELECT
    p.desk,
    p.trade_id,
    p.direction,
    pm.price_date,
    p.notional_usd 
      * COALESCE(pm.daily_return_pct, 0) 
      * p.direction_multiplier                          AS hypothetical_pnl_usd,
    p.notional_usd
      * COALESCE(pm.daily_return_pct, 0)
      * p.direction_multiplier
      * (1 + (HASH(p.trade_id) % 100) / 10000.0)        AS actual_pnl_usd
  FROM {{ ref('positions_enriched') }} p
  LEFT JOIN price_moves                          pm
    ON p.ticker = pm.ticker
),

-- Step 5: Aggregate to desk level.
desk_pnl AS (
  SELECT
    desk,
    price_date                                          AS pnl_date,
    COUNT(DISTINCT trade_id)                            AS position_count,
    SUM(hypothetical_pnl_usd)                           AS hypothetical_pnl_usd,
    SUM(actual_pnl_usd)                                 AS actual_pnl_usd,
    SUM(actual_pnl_usd) - SUM(hypothetical_pnl_usd)     AS unexplained_pnl_usd,
    SUM(CASE WHEN direction = 'LONG'
               THEN actual_pnl_usd ELSE 0 END) AS long_pnl_usd,
    SUM(CASE WHEN direction = 'SHORT'
               THEN actual_pnl_usd ELSE 0 END) AS short_pnl_usd
  FROM position_pnl
  WHERE price_date IS NOT NULL
  GROUP BY
    desk,
    price_date
)

SELECT
  pnl_date,
  desk,
  hypothetical_pnl_usd,
  actual_pnl_usd,
  unexplained_pnl_usd,
  {{ cast_to_int('position_count') }} AS position_count,
  long_pnl_usd,
  short_pnl_usd,
  current_timestamp()         AS _gold_loaded_at
FROM desk_pnl