{{
  config(
    materialized          = 'incremental',
    schema                = 'gold',
    unique_key            = ['desk', 'asset_class', 'calculation_date'],
    incremental_strategy  = 'merge',
    merge_update_columns  = [
      'position_count',
      'total_notional_usd',
      'var_95_usd',
      'var_97_5_usd',
      'var_99_usd',
      'var_10day_95_usd',
      'var_10day_97_5_usd',
      'var_10day_99_usd',
      'es_97_5_usd',
      'avg_daily_return_usd',
      'worst_day_loss_usd',
      'best_day_gain_usd',
      'scenario_count',
      '_gold_loaded_at'
    ],
    on_schema_change       = 'fail',
    tags                   = ['gold', 'var']
  )
}}

/*
  gold.var_daily
  ─────────────────────────────────────────────────────────────────────────────
  Purpose : Daily VaR, 10-day VaR and Expected Shortfall by desk and asset class
  Method  : Historical simulation using 260 trading days of price returns
  Grain   : One row per desk per asset_class per calculation_date
  Source  : silver.positions_enriched + silver.prices_cleaned
  Unique key: desk + asset_class + calculation_date
  Regulatory context:
    var_95_usd         — internal management reporting
    var_97_5_usd       — Basel III / FRTB standard confidence level
    var_99_usd         — senior management / board reporting
    var_10day_usd      — Basel III regulatory capital (1-day VaR × √10)
    es_97_5_usd        — FRTB Expected Shortfall, primary regulatory measure
  ─────────────────────────────────────────────────────────────────────────────
*/

WITH

-- Step 1: Calculate daily returns for each ticker.
--         Return = (today's close - yesterday's close) / yesterday's close
--         LAG() retrieves the previous row's close price within each
--         ticker partition ordered by date ascending.
daily_returns AS (
  SELECT
    ticker,
    price_date,
    close_price,
    LAG(close_price) OVER (
      PARTITION BY ticker
      ORDER BY price_date ASC
    ) AS prev_close_price
  FROM {{ ref('prices_cleaned') }}
),

-- Step 2: Calculate percentage return and filter to valid rows only.
--         Null return = first row per ticker (no previous day available).
--         Zero prev_close excluded to prevent division by zero.
--         Date filter keeps only the most recent 260 trading days —
--         the standard lookback window for historical simulation VaR.
return_pct AS (
  SELECT
    ticker,
    price_date,
    (close_price - prev_close_price) / prev_close_price AS daily_return_pct
  FROM daily_returns
  WHERE
    prev_close_price IS NOT NULL
    AND prev_close_price > 0
    AND price_date >= (
      SELECT DATE_SUB(MAX(price_date), 365)
      FROM {{ ref('prices_cleaned') }}
    )
),

-- Step 3: Join positions to return scenarios.
--         Each position generates one scenario P&L per historical trading day.
--         P&L = notional_usd × daily_return_pct × direction_multiplier
--         LONG positions lose when prices fall (multiplier=1, return negative).
--         SHORT positions gain when prices fall (multiplier=-1, return negative
--         × -1 = positive P&L).
position_scenarios AS (
  SELECT
    p.desk,
    p.asset_class,
    p.trade_id,
    p.notional_usd,
    p.direction_multiplier,
    r.price_date                                         AS scenario_date,
    ROUND(
      p.notional_usd * r.daily_return_pct * p.direction_multiplier,
      2
    )                                                    AS scenario_pnl_usd
  FROM {{ ref('positions_enriched') }} p
  INNER JOIN return_pct                          r
    ON p.ticker = r.ticker
),

-- Step 4: Aggregate scenario P&L to desk + asset_class level.
--         Each row represents one portfolio-level scenario
--         for one desk + asset_class on one historical date.
portfolio_scenarios AS (
  SELECT
    desk,
    asset_class,
    scenario_date,
    SUM(scenario_pnl_usd)    AS portfolio_pnl_usd,
    COUNT(DISTINCT trade_id) AS position_count,
    SUM(notional_usd)        AS total_notional_usd
  FROM position_scenarios
  GROUP BY
    desk,
    asset_class,
    scenario_date
),

-- Step 5: Calculate VaR at three confidence levels using PERCENTILE_CONT.
--         We use left tail percentiles:
--           0.05  → 95th percentile VaR (5% of days are worse)
--           0.025 → 97.5th percentile VaR (2.5% of days are worse)
--           0.01  → 99th percentile VaR (1% of days are worse)
--         ABS() converts negative loss values to positive VaR figures.
--         VaR is always reported as a positive number by convention.
var_base AS (
  SELECT
    desk,
    asset_class,
    MAX(position_count)                                  AS position_count,
    MAX(total_notional_usd)                              AS total_notional_usd,
    COUNT(*)                                             AS scenario_count,
    ABS(
      PERCENTILE_CONT(0.05) WITHIN GROUP (
        ORDER BY portfolio_pnl_usd ASC
      )
    )                                                    AS var_95_usd,
    ABS(
      PERCENTILE_CONT(0.025) WITHIN GROUP (
        ORDER BY portfolio_pnl_usd ASC
      )
    )                                                    AS var_97_5_usd,
    ABS(
      PERCENTILE_CONT(0.01) WITHIN GROUP (
        ORDER BY portfolio_pnl_usd ASC
      )
    )                                                    AS var_99_usd,
    AVG(portfolio_pnl_usd)                               AS avg_daily_return_usd,
    MIN(portfolio_pnl_usd)                               AS worst_day_loss_usd,
    MAX(portfolio_pnl_usd)                               AS best_day_gain_usd
  FROM portfolio_scenarios
  GROUP BY
    desk,
    asset_class
),

-- Step 6: Calculate Expected Shortfall at 97.5%.
--         ES = average of all losses WORSE than the VaR threshold.
--         Join back to portfolio_scenarios to get individual scenario P&Ls
--         then filter to only scenarios in the tail beyond VaR 97.5.
--         ES is calculated separately because it needs row-level scenario
--         data, not just the percentile value.
es_calculation AS (
  SELECT
    ps.desk,
    ps.asset_class,
    ABS(AVG(ps.portfolio_pnl_usd)) AS es_97_5_usd
  FROM portfolio_scenarios ps
  INNER JOIN var_base       vb
    ON  ps.desk        = vb.desk
    AND ps.asset_class = vb.asset_class
  -- Keep only scenarios worse than the 97.5% VaR threshold
  WHERE ps.portfolio_pnl_usd < -vb.var_97_5_usd
  GROUP BY
    ps.desk,
    ps.asset_class
),

-- Step 7: Combine VaR and ES.
--         10-day VaR derived using the square root of time rule:
--         10-day VaR = 1-day VaR × √10 = 1-day VaR × 3.16228
--         Assumption: daily returns are independent and identically distributed.
--         Known limitation: this assumption breaks down during market stress.
combined AS (
  SELECT
    vb.desk,
    vb.asset_class,
    vb.position_count,
    vb.total_notional_usd,
    vb.var_95_usd,
    vb.var_97_5_usd,
    vb.var_99_usd,
    ROUND(vb.var_95_usd   * SQRT(10), 2)                AS var_10day_95_usd,
    ROUND(vb.var_97_5_usd * SQRT(10), 2)                AS var_10day_97_5_usd,
    ROUND(vb.var_99_usd   * SQRT(10), 2)                AS var_10day_99_usd,
    -- Fall back to var_97_5 if no tail scenarios exist for ES calculation
    COALESCE(ROUND(es.es_97_5_usd, 2), vb.var_97_5_usd) AS es_97_5_usd,
    vb.avg_daily_return_usd,
    vb.worst_day_loss_usd,
    vb.best_day_gain_usd,
    vb.scenario_count
  FROM var_base            vb
  LEFT JOIN es_calculation es
    ON  vb.desk        = es.desk
    AND vb.asset_class = es.asset_class
)

SELECT
  CURRENT_DATE()                       AS calculation_date,
  desk,
  asset_class,
  CAST(position_count  AS INT)         AS position_count,
  ROUND(total_notional_usd, 2)         AS total_notional_usd,
  ROUND(var_95_usd, 2)                 AS var_95_usd,
  ROUND(var_97_5_usd, 2)               AS var_97_5_usd,
  ROUND(var_99_usd, 2)                 AS var_99_usd,
  var_10day_95_usd,
  var_10day_97_5_usd,
  var_10day_99_usd,
  es_97_5_usd,
  ROUND(avg_daily_return_usd, 2)       AS avg_daily_return_usd,
  ROUND(worst_day_loss_usd, 2)         AS worst_day_loss_usd,
  ROUND(best_day_gain_usd, 2)          AS best_day_gain_usd,
  CAST(scenario_count AS INT)          AS scenario_count,
  current_timestamp()                  AS _gold_loaded_at
FROM combined 