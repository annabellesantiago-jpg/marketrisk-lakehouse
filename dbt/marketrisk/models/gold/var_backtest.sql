{{
    config(
        materialized        ='incremental',
        schema              ='gold',
        unique_key          =['desk', 'backtest_date'],
        incremental_strategy='merge',
        merge_update_columns=[
            'var_97_5_usd',
            'actual_loss_usd',
            'is_exception',
            'exception_magnitude',
            'rolling_250d_exceptions',
            'basel_zone',
            'model_status',
            '_gold_loaded_at'
        ],
        on_schema_change    ='fail',
        tags                =['gold', 'var', 'backtest']
     )
}}

/*
    gold.var_backtest
    ─────────────────────────────────────────────────────────────────────────────
    Purpose : Backtest daily VaR predictions against actual P&L and apply
                Basel traffic light assessment based on 250-day rolling exceptions
    Method  : Join daily VaR to daily P&L, identify exceptions, count rolling
                exceptions and assign Basel zone and model status
    Grain   : One row per desk per backtest_date
    Source  : gold.var_daily + gold.pnl_attribution
    Unique key: desk + backtest_date
    Regulatory context:
        Basel traffic light (exceptions in rolling 250 trading days):
        GREEN  — 0 to 4  — model accepted, no capital add-on
        AMBER  — 5 to 9  — increased scrutiny, possible capital add-on
        RED    — 10+     — model rejected, mandatory capital add-on
    ─────────────────────────────────────────────────────────────────────────────
*/

WITH

-- Step 1: Aggregate var_daily to desk level.
--         var_daily grain is desk + asset_class but pnl_attribution
--         is at desk level only. We take MAX VaR across asset classes
--         as a conservative desk-level estimate.
--         In production this would use a proper portfolio aggregation method.
desk_var AS (
  SELECT
    calculation_date,
    desk,
    MAX(var_97_5_usd) AS var_97_5_usd
  FROM {{ ref('var_daily') }}
  GROUP BY
    calculation_date,
    desk
),

-- Step 2: Get daily actual P&L from pnl_attribution.
--         Convert to actual_loss_usd: positive = loss, negative = gain.
--         This sign convention makes exception logic cleaner —
--         loss > VaR is a straightforward greater-than comparison.
daily_pnl AS (
  SELECT
    pnl_date,
    desk,
    -actual_pnl_usd AS actual_loss_usd
  FROM {{ ref('pnl_attribution') }}
),

-- Step 3: Join VaR to actual P&L and identify exceptions.
--         An exception = actual_loss_usd > var_97_5_usd.
--         exception_magnitude shows severity of the model miss.
exceptions_identified AS (
  SELECT
    pnl.pnl_date                                         AS backtest_date,
    pnl.desk,
    vr.var_97_5_usd,
    pnl.actual_loss_usd,
    CASE
      WHEN pnl.actual_loss_usd > vr.var_97_5_usd THEN TRUE
      ELSE FALSE
    END                                                  AS is_exception,
    CASE
      WHEN pnl.actual_loss_usd > vr.var_97_5_usd
      THEN pnl.actual_loss_usd - vr.var_97_5_usd
      ELSE NULL
    END                                                  AS exception_magnitude
  FROM daily_pnl        pnl
  INNER JOIN desk_var   vr
    ON  pnl.desk     = vr.desk
    AND pnl.pnl_date = vr.calculation_date
),

-- Step 4: Calculate rolling 250-day exception count per desk.
--         SUM() OVER with ROWS BETWEEN counts exceptions in the 249 rows
--         before plus the current row = 250-day rolling window.
--         This is exactly the window the Basel Committee specifies
--         for the backtesting traffic light assessment.
rolling_exceptions AS (
  SELECT
    backtest_date,
    desk,
    var_97_5_usd,
    actual_loss_usd,
    is_exception,
    exception_magnitude,
    SUM({{ cast_to_int('is_exception') }}) OVER (
      PARTITION BY desk
      ORDER BY backtest_date ASC
      ROWS BETWEEN 249 PRECEDING AND CURRENT ROW
    )                                                    AS rolling_250d_exceptions
  FROM exceptions_identified
),

-- Step 5: Apply Basel traffic light zones.
--         Thresholds defined once here — single place to update
--         if regulatory requirements change.
basel_assessment AS (
  SELECT
    backtest_date,
    desk,
    var_97_5_usd,
    actual_loss_usd,
    is_exception,
    exception_magnitude,
    rolling_250d_exceptions,
    CASE
      WHEN rolling_250d_exceptions >= 10 THEN 'RED'
      WHEN rolling_250d_exceptions >=  5 THEN 'AMBER'
      ELSE                                    'GREEN'
    END                                                  AS basel_zone,
    CASE
      WHEN rolling_250d_exceptions >= 10 THEN 'REJECTED'
      WHEN rolling_250d_exceptions >=  5 THEN 'REVIEW'
      ELSE                                    'ACCEPTED'
    END                                                  AS model_status
  FROM rolling_exceptions
)

SELECT
  backtest_date,
  desk,
  var_97_5_usd                        AS var_97_5_usd,
  actual_loss_usd                     AS actual_loss_usd,
  is_exception,
  exception_magnitude,
  {{ cast_to_int('rolling_250d_exceptions') }} AS rolling_250d_exceptions,
  basel_zone,
  model_status,
  current_timestamp()                 AS _gold_loaded_at
FROM basel_assessment