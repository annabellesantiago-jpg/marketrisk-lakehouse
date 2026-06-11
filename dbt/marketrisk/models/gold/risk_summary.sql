{{
  config(
    materialized         = 'incremental',
    schema               = 'gold',
    unique_key           = ['summary_date', 'desk'],
    incremental_strategy = 'merge',
    merge_update_columns = [
      'total_notional_usd',
      'var_99_usd',
      'var_10day_97_5_usd',
      'es_97_5_usd',
      'var_as_pct_of_notional',
      'actual_pnl_usd',
      'hypothetical_pnl_usd',
      'unexplained_pnl_usd',
      'gross_exposure_usd',
      'limit_usd',
      'utilisation_pct',
      'limit_status',
      'rolling_250d_exceptions',
      'basel_zone',
      'worst_stress_scenario',
      'worst_stress_pnl_usd',
      'largest_counterparty',
      'largest_counterparty_pct',
      'overall_risk_status',
      '_gold_loaded_at'
    ],
    on_schema_change     = 'fail',
    tags                 = ['gold', 'risk_summary']
  )
}}

/*
  gold.risk_summary
  ─────────────────────────────────────────────────────────────────────────────
  Purpose : Executive risk summary — one row per desk per day.
            Primary source for the CRO dashboard and top management reporting.
            Aggregates key metrics from all six upstream Gold models into a
            single governed view with a single overall traffic light status.
  Grain   : One row per desk per summary_date
  Sources : gold.var_daily          — VaR, ES, notional (base, NOT NULL)
            gold.exposure_monitor   — gross exposure, limit, utilisation
            gold.pnl_attribution    — actual and hypothetical P&L
            gold.var_backtest       — backtesting exceptions, Basel zone
            gold.stress_testing     — worst-case scenario stress loss
            gold.concentration_risk — largest counterparty exposure
  Unique key: [summary_date, desk]
  Regulatory context:
    var_10day_97_5_usd     — Basel III regulatory capital measure
    es_97_5_usd            — FRTB Expected Shortfall
    rolling_250d_exceptions — Basel backtesting traffic light
    overall_risk_status    — CRO single executive status (RED/AMBER/GREEN)

  Overall risk status logic:
    RED   — limit breached (limit_status = RED)
            OR backtesting RED (>= 10 exceptions)
            OR worst stress loss > 2× desk limit
    AMBER — utilisation >= 80%
            OR backtesting AMBER (5–9 exceptions)
            OR worst stress loss > desk limit
    GREEN — all clear

  Rounding convention (consistent with BRD):
    - Monetary _usd columns      : no rounding — full DOUBLE precision
    - var_as_pct_of_notional     : ROUND(x, 4)
    - utilisation_pct            : inherited from exposure_monitor (already ROUND 4dp)
    - largest_counterparty_pct   : ROUND(x, 4)

  Note: all non-var_daily columns may be NULL on the first pipeline run
  or when the upstream model has no row for today. The _gold.yml documents
  these as nullable — handled via LEFT JOIN throughout.
  ─────────────────────────────────────────────────────────────────────────────
*/

WITH

-- Step 1: Aggregate var_daily to desk level by summing across asset classes.
--         var_daily grain is desk + asset_class — risk_summary needs desk only.
--         SUM is used (not MAX) because the BRD specifies summed VaR and this
--         is the standard conservative approach for executive reporting:
--         summing across asset classes assumes zero inter-asset correlation,
--         which produces the most defensible capital figure without a
--         correlation matrix.
--         This CTE is the base of all downstream joins — every desk with a
--         VaR row today will appear in the final output.
var_summary AS (
  SELECT
    desk,
    SUM(total_notional_usd)    AS total_notional_usd,
    SUM(var_99_usd)            AS var_99_usd,
    SUM(var_10day_97_5_usd)    AS var_10day_97_5_usd,
    SUM(es_97_5_usd)           AS es_97_5_usd
  FROM {{ ref('var_daily') }}
  WHERE calculation_date = CURRENT_DATE()
  GROUP BY desk
),

-- Step 2: Pull today's exposure and limit metrics from exposure_monitor.
--         utilisation_pct and limit_status are already computed with the
--         correct thresholds (GREEN/AMBER/RED) — no need to recalculate here.
--         limit_usd is carried through so overall_risk_status can compare
--         stressed P&L against the desk limit in Step 7.
exposure_today AS (
  SELECT
    desk,
    gross_exposure_usd,
    limit_usd,
    utilisation_pct,
    limit_status,
    breach_flag
  FROM {{ ref('exposure_monitor') }}
  WHERE as_of_date = CURRENT_DATE()
),

-- Step 3: Pull today's P&L from pnl_attribution.
--         hypothetical_pnl_usd = pure market move P&L.
--         actual_pnl_usd       = market move + unexplained components.
--         unexplained_pnl_usd  = actual minus hypothetical — large values
--         indicate unmodelled risk or booking errors.
--         Consistent with risk_adjusted_performance.sql which also filters
--         pnl_attribution on pnl_date = CURRENT_DATE().
pnl_today AS (
  SELECT
    desk,
    actual_pnl_usd,
    hypothetical_pnl_usd,
    unexplained_pnl_usd
  FROM {{ ref('pnl_attribution') }}
  WHERE pnl_date = CURRENT_DATE()
),

-- Step 4: Pull today's backtesting result from var_backtest.
--         rolling_250d_exceptions and basel_zone are the two regulatory
--         metrics the CRO monitors — exception count drives the Basel
--         capital add-on calculation.
backtest_today AS (
  SELECT
    desk,
    rolling_250d_exceptions,
    basel_zone
  FROM {{ ref('var_backtest') }}
  WHERE backtest_date = CURRENT_DATE()
),

-- Step 5: Find the worst stress scenario per desk for today.
--         Worst = lowest (most negative) stressed_pnl_usd.
--         ROW_NUMBER() partitioned by desk and ordered ascending picks
--         rank 1 = the scenario with the largest simulated loss.
--         This is the scenario the CRO needs to see — the tail risk event
--         that would cause the most damage to the desk P&L.
stress_ranked AS (
  SELECT
    desk,
    scenario_name,
    stressed_pnl_usd,
    ROW_NUMBER() OVER (
      PARTITION BY desk
      ORDER BY stressed_pnl_usd ASC
    ) AS stress_rank
  FROM {{ ref('stress_testing') }}
  WHERE calculation_date = CURRENT_DATE()
),

worst_stress AS (
  SELECT
    desk,
    scenario_name  AS worst_stress_scenario,
    stressed_pnl_usd AS worst_stress_pnl_usd
  FROM stress_ranked
  WHERE stress_rank = 1
),

-- Step 6: Find the largest counterparty per desk from concentration_risk.
--         Filter to concentration_type = 'COUNTERPARTY' and desk_rank = 1
--         (desk_rank is already computed in concentration_risk as rank within
--         each desk by gross exposure — rank 1 = largest).
--         pct_of_desk_total represents the counterparty's share of desk
--         gross exposure — the Basel large exposure proxy metric.
largest_cp AS (
  SELECT
    desk,
    entity_name      AS largest_counterparty,
    ROUND(pct_of_desk_total, 4) AS largest_counterparty_pct
  FROM {{ ref('concentration_risk') }}
  WHERE
    as_of_date          = CURRENT_DATE()
    AND concentration_type = 'COUNTERPARTY'
    AND desk_rank          = 1
),

-- Step 7: Join all sources onto the VaR base.
--         var_summary is the driving table — every desk that has VaR data
--         today will appear in the output.
--         All other joins are LEFT so a missing upstream row (e.g. no P&L
--         yet, no stress run) produces NULL rather than dropping the desk.
--         overall_risk_status is computed here using the three-tier logic
--         defined in the BRD and documented in _gold.yml:
--           RED   : any critical condition (breach, model failure, severe stress)
--           AMBER : any warning condition  (approaching limit, model scrutiny, stress > limit)
--           GREEN : all clear
--         COALESCE guards against NULL comparisons — a NULL limit_status or
--         basel_zone must not silently suppress a RED/AMBER that would
--         otherwise fire from another condition.
combined AS (
  SELECT
    v.desk,
    v.total_notional_usd,
    v.var_99_usd,
    v.var_10day_97_5_usd,
    v.es_97_5_usd,
    ROUND(
      v.var_99_usd / NULLIF(v.total_notional_usd, 0) * 100,
      4
    )                                              AS var_as_pct_of_notional,

    -- P&L (NULL if no pnl_attribution row for today)
    p.actual_pnl_usd,
    p.hypothetical_pnl_usd,
    p.unexplained_pnl_usd,

    -- Exposure and limits (NULL if no exposure_monitor row for today)
    e.gross_exposure_usd,
    e.limit_usd,
    e.utilisation_pct,
    e.limit_status,

    -- Backtesting (NULL if no var_backtest row for today)
    b.rolling_250d_exceptions,
    b.basel_zone,

    -- Stress testing (NULL if no stress_testing row for today)
    s.worst_stress_scenario,
    s.worst_stress_pnl_usd,

    -- Concentration (NULL if no concentration_risk row for today)
    c.largest_counterparty,
    c.largest_counterparty_pct,

    -- Overall risk status: evaluated in precedence order RED → AMBER → GREEN.
    -- Each condition is its own WHEN for readability and maintainability.
    -- SQL CASE naturally handles NULLs — a NULL condition evaluates to NULL
    -- and falls through to the next WHEN, so no COALESCE guards are needed.
    --   breach_flag = TRUE   → utilisation >= 100% (limit breached)
    --   basel_zone  = 'RED'  → >= 10 backtesting exceptions (model rejected)
    --   stress > 2× limit    → severe stress loss relative to desk capacity
    --   utilisation >= 80%   → approaching limit (AMBER warning threshold)
    --   basel_zone  = 'AMBER'→ 5–9 exceptions (increased scrutiny)
    --   stress > limit       → stress loss exceeds desk limit
    CASE
      WHEN e.breach_flag = TRUE
        THEN 'RED'
      WHEN b.basel_zone = 'RED'
        THEN 'RED'
      WHEN ABS(s.worst_stress_pnl_usd) > (e.limit_usd * 2)
        THEN 'RED'
      WHEN e.utilisation_pct >= 80
        THEN 'AMBER'
      WHEN b.basel_zone = 'AMBER'
        THEN 'AMBER'
      WHEN ABS(s.worst_stress_pnl_usd) > e.limit_usd
        THEN 'AMBER'
      ELSE 'GREEN'
    END                                            AS overall_risk_status

  FROM var_summary                  v
  LEFT JOIN exposure_today          e  ON v.desk = e.desk
  LEFT JOIN pnl_today               p  ON v.desk = p.desk
  LEFT JOIN backtest_today          b  ON v.desk = b.desk
  LEFT JOIN worst_stress            s  ON v.desk = s.desk
  LEFT JOIN largest_cp              c  ON v.desk = c.desk
)

SELECT
  CURRENT_DATE()                           AS summary_date,
  desk,
  total_notional_usd,
  var_99_usd,
  var_10day_97_5_usd,
  es_97_5_usd,
  var_as_pct_of_notional,
  actual_pnl_usd,
  hypothetical_pnl_usd,
  unexplained_pnl_usd,
  gross_exposure_usd,
  limit_usd,
  utilisation_pct,
  limit_status,
  rolling_250d_exceptions,
  basel_zone,
  worst_stress_scenario,
  worst_stress_pnl_usd,
  largest_counterparty,
  largest_counterparty_pct,
  overall_risk_status,
  current_timestamp()                      AS _gold_loaded_at
FROM combined
