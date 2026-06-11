{{
    config(
        materialized='incremental',
        schema='gold',
        incremental_strategy='merge',
        unique_key=['desk', 'calculation_date'],
        merge_update_columns=[
            'actual_pnl_usd',
            'var_99_usd',
            'pnl_to_var_ratio',
            'limit_utilisation_pct',
            'pnl_per_unit_utilisation',
            'unexplained_pnl_pct',
            'performance_flag',
            '_gold_loaded_at'
        ],
        on_schema_change='fail',
        tags=['gold', 'performance', 'pnl_efficiency']
    )
}}

/*
  gold.risk_adjusted_performance
  ─────────────────────────────────────────────────────────────────────────────
  Purpose : Risk-adjusted performance — evaluates whether PnL justifies
            the risk taken and limit consumed by each desk.
  Source  : gold.pnl_attribution + gold.var_daily + gold.exposure_monitor
  Grain   : One row per desk per calculation_date
  Unique key : [calculation_date, desk]
 
  Rounding convention:
    - Monetary _usd columns        : no rounding — full DOUBLE precision
    - pnl_to_var_ratio             : ROUND(x, 4)
    - pnl_per_unit_utilisation     : no rounding — monetary per pct unit
    - unexplained_pnl_pct          : ROUND(x, 4)
 
  Performance Flag logic:
    STRONG   — actual_pnl > var_99  (earned more than daily risk capacity)
    ADEQUATE — 0 < actual_pnl <= var_99  (positive but below risk capacity)
    LOSS     — actual_pnl <= 0  (desk lost money today)
  ─────────────────────────────────────────────────────────────────────────────
*/

WITH
 
-- Step 1: Today's PnL from pnl_attribution.
today_pnl AS (
  SELECT
    {{ cast_to_string('desk') }} AS desk,
    {{ cast_to_double('actual_pnl_usd') }} AS actual_pnl_usd,
    {{ cast_to_double('hypothetical_pnl_usd') }} AS hypothetical_pnl_usd,
    {{ cast_to_double('unexplained_pnl_usd') }} AS unexplained_pnl_usd
  FROM {{ ref('pnl_attribution') }}
  WHERE pnl_date = CURRENT_DATE()
),

-- Step 2: Desk-level VaR — sum across all asset classes.
--         VaR 99% serves as the proxy for economic capital consumed.
today_var AS (
  SELECT
    {{ cast_to_string('desk') }} AS desk,
    SUM(var_99_usd)         AS var_99_usd
  FROM {{ ref('var_daily') }}
  WHERE calculation_date = CURRENT_DATE()
  GROUP BY desk
),

-- Step 3: Today's limit utilisation.
today_exposure AS (
  SELECT
    {{ cast_to_string('desk') }} AS desk,
    {{ cast_to_double('utilisation_pct') }} AS utilisation_pct
  FROM {{ ref('exposure_monitor') }}
  WHERE as_of_date = CURRENT_DATE()
),
 
-- Step 4: Calculate performance metrics.
--
--         pnl_to_var_ratio:
--           > 1.0  = earned more than daily risk capacity (strong)
--           0 to 1 = positive P&L but below risk capacity (adequate)
--           < 0    = loss
--
--         pnl_per_unit_utilisation:
--           Higher = more P&L generated per 1% of limit consumed.
--           Enables fair comparison across desks — a desk using 90%
--           of its limit should generate more P&L than one using 30%.
--
--         unexplained_pnl_pct:
--           High % signals unmodelled risk or booking errors.
--           A model quality red flag when > 20%.
combined AS (
  SELECT
    p.desk,
    p.actual_pnl_usd,
    p.unexplained_pnl_usd,
    v.var_99_usd,
    e.utilisation_pct,
    -- PnL to VaR ratio — ROUND to 4dp
    CASE
      WHEN v.var_99_usd > 0
        THEN ROUND(p.actual_pnl_usd / v.var_99_usd, 4)
      ELSE NULL
    END                                                AS pnl_to_var_ratio,
    -- PnL per unit of limit utilisation
    CASE
      WHEN e.utilisation_pct > 0
        THEN p.actual_pnl_usd / e.utilisation_pct
      ELSE NULL
    END                                                AS pnl_per_unit_utilisation,
    -- Unexplained PnL as % of total actual PnL — ROUND to 4dp
    CASE
      WHEN ABS(p.actual_pnl_usd) > 0
        THEN ROUND(
          (p.unexplained_pnl_usd
           / ABS(p.actual_pnl_usd)) * 100,
          4
        )
      ELSE 0.0
    END                                                AS unexplained_pnl_pct
  FROM today_pnl              p
  LEFT JOIN today_var         v ON p.desk = v.desk
  LEFT JOIN today_exposure    e ON p.desk = e.desk
),

-- Step 5: Apply performance flag.
performance_flagged AS (
  SELECT
    desk,
    actual_pnl_usd,
    var_99_usd,
    pnl_to_var_ratio,
    utilisation_pct,
    pnl_per_unit_utilisation,
    unexplained_pnl_pct,
    CASE
      WHEN actual_pnl_usd > var_99_usd THEN 'STRONG'
      WHEN actual_pnl_usd > 0          THEN 'ADEQUATE'
      ELSE                                  'LOSS'
    END                                                AS performance_flag
  FROM combined
)

SELECT
  CURRENT_DATE()          AS calculation_date,
  desk,
  actual_pnl_usd,
  var_99_usd,
  pnl_to_var_ratio,
  utilisation_pct         AS limit_utilisation_pct,
  pnl_per_unit_utilisation,
  unexplained_pnl_pct,
  performance_flag,
  current_timestamp()     AS _gold_loaded_at
FROM performance_flagged