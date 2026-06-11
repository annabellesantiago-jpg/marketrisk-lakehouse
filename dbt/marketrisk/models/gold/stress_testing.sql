{{
  config(
    materialized         = 'incremental',
    schema               = 'gold',
    unique_key           = ['calculation_date', 'desk', 'scenario_name'],
    incremental_strategy = 'merge',
    merge_update_columns = [
      'scenario_type',
      'is_stressed_var_proxy',
      'position_count',
      'total_notional_usd',
      'portfolio_total_notional_usd',
      'coverage_pct',
      'stressed_pnl_usd',
      'stressed_pnl_equity_usd',
      'stressed_pnl_fx_usd',
      'stressed_pnl_rates_usd',
      'stressed_pnl_credit_usd',
      'worst_asset_class',
      'worst_asset_class_pnl_usd',
      'var_99_usd',
      'stressed_loss_to_var_ratio',
      'desk_limit_usd',
      'stressed_loss_to_limit_pct',
      '_gold_loaded_at'
    ],
    on_schema_change     = 'fail',
    tags                 = ['gold', 'stress_testing']
  )
}}

/*
  gold.stress_testing
  ─────────────────────────────────────────────────────────────────────────────
  Purpose : Named scenario stress test P&L impacts by desk. Applies
            predefined market shocks to all positions. Compares stressed
            losses to VaR and desk limits for regulatory context.
  Source  : silver.positions_enriched + hardcoded scenarios CTE
            + gold.var_daily + bronze.desk_limits
  Grain   : One row per desk per scenario_name per calculation_date
  Unique key: [calculation_date, desk, scenario_name]
  Regulatory: ICAAP regulatory stress testing. Basel III sVaR proxy.
              MAS prescribed scenarios.
 
  Rounding convention:
    - Monetary _usd columns        : no rounding — full DOUBLE precision
    - coverage_pct                 : ROUND(x, 4)
    - stressed_loss_to_var_ratio   : ROUND(x, 4)
    - stressed_loss_to_limit_pct   : ROUND(x, 4)

  CRITICAL — Sign Convention for Rates and Credit:
  Rates/credit shocks are in basis points (shock_bps), NOT percentages.
  The P&L formula includes × (-1) to capture the INVERSE relationship
  between rates/spreads and prices:
 
  Equity/FX  : stressed_pnl = notional × shock_pct × direction_multiplier
  Rates/Credit: stressed_pnl = notional × duration × shock_bps × 0.0001
                               × direction_multiplier × (-1)
 
  WITHOUT × (-1): a LONG bond position shows a GAIN when rates spike.
  This is WRONG — rates up = bond prices down = LONG bond LOSES.
 
  Coverage metric: only positions whose asset_class matches a scenario
  row are included. Positions with no matching scenario are excluded.
  coverage_pct shows what percentage of the portfolio was actually stressed.
  ─────────────────────────────────────────────────────────────────────────────
*/

WITH
 
-- Step 1: Load scenarios from the scenario_definitions seed maintained by risk management. 
-- This is the "shock table" that defines how we stress each asset class.
--         Each row is one scenario × asset_class combination.
--         shock_pct: direct percentage applied to notional (Equity, FX).
--                    NULL for Rates and Credit.
--         shock_bps: basis points applied via duration (Rates, Credit).
--                    NULL for Equity and FX.
--         Positive shock_pct = price increase.
--         Positive shock_bps = rate/spread increase.
--         scenario_type distinguishes regulatory (must report) from
--         management (internal) scenarios.
scenarios AS (
  SELECT
    {{ cast_to_string('scenario_name') }}            AS scenario_name,
    {{ cast_to_string('asset_class') }}              AS asset_class,
    {{ cast_to_double('shock_pct') }}                AS shock_pct,
    {{ cast_to_double('shock_bps') }}                AS shock_bps,
    {{ cast_to_string('scenario_type') }}            AS scenario_type,
    CAST(is_stressed_var_proxy AS BOOLEAN)           AS is_stressed_var_proxy
  FROM {{ ref('stress_scenarios') }}
),

-- Step 2: Calculate total portfolio notional — denominator for coverage %.
portfolio_totals AS (
  SELECT
    SUM(ABS({{ cast_to_double('notional_usd') }})) AS portfolio_gross_usd
  FROM {{ ref('positions_enriched') }}
),

-- Step 3: Add duration proxy to all positions.
--         Duration only materially affects Rates/Credit calculations
--         but we calculate it for all positions for completeness.
positions_with_duration AS (
  SELECT
    {{ cast_to_string('desk') }}                 AS desk,
    {{ cast_to_string('asset_class') }}          AS asset_class,
    {{ cast_to_string('trade_id') }}             AS trade_id,
    {{ cast_to_double('notional_usd') }}         AS notional_usd,
    {{ cast_to_int('direction_multiplier') }}    AS direction_multiplier,
    GREATEST(
      {{ cast_to_double('COALESCE(days_to_maturity, 0)') }} / 365.0,
      0.0
    )                                    AS duration_years
  FROM {{ ref('positions_enriched') }}
  WHERE trade_id IS NOT NULL
),

-- Step 4: Cross-join positions to scenarios on matching asset class.
--         Apply the correct P&L formula per asset class type:
--
--         Equity/FX   : notional × shock_pct × direction_multiplier
--         Rates/Credit: notional × duration × shock_bps × 0.0001
--                       × direction_multiplier × (-1)
--
--         The × (-1) for Rates/Credit is CRITICAL.
--         Without it: long bond + rates spike = gain. WRONG.
--         With  it: long bond + rates spike = loss. CORRECT.
position_stress AS (
  SELECT
    p.desk,
    p.asset_class,
    p.trade_id,
    p.notional_usd,
    s.scenario_name,
    s.scenario_type,
    s.is_stressed_var_proxy,
    CASE
      WHEN s.shock_pct IS NOT NULL
        THEN p.notional_usd * s.shock_pct * p.direction_multiplier
      WHEN s.shock_bps IS NOT NULL
        THEN p.notional_usd
             * p.duration_years
             * s.shock_bps
             * 0.0001
             * p.direction_multiplier
             * (-1)
      ELSE 0.0
    END AS stressed_pnl_usd
  FROM positions_with_duration         p
  INNER JOIN scenarios                 s
    ON UPPER(p.asset_class) = UPPER(s.asset_class)
),

-- Step 5: Aggregate stressed P&L to desk + scenario level.
desk_scenario_agg AS (
  SELECT
    desk,
    scenario_name,
    MAX(scenario_type)                                AS scenario_type,
    MAX({{ cast_to_int('is_stressed_var_proxy') }}) > 0       AS is_stressed_var_proxy,
    COUNT(DISTINCT trade_id)                          AS position_count,
    SUM(ABS(notional_usd))                           AS total_notional_usd,
    SUM(stressed_pnl_usd)                            AS stressed_pnl_usd,
    SUM(CASE WHEN UPPER(asset_class) = 'EQUITY'
             THEN stressed_pnl_usd ELSE 0 END)       AS stressed_pnl_equity_usd,
    SUM(CASE WHEN UPPER(asset_class) = 'FX'
             THEN stressed_pnl_usd ELSE 0 END)       AS stressed_pnl_fx_usd,
    SUM(CASE WHEN UPPER(asset_class) = 'RATES'
             THEN stressed_pnl_usd ELSE 0 END)       AS stressed_pnl_rates_usd,
    SUM(CASE WHEN UPPER(asset_class) = 'CREDIT'
             THEN stressed_pnl_usd ELSE 0 END)       AS stressed_pnl_credit_usd
  FROM position_stress
  GROUP BY
    desk,
    scenario_name
),

-- Step 6: Identify worst asset class per desk per scenario.
--         worst = most negative P&L (largest loss contributor).
worst_asset_class AS (
  SELECT
    desk,
    scenario_name,
    asset_class                                       AS worst_asset_class,
    SUM(stressed_pnl_usd)                            AS asset_class_pnl,
    ROW_NUMBER() OVER (
      PARTITION BY desk, scenario_name
      ORDER BY SUM(stressed_pnl_usd) ASC
    )                                                AS loss_rank
  FROM position_stress
  GROUP BY
    desk,
    scenario_name,
    asset_class
),

-- Step 7: Get desk-level VaR for stressed loss comparison.
--         Sum across asset classes for desk-level total VaR.
desk_var AS (
  SELECT
    desk,
    SUM(var_99_usd) AS var_99_usd
  FROM {{ ref('var_daily') }}
  WHERE calculation_date = CURRENT_DATE()
  GROUP BY desk
),
 
-- Step 8: Get desk limits for stressed loss to limit ratio.
desk_limits AS (
  SELECT
    {{ cast_to_string('desk') }} AS desk,
    {{ cast_to_double('limit_usd') }} AS limit_usd
  FROM {{ source('bronze', 'desk_limits') }}
),
 
-- Step 9: Combine all CTEs.
combined AS (
  SELECT
    dsa.desk,
    dsa.scenario_name,
    dsa.scenario_type,
    dsa.is_stressed_var_proxy,
    dsa.position_count,
    dsa.total_notional_usd,
    pt.portfolio_gross_usd                            AS portfolio_total_notional_usd,
    ROUND(
      (dsa.total_notional_usd
       / NULLIF(pt.portfolio_gross_usd, 0)) * 100,
      4
    )                                                AS coverage_pct,
    dsa.stressed_pnl_usd,
    dsa.stressed_pnl_equity_usd,
    dsa.stressed_pnl_fx_usd,
    dsa.stressed_pnl_rates_usd,
    dsa.stressed_pnl_credit_usd,
    wac.worst_asset_class,
    wac.asset_class_pnl                              AS worst_asset_class_pnl_usd,
    dv.var_99_usd,
    CASE
      WHEN dv.var_99_usd > 0
        THEN ROUND(
          ABS(dsa.stressed_pnl_usd) / dv.var_99_usd,
          4
        )
      ELSE NULL
    END                                              AS stressed_loss_to_var_ratio,
    dl.limit_usd                                     AS desk_limit_usd,
    CASE
      WHEN dl.limit_usd > 0
        THEN ROUND(
          ABS(dsa.stressed_pnl_usd) / dl.limit_usd * 100,
          4
        )
      ELSE NULL
    END                                              AS stressed_loss_to_limit_pct
  FROM desk_scenario_agg                             dsa
  CROSS JOIN portfolio_totals                        pt
  LEFT JOIN worst_asset_class                        wac
    ON  dsa.desk          = wac.desk
    AND dsa.scenario_name = wac.scenario_name
    AND wac.loss_rank      = 1
  LEFT JOIN desk_var                                 dv
    ON  dsa.desk           = dv.desk
  LEFT JOIN desk_limits                              dl
    ON  UPPER(dsa.desk)    = UPPER(dl.desk)
)
 
SELECT
  CURRENT_DATE()                          AS calculation_date,
  desk,
  scenario_name,
  scenario_type,
  CAST(is_stressed_var_proxy AS BOOLEAN)  AS is_stressed_var_proxy,
  {{ cast_to_int('position_count') }}             AS position_count,
  total_notional_usd,
  portfolio_total_notional_usd,
  coverage_pct,
  stressed_pnl_usd,
  stressed_pnl_equity_usd,
  stressed_pnl_fx_usd,
  stressed_pnl_rates_usd,
  stressed_pnl_credit_usd,
  worst_asset_class,
  worst_asset_class_pnl_usd,
  var_99_usd,
  stressed_loss_to_var_ratio,
  desk_limit_usd,
  stressed_loss_to_limit_pct,
  current_timestamp()                     AS _gold_loaded_at
FROM combined