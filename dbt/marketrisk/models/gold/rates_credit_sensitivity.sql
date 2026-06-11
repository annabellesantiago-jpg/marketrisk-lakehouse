{{
  config(
    materialized         = 'incremental',
    schema               = 'gold',
    unique_key           = ['calculation_date', 'desk', 'asset_class', 'tenor_bucket', 'rate_currency'],
    incremental_strategy = 'merge',
    merge_update_columns = [
      'position_count',
      'matured_position_count',
      'gross_notional_usd',
      'net_notional_usd',
      'weighted_avg_duration_years',
      'pv01_usd',
      'pv01_up_1bp_usd',
      'pv01_down_1bp_usd',
      'dv01_100bps_usd',
      'cs01_usd',
      'cs01_widen_1bp_usd',
      'cs01_tighten_1bp_usd',
      'cs01_100bps_usd',
      '_gold_loaded_at'
    ],
    on_schema_change     = 'fail',
    tags                 = ['gold', 'sensitivity', 'rates', 'credit']
  )
}}

/*
  gold.rates_credit_sensitivity
  ─────────────────────────────────────────────────────────────────────────────
  Purpose : PV01 and CS01 sensitivity measures by desk, asset class, tenor
            bucket and rate currency. Measures P&L impact of 1 basis point
            interest rate or credit spread move.
  Source  : silver.positions_enriched
  Filter  : Rates and Credit positions with days_to_maturity > 0 only.
            Matured positions (days_to_maturity <= 0) are excluded but counted
            in matured_position_count for data quality monitoring.
  Grain   : One row per desk per asset_class per tenor_bucket per rate_currency
            per calculation_date
  Unique key: [calculation_date, desk, asset_class, tenor_bucket, rate_currency]
  Regulatory: FRTB tenor-bucketed sensitivity reporting. PV01 for Rates
              capital, CS01 for Credit capital.
 
  Rounding convention:
    - Monetary _usd columns        : no rounding — full DOUBLE precision
    - weighted_avg_duration_years  : ROUND(x, 4) — already a proxy
    - Percentage/ratio cols        : ROUND(x, 4) — e.g. 0.1234% = 0.001234
 
  CRITICAL — Sign Convention:
  Rates and bond prices move in OPPOSITE directions:
    - Rates RISE  → bond prices FALL → LONG bond position LOSES
    - Rates FALL  → bond prices RISE → LONG bond position GAINS
  Same relationship applies to credit spreads:
    - Spreads WIDEN    → credit prices FALL → LONG credit LOSES
    - Spreads TIGHTEN  → credit prices RISE → LONG credit GAINS
 
  pv01_up_1bp_usd   = signed_sensitivity × (-1)
  pv01_down_1bp_usd = signed_sensitivity × (+1)
 
  KNOWN LIMITATION:
  Duration is approximated as days_to_maturity / 365.
  True modified duration requires yield curve data, coupon schedules,
  and a bond pricing engine. Suitable for portfolio project demonstration —
  not for regulatory submission without a validated pricing model.
  ─────────────────────────────────────────────────────────────────────────────
*/


WITH
 
-- Step 1: Count matured positions for data quality reporting.
--         These are excluded from sensitivity calculations but we track
--         how many were excluded so downstream users can assess coverage.
matured_counts AS (
  SELECT
    {{ cast_to_string(desk       ) }} AS desk,
    {{ cast_to_string(asset_class) }} AS asset_class,
    {{ cast_to_string(currency   ) }} AS rate_currency,
    COUNT(DISTINCT trade_id)          AS matured_position_count
  FROM {{ ref('positions_enriched') }}
  WHERE
    UPPER(asset_class) IN ('RATES', 'CREDIT')
    AND COALESCE(days_to_maturity, 0) <= 0
  GROUP BY
    desk,
    asset_class,
    currency
),

-- Step 2: Filter to live Rates and Credit positions.
--         Calculate duration proxy: years to maturity.
--         GREATEST ensures we never get a negative duration from
--         positions that are technically expired but not yet settled.
live_positions AS (
  SELECT
    {{ cast_to_string('desk'                 ) }} AS desk,
    {{ cast_to_string('asset_class'          ) }} AS asset_class,
    {{ cast_to_string('currency'             ) }} AS rate_currency,
    {{ cast_to_string('trade_id'             ) }} AS trade_id,
    {{ cast_to_double('notional_usd'         ) }} AS notional_usd,
    {{ cast_to_int('direction_multiplier'    ) }} AS direction_multiplier,
    {{ cast_to_int('days_to_maturity'        ) }} AS days_to_maturity,
    -- Duration proxy: years to maturity, floored at 0
    GREATEST ({{ cast_to_double('days_to_maturity') }} / 365.0, 0.0) AS duration_years
  FROM {{ ref('positions_enriched') }}
  WHERE
    UPPER(asset_class) IN ('RATES', 'CREDIT')
    AND days_to_maturity > 0
    AND trade_id IS NOT NULL
),

-- Step 3: Assign FRTB tenor bucket based on days_to_maturity.
--         FRTB standard tenor buckets group positions by remaining maturity.
--         The bucket determines which part of the yield curve the position
--         is most sensitive to. A 2-year bond is sensitive to the 2Y rate,
--         not the 10Y rate — tenor bucketing captures this.
positions_with_tenor AS (
  SELECT
    desk,
    asset_class,
    rate_currency,
    trade_id,
    notional_usd,
    direction_multiplier,
    duration_years,
    CASE
      WHEN days_to_maturity <=  365 THEN '0-1Y'
      WHEN days_to_maturity <= 1825 THEN '1-5Y'
      WHEN days_to_maturity <= 3650 THEN '5-10Y'
      ELSE                               '10Y+'
    END AS tenor_bucket
  FROM live_positions
),

-- Step 4: Aggregate to desk + asset_class + tenor_bucket + rate_currency level.
--         Weighted average duration weights larger positions more heavily —
--         a 50M position contributes more to sensitivity than a 1M position.
--         signed_sensitivity captures the directional P&L of a 1bp move.
sensitivity_aggregated AS (
  SELECT
    desk,
    asset_class,
    tenor_bucket,
    rate_currency,
    COUNT(DISTINCT trade_id)                              AS position_count,
    SUM(ABS(notional_usd))                                AS gross_notional_usd,
    SUM(notional_usd * direction_multiplier)              AS net_notional_usd,
    -- Notional-weighted average duration
    SUM(ABS(notional_usd) * duration_years)
      / NULLIF(SUM(ABS(notional_usd)), 0)                 AS weighted_avg_duration_years,
    -- Signed sensitivity: P&L of a 1bp RATE INCREASE (before sign correction)
    -- For a long bond: notional positive, direction=+1, duration positive
    -- → signed_sensitivity is positive
    -- After × (-1): pv01_up becomes negative = LOSS when rates rise. Correct.
    SUM(notional_usd * direction_multiplier * duration_years * 0.0001)
                                                          AS signed_sensitivity
  FROM positions_with_tenor
  GROUP BY
    desk,
    asset_class,
    tenor_bucket,
    rate_currency
),

-- Step 5: Derive all PV01 and CS01 metrics from signed_sensitivity.
--         PV01 applies to Rates positions; CS01 applies to Credit positions.
--         The formulas are identical — the naming convention differs by
--         regulatory context (rates vs credit).
--
--         SIGN CONVENTION — CRITICAL:
--         Rates UP → bond prices DOWN → long bond LOSES:
--           pv01_up_1bp_usd   = signed_sensitivity × (-1)  → negative for long bonds ✓
--         Rates DOWN → bond prices UP → long bond GAINS:
--           pv01_down_1bp_usd = signed_sensitivity × (+1)  → positive for long bonds ✓
--         Same logic applies to CS01 (spreads WIDEN = prices DOWN).
sensitivity_with_metrics AS (
  SELECT
    desk,
    asset_class,
    tenor_bucket,
    rate_currency,
    position_count,
    gross_notional_usd,
    net_notional_usd,
    weighted_avg_duration_years,
    signed_sensitivity,
    -- PV01: applies to Rates positions only
    CASE WHEN UPPER(asset_class) = 'RATES'
      THEN ABS(signed_sensitivity)
      ELSE 0.0
    END                                                   AS pv01_usd,
    -- pv01_up_1bp: P&L if rates RISE 1bp
    -- × (-1) because rates up = prices down = LOSS for long bond
    CASE WHEN UPPER(asset_class) = 'RATES'
      THEN signed_sensitivity * (-1)
      ELSE 0.0
    END                                                   AS pv01_up_1bp_usd,
    -- pv01_down_1bp: P&L if rates FALL 1bp
    -- Positive for long bonds (prices rise when rates fall)
    CASE WHEN UPPER(asset_class) = 'RATES'
      THEN signed_sensitivity * (+1)
      ELSE 0.0
    END                                                   AS pv01_down_1bp_usd,
    -- DV01 for 100bp shock — standard management reporting threshold
    CASE WHEN UPPER(asset_class) = 'RATES'
      THEN ABS(signed_sensitivity) * 100
      ELSE 0.0
    END                                                   AS dv01_100bps_usd,
    -- CS01: applies to Credit positions only
    CASE WHEN UPPER(asset_class) = 'CREDIT'
      THEN ABS(signed_sensitivity)
      ELSE 0.0
    END                                                   AS cs01_usd,
    -- cs01_widen_1bp: P&L if spreads WIDEN 1bp
    -- × (-1) because spread widening = prices down = LOSS for long credit
    CASE WHEN UPPER(asset_class) = 'CREDIT'
      THEN signed_sensitivity * (-1)
      ELSE 0.0
    END                                                   AS cs01_widen_1bp_usd,
    -- cs01_tighten_1bp: P&L if spreads TIGHTEN 1bp
    -- Positive for long credit (tighter spreads = prices rise)
    CASE WHEN UPPER(asset_class) = 'CREDIT'
      THEN signed_sensitivity * (+1)
      ELSE 0.0
    END                                                   AS cs01_tighten_1bp_usd,
    -- CS01 for 100bp shock — standard management reporting
    CASE WHEN UPPER(asset_class) = 'CREDIT'
      THEN ABS(signed_sensitivity) * 100
      ELSE 0.0
    END                                                   AS cs01_100bps_usd
  FROM sensitivity_aggregated
)

SELECT
  CURRENT_DATE()                           AS calculation_date,
  s.desk,
  s.asset_class,
  s.tenor_bucket,
  s.rate_currency,
  {{ cast_to_int('s.position_count') }}      AS position_count,
  COALESCE({{ cast_to_int('m.matured_position_count') }}, 0) AS matured_position_count,
  s.gross_notional_usd,
  s.net_notional_usd,
  s.weighted_avg_duration_years,
  s.pv01_usd,
  s.pv01_up_1bp_usd,
  s.pv01_down_1bp_usd,
  s.dv01_100bps_usd,
  s.cs01_usd,
  s.cs01_widen_1bp_usd,
  s.cs01_tighten_1bp_usd,
  s.cs01_100bps_usd,
  current_timestamp()                      AS _gold_loaded_at
FROM sensitivity_with_metrics           s
LEFT JOIN matured_counts                m
  ON  s.desk         = m.desk
  AND s.asset_class  = m.asset_class
  AND s.rate_currency = m.rate_currency