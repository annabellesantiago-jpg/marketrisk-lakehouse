{{
  config(
    materialized         = 'incremental',
    schema               = 'gold',
    unique_key           = ['calculation_date', 'desk', 'currency', 'asset_class'],
    incremental_strategy = 'merge',
    merge_update_columns = [
      'position_count',
      'long_position_count',
      'short_position_count',
      'gross_notional_usd',
      'long_notional_usd',
      'short_notional_usd',
      'net_notional_usd',
      'net_open_position_usd',
      'fx_rate_vs_usd',
      'fx_delta_1pct_usd',
      'fx_delta_up_1pct_usd',
      'fx_delta_down_1pct_usd',
      '_gold_loaded_at'
    ],
    on_schema_change     = 'fail',
    tags                 = ['gold', 'sensitivity', 'fx']
  )
}}

/*
  gold.fx_sensitivity
  ─────────────────────────────────────────────────────────────────────────────
  Purpose : FX Delta — P&L impact of a 1% move in each FX rate by desk
            and currency. Includes Net Open Position (NOP) for Basel III
            FX capital charge calculation.
  Source  : silver.positions_enriched
  Filter  : Non-USD positions only — USD positions have zero FX translation
            exposure since USD is the reporting currency.
  Grain   : One row per desk per currency per asset_class per calculation_date
  Unique key: [calculation_date, desk, currency, asset_class]
  Regulatory: Basel III FX capital charge. Net Open Position reporting.
  Limitation: Captures translation risk only (non-USD denomination).
              Does not capture market risk on FX instruments
              (USD-denominated FX products). Full FX Delta requires
              instrument-level pricing.
  ─────────────────────────────────────────────────────────────────────────────
*/

WITH 

-- Step 1: filter non-USD positions only
--         USD positions are excluded because USD is the reporting currency —
--         a USD notional has no FX translation sensitivity by definition.
--         We keep all asset classes because any desk can hold non-USD positions.
fx_exposed_positions AS (
   SELECT 
         {{ cast_to_string('desk') }} AS desk,
         {{ cast_to_string('currency') }} AS currency,
         {{ cast_to_string('asset_class') }} AS asset_class,
         {{ cast_to_string('trade_id') }} AS trade_id,
         {{ cast_to_string('direction') }} AS direction,
         {{ cast_to_double('notional_usd') }} AS notional_usd,
         {{ cast_to_double('direction_multiplier') }} AS direction_multiplier,
         {{ cast_to_double('fx_rate_vs_usd') }} AS fx_rate_vs_usd
      FROM {{ ref('positions_enriched') }}
     WHERE UPPER(currency) != 'USD'
       AND trade_id IS NOT NULL
),

-- Step 2: calculate FX sensitivity metrics by desk, currency and asset class
--        Position Count = number of trades contributing to FX exposure
--        Gross Notional = sum of absolute notional across all trades - total FX exposure
--        long/short notional split for NOP and regulatory reporting
--        Net Notional = sum of notional with direction (long positive, short negative)
desks_currency_asset_class_sensitivity AS (
SELECT 
      desk,
      currency,
      asset_class,
      count(DISTINCT trade_id) AS position_count,    
      count(DISTINCT CASE WHEN UPPER(direction) = 'LONG' THEN trade_id END) AS long_position_count,
      count(DISTINCT CASE WHEN UPPER(direction) = 'SHORT' THEN trade_id END) AS short_position_count,
      SUM(CASE WHEN direction_multiplier = 1 THEN notional_usd ELSE 0 END) AS long_notional_usd,
      SUM(CASE WHEN direction_multiplier = -1 THEN notional_usd ELSE 0 END) AS short_notional_usd,
      SUM(ABS(notional_usd)) AS gross_notional_usd,
      SUM(notional_usd * direction_multiplier) AS net_notional_usd,
      MAX(fx_rate_vs_usd) AS fx_rate_vs_usd      
  FROM fx_exposed_positions fx_pos
 GROUP BY desk, currency, asset_class
 ),

-- Step 3: Calculate FX deltas and Net Open Position — P&L impact of a 1% move in the FX rate
--         fx_delta_1pct_usd = ABS(net_notional) × 0.01
--         This is always positive — it represents the magnitude of P&L
--         impact regardless of direction.
--         fx_delta_up/down are signed — they show whether the desk gains
--         or loses when the foreign currency strengthens or weakens.
--         NOP = MAX(long_notional, ABS(short_notional)) — this is the
--         regulatory metric. A net long EUR 10M and short EUR 8M gives
--         NOP = 10M (the larger of the two sides).

sensitivity_calculations AS (
  SELECT
      desk,
      currency,
      asset_class,
      position_count,
      long_position_count,
      short_position_count,
      gross_notional_usd,
      long_notional_usd,
      short_notional_usd,
      net_notional_usd,
      fx_rate_vs_usd,
      -- Absolute delta: magnitude of P&L from 1% FX move, regardless of direction      
      ABS(net_notional_usd) * 0.01 AS fx_delta_1pct_usd,
      -- Directional delta up: P&L if foreign currency STRENGTHENS 1% vs USD
      -- Positive for net long position (you hold EUR, EUR gets stronger = gain)
      net_notional_usd * 0.01 AS fx_delta_up_1pct_usd,
      -- Directional delta down: P&L if foreign currency WEAKENS 1% vs USD
      -- Negative for net long position (you hold EUR, EUR gets weaker = loss)      
      net_notional_usd * -0.01 AS fx_delta_down_1pct_usd,
      GREATEST(long_notional_usd, ABS(short_notional_usd)) AS net_open_position_usd
  FROM desks_currency_asset_class_sensitivity
)

 SELECT 
        CURRENT_DATE()                            AS calculation_date,
        desk,
        currency,
        asset_class,
        {{ cast_to_int('position_count') }}       AS position_count,
        {{ cast_to_int('long_position_count') }}  AS long_position_count,
        {{ cast_to_int('short_position_count') }} AS short_position_count,
        gross_notional_usd,
        long_notional_usd,
        short_notional_usd,
        net_notional_usd,
        net_open_position_usd,
        fx_rate_vs_usd,
        fx_delta_1pct_usd,
        fx_delta_up_1pct_usd,
        fx_delta_down_1pct_usd,
        CURRENT_TIMESTAMP()                       AS _gold_loaded_at
   FROM sensitivity_calculations