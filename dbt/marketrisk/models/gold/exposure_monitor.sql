{{
    config(
        materialized        ='incremental',
        schema              ='gold',
        unique_key          =['as_of_date', 'desk'],
        incremental_strategy='merge',
        merge_update_columns=[
            'gross_exposure_usd',
            'net_exposure_usd',  
            'limit_usd',         
            'utilisation_pct',   
            'limit_status',      
            'warning_flag',      
            'breach_flag',       
            'long_exposure_usd', 
            'short_exposure_usd',
            'position_count',    
            '_gold_loaded_at'
        ],
        on_schema_change    ='fail',
        tags                =['gold', 'exposure_monitor']
    )
}}


/*
    gold.exposure_monitor
    ─────────────────────────────────────────────────────────────────────────────
    Purpose : Monitor daily market risk exposure against limits at the desk level
    Method  : Aggregate enriched positions to desk level, join to limits, calculate
              utilisation and apply traffic light logic for risk assessment.
    Grain   : One row per desk per day (as_of_date)
    Source  : silver.positions_enriched + bronze.desk_limits
    Unique key: as_of_date + desk
    Notes   : Gross exposure = sum of absolute notional values (LONG and SHORT both consume limit).
              Net exposure = sum of signed notional values (SHORT reduces net exposure).
              Utilisation = gross exposure as % of limit.
              Traffic light thresholds: GREEN < 80%, AMBER 80-99.99%, RED >= 100%.
              Warning flag raised at AMBER to trigger pre-breach review.
              Breach flag raised at RED to trigger breach response.
    ─────────────────────────────────────────────────────────────────────────────
*/


WITH
-- Step 1: Aggregate positions to desk level.
--         Gross exposure = sum of absolute notional values.
--         Most risk limits are measured against gross exposure because
--         SHORT positions consume limit capacity the same as LONG positions.
desk_exposure AS (
  SELECT
    desk,
    COUNT(DISTINCT trade_id)                              AS position_count,
    SUM(ABS(notional_usd))                                AS gross_exposure_usd,
    SUM(net_exposure_usd)                                 AS net_exposure_usd,
    SUM(
      CASE WHEN direction = 'LONG'
           THEN notional_usd ELSE 0 END
    )                                                     AS long_exposure_usd,
    SUM(
      CASE WHEN direction = 'SHORT'
               THEN notional_usd ELSE 0 END
    )                                                     AS short_exposure_usd
  FROM {{ ref('positions_enriched') }}
  GROUP BY desk
),

-- Step 2: Join desk limits from Bronze reference table.
--         INNER JOIN because every desk must have a limit defined.
--         A missing limit is a data quality problem that should surface
--         as a missing row rather than a null utilisation figure.
exposure_with_limits AS (
  SELECT
    de.desk,
    de.position_count,
    de.gross_exposure_usd,
    de.net_exposure_usd,
    de.long_exposure_usd,
    de.short_exposure_usd,
    {{ cast_to_double('dl.limit_usd') }}                          AS limit_usd,
    ROUND(
      (de.gross_exposure_usd / {{ cast_to_double('dl.limit_usd') }}) * 100,
      4
    )                                                     AS utilisation_pct
  FROM desk_exposure                            de
  INNER JOIN {{ source('bronze', 'desk_limits') }} dl
    ON UPPER(de.desk) = UPPER(dl.desk)
),

-- Step 3: Apply traffic light logic.
--         All three status columns derived from utilisation_pct in one CTE
--         so thresholds are defined once — single place to update if
--         business rules change (e.g. warning threshold moves from 80% to 75%).
status_applied AS (
  SELECT
    desk,
    position_count,
    gross_exposure_usd,
    net_exposure_usd,
    limit_usd,
    utilisation_pct,
    long_exposure_usd,
    short_exposure_usd,
    CASE
      WHEN utilisation_pct >= 100 THEN 'RED'
      WHEN utilisation_pct >=  80 THEN 'AMBER'
      ELSE                             'GREEN'
    END                                                   AS limit_status,
    CASE
      WHEN utilisation_pct >= 80
       AND utilisation_pct <  100 THEN TRUE
      ELSE FALSE
    END                                                   AS warning_flag,
    CASE
      WHEN utilisation_pct >= 100 THEN TRUE
      ELSE FALSE
    END                                                   AS breach_flag
  FROM exposure_with_limits
)

SELECT
  CURRENT_DATE()                      AS as_of_date,
  desk,                              
  gross_exposure_usd,
  net_exposure_usd,
  limit_usd,
  utilisation_pct,
  limit_status,
  warning_flag,
  breach_flag,
  long_exposure_usd,
  short_exposure_usd,
  {{ cast_to_int('position_count') }} AS position_count,
  current_timestamp()                 AS _gold_loaded_at
FROM status_applied