{{
    config(
        materialized='incremental',
        schema='gold',
        incremental_strategy='merge',
        unique_key=['desk', 'breach_start_date'],
        merge_update_columns=[
            'breach_end_date',
            'breach_duration_days',
            'is_active',
            'peak_utilisation_pct',
            'peak_utilisation_date',
            'warning_start_date',
            'warning_duration_days',
            '_gold_loaded_at'
        ],
        on_schema_change='fail',
        tags=['gold', 'limit', 'audit']
    )
}}


/*
  gold.limit_breach_log
  ─────────────────────────────────────────────────────────────────────────────
  Purpose : Permanent historical audit trail of all limit breach episodes.
            Tracks start, duration, peak utilisation, and resolution.
  Source  : gold.exposure_monitor
  Grain   : One row per desk per breach episode (breach_start_date)
  Unique key : [desk, breach_start_date]
  Regulatory : MAS Notice 637 breach reporting. Regulatory audit trail.
 
  Rounding convention:
    - Monetary _usd columns  : no rounding — full DOUBLE precision
    - peak_utilisation_pct   : ROUND(x, 4)
    - breach_start_utilisation_pct : ROUND(x, 4)
 
  Incremental logic (three-way merge):
    First run   : INSERT all desks currently in breach.
    Subsequent  :
      Continuing breach — desk still in breach today:
        UPDATE duration + peak utilisation.
      Resolved breach — desk was breaching, no longer is:
        UPDATE breach_end_date, set is_active = FALSE.
      New breach — desk in breach with no active episode:
        INSERT new row with breach_start_date = today.
 
    The dbt MERGE handles this correctly because:
      Continuing and resolved breaches retain the original breach_start_date
      → MERGE finds the existing row and UPDATEs it.
      New breaches use breach_start_date = today
      → MERGE finds no match and INSERTs.
  ─────────────────────────────────────────────────────────────────────────────
*/


WITH
 
-- Step 1: Today's exposure snapshot from exposure_monitor.
today_exposure AS (
  SELECT
    {{ cast_to_string('desk') }}               AS desk,
    CAST(as_of_date         AS DATE)    AS as_of_date,
    {{ cast_to_double('utilisation_pct') }}    AS utilisation_pct,
    CAST(breach_flag        AS BOOLEAN) AS breach_flag,
    CAST(warning_flag       AS BOOLEAN) AS warning_flag,
    {{ cast_to_double('limit_usd') }}          AS limit_usd,
    {{ cast_to_double('gross_exposure_usd') }}  AS gross_exposure_usd
  FROM {{ ref('exposure_monitor') }}
  WHERE as_of_date = CURRENT_DATE()
),

{% if is_incremental() %}

-- ── INCREMENTAL RUN: three-way merge ─────────────────────────────────────
 
-- Active breach episodes in the target table.
active_breaches AS (
  SELECT
    {{ cast_to_string('desk') }}                          AS desk,
    CAST(breach_start_date             AS DATE)           AS breach_start_date,
    {{ cast_to_double('peak_utilisation_pct') }}          AS peak_utilisation_pct,
    CAST(peak_utilisation_date         AS DATE)           AS peak_utilisation_date,
    {{ cast_to_double('breach_start_utilisation_pct') }}  AS breach_start_utilisation_pct,
    {{ cast_to_double('limit_usd') }}                     AS limit_usd,
    {{ cast_to_double('gross_exposure_at_start_usd') }}   AS gross_exposure_at_start_usd,
    CAST(warning_start_date            AS DATE)           AS warning_start_date,
    {{ cast_to_int('warning_duration_days') }}            AS warning_duration_days
  FROM {{ this }}
  WHERE is_active = TRUE
),
 
-- Case 1: Desks STILL in breach today — update duration and peak.
continuing_breaches AS (
  SELECT
    ab.desk,
    ab.breach_start_date,
    CAST(NULL AS DATE)                                   AS breach_end_date,
    {{ cast_to_int('DATEDIFF(CURRENT_DATE(), ab.breach_start_date)') }}    AS breach_duration_days,
    TRUE                                                 AS is_active,
    ROUND(
      GREATEST(ab.peak_utilisation_pct, te.utilisation_pct),
      4
    )                                                    AS peak_utilisation_pct,
    CASE
      WHEN te.utilisation_pct >= ab.peak_utilisation_pct
        THEN CURRENT_DATE()
      ELSE ab.peak_utilisation_date
    END                                                  AS peak_utilisation_date,
    ab.breach_start_utilisation_pct,
    ab.limit_usd,
    ab.gross_exposure_at_start_usd,
    ab.warning_start_date,
    ab.warning_duration_days
  FROM active_breaches               ab
  INNER JOIN today_exposure          te ON ab.desk = te.desk
  WHERE te.breach_flag = TRUE
),
 
-- Case 2: Desks that WERE breaching but are no longer — close the episode.
resolved_breaches AS (
  SELECT
    ab.desk,
    ab.breach_start_date,
    CURRENT_DATE()                                       AS breach_end_date,
    {{ cast_to_int('DATEDIFF(CURRENT_DATE(), ab.breach_start_date)') }}    AS breach_duration_days,
    FALSE                                                AS is_active,
    ab.peak_utilisation_pct,
    ab.peak_utilisation_date,
    ab.breach_start_utilisation_pct,
    ab.limit_usd,
    ab.gross_exposure_at_start_usd,
    ab.warning_start_date,
    ab.warning_duration_days
  FROM active_breaches               ab
  LEFT JOIN today_exposure           te
    ON  ab.desk        = te.desk
    AND te.breach_flag = TRUE
  WHERE te.desk IS NULL
),
 
-- Case 3: New breaches — no existing active episode for this desk.
new_breaches AS (
  SELECT
    te.desk,
    CURRENT_DATE()                                       AS breach_start_date,
    CAST(NULL AS DATE)                                   AS breach_end_date,
    {{ cast_to_int('1') }}                               AS breach_duration_days,
    TRUE                                                 AS is_active,
    ROUND(te.utilisation_pct, 4)                         AS peak_utilisation_pct,
    CURRENT_DATE()                                       AS peak_utilisation_date,
    ROUND(te.utilisation_pct, 4)                         AS breach_start_utilisation_pct,
    te.limit_usd,
    te.gross_exposure_usd                                AS gross_exposure_at_start_usd,
    CAST(NULL AS DATE)                                   AS warning_start_date,
    {{ cast_to_int('NULL') }}                            AS warning_duration_days
  FROM today_exposure                te
  LEFT JOIN active_breaches          ab ON te.desk = ab.desk
  WHERE te.breach_flag = TRUE
    AND ab.desk IS NULL
)
 
SELECT desk, breach_start_date, breach_end_date,
       breach_duration_days, is_active,
       peak_utilisation_pct, peak_utilisation_date,
       breach_start_utilisation_pct, limit_usd,
       gross_exposure_at_start_usd,
       warning_start_date, warning_duration_days,
       current_timestamp() AS _gold_loaded_at
FROM continuing_breaches
 
UNION ALL
 
SELECT desk, breach_start_date, breach_end_date,
       breach_duration_days, is_active,
       peak_utilisation_pct, peak_utilisation_date,
       breach_start_utilisation_pct, limit_usd,
       gross_exposure_at_start_usd,
       warning_start_date, warning_duration_days,
       current_timestamp() AS _gold_loaded_at
FROM resolved_breaches
 
UNION ALL
 
SELECT desk, breach_start_date, breach_end_date,
       breach_duration_days, is_active,
       peak_utilisation_pct, peak_utilisation_date,
       breach_start_utilisation_pct, limit_usd,
       gross_exposure_at_start_usd,
       warning_start_date, warning_duration_days,
       current_timestamp() AS _gold_loaded_at
FROM new_breaches
 
{% else %}
 
-- ── FIRST RUN: insert all desks currently in breach ───────────────────────
SELECT
  desk,
  CURRENT_DATE()               AS breach_start_date,
  CAST(NULL AS DATE)           AS breach_end_date,
  {{ cast_to_int('1') }}       AS breach_duration_days,
  TRUE                         AS is_active,
  ROUND(utilisation_pct, 4)    AS peak_utilisation_pct,
  CURRENT_DATE()               AS peak_utilisation_date,
  ROUND(utilisation_pct, 4)    AS breach_start_utilisation_pct,
  limit_usd,
  gross_exposure_usd           AS gross_exposure_at_start_usd,
  CAST(NULL AS DATE)           AS warning_start_date,
  {{ cast_to_int('NULL') }}    AS warning_duration_days,
  current_timestamp()          AS _gold_loaded_at
FROM today_exposure
WHERE breach_flag = TRUE
 
{% endif %}