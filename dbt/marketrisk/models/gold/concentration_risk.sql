{{
    config(
        materialized        ='incremental',
        schema              ='gold',
        unique_key          =['as_of_date', 'desk', 'concentration_type', 'entity_name'],
        incremental_strategy='merge',
        merge_update_columns=[
      'position_count',
      'gross_exposure_usd',
      'net_exposure_usd',
      'net_abs_exposure_usd',
      'pct_of_desk_total',
      'pct_of_portfolio_total',
      'concentration_level',
      'desk_concentration_level',
      'desk_rank',
      'portfolio_rank',
      'desk_hhi',
      'large_exposure_flag',
      '_gold_loaded_at'
        ],
        on_schema_change    ='fail',
        tags                =['gold', 'concentration']
    )
}}

/*
  gold.concentration_risk
  ─────────────────────────────────────────────────────────────────────────────
  Purpose : Multi-dimensional concentration risk across six types:
            COUNTERPARTY, INSTRUMENT, SECTOR, GEOGRAPHY,
            ASSET_CLASS, MATURITY_BUCKET.
            Includes HHI index and Basel large exposure flag.
  Source  : silver.positions_enriched + seeds.ticker_classifications
  Grain   : One row per desk per concentration_type per entity_name
            per as_of_date
  Unique key : [as_of_date, desk, concentration_type, entity_name]
  Regulatory : Basel Large Exposures (25% Tier 1 proxy). MAS 637.
 
  Rounding convention:
    - Monetary _usd columns  : no rounding — full DOUBLE precision
    - pct_of_desk_total       : ROUND(x, 4)
    - pct_of_portfolio_total  : ROUND(x, 4)
    - desk_hhi                : ROUND(x, 2) — index metric
 
  Thresholds:
    Portfolio : HIGH > 10%, MEDIUM > 5%, LOW <= 5%
    Desk      : HIGH > 20%, MEDIUM > 10%, LOW <= 10%
 
  Note on ticker_classifications seed:
    The seed has no asset_class column. The same ticker maps to the
    same sector/geography across all asset classes — sector/geography
    are company-level properties, not position-level.
  ─────────────────────────────────────────────────────────────────────────────
*/

WITH

-- Step 1: Load ticker classification from seed.
--         Maintained in seeds/ticker_classifications.csv.
ticker_classifications AS (
  SELECT
    {{ cast_to_string('ticker') }} AS ticker,
    {{ cast_to_string('sector') }} AS sector,
    {{ cast_to_string('geography') }} AS geography
    FROM {{ ref('ticker_classifications') }}
),

-- Step 2: Enrich positions with sector, geography, and maturity bucket.
enriched AS (
  SELECT
    {{ cast_to_string('p.desk') }} AS desk,
    {{ cast_to_string('p.trade_id') }} AS trade_id,
    {{ cast_to_string('p.counterparty') }} AS counterparty,
    {{ cast_to_string('p.ticker') }} AS ticker,
    {{ cast_to_string('p.asset_class') }} AS asset_class,
    {{ cast_to_double('p.notional_usd') }} AS notional_usd,
    {{ cast_to_double('p.net_exposure_usd') }} AS net_exposure_usd,
    COALESCE(tc.sector,    'Unknown') AS sector,
    COALESCE(tc.geography, 'Unknown') AS geography,
    CASE
      WHEN COALESCE(p.days_to_maturity, 0) <=  365 THEN '0-1Y'
      WHEN p.days_to_maturity             <= 1825  THEN '1-5Y'
      WHEN p.days_to_maturity             <= 3650  THEN '5-10Y'
      ELSE                                              '10Y+'
    END AS maturity_bucket
  FROM {{ ref('positions_enriched') }}     p
  LEFT JOIN ticker_classifications         tc
    ON UPPER(p.ticker) = UPPER(tc.ticker)
  WHERE p.trade_id IS NOT NULL
),

-- Step 3: Calculate desk totals and portfolio total — denominators.
desk_totals AS (
  SELECT
    desk,
    SUM(ABS(notional_usd)) AS desk_gross_usd
  FROM enriched
  GROUP BY desk
),
 
portfolio_total AS (
  SELECT
    SUM(ABS(notional_usd)) AS portfolio_gross_usd
  FROM enriched
),

-- Step 4: Build all six concentration types using UNION ALL.
all_exposures AS (
  SELECT desk, 'COUNTERPARTY'   AS concentration_type,
         counterparty           AS entity_name,
         COUNT(DISTINCT trade_id) AS position_count,
         SUM(ABS(notional_usd)) AS gross_exposure_usd,
         SUM(net_exposure_usd)  AS net_exposure_usd
  FROM enriched GROUP BY desk, counterparty
 
  UNION ALL
 
  SELECT desk, 'INSTRUMENT'     AS concentration_type,
         ticker                 AS entity_name,
         COUNT(DISTINCT trade_id),
         SUM(ABS(notional_usd)),
         SUM(net_exposure_usd)
  FROM enriched GROUP BY desk, ticker
 
  UNION ALL
 
  SELECT desk, 'SECTOR'         AS concentration_type,
         sector                 AS entity_name,
         COUNT(DISTINCT trade_id),
         SUM(ABS(notional_usd)),
         SUM(net_exposure_usd)
  FROM enriched GROUP BY desk, sector
 
  UNION ALL
 
  SELECT desk, 'GEOGRAPHY'      AS concentration_type,
         geography              AS entity_name,
         COUNT(DISTINCT trade_id),
         SUM(ABS(notional_usd)),
         SUM(net_exposure_usd)
  FROM enriched GROUP BY desk, geography
 
  UNION ALL
 
  SELECT desk, 'ASSET_CLASS'    AS concentration_type,
         asset_class            AS entity_name,
         COUNT(DISTINCT trade_id),
         SUM(ABS(notional_usd)),
         SUM(net_exposure_usd)
  FROM enriched GROUP BY desk, asset_class
 
  UNION ALL
 
  SELECT desk, 'MATURITY_BUCKET' AS concentration_type,
         maturity_bucket         AS entity_name,
         COUNT(DISTINCT trade_id),
         SUM(ABS(notional_usd)),
         SUM(net_exposure_usd)
  FROM enriched GROUP BY desk, maturity_bucket
),

-- Step 5: Calculate concentration percentages.
concentration_pcts AS (
  SELECT
    ae.desk,
    ae.concentration_type,
    ae.entity_name,
    {{ cast_to_int('ae.position_count') }}           AS position_count,
    ae.gross_exposure_usd,
    ae.net_exposure_usd,
    ABS(ae.net_exposure_usd)                         AS net_abs_exposure_usd,
    ROUND(
      (ae.gross_exposure_usd
       / NULLIF(dt.desk_gross_usd, 0)) * 100,
      4
    )                                                AS pct_of_desk_total,
    ROUND(
      (ae.gross_exposure_usd
       / NULLIF(pt.portfolio_gross_usd, 0)) * 100,
      4
    )                                                AS pct_of_portfolio_total
  FROM all_exposures                                 ae
  INNER JOIN desk_totals                             dt ON ae.desk = dt.desk
  CROSS JOIN portfolio_total                         pt
),

-- Step 6: Apply thresholds, rankings, and large exposure flag.
ranked AS (
  SELECT
    desk,
    concentration_type,
    entity_name,
    position_count,
    gross_exposure_usd,
    net_exposure_usd,
    net_abs_exposure_usd,
    pct_of_desk_total,
    pct_of_portfolio_total,
    -- Portfolio-level threshold (Basel large exposure basis)
    CASE
      WHEN pct_of_portfolio_total > 10 THEN 'HIGH'
      WHEN pct_of_portfolio_total >  5 THEN 'MEDIUM'
      ELSE                                  'LOW'
    END                                              AS concentration_level,
    -- Desk-level threshold (internal governance)
    CASE
      WHEN pct_of_desk_total > 20 THEN 'HIGH'
      WHEN pct_of_desk_total > 10 THEN 'MEDIUM'
      ELSE                             'LOW'
    END                                              AS desk_concentration_level,
    ROW_NUMBER() OVER (
      PARTITION BY desk, concentration_type
      ORDER BY gross_exposure_usd DESC
    )                                                AS desk_rank,
    ROW_NUMBER() OVER (
      PARTITION BY concentration_type
      ORDER BY gross_exposure_usd DESC
    )                                                AS portfolio_rank,
    -- Basel large exposure proxy: > 25% of portfolio
    CASE
      WHEN pct_of_portfolio_total > 25 THEN TRUE
      ELSE FALSE
    END                                              AS large_exposure_flag
  FROM concentration_pcts
),

-- Step 7: Calculate HHI per desk per concentration type.
--         HHI = SUM of squared percentage shares.
--         < 1500 = low, 1500-2500 = moderate, > 2500 = high concentration.
hhi AS (
  SELECT
    desk,
    concentration_type,
    ROUND(SUM(POWER(pct_of_desk_total, 2)), 2)       AS desk_hhi
  FROM ranked
  GROUP BY
    desk,
    concentration_type
)

SELECT
  CURRENT_DATE()              AS as_of_date,
  r.desk,
  r.concentration_type,
  r.entity_name,
  r.position_count,
  r.gross_exposure_usd,
  r.net_exposure_usd,
  r.net_abs_exposure_usd,
  r.pct_of_desk_total,
  r.pct_of_portfolio_total,
  r.concentration_level,
  r.desk_concentration_level,
  {{ cast_to_int('r.desk_rank') }}      AS desk_rank,
  {{ cast_to_int('r.portfolio_rank') }} AS portfolio_rank,
  h.desk_hhi,
  r.large_exposure_flag,
  current_timestamp()         AS _gold_loaded_at
FROM ranked                   r
INNER JOIN hhi                h
  ON  r.desk               = h.desk
  AND r.concentration_type = h.concentration_type