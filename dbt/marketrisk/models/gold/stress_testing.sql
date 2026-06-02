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
 
-- Step 1: Define all stress scenarios.
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
    scenario_name,
    asset_class,
    shock_pct,
    shock_bps,
    scenario_type,
    is_stressed_var_proxy
  FROM (
    VALUES
    -- ── 2008 Global Financial Crisis ─────────────────────────────────────
    ('2008 GFC', 'Equity', -0.40,  NULL, 'REGULATORY', TRUE),
    ('2008 GFC', 'FX',     -0.15,  NULL, 'REGULATORY', TRUE),
    ('2008 GFC', 'Rates',   NULL,   300, 'REGULATORY', TRUE),
    ('2008 GFC', 'Credit',  NULL,   500, 'REGULATORY', TRUE),
    -- ── 2020 COVID Crash ────────────────────────────────────────────────
    ('2020 COVID', 'Equity', -0.30, NULL, 'REGULATORY', FALSE),
    ('2020 COVID', 'FX',     -0.10, NULL, 'REGULATORY', FALSE),
    ('2020 COVID', 'Rates',   NULL, -150, 'REGULATORY', FALSE),
    ('2020 COVID', 'Credit',  NULL,  350, 'REGULATORY', FALSE),
    -- ── 1997 Asian Financial Crisis ─────────────────────────────────────
    ('1997 Asian Crisis', 'Equity', -0.50, NULL, 'REGULATORY', FALSE),
    ('1997 Asian Crisis', 'FX',     -0.25, NULL, 'REGULATORY', FALSE),
    ('1997 Asian Crisis', 'Rates',   NULL,  400, 'REGULATORY', FALSE),
    ('1997 Asian Crisis', 'Credit',  NULL,  600, 'REGULATORY', FALSE),
    -- ── Regulatory: Rates Shock +200bps ─────────────────────────────────
    ('Rates Shock +200bps', 'Equity',  0.00, NULL, 'REGULATORY', FALSE),
    ('Rates Shock +200bps', 'FX',      0.00, NULL, 'REGULATORY', FALSE),
    ('Rates Shock +200bps', 'Rates',   NULL,  200, 'REGULATORY', FALSE),
    ('Rates Shock +200bps', 'Credit',  NULL,    0, 'REGULATORY', FALSE),
    -- ── Management: Equity Crash -30% ───────────────────────────────────
    ('Equity Crash -30%', 'Equity', -0.30, NULL, 'MANAGEMENT', FALSE),
    ('Equity Crash -30%', 'FX',      0.00, NULL, 'MANAGEMENT', FALSE),
    ('Equity Crash -30%', 'Rates',   NULL,    0, 'MANAGEMENT', FALSE),
    ('Equity Crash -30%', 'Credit',  NULL,    0, 'MANAGEMENT', FALSE),
    -- ── Management: USD Strengthens +10% ────────────────────────────────
    ('USD Strengthens +10%', 'Equity',  0.00,  NULL, 'MANAGEMENT', FALSE),
    ('USD Strengthens +10%', 'FX',     -0.10,  NULL, 'MANAGEMENT', FALSE),
    ('USD Strengthens +10%', 'Rates',   NULL,     0, 'MANAGEMENT', FALSE),
    ('USD Strengthens +10%', 'Credit',  NULL,     0, 'MANAGEMENT', FALSE)
  ) AS t(
    scenario_name, asset_class, shock_pct, shock_bps,
    scenario_type, is_stressed_var_proxy
  )
),