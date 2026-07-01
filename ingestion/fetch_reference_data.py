"""
fetch_reference_data.py
─────────────────────────────────────────────────────────────────────────────
Generates reference data files: desk risk limits and FX conversion rates.

FX rates are read from the price CSV files already written by
fetch_market_data.py — the latest close price for each FX pair IS the
current spot rate. This avoids duplication and keeps rates live.

DEPENDENCY: fetch_market_data.py must run before this script.

Run order:
  1. python ingestion/fetch_market_data.py    ← downloads FX price history
  2. python ingestion/fetch_reference_data.py ← reads latest close as spot rate
  3. python ingestion/generate_positions.py
  4. python ingestion/upload_to_s3.py

In a real bank:
  - Desk limits come from the risk governance system (board-approved annually)
  - FX rates come from a real-time feed (Bloomberg BFIX, WM/Reuters 4pm fix)
─────────────────────────────────────────────────────────────────────────────
"""

import logging
import pandas as pd
from datetime import datetime, date, timezone
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.config import DESK_LIMITS, FX_TICKERS, RUN_DATE, RUN_YEAR, RUN_MONTH, RUN_DAY, S3_BUCKET
from ingestion.s3_utils import get_client, verify_bucket, upload_df, read_df

logger = logging.getLogger(__name__)


def get_live_fx_rates(client) -> dict:
    """
    Read the most recent closing price from each FX price CSV written
    to S3 by fetch_market_data.py.

    FX pair format:  EURUSD=X  →  1 EUR = {close} USD
    Currency code extracted by stripping "USD" from the normalized filename:
      EURUSD → EUR    GBPUSD → GBP    JPYUSD → JPY

    Returns dict: {currency_code: {"rate": float, "as_of": str}}

    Raises FileNotFoundError if any expected price file is missing —
    this means fetch_market_data.py has not been run yet.
    """
    rates = {}

    for raw_ticker in FX_TICKERS:
        # Normalized filename matches what fetch_market_data.py wrote:
        # EURUSD=X  →  EURUSD.csv
        filename  = raw_ticker.replace("=X", "").replace(".", "_")
        currency  = filename.replace("USD", "")   # EURUSD → EUR
        key = f"raw/prices/year={RUN_YEAR}/month={RUN_MONTH}/day={RUN_DAY}/{filename}.csv"

        df = read_df(client, S3_BUCKET, key)

        if df.empty:
            raise ValueError(
                f"Price file is empty: s3://{S3_BUCKET}/{key}\n"
                f"Yahoo Finance may have returned no data for {raw_ticker}."
            )

        # Sort descending by date and take the most recent row
        df["date"] = pd.to_datetime(df["date"])
        latest     = df.sort_values("date", ascending=False).iloc[0]
        rate       = round(float(latest["close"]), 6)
        as_of      = latest["date"].strftime("%Y-%m-%d")

        rates[currency] = {"rate": rate, "as_of": as_of}
        logger.info("%s  rate_vs_usd = %s  (close on %s)", currency, rate, as_of)

    return rates


def generate_desk_limits() -> pd.DataFrame:
    """
    Generate the desk_limits reference file from config.DESK_LIMITS.

    effective_date: start of current calendar year — annual board review.
    review_date:    start of next calendar year — next scheduled review.
    """
    today          = date.today()
    effective_date = date(today.year, 1, 1).strftime("%Y-%m-%d")
    review_date    = date(today.year + 1, 1, 1).strftime("%Y-%m-%d")

    rows = [
        {
            "desk":           desk,
            "limit_usd":      limit,
            "limit_currency": "USD",
            "effective_date": effective_date,
            "review_date":    review_date,
            "generated_at":  datetime.now(timezone.utc).isoformat(),
        }
        for desk, limit in DESK_LIMITS.items()
    ]
    return pd.DataFrame(rows)


def generate_fx_rates(rates: dict) -> pd.DataFrame:
    """
    Convert the live FX rates dict into a DataFrame for CSV output.
    Adds USD explicitly at rate 1.0 — the reporting currency is always 1.0.
    Having USD in the Bronze table makes the Silver fx_with_usd CTE
    cleaner (no need to UNION ALL a hardcoded 1.0).
    """
    rows = [
        {
            "currency":      ccy,
            "rate_vs_usd":   data["rate"],
            "as_of_date":    data["as_of"],
            "generated_at": datetime.now(timezone.utc),
        }
        for ccy, data in rates.items()
    ]

    # Add USD at 1.0 — reporting currency by definition
    rows.append({
        "currency":      "USD",
        "rate_vs_usd":   1.0,
        "as_of_date":    date.today().strftime("%Y-%m-%d"),
        "generated_at": datetime.now(timezone.utc),
    })

    return pd.DataFrame(rows)


def main():
    logger.info("\nGenerating reference data for run date: %s", RUN_DATE)

    client = get_client()
    verify_bucket(client, S3_BUCKET)

    # ── Desk limits ──────────────────────────────────────────────────────
    limits      = generate_desk_limits()
    key    = f"raw/reference/year={RUN_YEAR}/month={RUN_MONTH}/day={RUN_DAY}/desk_limits.csv"
    upload_df(client, limits, S3_BUCKET, key)
    logger.info(
        "Desk limits (%d rows):\n%s",
        len(limits),
        limits[["desk", "limit_usd", "effective_date", "review_date"]].to_string(index=False)
    )

    # ── FX rates — live from price files ─────────────────────────────────
    logger.info("Reading latest FX close prices from S3....")
    try:
        live_rates = get_live_fx_rates(client)
    except FileNotFoundError as e:
        logger.error("\nERROR: %s",)
        sys.exit(1)

    fx      = generate_fx_rates(live_rates)
    key    = f"raw/reference/year={RUN_YEAR}/month={RUN_MONTH}/day={RUN_DAY}/fx_rates.csv"
    upload_df(client, fx, S3_BUCKET, key)
    logger.info(
        "FX rates (%d rows):\n%s",
        len(fx),
        fx[["currency", "rate_vs_usd", "as_of_date"]].to_string(index=False)
    )

    logger.info("Reference data generation complete.")


if __name__ == "__main__":
    main()