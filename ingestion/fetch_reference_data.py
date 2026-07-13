"""
fetch_reference_data.py
─────────────────────────────────────────────────────────────────────────────
Generates reference data files: FX conversion rates.
Desk limits are being handled separately in load_desk_limits.py

FX rates are read from the price CSV files already written by
fetch_market_data.py — the latest close price for each FX pair IS the
current spot rate. This avoids duplication and keeps rates live.

DEPENDENCY: fetch_market_data.py must run before this script.

Run order:
  1. python ingestion/fetch_market_data.py    ← downloads FX price history
  2. python ingestion/fetch_reference_data.py ← reads latest close as spot rate
  3. python ingestion/generate_positions.py

In a real bank:
  - FX rates come from a real-time feed (Bloomberg BFIX, WM/Reuters 4pm fix)
─────────────────────────────────────────────────────────────────────────────
"""

import logging
import pandas as pd
from datetime import datetime, date, timezone
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.config import FX_TICKERS, RUN_DATE, RUN_YEAR, RUN_MONTH, RUN_DAY, S3_BUCKET, setup_logging
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

    # ── FX rates — live from price files ─────────────────────────────────
    logger.info("Reading latest FX close prices from S3....")

    try:
        live_rates = get_live_fx_rates(client)
    except (FileNotFoundError, ValueError) as e:
        logger.error("\nERROR: %s", e)
        sys.exit(1)

    fx      = generate_fx_rates(live_rates)
    key    = "raw/reference/fx_rates.csv"
    upload_df(client, fx, S3_BUCKET, key)
    logger.info(
        "FX rates (%d rows):\n%s",
        len(fx),
        fx[["currency", "rate_vs_usd", "as_of_date"]].to_string(index=False)
    )

    logger.info("Reference data generation complete.")

if __name__ == "__main__":
    setup_logging()
    main()