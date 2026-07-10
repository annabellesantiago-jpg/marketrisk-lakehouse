"""
fetch_market_data.py
─────────────────────────────────────────────────────────────────────────────
Pulls FX rates and equity prices from Yahoo Finance.
Writes one CSV per ticker directly to AWS S3 under:
  raw/prices/year={YYYY}/month={MM}/day={DD}/{ticker}.csv

Column names are saved in lowercase to match the Bronze Delta table schema.
The ticker column stores the raw Yahoo Finance symbol (e.g. EURUSD=X, HSBA.L)
exactly as received — Silver layer normalises these to match positions.

File naming uses the same normalization as generate_positions.py so filenames
are consistent with the tickers stored in bronze.positions.
─────────────────────────────────────────────────────────────────────────────
"""

import yfinance as yf
import pandas as pd
import time
import logging
from datetime import datetime, timedelta, timezone
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.config import ALL_TICKERS, PRICE_HISTORY_DAYS, S3_BUCKET, RUN_DATE, RUN_YEAR, RUN_MONTH, RUN_DAY, setup_logging
from ingestion.s3_utils import get_client, verify_bucket, upload_df

logger = logging.getLogger(__name__)


def ticker_to_filename(ticker: str) -> str:
    """
    Convert a raw Yahoo Finance ticker to a filesystem-safe filename.
    Matches the normalization used in generate_positions.py so filenames
    are consistent with the tickers stored in bronze.positions.
      EURUSD=X  →  EURUSD
      HSBA.L    →  HSBA_L
      SAN.MC    →  SAN_MC
    """
    return ticker.replace("=X", "").replace(".", "_")


def fetch_ticker(ticker: str, days: int) -> pd.DataFrame:
    """
    Fetch historical OHLCV data for a single ticker from Yahoo Finance.

    Returns a DataFrame with lowercase column names matching the
    Bronze Delta table schema:
      date, open, high, low, close, volume, ticker, fetched_at

    The ticker column stores the raw Yahoo Finance symbol exactly
    as received (e.g. EURUSD=X) — Silver normalises it downstream.
    Volume is NULL for FX pairs — Yahoo Finance does not report
    FX volume and returns 0 which we convert to NULL for accuracy.
    """
    end_date   = datetime.today()
    start_date = end_date - timedelta(days=days)

    data = pd.DataFrame()

    for attempt in range(1, 3):        
        logger.info("Fetching %s from %s to %s", ticker, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        data = yf.download(
            ticker,
            start=start_date,
            end=end_date,
            progress=False,
            auto_adjust=True,
        )

        if not data.empty:
            break
        
        wait = 5 * attempt #5s, 10s, 15s
        logger.warning("No data for %s - waiting %d before retry", ticker, wait)
        time.sleep(wait)

    if data.empty:
        logger.warning("No data returned for %s — check if ticker is still valid", ticker)
        return pd.DataFrame()

    # Flatten MultiIndex columns produced by yfinance for single tickers
    data.reset_index(inplace=True)
    data.columns = [
        col[0] if isinstance(col, tuple) else col
        for col in data.columns
    ]

    # Lowercase all column names for Bronze schema consistency
    data.columns = [c.lower() for c in data.columns]

    # Add ticker column with the raw Yahoo Finance symbol.
    # Silver prices_cleaned.sql will normalize this via REGEXP_REPLACE.
    data["ticker"]     = ticker
    data["fetched_at"] = datetime.now(timezone.utc)

    # FX pairs report volume as 0 — convert to NULL for accuracy.
    # A volume of 0 in Bronze would mislead any downstream volume analysis.
    if "volume" in data.columns:
        data["volume"] = data["volume"].replace(0, None)

    # Ensure column order matches Bronze table expectation
    cols = ["date", "open", "high", "low", "close", "volume",
            "ticker", "fetched_at"]
    data = data[[c for c in cols if c in data.columns]]

    return data


def main():
    logger.info("\nFetching %d tickers (%d calendar days of history) for run date %s...\n", 
                len(ALL_TICKERS), PRICE_HISTORY_DAYS, RUN_DATE)

    client = get_client()
    verify_bucket(client, S3_BUCKET)
    
    saved   = 0
    skipped = 0

    for ticker in ALL_TICKERS:
        df = fetch_ticker(ticker, PRICE_HISTORY_DAYS)

        if df.empty:
            skipped += 1
            continue

        filename    = ticker_to_filename(ticker)
        key = f"raw/prices/year={RUN_YEAR}/month={RUN_MONTH}/day={RUN_DAY}/{filename}.csv"
        upload_df(client, df, S3_BUCKET, key)
        saved += 1
        time.sleep(3) # wait 1s to fetch the next ticker

    logger.info("\nFetch complete - %d tickers uploaded, %d skipped.", saved, skipped)


if __name__ == "__main__":
    setup_logging()
    main()