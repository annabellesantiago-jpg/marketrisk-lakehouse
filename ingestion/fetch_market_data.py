"""
fetch_market_data.py
─────────────────────────────────────────────────────────────────────────────
Pulls FX rates and equity prices from Yahoo Finance.
Saves one CSV per ticker into data/raw/prices/

Column names are saved in lowercase to match the Bronze Delta table schema.
The ticker column stores the raw Yahoo Finance symbol (e.g. EURUSD=X, HSBA.L)
exactly as received — Silver layer normalises these to match positions.

File naming uses the same normalization as generate_positions.py so filenames
are human-readable on disk, but the ticker column inside the CSV retains the
raw Yahoo Finance format for Bronze fidelity.
─────────────────────────────────────────────────────────────────────────────
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.config import ALL_TICKERS, PRICE_HISTORY_DAYS

OUTPUT_DIR = Path("data/raw/prices")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


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

    print(f"  Fetching {ticker:<12} from {start_date:%Y-%m-%d} to {end_date:%Y-%m-%d}")

    data = yf.download(
        ticker,
        start=start_date,
        end=end_date,
        progress=False,
        auto_adjust=True,
    )

    if data.empty:
        print(f"  WARNING: No data returned for {ticker}. "
              f"Check if the ticker symbol is still valid.")
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
    print(
        f"\nFetching {len(ALL_TICKERS)} tickers "
        f"({PRICE_HISTORY_DAYS} calendar days of history)...\n"
    )

    saved   = 0
    skipped = 0

    for ticker in ALL_TICKERS:
        df = fetch_ticker(ticker, PRICE_HISTORY_DAYS)

        if df.empty:
            skipped += 1
            continue

        # Use normalized filename for readability on disk
        filename    = ticker_to_filename(ticker)
        output_file = OUTPUT_DIR / f"{filename}.csv"
        df.to_csv(output_file, index=False)
        print(f"  Saved {len(df):>4} rows → {output_file}")
        saved += 1

    print(
        f"\nFetch complete — "
        f"{saved} tickers saved, {skipped} skipped."
    )
    print(f"Files in: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()