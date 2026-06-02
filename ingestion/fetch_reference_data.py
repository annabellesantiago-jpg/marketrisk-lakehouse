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

import pandas as pd
from datetime import datetime, date, timezone
from pathlib import Path
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.config import DESK_LIMITS, FX_TICKERS

OUTPUT_DIR = Path("data/raw/reference")
PRICES_DIR = Path("data/raw/prices")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def get_live_fx_rates() -> dict:
    """
    Read the most recent closing price from each FX price CSV file
    written by fetch_market_data.py.

    FX pair format:  EURUSD=X  →  1 EUR = {close} USD
                     GBPUSD=X  →  1 GBP = {close} USD
                     JPYUSD=X  →  1 JPY = {close} USD

    The close price of an XY=X pair IS the rate_vs_usd for currency X —
    no conversion required.

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
        file_path = PRICES_DIR / f"{filename}.csv"

        if not file_path.exists():
            raise FileNotFoundError(
                f"Price file not found: {file_path}\n"
                f"Run fetch_market_data.py before fetch_reference_data.py."
            )

        df = pd.read_csv(file_path)

        if df.empty:
            raise ValueError(
                f"Price file is empty: {file_path}\n"
                f"Yahoo Finance may have returned no data for {raw_ticker}."
            )

        # Sort descending by date and take the most recent row
        df["date"] = pd.to_datetime(df["date"])
        latest     = df.sort_values("date", ascending=False).iloc[0]
        rate       = round(float(latest["close"]), 6)
        as_of      = latest["date"].strftime("%Y-%m-%d")

        rates[currency] = {"rate": rate, "as_of": as_of}
        print(f"  {currency:<4}  rate_vs_usd = {rate:<12}  (close on {as_of})")

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
            "generated_at":  datetime.utcnow().isoformat(),
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
    print("\nGenerating reference data...")

    # ── Desk limits ──────────────────────────────────────────────────────
    limits      = generate_desk_limits()
    limits_path = OUTPUT_DIR / "desk_limits.csv"
    limits.to_csv(limits_path, index=False)
    print(f"\nDesk limits ({len(limits)} rows) → {limits_path}")
    print(
        limits[["desk", "limit_usd", "effective_date", "review_date"]]
        .to_string(index=False)
    )

    # ── FX rates — live from price files ─────────────────────────────────
    print(f"\nReading latest FX close prices from {PRICES_DIR}...")
    try:
        live_rates = get_live_fx_rates()
    except FileNotFoundError as e:
        print(f"\nERROR: {e}")
        sys.exit(1)

    fx      = generate_fx_rates(live_rates)
    fx_path = OUTPUT_DIR / "fx_rates.csv"
    fx.to_csv(fx_path, index=False)
    print(f"\nFX rates ({len(fx)} rows) → {fx_path}")
    print(
        fx[["currency", "rate_vs_usd", "as_of_date"]]
        .to_string(index=False)
    )

    print("\nReference data generation complete.")


if __name__ == "__main__":
    main()