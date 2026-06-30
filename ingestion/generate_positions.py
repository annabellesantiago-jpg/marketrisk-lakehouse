"""
generate_positions.py
─────────────────────────────────────────────────────────────────────────────
Generates a realistic synthetic trading book — simulates what a real
trade booking system (Murex, Calypso, Finastra) would export.

Realism rules enforced:
  - Each desk trades ONLY instruments within its mandate.
  - Each position has exactly ONE asset_class matching its desk.
  - Equity positions use real published ISINs and CUSIPs.
  - Bond proxies (Rates desk) and CDS proxies (Credit desk) use
    synthetic ISINs in the correct country format.
  - FX positions have NULL isin and cusip — OTC FX has no ISIN.
  - Tickers are normalized to match bronze.market_prices after Silver
    transformation: replace("=X", "").replace(".", "_")
─────────────────────────────────────────────────────────────────────────────
"""

import logging
import random
import string
import pandas as pd
import numpy as np
from datetime import date, timedelta, timezone
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.config import (
    FX_TICKERS,
    EQUITY_TICKERS,
    COUNTERPARTIES,
    TRADER_IDS,
    RANDOM_SEED,
    S3_BUCKET, 
    RUN_DATE, 
    RUN_YEAR, 
    RUN_MONTH,
    RUN_DAY
)
from ingestion.s3_utils import get_client, verify_bucket, upload_df

logger = logging.getLogger(__name__)



# ── Ticker normalization ──────────────────────────────────────────────────
# Normalize to match bronze.positions format used by Silver join logic.
# Bronze stores normalized tickers so they match the Silver prices_cleaned
# normalization applied in models/silver/prices_cleaned.sql.
FX_NORMALIZED     = [t.replace("=X", "").replace(".", "_") for t in FX_TICKERS]
EQUITY_NORMALIZED = [t.replace(".", "_") for t in EQUITY_TICKERS]

# ── Real ISINs for equity instruments ────────────────────────────────────
# Source: public regulatory filings and exchange listings.
# CUSIP = characters 3-11 of a US ISIN (embedded within the ISIN).
# Non-US equities (HSBA_L, SAN_MC) have no CUSIP.
EQUITY_ISIN = {
    "AAPL":   {"isin": "US0378331005", "cusip": "037833100"},
    "MSFT":   {"isin": "US5949181045", "cusip": "594918104"},
    "JPM":    {"isin": "US46625H1005", "cusip": "46625H100"},
    "GS":     {"isin": "US38141G1040", "cusip": "38141G104"},
    "HSBA_L": {"isin": "GB0005405286", "cusip": None},
    "SAN_MC": {"isin": "ES0113900J37", "cusip": None},
}


def _synthetic_isin(country_code: str, seed_str: str) -> str:
    """
    Generate a deterministic synthetic ISIN for bond and CDS proxy instruments.
    Format: {2-char country}{9-char national ID}{1 check digit}
    Deterministic — the same seed_str always produces the same ISIN,
    ensuring consistent data across pipeline runs.
    """
    rng = random.Random(seed_str)
    national = "".join(
        rng.choices(string.ascii_uppercase + string.digits, k=9)
    )
    check = str(sum(int(c) for c in national if c.isdigit()) % 10)
    return f"{country_code}{national}{check}"


# Synthetic ISINs for corporate bond proxies (Rates desk).
# Each company issues bonds separate from their equity.
# XS prefix = international Eurobond (used for cross-border bonds).
BOND_ISIN = {
    "JPM":    {"isin": _synthetic_isin("US", "JPM_CORP_BOND"),  "cusip": None},
    "GS":     {"isin": _synthetic_isin("US", "GS_CORP_BOND"),   "cusip": None},
    "MSFT":   {"isin": _synthetic_isin("US", "MSFT_CORP_BOND"), "cusip": None},
    "HSBA_L": {"isin": _synthetic_isin("GB", "HSBA_CORP_BOND"), "cusip": None},
    "SAN_MC": {"isin": _synthetic_isin("ES", "SAN_CORP_BOND"),  "cusip": None},
    "EURUSD": {"isin": _synthetic_isin("XS", "EUR_GOVBOND"),    "cusip": None},
}

# Synthetic ISINs for CDS proxy instruments (Credit desk).
# CDS reference entities are identified by their bond ISIN in practice
# (Markit RED codes), but we use synthetic ISINs for this project.
CDS_ISIN = {
    "AAPL":   {"isin": _synthetic_isin("US", "AAPL_CDS"),  "cusip": None},
    "MSFT":   {"isin": _synthetic_isin("US", "MSFT_CDS"),  "cusip": None},
    "JPM":    {"isin": _synthetic_isin("US", "JPM_CDS"),   "cusip": None},
    "GS":     {"isin": _synthetic_isin("US", "GS_CDS"),    "cusip": None},
    "HSBA_L": {"isin": _synthetic_isin("GB", "HSBA_CDS"),  "cusip": None},
    "SAN_MC": {"isin": _synthetic_isin("ES", "SAN_CDS"),   "cusip": None},
}

# ── Instrument universe per desk ──────────────────────────────────────────
# Each desk trades ONLY instruments within its mandate.
# asset_class, instrument_type, and instruments are desk-specific constants.
#
# Rates and Credit desks use equity company tickers as proxies for their
# bond and CDS instruments — same company, different instrument type.
# In a real bank, a unique ISIN distinguishes JPM equity from JPM bond.
# Here we use synthetic ISINs to represent this distinction.
DESK_CONFIG = {
    "FX Desk": {
        "asset_class":     "FX",
        "instrument_type": "FX_FORWARD",
        # OTC FX instruments — no ISIN or CUSIP
        "instruments": [
            {"ticker": "EURUSD", "currency": "EUR", "isin": None, "cusip": None},
            {"ticker": "GBPUSD", "currency": "GBP", "isin": None, "cusip": None},
            {"ticker": "JPYUSD", "currency": "JPY", "isin": None, "cusip": None},
            {"ticker": "SGDUSD", "currency": "SGD", "isin": None, "cusip": None},
            {"ticker": "AUDUSD", "currency": "AUD", "isin": None, "cusip": None},
        ],
        "n_positions":    75,
        "notional_range": (1_000_000, 50_000_000),
        # FX forwards: short dated, 1 month to 1 year
        "maturity_days":  (30, 365),
        "books": ["FX_SPOT_BOOK", "FX_FORWARD_BOOK", "FX_OPTIONS_BOOK"],
    },
    "Equity Desk": {
        "asset_class":     "Equity",
        "instrument_type": "EQUITY",
        # Listed equities — real ISINs and CUSIPs from public filings
        "instruments": [
            {"ticker": "AAPL",   "currency": "USD", **EQUITY_ISIN["AAPL"]},
            {"ticker": "MSFT",   "currency": "USD", **EQUITY_ISIN["MSFT"]},
            {"ticker": "JPM",    "currency": "USD", **EQUITY_ISIN["JPM"]},
            {"ticker": "GS",     "currency": "USD", **EQUITY_ISIN["GS"]},
            {"ticker": "HSBA_L", "currency": "GBP", **EQUITY_ISIN["HSBA_L"]},
            {"ticker": "SAN_MC", "currency": "EUR", **EQUITY_ISIN["SAN_MC"]},
        ],
        "n_positions":    75,
        "notional_range": (500_000, 20_000_000),
        # Equity positions: medium to long holding period
        "maturity_days":  (180, 1095),
        "books": ["EQ_LONG_BOOK", "EQ_SHORT_BOOK", "EQ_ARBI_BOOK"],
    },
    "Rates Desk": {
        "asset_class":     "Rates",
        "instrument_type": "CORPORATE_BOND",
        # Corporate bond proxies — same companies as Equity desk but
        # representing fixed income instruments with synthetic ISINs.
        # Currency reflects the bond issuance currency.
        "instruments": [
            {"ticker": "JPM",    "currency": "USD", **BOND_ISIN["JPM"]},
            {"ticker": "GS",     "currency": "USD", **BOND_ISIN["GS"]},
            {"ticker": "MSFT",   "currency": "USD", **BOND_ISIN["MSFT"]},
            {"ticker": "HSBA_L", "currency": "GBP", **BOND_ISIN["HSBA_L"]},
            {"ticker": "SAN_MC", "currency": "EUR", **BOND_ISIN["SAN_MC"]},
            {"ticker": "EURUSD", "currency": "EUR", **BOND_ISIN["EURUSD"]},
        ],
        "n_positions":    75,
        "notional_range": (2_000_000, 100_000_000),
        # Bonds: 1 year to 10 year maturities
        "maturity_days":  (365, 3650),
        "books": ["RATES_FLOW_BOOK", "RATES_PROP_BOOK", "RATES_HEDGE_BOOK"],
    },
    "Credit Desk": {
        "asset_class":     "Credit",
        "instrument_type": "CDS",
        # CDS proxy instruments — reference entities are corporate names.
        # Synthetic ISINs represent the CDS reference obligation.
        "instruments": [
            {"ticker": "AAPL",   "currency": "USD", **CDS_ISIN["AAPL"]},
            {"ticker": "MSFT",   "currency": "USD", **CDS_ISIN["MSFT"]},
            {"ticker": "JPM",    "currency": "USD", **CDS_ISIN["JPM"]},
            {"ticker": "GS",     "currency": "USD", **CDS_ISIN["GS"]},
            {"ticker": "HSBA_L", "currency": "GBP", **CDS_ISIN["HSBA_L"]},
            {"ticker": "SAN_MC", "currency": "EUR", **CDS_ISIN["SAN_MC"]},
        ],
        "n_positions":    75,
        "notional_range": (1_000_000, 50_000_000),
        # Credit instruments: 1 year to 5 year tenors
        "maturity_days":  (365, 1825),
        "books": ["CREDIT_FLOW_BOOK", "CREDIT_PROP_BOOK", "CREDIT_HEDGE_BOOK"],
    },
}

# Desk code prefix for trade IDs
DESK_CODE = {
    "FX Desk":     "FX",
    "Equity Desk": "EQ",
    "Rates Desk":  "RT",
    "Credit Desk": "CR",
}


def generate_book() -> pd.DataFrame:
    """
    Generate a realistic synthetic trading book with 300 positions.
    Uses RANDOM_SEED for reproducibility — same output on every run.
    """
    random.seed(RANDOM_SEED)
    np.random.seed(RANDOM_SEED)

    today   = date.today()
    rows    = []
    counter = 1

    for desk, cfg in DESK_CONFIG.items():
        for _ in range(cfg["n_positions"]):
            instrument    = random.choice(cfg["instruments"])
            direction     = random.choice(["LONG", "SHORT"])
            notional      = round(random.uniform(*cfg["notional_range"]), 2)
            trade_date    = today - timedelta(days=random.randint(30, 730))
            maturity_date = today + timedelta(
                days=random.randint(*cfg["maturity_days"])
            )

            rows.append({
                "trade_id":        f"{DESK_CODE[desk]}-{str(counter).zfill(5)}",
                "desk":            desk,
                "book_id":         random.choice(cfg["books"]),
                "trader_id":       random.choice(TRADER_IDS[desk]),
                "counterparty":    random.choice(COUNTERPARTIES),
                "asset_class":     cfg["asset_class"],
                "instrument_type": cfg["instrument_type"],
                "ticker":          instrument["ticker"],
                "isin":            instrument["isin"],
                "cusip":           instrument["cusip"],
                "direction":       direction,
                "notional":        notional,
                "currency":        instrument["currency"],
                "trade_date":      trade_date.strftime("%Y-%m-%d"),
                "maturity_date":   maturity_date.strftime("%Y-%m-%d"),
                "generated_at":   pd.Timestamp.now(timezone.utc).isoformat(),
            })
            counter += 1

    return pd.DataFrame(rows)


def validate(df: pd.DataFrame) -> None:
    """
    Data quality checks before saving.
    Raises AssertionError immediately if any check fails.
    """
    assert df["trade_id"].nunique() == len(df), \
        "Duplicate trade_ids detected"

    assert df["asset_class"].isin(
        ["FX", "Equity", "Rates", "Credit"]
    ).all(), "Unexpected asset_class values"

    assert df["direction"].isin(["LONG", "SHORT"]).all(), \
        "Unexpected direction values"

    assert (df["notional"] > 0).all(), \
        "Non-positive notional values found"

    assert df["isin"].dropna().str.len().eq(12).all(), \
        "All non-null ISINs must be exactly 12 characters"

    assert df["cusip"].dropna().str.len().eq(9).all(), \
        "All non-null CUSIPs must be exactly 9 characters"

    # FX positions must have NULL isin and cusip
    # OTC FX forwards have no exchange-assigned ISIN
    fx = df[df["asset_class"] == "FX"]
    assert fx["isin"].isna().all(),  "FX positions must have NULL isin"
    assert fx["cusip"].isna().all(), "FX positions must have NULL cusip"

    # Equity positions must all have real ISINs
    eq = df[df["asset_class"] == "Equity"]
    assert eq["isin"].notna().all(), "All Equity positions must have an ISIN"

    # Desk → asset_class integrity — no cross-contamination
    desk_ac = {
        "FX Desk":     "FX",
        "Equity Desk": "Equity",
        "Rates Desk":  "Rates",
        "Credit Desk": "Credit",
    }
    for desk, ac in desk_ac.items():
        assert (df[df["desk"] == desk]["asset_class"] == ac).all(), \
            f"{desk} contains non-{ac} positions — desk/asset_class mismatch"

    logger.info("Validation PASSED — %d positions", len(df))
    logger.info(
        "Positions by desk:\n%s",
        df.groupby(["desk", "asset_class", "instrument_type"])["trade_id"]
          .count()
          .rename("count")
          .to_string()
    )
    logger.info(
        "ISIN coverage by asset class:\n%s",
        df.groupby("asset_class")["isin"]
          .apply(lambda x: f"{x.notna().sum()}/{len(x)} have ISIN")
          .to_string()
    )


def main():
    logger.info("Generating synthetic trading book for run date %s", RUN_DATE)
    df = generate_book()
    validate(df)

    key = f"raw/positions/year={RUN_YEAR}/month={RUN_MONTH}/day={RUN_DAY}/positions.csv"
    client = get_client()
    verify_bucket(client, S3_BUCKET)
    upload_df(client, df, S3_BUCKET,key)

    logger.info("Sample (first 3 rows):\n%s", df.head(3).to_string())


if __name__ == "__main__":
    main()