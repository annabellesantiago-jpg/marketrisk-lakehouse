"""
generate_positions.py
Generates a synthetic trading book — simulates what a real
trade booking system would provide.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.config import (
    DESKS, COUNTERPARTIES, ASSET_CLASSES,
    EQUITY_TICKERS, FX_TICKERS, NUM_POSITIONS
)

OUTPUT_DIR = Path("data/raw/positions")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

np.random.seed(42)   # makes the data reproducible every time

def generate_book() -> pd.DataFrame:
    today = datetime.today()

    tickers = [t.replace("=X", "").replace(".", "_") for t in FX_TICKERS + EQUITY_TICKERS]

    rows = []
    for i in range(NUM_POSITIONS):
        desk         = np.random.choice(DESKS)
        asset_class  = np.random.choice(ASSET_CLASSES)
        ticker       = np.random.choice(tickers)
        trade_date   = today - timedelta(days=int(np.random.randint(0, 180)))
        notional     = round(float(np.random.uniform(100_000, 5_000_000)), 2)
        direction    = np.random.choice(["LONG", "SHORT"])
        currency     = np.random.choice(["USD", "EUR", "GBP", "JPY", "SGD"])

        rows.append({
            "trade_id":       f"TRD-{i+1:05d}",
            "desk":           desk,
            "counterparty":   np.random.choice(COUNTERPARTIES),
            "asset_class":    asset_class,
            "ticker":         ticker,
            "direction":      direction,
            "notional":       notional,
            "currency":       currency,
            "trade_date":     trade_date.strftime("%Y-%m-%d"),
            "maturity_date":  (trade_date + timedelta(days=int(np.random.randint(30, 730)))).strftime("%Y-%m-%d"),
            "generated_at":   datetime.utcnow().isoformat(),
        })

    return pd.DataFrame(rows)

def main():
    print("\nGenerating synthetic trading book...")
    df = generate_book()
    path = OUTPUT_DIR / "positions.csv"
    df.to_csv(path, index=False)
    print(f"Generated {len(df)} positions → {path}")
    print(f"\nSample:\n{df.head(3).to_string()}")

if __name__ == "__main__":
    main()
