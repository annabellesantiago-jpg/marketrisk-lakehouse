"""
Unit tests for ingestion/fetch_reference_data.py
Tests: get_live_fx_rates with mocked S3 reads, generate_fx_rates output shape,
       USD inclusion, error handling for empty/missing price files.
"""
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import date

from ingestion.fetch_reference_data import get_live_fx_rates, generate_fx_rates


# ── Helpers ──────────────────────────────────────────────────────────────

def _price_df(close: float = 1.1234, dt: str = "2026-08-01") -> pd.DataFrame:
    """Minimal price DataFrame matching what fetch_market_data writes."""
    return pd.DataFrame({
        "date": [dt],
        "open": [close - 0.01],
        "high": [close + 0.01],
        "low":  [close - 0.02],
        "close": [close],
        "volume": [0],
        "ticker": ["EURUSD=X"],
        "fetched_at": ["2026-08-01T12:00:00+00:00"],
    })


# ── get_live_fx_rates ───────────────────────────────────────────────────

class TestGetLiveFxRates:
    """Test FX rate extraction from mocked S3 price files."""

    @patch("ingestion.fetch_reference_data.read_df")
    def test_returns_all_currencies(self, mock_read):
        mock_read.return_value = _price_df(1.1, "2026-08-01")
        client = MagicMock()
        rates = get_live_fx_rates(client)
        expected = {"EUR", "GBP", "JPY", "SGD", "AUD"}
        assert set(rates.keys()) == expected

    @patch("ingestion.fetch_reference_data.read_df")
    def test_rate_structure(self, mock_read):
        mock_read.return_value = _price_df(1.1, "2026-08-01")
        client = MagicMock()
        rates = get_live_fx_rates(client)
        for ccy, data in rates.items():
            assert "rate" in data, f"{ccy} missing 'rate'"
            assert "as_of" in data, f"{ccy} missing 'as_of'"
            assert isinstance(data["rate"], float)

    @patch("ingestion.fetch_reference_data.read_df")
    def test_rate_uses_latest_close(self, mock_read):
        """Should pick the most recent date's close price."""
        multi = pd.DataFrame({
            "date": ["2026-07-30", "2026-08-01", "2026-07-31"],
            "open": [1.0, 1.1, 1.05],
            "high": [1.1, 1.2, 1.15],
            "low": [0.9, 1.0, 0.95],
            "close": [1.05, 1.12, 1.08],
            "volume": [0, 0, 0],
            "ticker": ["EURUSD=X"] * 3,
            "fetched_at": ["2026-08-01T12:00:00+00:00"] * 3,
        })
        mock_read.return_value = multi
        client = MagicMock()
        rates = get_live_fx_rates(client)
        # Aug 1 is latest → close = 1.12
        assert rates["EUR"]["rate"] == 1.12

    @patch("ingestion.fetch_reference_data.read_df")
    def test_raises_on_empty_df(self, mock_read):
        mock_read.return_value = pd.DataFrame()
        client = MagicMock()
        with pytest.raises(ValueError, match="Price file is empty"):
            get_live_fx_rates(client)


# ── generate_fx_rates ───────────────────────────────────────────────────

class TestGenerateFxRates:
    """Test the DataFrame builder for FX rates."""

    def _sample_rates(self):
        return {
            "EUR": {"rate": 1.1234, "as_of": "2026-08-01"},
            "GBP": {"rate": 1.2700, "as_of": "2026-08-01"},
        }

    def test_includes_usd(self):
        df = generate_fx_rates(self._sample_rates())
        assert "USD" in df["currency"].values

    def test_usd_rate_is_one(self):
        df = generate_fx_rates(self._sample_rates())
        usd = df[df["currency"] == "USD"]
        assert usd.iloc[0]["rate_vs_usd"] == 1.0

    def test_expected_columns(self):
        df = generate_fx_rates(self._sample_rates())
        for col in ["currency", "rate_vs_usd", "as_of_date", "generated_at"]:
            assert col in df.columns

    def test_row_count(self):
        rates = self._sample_rates()
        df = generate_fx_rates(rates)
        # Input currencies + USD
        assert len(df) == len(rates) + 1

    def test_rate_values_match(self):
        rates = self._sample_rates()
        df = generate_fx_rates(rates)
        eur = df[df["currency"] == "EUR"]
        assert eur.iloc[0]["rate_vs_usd"] == 1.1234
