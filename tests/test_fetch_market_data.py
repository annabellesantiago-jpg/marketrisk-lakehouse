"""
Unit tests for ingestion/fetch_market_data.py
Tests: ticker_to_filename normalization, fetch_ticker with mocked yfinance,
       retry/error handling, column schema, FX volume NULL conversion,
       file_exists_in_s3 with mocked boto3.
"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
from botocore.exceptions import ClientError

from ingestion.fetch_market_data import (
    ticker_to_filename,
    fetch_ticker,
    file_exists_in_s3,
    MarketDataFetchError,
)


# ── ticker_to_filename ──────────────────────────────────────────────────

class TestTickerToFilename:
    """Test the ticker normalization for S3 filenames."""

    def test_fx_ticker(self):
        assert ticker_to_filename("EURUSD=X") == "EURUSD"

    def test_uk_equity(self):
        assert ticker_to_filename("HSBA.L") == "HSBA_L"

    def test_spain_equity(self):
        assert ticker_to_filename("SAN.MC") == "SAN_MC"

    def test_us_equity_unchanged(self):
        assert ticker_to_filename("AAPL") == "AAPL"

    def test_plain_ticker(self):
        assert ticker_to_filename("MSFT") == "MSFT"


# ── fetch_ticker ────────────────────────────────────────────────────────

def _mock_ohlcv(ticker: str, rows: int = 5) -> pd.DataFrame:
    """Build a fake yfinance DataFrame with MultiIndex columns."""
    dates = pd.date_range(end=datetime.today(), periods=rows, freq="B")
    rng = np.random.default_rng(42)
    df = pd.DataFrame({
        ("Open", ticker):   rng.uniform(100, 200, rows),
        ("High", ticker):   rng.uniform(100, 200, rows),
        ("Low", ticker):    rng.uniform(100, 200, rows),
        ("Close", ticker):  rng.uniform(100, 200, rows),
        ("Volume", ticker): rng.integers(0, 1_000_000, rows),
    })
    df.columns = pd.MultiIndex.from_tuples(df.columns)
    df.index = dates
    df.index.name = "Date"
    return df


class TestFetchTicker:
    """Test fetch_ticker with mocked yfinance."""

    @patch("ingestion.fetch_market_data.yf.download")
    def test_returns_expected_columns(self, mock_download):
        mock_download.return_value = _mock_ohlcv("AAPL")
        result = fetch_ticker("AAPL", 400)
        for col in ["date", "open", "high", "low", "close", "volume",
                     "ticker", "fetched_at"]:
            assert col in result.columns, f"Missing column: {col}"

    @patch("ingestion.fetch_market_data.yf.download")
    def test_ticker_column_raw(self, mock_download):
        mock_download.return_value = _mock_ohlcv("EURUSD=X")
        result = fetch_ticker("EURUSD=X", 400)
        assert (result["ticker"] == "EURUSD=X").all()

    @patch("ingestion.fetch_market_data.yf.download")
    def test_columns_lowercase(self, mock_download):
        mock_download.return_value = _mock_ohlcv("AAPL")
        result = fetch_ticker("AAPL", 400)
        for col in result.columns:
            assert col == col.lower(), f"Column '{col}' not lowercase"

    @patch("ingestion.fetch_market_data.yf.download")
    def test_fx_volume_null(self, mock_download):
        """FX pairs report volume as 0 — should be converted to NULL."""
        ohlcv = _mock_ohlcv("EURUSD=X")
        # Set all volume to 0 (as Yahoo does for FX)
        ohlcv[("Volume", "EURUSD=X")] = 0
        mock_download.return_value = ohlcv
        result = fetch_ticker("EURUSD=X", 400)
        assert result["volume"].isna().all(), "FX zero volume should be NULL"

    @patch("ingestion.fetch_market_data.yf.download")
    def test_fetched_at_utc(self, mock_download):
        mock_download.return_value = _mock_ohlcv("AAPL")
        result = fetch_ticker("AAPL", 400)
        assert result["fetched_at"].notna().all()

    @patch("ingestion.fetch_market_data.time.sleep")
    @patch("ingestion.fetch_market_data.yf.download")
    def test_raises_after_3_empty_attempts(self, mock_download, mock_sleep):
        mock_download.return_value = pd.DataFrame()
        with pytest.raises(MarketDataFetchError, match="No market data returned"):
            fetch_ticker("BADTICKER", 400)
        # Should have retried (slept twice: after attempt 1 and 2)
        assert mock_sleep.call_count == 2

    @patch("ingestion.fetch_market_data.time.sleep")
    @patch("ingestion.fetch_market_data.yf.download")
    def test_retry_succeeds_on_second_attempt(self, mock_download, mock_sleep):
        mock_download.side_effect = [pd.DataFrame(), _mock_ohlcv("AAPL")]
        result = fetch_ticker("AAPL", 400)
        assert len(result) == 5
        assert mock_sleep.call_count == 1

    @patch("ingestion.fetch_market_data.time.sleep")
    @patch("ingestion.fetch_market_data.yf.download")
    def test_retry_on_exception(self, mock_download, mock_sleep):
        mock_download.side_effect = [
            ConnectionError("network down"),
            _mock_ohlcv("AAPL"),
        ]
        result = fetch_ticker("AAPL", 400)
        assert len(result) == 5


# ── file_exists_in_s3 ──────────────────────────────────────────────────

class TestFileExistsInS3:
    """Test S3 existence check with mocked boto3."""

    def test_file_exists(self):
        client = MagicMock()
        client.head_object.return_value = {}
        assert file_exists_in_s3(client, "bucket", "key") is True

    def test_file_not_found(self):
        client = MagicMock()
        error_response = {"Error": {"Code": "404"}}
        client.head_object.side_effect = ClientError(error_response, "HeadObject")
        assert file_exists_in_s3(client, "bucket", "key") is False

    def test_no_such_key(self):
        client = MagicMock()
        error_response = {"Error": {"Code": "NoSuchKey"}}
        client.head_object.side_effect = ClientError(error_response, "HeadObject")
        assert file_exists_in_s3(client, "bucket", "key") is False

    def test_other_error_reraises(self):
        client = MagicMock()
        error_response = {"Error": {"Code": "403"}}
        client.head_object.side_effect = ClientError(error_response, "HeadObject")
        with pytest.raises(ClientError):
            file_exists_in_s3(client, "bucket", "key")
