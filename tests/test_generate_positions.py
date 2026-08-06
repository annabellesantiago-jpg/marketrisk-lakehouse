"""
Unit tests for ingestion/generate_positions.py
Tests: book shape, trade IDs, desk-asset_class integrity, ISIN/CUSIP rules,
       notional ranges, direction, counterparties, traders, dates, determinism,
       and validate() edge cases.
"""
import pytest
import pandas as pd
import ingestion.generate_positions as gp
from ingestion.config import COUNTERPARTIES


class TestGenerateBook:
    """Test the generate_book() output."""

    @pytest.fixture(scope="class")
    def book(self):
        return gp.generate_book()

    # ── Shape & Schema ───────────────────────────────────────────────

    def test_row_count(self, book):
        assert len(book) == 300

    def test_expected_columns(self, book):
        expected = [
            "trade_id", "desk", "book_id", "trader_id", "counterparty",
            "asset_class", "instrument_type", "ticker", "isin", "cusip",
            "direction", "notional", "currency", "trade_date", "maturity_date",
            "generated_at",
        ]
        for col in expected:
            assert col in book.columns, f"Missing column: {col}"

    def test_positions_per_desk(self, book):
        counts = book.groupby("desk").size()
        for desk in ["FX Desk", "Equity Desk", "Rates Desk", "Credit Desk"]:
            assert counts[desk] == 75, f"{desk} has {counts[desk]} positions, expected 75"

    # ── Trade ID ─────────────────────────────────────────────────────

    def test_unique_trade_ids(self, book):
        assert book["trade_id"].nunique() == 300

    def test_trade_id_format(self, book):
        # Format: XX-NNNNN (2 letter prefix, dash, 5 zero-padded digits)
        assert book["trade_id"].str.match(r"^[A-Z]{2}-\d{5}$").all()

    def test_trade_id_prefix_fx(self, book):
        fx = book[book["desk"] == "FX Desk"]
        assert fx["trade_id"].str.startswith("FX-").all()

    def test_trade_id_prefix_equity(self, book):
        eq = book[book["desk"] == "Equity Desk"]
        assert eq["trade_id"].str.startswith("EQ-").all()

    def test_trade_id_prefix_rates(self, book):
        rt = book[book["desk"] == "Rates Desk"]
        assert rt["trade_id"].str.startswith("RT-").all()

    def test_trade_id_prefix_credit(self, book):
        cr = book[book["desk"] == "Credit Desk"]
        assert cr["trade_id"].str.startswith("CR-").all()

    # ── Desk-Asset Class Integrity ───────────────────────────────────

    def test_fx_desk_asset_class(self, book):
        fx = book[book["desk"] == "FX Desk"]
        assert (fx["asset_class"] == "FX").all()
        assert (fx["instrument_type"] == "FX_FORWARD").all()

    def test_equity_desk_asset_class(self, book):
        eq = book[book["desk"] == "Equity Desk"]
        assert (eq["asset_class"] == "Equity").all()
        assert (eq["instrument_type"] == "EQUITY").all()

    def test_rates_desk_asset_class(self, book):
        rt = book[book["desk"] == "Rates Desk"]
        assert (rt["asset_class"] == "Rates").all()
        assert (rt["instrument_type"] == "CORPORATE_BOND").all()

    def test_credit_desk_asset_class(self, book):
        cr = book[book["desk"] == "Credit Desk"]
        assert (cr["asset_class"] == "Credit").all()
        assert (cr["instrument_type"] == "CDS").all()

    # ── ISIN / CUSIP Rules ───────────────────────────────────────────

    def test_fx_isin_null(self, book):
        fx = book[book["asset_class"] == "FX"]
        assert fx["isin"].isna().all(), "FX positions must have NULL isin"
        assert fx["cusip"].isna().all(), "FX positions must have NULL cusip"

    def test_equity_isin_present(self, book):
        eq = book[book["asset_class"] == "Equity"]
        assert eq["isin"].notna().all(), "All Equity positions must have ISIN"
        assert eq["isin"].str.len().eq(12).all(), "All ISINs must be 12 chars"

    def test_equity_us_cusip(self, book):
        eq = book[book["asset_class"] == "Equity"]
        us_tickers = ["AAPL", "MSFT", "JPM", "GS"]
        us_eq = eq[eq["ticker"].isin(us_tickers)]
        non_us_eq = eq[~eq["ticker"].isin(us_tickers)]
        assert us_eq["cusip"].notna().all(), "US equities must have CUSIP"
        assert us_eq["cusip"].str.len().eq(9).all(), "CUSIPs must be 9 chars"
        assert non_us_eq["cusip"].isna().all(), "Non-US equities must have NULL CUSIP"

    def test_rates_isin_synthetic(self, book):
        rt = book[book["asset_class"] == "Rates"]
        assert rt["isin"].notna().all(), "Rates positions must have synthetic ISIN"
        assert rt["isin"].str.len().eq(12).all()

    def test_credit_isin_synthetic(self, book):
        cr = book[book["asset_class"] == "Credit"]
        assert cr["isin"].notna().all(), "Credit positions must have synthetic ISIN"
        assert cr["isin"].str.len().eq(12).all()

    # ── Notionals ────────────────────────────────────────────────────

    def test_notionals_positive(self, book):
        assert (book["notional"] > 0).all()

    def test_fx_notional_range(self, book):
        fx = book[book["desk"] == "FX Desk"]
        assert (fx["notional"] >= 1_000_000).all()
        assert (fx["notional"] <= 50_000_000).all()

    def test_equity_notional_range(self, book):
        eq = book[book["desk"] == "Equity Desk"]
        assert (eq["notional"] >= 500_000).all()
        assert (eq["notional"] <= 20_000_000).all()

    def test_rates_notional_range(self, book):
        rt = book[book["desk"] == "Rates Desk"]
        assert (rt["notional"] >= 2_000_000).all()
        assert (rt["notional"] <= 100_000_000).all()

    def test_credit_notional_range(self, book):
        cr = book[book["desk"] == "Credit Desk"]
        assert (cr["notional"] >= 1_000_000).all()
        assert (cr["notional"] <= 50_000_000).all()

    # ── Direction ────────────────────────────────────────────────────

    def test_direction_values(self, book):
        assert book["direction"].isin(["LONG", "SHORT"]).all()

    # ── Counterparties & Traders ─────────────────────────────────────

    def test_valid_counterparties(self, book):
        assert book["counterparty"].isin(COUNTERPARTIES).all()

    def test_trader_desk_match(self, book):
        desk_prefix = {
            "FX Desk":     "T-FX-",
            "Equity Desk": "T-EQ-",
            "Rates Desk":  "T-RT-",
            "Credit Desk": "T-CR-",
        }
        for desk, prefix in desk_prefix.items():
            rows = book[book["desk"] == desk]
            assert rows["trader_id"].str.startswith(prefix).all(), \
                f"{desk} traders don't match prefix {prefix}"

    # ── Dates ────────────────────────────────────────────────────────

    def test_maturity_after_trade_date(self, book):
        trade_dates = pd.to_datetime(book["trade_date"])
        maturity_dates = pd.to_datetime(book["maturity_date"])
        assert (maturity_dates > trade_dates).all()

    # ── Determinism ──────────────────────────────────────────────────

    def test_reproducibility(self):
        book1 = gp.generate_book()
        book2 = gp.generate_book()
        # Exclude generated_at (wall-clock timestamp differs between calls)
        cols = [c for c in book1.columns if c != "generated_at"]
        pd.testing.assert_frame_equal(book1[cols], book2[cols])


class TestValidate:
    """Test the validate() function edge cases."""

    @pytest.fixture
    def valid_book(self):
        return gp.generate_book()

    def test_validate_passes(self, valid_book):
        # Should not raise
        gp.validate(valid_book)

    def test_validate_duplicate_trade_id(self, valid_book):
        bad = valid_book.copy()
        bad.iloc[1, bad.columns.get_loc("trade_id")] = bad.iloc[0]["trade_id"]
        with pytest.raises(AssertionError, match="Duplicate trade_ids"):
            gp.validate(bad)

    def test_validate_negative_notional(self, valid_book):
        bad = valid_book.copy()
        bad.iloc[0, bad.columns.get_loc("notional")] = -1000
        with pytest.raises(AssertionError, match="Non-positive notional"):
            gp.validate(bad)

    def test_validate_invalid_direction(self, valid_book):
        bad = valid_book.copy()
        bad.iloc[0, bad.columns.get_loc("direction")] = "BUY"
        with pytest.raises(AssertionError, match="Unexpected direction"):
            gp.validate(bad)

    def test_validate_fx_with_isin(self, valid_book):
        bad = valid_book.copy()
        fx_idx = bad[bad["asset_class"] == "FX"].index[0]
        bad.at[fx_idx, "isin"] = "US1234567890"
        with pytest.raises(AssertionError, match="FX positions must have NULL isin"):
            gp.validate(bad)

    def test_validate_invalid_asset_class(self, valid_book):
        bad = valid_book.copy()
        bad.iloc[0, bad.columns.get_loc("asset_class")] = "Commodities"
        with pytest.raises(AssertionError, match="Unexpected asset_class"):
            gp.validate(bad)
