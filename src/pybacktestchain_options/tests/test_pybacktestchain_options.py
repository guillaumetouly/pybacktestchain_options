from pybacktestchain_options.src.pybacktestchain_options.data_module import get_commodity_data, SpreadStrategy, DataModule
from pybacktestchain_options.src.pybacktestchain_options.broker import CommoBroker
import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from unittest.mock import MagicMock, patch




def test_get_commodity_data():
    """Test the get_commodity_data function."""

    ticker = "TEST_TICKER"
    start_date = "2023-01-01"
    end_date = "2023-12-31"

    mock_data = pd.DataFrame({
        "Open": [100, 102, 104],
        "High": [101, 103, 105],
        "Low": [99, 101, 103],
        "Close": [100, 102, 104],
        "Volume": [1000, 1100, 1200],
    }, index=pd.date_range(start=start_date, periods=3, freq="D"))

    with patch("yfinance.Ticker") as MockTicker:
        mock_ticker_instance = MagicMock()
        MockTicker.return_value = mock_ticker_instance
        mock_ticker_instance.history.return_value = mock_data

        result = get_commodity_data(ticker, start_date, end_date)

        assert not result.empty, "Resulting DataFrame should not be empty."
        assert "ticker" in result.columns, "Resulting DataFrame should contain a 'ticker' column."
        assert result["ticker"].iloc[0] == ticker, f"Ticker column should contain the value '{ticker}'."
        assert len(result) == len(mock_data), "Resulting DataFrame should have the same number of rows as mock data."

def test_get_commodity_data_no_data():
    """Test get_commodity_data with no data returned."""

    ticker = "INVALID_TICKER"
    start_date = "2023-01-01"
    end_date = "2023-12-31"

    with patch("yfinance.Ticker") as MockTicker:
        mock_ticker_instance = MagicMock()
        MockTicker.return_value = mock_ticker_instance
        mock_ticker_instance.history.return_value = pd.DataFrame()

        result = get_commodity_data(ticker, start_date, end_date)

        assert result.empty, "Resulting DataFrame should be empty for invalid data."

    



@pytest.fixture
def mock_data():
    """Fixture for providing mock DataFrame."""
    data = pd.DataFrame({
        "Date": pd.date_range("2023-01-01", periods=8, freq="D"),
        "Contract": [
            "CORN - Near Term", "CORN - Long Term",
            "GAS - Near Term", "GAS - Long Term",
            "OIL - Near Term", "OIL - Long Term",
            "WHEAT - Near Term", "WHEAT - Long Term"
        ],
        "Close": [100, 95, 50, 45, 70, 65, 120, 115]
    })
    return DataModule(data)



def test_compute_statistics(mock_data):
    """Test the compute_statistics method."""

    strategy = SpreadStrategy(data_module=mock_data)

    spread_data = pd.DataFrame({
        "Spread": [5, 6, 4, 5, 7]
    })

    mean_return, std_dev = strategy.compute_statistics(spread_data)

    expected_spread_return = spread_data['Spread'].pct_change()
    expected_mean_return = expected_spread_return.mean()
    expected_std_dev = expected_spread_return.std()

    assert np.isclose(mean_return, expected_mean_return, atol=1e-6), "Mean return is incorrect."
    assert np.isclose(std_dev, expected_std_dev, atol=1e-6), "Standard deviation is incorrect."


def test_compute_statistics_empty_data(mock_data):
    """Test compute_statistics with an empty DataFrame."""

    strategy = SpreadStrategy(data_module=mock_data)
    spread_data = pd.DataFrame(columns=["Spread"])
    mean_return, std_dev = strategy.compute_statistics(spread_data)
    assert pd.isna(mean_return), "Mean return should be NaN for empty data."
    assert pd.isna(std_dev), "Standard deviation should be NaN for empty data."

