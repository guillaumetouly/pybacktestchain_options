
from ..src.pybacktestchain_options.data_module import get_commodity_data, SpreadStrategy
from ..src.pybacktestchain_options.broker import CommoBroker
import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from unittest.mock import MagicMock

def test_update_pos():
    strategy = CommoBroker(1000000)
    strategy.positions = {}

    # Test adding a new position
    strategy.update_pos('CORN', 10, 5, 1.5, 2.0, datetime(2025, 1, 1))
    assert 'CORN' in strategy.positions
    assert strategy.positions['CORN'].near_term_quantity == 10
    assert strategy.positions['CORN'].long_term_quantity == 5

    # Test updating an existing position
    strategy.update_pos('CORN', 5, 10, 1.8, 2.2, datetime(2025, 1, 2))
    assert strategy.positions['CORN'].near_term_quantity > 10
    assert strategy.positions['CORN'].long_term_quantity >= 5

def test_execute_spread_strategy():
    strategy = CommoBroker(1000000)
    strategy.positions = {}
    strategy.update_pos = MagicMock()

    long_term = {'CORN': [2.0, 1.8]}
    short_term = {'CORN': 1.5}

    strategy.execute_spread_strategy(long_term, short_term, datetime(2025, 1, 1))

    strategy.update_pos.assert_called_with('CORN', 1, 1, 1.5, 2.0, datetime(2025, 1, 1))

def test_get_commodity_data():
    data = get_commodity_data('AAPL', '2025-01-01', '2025-01-10')
    assert isinstance(data, pd.DataFrame)
    assert 'ticker' in data.columns

def test_compute_spread():
    class MockDataModule:
        def __init__(self):
            self.data = pd.DataFrame({
                'time': ['2025-01-01', '2025-01-02'],
                'CORN - Near Term': [100, 105],
                'CORN - Long Term': [95, 102],
            }).rename_axis(columns='contract')

    strategy = CommoBroker(1000000)
    strategy.data_module = MockDataModule()
    result = strategy.compute_spread()

    assert 'CORN - Spread' in result.columns
    assert not result.empty

def test_optimize_spread():
    mean_return = 0.01
    std_dev = 0.02
    correlation = 0.5

    strategy = CommoBroker(1000000)
    weights = strategy.optimize_spread(mean_return, std_dev, correlation)

    assert weights is not None
    assert np.isclose(weights.sum(), 0)

def test_compute_statistics():
    spread_data = pd.DataFrame({
        'Spread': [100, 102, 104, 103, 105]
    })
    strategy = CommoBroker(1000000)

    mean_return, std_dev = strategy.compute_statistics(spread_data)

    assert mean_return > 0
    assert std_dev > 0
