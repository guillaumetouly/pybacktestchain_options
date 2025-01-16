import pandas as pd
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pybacktestchain.broker import Backtest, EndOfMonth, Information, StopLoss, Broker
from broker import CommoBackTest, CommoBroker
from data_module import COMMODITY_TICKER_PAIRS
from pybacktestchain.utils import generate_random_name

@dataclass
class UniversalBackTest():
    initial_date: datetime
    final_date: datetime
    commo_equity: str = "COMMO" #or "EQUITY"
    commodity_pairs: dict = field(default_factory=lambda: {
        "OIL": {"Near Term": "CL=F", "Long Term": "CLM25.NYM"},  # Crude Oil
        "GAS": {"Near Term": "NG=F", "Long Term": "NGM25.NYM"},  # Natural Gas
        "WHEAT": {"Near Term": "ZW=F", "Long Term": "ZWN25.CBT"}, # Wheat
        "CORN": {"Near Term": "ZC=F", "Long Term": "ZCN25.CBT"}  # Corn
    })
    cash: float = 1000000  # Initial cash in the portfolio
    verbose: bool = True
    universe = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA', 'INTC', 'CSCO', 'NFLX']
    information_class : type  = Information
    s: timedelta = timedelta(days=360)
    time_column: str = 'Date'
    company_column: str = 'ticker'
    adj_close_column : str ='Adj Close'
    rebalance_flag : type = EndOfMonth
    risk_model : type = StopLoss
    initial_cash: int = 1000000  # Initial cash in the portfolio
    name_blockchain: str = 'backtest'
    verbose: bool = True

    def __post_init__(self):
        self.backtest_name = generate_random_name()
        
    def define_backtest(self):
        if self.commo_equity == "EQUITY":
            self.broker = Broker(cash=self.initial_cash, verbose=self.verbose)
            self.broker.initialize_blockchain(self.name_blockchain)
            self.backtest = Backtest(
                self.initial_date,
                self.final_date,
                self.universe,
                self.information_class,
                self.s,
                self.time_column,
                self.company_column,
                self.adj_close_column,
                self.rebalance_flag,
                self.risk_model,
                self.initial_cash,
                self.name_blockchain,
                self.verbose,
            )
        elif self.commo_equity == "COMMO":
            self.broker = CommoBroker(self.cash, verbose=self.verbose)
            self.broker.initialize_blockchain(self.name_blockchain)
            self.backtest = CommoBackTest(self.initial_date,
                                     self.final_date,
                                     self.commodity_pairs,
                                     self.cash,
                                     self.verbose,
                                     self.backtest_name)

        else:
            pass

    def run_backtest(self):
        self.define_backtest()
        self.backtest.run_backtest()


    