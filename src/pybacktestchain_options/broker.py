import pandas as pd
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

import os 
import pickle
from data_module import get_commodities_data, SpreadStrategy, DataModule
from pybacktestchain.utils import generate_random_name
from pybacktestchain.blockchain import Block, Blockchain

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#---------------------------------------------------------
# Classes
#---------------------------------------------------------

@dataclass
class SpreadPosition:
    commodity: str
    near_term_quantity: int
    long_term_quantity: int
    entry_spread: float

@dataclass
class CommoBroker:
    cash: float
    positions: dict = None
    transaction_log: pd.DataFrame = None
    verbose: bool = True

    def __post_init__(self):
        # Initialize positions as a dictionary of SpreadPosition objects
        if self.positions is None:
            self.positions = {}

        # Initialize the transaction log as an empty DataFrame if none is provided
        if self.transaction_log is None:
            self.transaction_log = pd.DataFrame(columns=['Date', 'Action', 'Commodity', 'Near Term Qty', 'Long Term Qty', 'Spread', 'Cash'])
    
    def initialize_blockchain(self, name: str):
        # Check if the blockchain is already initialized and stored in the blockchain folder
        # if folder blockchain does not exist, create it
        if not os.path.exists('blockchain'):
            os.makedirs('blockchain')
        chains = os.listdir('blockchain')
        ending = f'{name}.pkl'
        if ending in chains:
            if self.verbose:
                logging.warning(f"Blockchain with name {name} already exists. Please use a different name.")
            with open(f'blockchain/{name}.pkl', 'rb') as f:
                self.blockchain = pickle.load(f)
            return

        self.blockchain = Blockchain(name)
        # Store the blockchain
        self.blockchain.store()

        if self.verbose:
            logging.info(f"Blockchain with name {name} initialized and stored in the blockchain folder.")

    def buy_spread(self, commodity: str, near_qty: int, long_qty: int, spread: float, date: datetime):
        """Executes a buy spread order for the specified commodity."""
        total_cost = abs(spread * (near_qty - long_qty))
        if self.cash >= total_cost:
            self.cash -= total_cost
            if commodity in self.positions:
                position = self.positions[commodity]
                new_near_qty = position.near_term_quantity + near_qty
                new_long_qty = position.long_term_quantity + long_qty
                new_entry_spread = ((position.entry_spread * abs(position.near_term_quantity - position.long_term_quantity)) + (spread * abs(near_qty - long_qty))) / (abs(new_near_qty - new_long_qty))
                position.near_term_quantity = new_near_qty
                position.long_term_quantity = new_long_qty
                position.entry_spread = new_entry_spread
            else:
                self.positions[commodity] = SpreadPosition(commodity, near_qty, long_qty, spread)
            self.log_transaction(date, 'BUY_SPREAD', commodity, near_qty, long_qty, spread)
        else:
            if self.verbose:
                logging.warning(f"Not enough cash to buy spread for {commodity}. Required: {total_cost}, Available: {self.cash}")

    def sell_spread(self, commodity: str, near_qty: int, long_qty: int, spread: float, date: datetime):
        """Executes a sell spread order for the specified commodity."""
        if commodity in self.positions:
            position = self.positions[commodity]
            if position.near_term_quantity >= near_qty and position.long_term_quantity >= long_qty:
                position.near_term_quantity -= near_qty
                position.long_term_quantity -= long_qty
                self.cash += abs(spread * (near_qty - long_qty))

                if position.near_term_quantity == 0 and position.long_term_quantity == 0:
                    del self.positions[commodity]

                self.log_transaction(date, 'SELL_SPREAD', commodity, near_qty, long_qty, spread)
            else:
                if self.verbose:
                    logging.warning(f"Not enough positions to sell spread for {commodity}.")
        else:
            if self.verbose:
                logging.warning(f"No position found for commodity {commodity}.")

    def log_transaction(self, date, action, commodity, near_qty, long_qty, spread):
        """Logs the transaction."""
        transaction = pd.DataFrame([{
            'Date': date,
            'Action': action,
            'Commodity': commodity,
            'Near Term Qty': near_qty,
            'Long Term Qty': long_qty,
            'Spread': spread,
            'Cash': self.cash
        }])

        self.transaction_log = pd.concat([self.transaction_log, transaction], ignore_index=True)

    def get_cash_balance(self):
        return self.cash

    def get_transaction_log(self):
        return self.transaction_log

    def get_portfolio_value(self, market_spreads: dict):
        """Calculates the total portfolio value based on the current market spreads."""
        portfolio_value = self.cash
        for commodity, position in self.positions.items():
            current_spread = market_spreads.get(commodity)
            if current_spread is not None:
                portfolio_value += abs(current_spread * (position.near_term_quantity - position.long_term_quantity))
        return portfolio_value

    def execute_spread_strategy(self, strategy, market_spreads, date):
        """Executes the trades for the spread strategy."""
        print("STRATEGY IS ", strategy)
        for commodity, target_spread in strategy.items():
            print("COMMODITY IS ", commodity)
            print("SCD TARGET SPREAD IS ", target_spread)
            current_spread = market_spreads[commodity]
            print("CURRENT SPREAD IS ", current_spread)
            if  target_spread == {}:
                if self.verbose:
                    logging.warning(f"Spread for {commodity} not available on {date}")
                continue
            if current_spread is None :
                if self.verbose:
                    logging.warning(f"Spread for {commodity} not available on {date}")
                continue

            # Determine the quantity to trade based on the target spread
            if current_spread < target_spread:
                self.buy_spread(commodity, near_qty=1, long_qty=1, spread=current_spread, date=date)
            elif current_spread > target_spread:
                self.sell_spread(commodity, near_qty=1, long_qty=1, spread=current_spread, date=date)

@dataclass
class CommoBackTest:
    initial_date: datetime
    final_date: datetime
    commodity_pairs: dict
    cash: float = 1000000  # Initial cash in the portfolio
    verbose: bool = True
    broker = CommoBroker(cash)


    def __post_init__(self):
        self.broker = CommoBroker(cash=self.cash, verbose=self.verbose)

    def run_backtest(self):
        logging.info(f"Running backtest from {self.initial_date} to {self.final_date}.")
        data = get_commodities_data(self.commodity_pairs, self.initial_date.strftime('%Y-%m-%d'), self.final_date.strftime('%Y-%m-%d'))
        data_module = DataModule(data)
        strategy = SpreadStrategy(data_module=data_module)
        spread_data = strategy.compute_spread()
        commo = ["CORN", "GAS", "OIL", "WHEAT"]

        for t in pd.date_range(start=self.initial_date, end=self.final_date, freq='B'):
            spreads = {commodity : spread_data.loc[t.strftime('%Y-%m-%d')][commodity+ " - Spread"] if t.strftime('%Y-%m-%d') in spread_data.index else {} for commodity in commo}
            print("SPREADS  ", spreads)
            target_spreads = {commodity: spread_data[commodity+" - Spread"].mean() for commodity in commo}
            print("TARGET SPREADS ", target_spreads)
            self.broker.execute_spread_strategy(spreads, target_spreads, t)

        logging.info(f"Backtest completed. Final portfolio value: {self.broker.get_portfolio_value({})}")
        logging.info("Transaction Log:")
        logging.info(self.broker.get_transaction_log())
