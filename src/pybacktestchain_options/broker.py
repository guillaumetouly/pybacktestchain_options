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
        if self.positions is None:
            self.positions = {}

        if self.transaction_log is None: #no transactions already logged, we set up an empty dataframe with all the columns we want
            self.transaction_log = pd.DataFrame(columns=['Date', 'Action', 'Commodity', 'Near Term Qty', 'Long Term Qty', 'Spread', 'Cash', 'Portfolio Value'])
    
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

 
    def update_pos(self, commodity: str, near_qty: int, long_qty: int, st_spread: float, lt_spread: float, date: datetime):
        """Executes a sell or buy spread order for the specified commodity.
        We sell as much as possible short term and buy as much as possible long term and conversely for the buy side"""

        spread = lt_spread - st_spread
        type_of_transac = "None"
        if commodity in self.positions: #we look if we already have positionsi n this commodity because we don't want to get too much exposure

            position = self.positions[commodity]
            if spread > 0: #we buy short term and sell long term
                type_of_transac = "Long ST, Short LT"
                if position.long_term_quantity > spread:
                    total_cost = spread*(-lt_spread + st_spread) #we can sell enough LT, this is good because it reduces the transavtion cost for the strategy
                    if total_cost < self.cash: #else we can't play and we loose this part
                        position.near_term_quantity += spread
                        position.long_term_quantity -= spread
                        self.cash -= total_cost
                elif position.long_term_quantity > 0:
                    total_cost = - position.long_term_quantity*lt_spread + spread*st_spread #we hedge the maximum we can
                    if total_cost <= self.cash:
                        self.cash -= total_cost
                        position.near_term_quantity += spread
                        position.long_term_quantity = 0
                    else:
                        max_total_cost = - position.long_term_quantity*lt_spread + self.cash
                        max_position = max_total_cost / st_spread
                        position.near_term_quantity += max_position
                        self.cash = 0
                        position.long_term_quantity = 0

                else:
                    total_cost = spread*st_spread
                    if total_cost <= self.cash:
                        self.cash -= total_cost
                        position.near_term_quantity += spread
                    else:
                        max_position = self.cash / st_spread
                        position.near_term_quantity += max_position
                        self.cash = 0

            else: #we sell short term and buy long term
                type_of_transac = "Long LT, Short ST"
                spread = -spread
                if position.near_term_quantity > spread:
                    total_cost = spread*(lt_spread - st_spread)
                    if total_cost <= self.cash:
                        position.near_term_quantity -= spread
                        position.long_term_quantity += spread
                        self.cash -= total_cost
                elif position.near_term_quantity > 0:
                    total_cost = + position.long_term_quantity*lt_spread - spread*st_spread #we hedge the maximum we can
                    if total_cost <= self.cash:
                        self.cash -= total_cost
                        position.near_term_quantity = 0
                        position.long_term_quantity += spread
                    else:
                        max_total_cost = - position.near_term_quantity*st_spread + self.cash
                        max_position = max_total_cost / lt_spread
                        position.long_term_quantity += max_position
                        self.cash = 0
                        position.near_term_quantity = 0
                else:
                    total_cost = spread*lt_spread
                    if total_cost <= self.cash:
                        self.cash -= total_cost
                        position.long_term_quantity += spread
                    else:
                        max_position = self.cash / lt_spread
                        position.long_term_quantity += max_position
                        self.cash = 0
            self.positions[commodity] = position
            self.log_transaction(date, type_of_transac, commodity, position.near_term_quantity, position.long_term_quantity, st_spread, 1)

        else:
            self.positions[commodity] = SpreadPosition(commodity, near_qty, long_qty, st_spread)



    def log_transaction(self, date, action, commodity, near_qty, long_qty, spread, portfolio_value):
        """Logs the transaction."""
        transaction = pd.DataFrame([{
            'Date': date,
            'Action': action,
            'Commodity': commodity,
            'Near Term Qty': near_qty,
            'Long Term Qty': long_qty,
            'Spread': spread,
            'Cash': self.cash,
            "Portfolio Value": portfolio_value
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
            print(current_spread)
            if current_spread is not None:
                portfolio_value += (current_spread[0]*position.near_term_quantity + current_spread[1]*position.long_term_quantity)
        return portfolio_value

    def execute_spread_strategy(self, spread_items, short_term, date):
        """Executes the trades for the spread strategy.
            """
        
        for commodity, spreads in spread_items.items():
            if  spreads == [{}, {}]:
                if self.verbose:
                    logging.warning(f"Spread for {commodity} not available on {date}")
                continue
            if spreads[1] is None :
                if self.verbose:
                    logging.warning(f"ST Spread for {commodity} not available on {date}")
                continue
            if spreads[0] is None :
                if self.verbose:
                    logging.warning(f"LT Spread for {commodity} not available on {date}")
                continue

            self.update_pos(commodity, 1, 1, spreads[1], spreads[0], date)
            


@dataclass
class CommoBackTest:
    initial_date: datetime
    final_date: datetime
    commodity_pairs: dict
    cash: float = 1000000  # Initial cash in the portfolio
    verbose: bool = True
    backtest_name: str = ""
    broker = CommoBroker(cash)
    name_blockchain: str = 'backtest'
    



    def __post_init__(self):
        self.broker = CommoBroker(cash=self.cash, verbose=self.verbose)
        if self.backtest_name is None:
            self.backtest_name = generate_random_name()
        
        self.broker.initialize_blockchain(self.name_blockchain)

    def run_backtest(self):
        logging.info(f"Running backtest from {self.initial_date} to {self.final_date}.")
        data = get_commodities_data(self.commodity_pairs, self.initial_date.strftime('%Y-%m-%d'), self.final_date.strftime('%Y-%m-%d'))
    
        data_module = DataModule(data)
        strategy = SpreadStrategy(data_module=data_module)
        spread_data = strategy.compute_spread()
        data = strategy.set_up_dataframe()
        commo = ["CORN", "GAS", "OIL", "WHEAT"]
        last_date = None
        dico = {}
        for t in pd.date_range(start=self.initial_date, end=self.final_date, freq='B'):
            long_term_spreads = {commodity : [spread_data.loc[t.strftime('%Y-%m-%d')][commodity+ " - Long Term"] if t.strftime('%Y-%m-%d') in spread_data.index else {},
                                              spread_data.loc[t.strftime('%Y-%m-%d')][commodity+ " - Near Term"] if t.strftime('%Y-%m-%d') in spread_data.index else {}] for commodity in commo}
            if long_term_spreads != {'CORN': [{}, {}], 'GAS': [{}, {}], 'OIL': [{}, {}], 'WHEAT': [{}, {}]}:
                dico = long_term_spreads
            
            short_term_spreads = 1
            # short_term_spreads = {commodity : data.loc[t.strftime('%Y-%m-%d')][commodity+ " - Long Term"] if t.strftime('%Y-%m-%d') in spread_data.index else {} for commodity in commo}
            self.broker.execute_spread_strategy(long_term_spreads, short_term_spreads, t)

        logging.info(f"Backtest completed. Final portfolio value: {self.broker.get_portfolio_value(dico)}")
        logging.info("Transaction Log:")
        logging.info(self.broker.get_transaction_log())

        df = self.broker.get_transaction_log()

        # create backtests folder if it does not exist
        if not os.path.exists('backtests'):
            os.makedirs('backtests')


        # save to csv, use the backtest name 
        df.to_csv(f"backtests/{self.backtest_name}.csv")
        # store the backtest in the blockchain
        self.broker.blockchain.add_block(self.backtest_name, df.to_string())
    
