from blockchain import add_reward, withdraw_reward
from pybacktestchain.data_module import get_stocks_data
from strategies import OptionStrategies
import pandas as pd
from datetime import datetime


class OptionBroker:
    """
    Backtesting framework for options strategies, integrated with blockchain rewards.
    """

    def __init__(self, ticker, strike, start_date, end_date, risk_free_rate, volatility):
        """
        Initialize the broker with real stock data and strategy parameters.

        :param ticker: Stock ticker symbol.
        :param strike: Strike price for the option.
        :param start_date: Start date for historical stock data.
        :param end_date: End date for historical stock data.
        :param risk_free_rate: Risk-free interest rate.
        :param volatility: Implied volatility.
        """
        self.ticker = ticker
        self.strike = strike
        self.start_date = start_date
        self.end_date = end_date
        self.risk_free_rate = risk_free_rate
        self.volatility = volatility
        self.stock_data = self._get_historical_stock_data()
        self.stock_data["T"] = self._calculate_time_to_maturity()

    def _get_historical_stock_data(self):
        """
        Retrieve historical stock data using the data retriever module.
        :return: DataFrame with stock prices.
        """
        stock_data = get_stocks_data([self.ticker], self.start_date, self.end_date)
        if stock_data.empty:
            raise ValueError(f"No data retrieved for ticker {self.ticker}.")
        
        # Use 'Adj Close' for adjusted close prices
        stock_data.rename(columns={"Adj Close": "Adj Close"}, inplace=True)
        stock_data.index = pd.to_datetime(stock_data.index)  # Ensure datetime index
        return stock_data

    def _calculate_time_to_maturity(self):
        """
        Calculate the time-to-maturity (in years) for each trading day.
        :return: Series with time-to-maturity values.
        """
        end_date_obj = pd.to_datetime(self.end_date)
        return (end_date_obj - self.stock_data.index).days / 365.0

    def run_strategy(self, strategy_name):
        """
        Run a specific options strategy and compute results.

        :param strategy_name: Name of the strategy to run.
        :return: DataFrame with strategy results.
        """
        if strategy_name == "delta_hedged":
            self.stock_data["Delta"] = self.stock_data.apply(
                lambda row: OptionStrategies.delta_hedged_strategy(
                    S=row["Adj Close"],
                    K=self.strike,
                    T=row["T"],
                    r=self.risk_free_rate,
                    sigma=self.volatility,
                    option_type="call",
                ),
                axis=1,
            )
        elif strategy_name == "gamma_positive":
            self.stock_data["GammaPositive"] = self.stock_data.apply(
                lambda row: OptionStrategies.gamma_positive_strategy(
                    S=row["Adj Close"],
                    K=self.strike,
                    T=row["T"],
                    r=self.risk_free_rate,
                    sigma=self.volatility,
                ),
                axis=1,
            )
        elif strategy_name == "volatility_play":
            self.stock_data["VolatilityPlay"] = self.stock_data.apply(
                lambda row: OptionStrategies.volatility_play_strategy(
                    S=row["Adj Close"],
                    K=self.strike,
                    T=row["T"],
                    r=self.risk_free_rate,
                    sigma=self.volatility,
                ),
                axis=1,
            )
        else:
            raise ValueError(f"Unknown strategy: {strategy_name}")

        return self.stock_data

    def reward_contributor(self, contributor_address, reward_amount, owner_address):
        """
        Reward a contributor for a profitable strategy via blockchain.

        :param contributor_address: Ethereum address of the contributor.
        :param reward_amount: Reward amount in Ether.
        :param owner_address: Ethereum address of the contract owner.
        """
        add_reward(contributor_address, reward_amount, owner_address)

    def withdraw_rewards(self, contributor_address):
        """
        Allow contributors to withdraw their rewards from the blockchain.

        :param contributor_address: Ethereum address of the contributor.
        """
        withdraw_reward(contributor_address)
