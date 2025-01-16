import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
from scipy.optimize import minimize
import logging
from dataclasses import dataclass

# Setup logging
logging.basicConfig(level=logging.INFO)

###########################
####### FIXED DATA #######
##########################

COMMODITY_TICKER_PAIRS = {
    "OIL": {"Near Term": "CL=F", "Long Term": "CLM24.NYM"},  # Crude Oil
    "GAS": {"Near Term": "NG=F", "Long Term": "NGM24.NYM"},  # Natural Gas
    "GOLD": {"Near Term": "GC=F", "Long Term": "GCM24.NYM"}, # Gold
    "SILVER": {"Near Term": "SI=F", "Long Term": "SIM24.NYM"}, # Silver
    "WHEAT": {"Near Term": "ZW=F", "Long Term": "ZWN24.CBT"}, # Wheat
    "CORN": {"Near Term": "ZC=F", "Long Term": "ZCN24.CBT"}  # Corn
}

##########################
####### FUNCTIONS #######
#########################

def get_commodity_data(ticker, start_date, end_date):
    """Retrieve historical data for a given commodity ticker."""
    try:
        data = yf.Ticker(ticker).history(start=start_date, end=end_date, auto_adjust=False, actions=False)
        if data.empty:
            logging.warning(f"No data found for ticker {ticker} from {start_date} to {end_date}.")
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df['ticker'] = ticker
        df.reset_index(inplace=True)
        return df
    except Exception as e:
        logging.warning(f"Error retrieving data for {ticker}: {e}")
        return pd.DataFrame()

def get_commodities_data(tickers, start_date, end_date):
    """Retrieve historical data for a list of commodity tickers."""
    dfs = []
    for name, ticker_info in tickers.items():
        # Case 1: Single ticker
        if isinstance(ticker_info, str):
            df = get_commodity_data(ticker_info, start_date, end_date)
            if not df.empty:
                df["Contract"] = name  # Add the commodity name as the contract
                dfs.append(df)
            else:
                logging.warning(f"No data retrieved for ticker {ticker_info} ({name}).")
        
        # Case 2: Ticker pairs (Near Term / Long Term)
        elif isinstance(ticker_info, dict):
            # Retrieve Near Term data
            near_term_ticker = ticker_info.get("Near Term")
            if near_term_ticker:
                near_term_df = get_commodity_data(near_term_ticker, start_date, end_date)
                if not near_term_df.empty:
                    near_term_df["Contract"] = f"{name} - Near Term"
                    dfs.append(near_term_df)
                else:
                    logging.warning(f"No data retrieved for Near Term ticker {near_term_ticker} ({name}).")
            
            # Retrieve Long Term data
            long_term_ticker = ticker_info.get("Long Term")
            if long_term_ticker:
                long_term_df = get_commodity_data(long_term_ticker, start_date, end_date)
                if not long_term_df.empty:
                    long_term_df["Contract"] = f"{name} - Long Term"
                    dfs.append(long_term_df)
                else:
                    logging.warning(f"No data retrieved for Long Term ticker {long_term_ticker} ({name}).")
        else:
            logging.warning(f"Invalid ticker format for {name}: {ticker_info}. Expected a string or a dictionary.")

    # Handle case where no data is retrieved
    if not dfs:
        logging.error(f"No valid data retrieved for any tickers. Returning an empty DataFrame.")
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)

##########################
####### CLASSES #######
#########################


@dataclass
class DataModule:
    data: pd.DataFrame

@dataclass
class SpreadStrategy:
    data_module: DataModule
    time_column: str = 'Date'
    price_column: str = 'Close'
    contract_column: str = 'Contract'

    def compute_spread(self):
        """Calculate the spread (near term - long term) over time."""
        data = self.data_module.data
        pivot_data = data.pivot(index=self.time_column, columns=self.contract_column, values=self.price_column)
        commo = ["CORN", "GAS", "OIL", "WHEAT"]
        for name in commo:
            pivot_data[name+ ' - Spread'] = pivot_data[name+ " - Near Term"] - pivot_data[name+" - Long Term"]
        return pivot_data.dropna()

    def set_up_dataframe(self):
        """Calculate the spread (near term - long term) over time."""
        data = self.data_module.data
        pivot_data = data.pivot(index=self.time_column, columns=self.contract_column, values=self.price_column)
        return pivot_data.dropna()

    def compute_statistics(self, spread_data):
        """Calculate required statistics for the strategy."""
        # Spread returns
        spread_data['Spread Return'] = spread_data['Spread'].pct_change()
        # Mean and standard deviation of spread returns
        mean_return = spread_data['Spread Return'].mean()
        std_dev = spread_data['Spread Return'].std()
        return mean_return, std_dev

    def optimize_spread(self, mean_return, std_dev, correlation, margin_limit=1_000_000):
        """Optimize weights for the spread strategy.
        NOT USED"""
        sigma_near = std_dev  # Proxy for near-term volatility
        sigma_long = std_dev * correlation  # Adjusted long-term volatility

        # Covariance matrix
        cov_matrix = np.array([
            [sigma_near**2, correlation * sigma_near * sigma_long],
            [correlation * sigma_near * sigma_long, sigma_long**2]
        ])

        # Objective function (minimize spread variance)
        def spread_variance(weights):
            return weights.T @ cov_matrix @ weights

        # Constraints
        constraints = [
            {'type': 'eq', 'fun': lambda w: w[0] + w[1]},  # Hedging: weights sum to 0
            {'type': 'ineq', 'fun': lambda w: margin_limit - abs(w[0]) - abs(w[1])}  # Margin limit
        ]

        # Initial guess (equal weights)
        x0 = np.array([0.5, -0.5])

        # Optimize
        result = minimize(spread_variance, x0, constraints=constraints)
        return result.x if result.success else None