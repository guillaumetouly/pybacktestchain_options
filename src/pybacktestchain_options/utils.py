import numpy as np
from scipy.stats import norm


class OptionUtils:
    """
    Utility functions for pricing vanilla options and computing Greeks.
    """

    @staticmethod
    def black_scholes_price(S, K, T, r, sigma, option_type="call"):
        """
        Calculate the Black-Scholes price of a vanilla option.

        :param S: Current stock price
        :param K: Strike price
        :param T: Time to maturity (in years)
        :param r: Risk-free interest rate
        :param sigma: Volatility of the underlying asset
        :param option_type: "call" or "put"
        :return: Option price
        """
        d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)

        if option_type == "call":
            return S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
        elif option_type == "put":
            return K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
        else:
            raise ValueError("option_type must be 'call' or 'put'")

    @staticmethod
    def delta(S, K, T, r, sigma, option_type="call"):
        """
        Calculate the Delta of a vanilla option.

        :param S: Current stock price
        :param K: Strike price
        :param T: Time to maturity
        :param r: Risk-free rate
        :param sigma: Volatility
        :param option_type: "call" or "put"
        :return: Delta value
        """
        d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
        if option_type == "call":
            return norm.cdf(d1)
        elif option_type == "put":
            return norm.cdf(d1) - 1
        else:
            raise ValueError("option_type must be 'call' or 'put'")

    @staticmethod
    def gamma(S, K, T, r, sigma):
        """
        Calculate the Gamma of a vanilla option.

        :param S: Current stock price
        :param K: Strike price
        :param T: Time to maturity
        :param r: Risk-free rate
        :param sigma: Volatility
        :return: Gamma value
        """
        d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
        return norm.pdf(d1) / (S * sigma * np.sqrt(T))

    @staticmethod
    def vega(S, K, T, r, sigma):
        """
        Calculate the Vega of a vanilla option.

        :param S: Current stock price
        :param K: Strike price
        :param T: Time to maturity
        :param r: Risk-free rate
        :param sigma: Volatility
        :return: Vega value (per 1% volatility change)
        """
        d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
        return S * norm.pdf(d1) * np.sqrt(T) / 100

    @staticmethod
    def theta(S, K, T, r, sigma, option_type="call"):
        """
        Calculate the Theta of a vanilla option.

        :param S: Current stock price
        :param K: Strike price
        :param T: Time to maturity
        :param r: Risk-free rate
        :param sigma: Volatility
        :param option_type: "call" or "put"
        :return: Theta value (per day)
        """
        d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)

        if option_type == "call":
            theta = (
                -S * norm.pdf(d1) * sigma / (2 * np.sqrt(T))
                - r * K * np.exp(-r * T) * norm.cdf(d2)
            )
        elif option_type == "put":
            theta = (
                -S * norm.pdf(d1) * sigma / (2 * np.sqrt(T))
                + r * K * np.exp(-r * T) * norm.cdf(-d2)
            )
        else:
            raise ValueError("option_type must be 'call' or 'put'")
        return theta / 365  # Convert to per-day value
