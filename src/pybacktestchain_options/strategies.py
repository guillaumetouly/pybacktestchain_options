from utils import OptionUtils


class OptionStrategies:
    """
    Defines option trading strategies.
    """

    @staticmethod
    def delta_hedged_strategy(S, K, T, r, sigma, option_type="call"):
        """
        Delta-hedged strategy: Adjust positions to maintain a neutral delta.
        """
        delta = OptionUtils.delta(S, K, T, r, sigma, option_type)
        return f"Delta hedge requires offsetting {delta:.2f} delta with stock."

    @staticmethod
    def gamma_positive_strategy(S, K, T, r, sigma):
        """
        Gamma-positive strategy: Buy straddles to benefit from large moves.
        """
        call_price = OptionUtils.black_scholes_price(S, K, T, r, sigma, "call")
        put_price = OptionUtils.black_scholes_price(S, K, T, r, sigma, "put")
        return f"Buy 1 call and 1 put at strike {K}: Call={call_price:.2f}, Put={put_price:.2f}"

    @staticmethod
    def volatility_play_strategy(S, K, T, r, sigma):
        """
        Play volatility changes with high Vega options.
        """
        vega = OptionUtils.vega(S, K, T, r, sigma)
        return f"Trade options with high Vega: Vega={vega:.2f}."
