from datetime import datetime
import pandas as pd
from pybacktestchain.data_module import get_stocks_data 
from universal_backtest import UniversalBackTest

if __name__ == "__main__":
    # Parameters
    commo_equity = "COMMO"
    backtest = UniversalBackTest(initial_date=datetime(2023, 4, 1),
                                  final_date=datetime(2023, 12, 5),
                                    commo_equity=commo_equity)

    backtest.run_backtest()
