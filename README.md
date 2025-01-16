# pybacktestchain_options

A backtesting package for equity and commodity.

On the equity side you can test all the strategies defined in pybacktestchain

On the commo side you can try the mean reversion play of the long term price on different commodities (OIL, GAS, WHEAT, CORN).

## Installation

```bash
$ pip install pybacktestchain_options
```

## Usage

You can either use the following code : 

```python

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

```

Or you can replace commo_equity with "EQUITY" and adjist the parameters as it is done in pybacktestchain.



The other way to use it is through an API call : 

- run the file api.py
- open a new terminal and copy the following command lines : 

```bash 

curl -X POST http://127.0.0.1:5000/run_backtest \
-H "Content-Type: application/json" \
-d '{"commo_equity": "COMMO", "initial_date": "2023-01-01", "final_date": "2023-12-31", "cash": 1000000, "verbose": true}'
```


## Contributing

Interested in contributing? Check out the contributing guidelines. Please note that this project is released with a Code of Conduct. By contributing to this project, you agree to abide by its terms.

## License

`pybacktestchain_options` was created by Guillaume Touly. It is licensed under the terms of the MIT license.

## Credits

`pybacktestchain_options` was created with [`cookiecutter`](https://cookiecutter.readthedocs.io/en/latest/) and the `py-pkgs-cookiecutter` [template](https://github.com/py-pkgs/py-pkgs-cookiecutter).
