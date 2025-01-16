from flask import Flask, request, jsonify
from datetime import datetime
from universal_backtest import UniversalBackTest  

app = Flask(__name__)

@app.route('/run_backtest', methods=['POST'])


def run_backtest():
    try:
        data = request.json

        commo_equity = data.get('commo_equity', 'COMMO')
        if commo_equity not in ['COMMO', 'EQUITY']:
            return jsonify({"error": "Invalid value for commo_equity. Must be 'COMMO' or 'EQUITY'."}), 400

        initial_date = data.get('initial_date')
        final_date = data.get('final_date')
        try:
            initial_date = datetime.strptime(initial_date, '%Y-%m-%d')
            final_date = datetime.strptime(final_date, '%Y-%m-%d')
        except (ValueError, TypeError):
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400

        cash = data.get('cash', 1000000)  # Valeur par défaut de 1 000 000
        if not isinstance(cash, (int, float)) or cash <= 0:
            return jsonify({"error": "Cash must be a positive number."}), 400

        verbose = data.get('verbose', True)

        backtest = UniversalBackTest(
            initial_date=initial_date,
            final_date=final_date,
            commo_equity=commo_equity,
            cash=cash,
            verbose=verbose
        )

        backtest.run_backtest()

        return jsonify({"message": "Backtest completed successfully!", "backtest_name": backtest.backtest_name})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)
