import unittest
from backtest import BacktestStrategy


class TestBacktestStrategy(unittest.TestCase):

    def test_lezheboka(self):
        """
        Проверка правильной работы стратегии "Лежебока".
        :return:
        """
        bt = BacktestStrategy()
        date_start = '1999-12-31'
        date_finish = '2015-12-31'
        initial_balance = 100000
        assets_to_buy = ['DOBR', 'MURM']
        bt.set_dates(date_start, date_finish)
        self.assertEqual(bt.date_start.strftime(bt.DEFAULT_DATE_FORMAT), date_start)
        self.assertEqual(bt.date_finish.strftime(bt.DEFAULT_DATE_FORMAT), date_finish)
        bt.set_assets_for_strategy(assets_to_buy)
        self.assertEqual(bt.assets_for_strategy, assets_to_buy)
        bt.topup_cash_balance(initial_balance)
        self.assertEqual(bt.cash_balance, initial_balance)
        bt.backtest()

        expected_price = 540.08
        self.assertEqual(bt.historical_data[date_start]['DOBR']['price_per_item'], expected_price)
        self.assertEqual(
            bt.historical_data[date_start]['DOBR']['price_total'],
            expected_price * bt.historical_data[date_start]['DOBR']['quantity']
        )
        expected_price = 1818.18
        self.assertEqual(bt.historical_data[date_start]['MURM']['price_per_item'], expected_price)
        self.assertEqual(
            bt.historical_data[date_start]['MURM']['price_total'],
            expected_price * bt.historical_data[date_start]['MURM']['quantity']
        )

        expected_price = 7575.45
        self.assertEqual(bt.historical_data[date_finish]['DOBR']['price_per_item'], expected_price)
        self.assertEqual(
            bt.historical_data[date_finish]['DOBR']['price_total'],
            expected_price * bt.historical_data[date_finish]['DOBR']['quantity']
        )
        expected_price = 24912.61
        self.assertEqual(bt.historical_data[date_finish]['MURM']['price_per_item'], expected_price)
        self.assertEqual(
            bt.historical_data[date_finish]['MURM']['price_total'],
            expected_price * bt.historical_data[date_finish]['MURM']['quantity']
        )

        historical_dates = list(bt.historical_data)
        first_date, last_date = historical_dates[0], historical_dates[-1]
        self.assertEqual(date_start, first_date)
        self.assertEqual(date_finish, last_date)
        rebalance_data = dict(bt.historical_data[date_finish])
        del rebalance_data['all_assets_price']
        del rebalance_data['GOLD']
        bt.rebalancing(rebalance_data)

    def test_check_assets_distribution(self):
        bt = BacktestStrategy()
        cash_amount = 9
        asset_prices = {'A': 3, 'B': 3, 'C': 3}
        expected_distribution = {'A': 1, 'B': 1, 'C': 1}
        distributed_cash = bt.distribute_cash_for_assets(cash_amount, asset_prices)
        for ticker, volume in distributed_cash.items():
            self.assertEqual(distributed_cash[ticker], expected_distribution[ticker])

    def test_rebalancing(self):
        """
        Проверка того что ребалансировка портфеля правильно работает
        :return:
        """
        bt = BacktestStrategy()
        bt.current_date = '2024-08-02'
        bt.trade_data[bt.current_date] = {
            'A': 1, 'B': 1
        }
        bt.transactions_history = {'buy': {}, 'sell': {}, 'rebalancing': []}
        trade_data = {
            'A': {'current_price': 1, 'quantity': 12, 'price_total': 10, 'price_per_item': 1},
            'B': {'current_price': 1, 'quantity': 2, 'price_total': 2, 'price_per_item': 1},
        }
        for ticker, data in trade_data.items():
            bt.update_history(ticker, bt.current_date, data['current_price'], data['quantity'])

        bt.assets = bt.historical_data[bt.current_date]

        bt.rebalancing(bt.assets)
        bt.update_assets_balance_for_date(bt.current_date)
        expected_assets = {
            'A': {'price_total': 7, 'price_per_item': 1, 'quantity': 7, 'current_price': 1},
            'B': {'price_total': 7, 'price_per_item': 1, 'quantity': 7, 'current_price': 1},
        }
        self.assertEqual(bt.assets['A'], expected_assets['A'])
        self.assertEqual(bt.assets['B'], expected_assets['B'])

        expected_history = {
            'buy': {'2024-08-02': {'B': {'price': 1, 'quantity': 5}}},
            'sell': {'2024-08-02': {'A': {'price': 1, 'quantity': 5}}},
            'rebalancing': ['2024-08-02']
        }
        self.assertEqual(bt.transactions_history, expected_history)
