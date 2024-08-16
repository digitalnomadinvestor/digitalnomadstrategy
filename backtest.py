import csv
import datetime


class BacktestStrategy:

    cash_balance = 0.0
    stop_losses = dict()
    date_start = None
    strategy_date_start = None
    date_finish = None
    strategy_date_finish = None
    current_date: str = None
    rebalance_enabled: bool = False
    rebalance_dates = []

    trade_data = dict()
    assets_for_strategy = []
    key_trade_dates = dict()
    historical_data = dict()
    transactions_history = {'buy': {}, 'sell': {}, 'rebalancing': []}
    DEFAULT_DATE_FORMAT = '%Y-%m-%d'

    assets = {
        'LQDT': {
            'csv_file': 'data/BBG00RPRPX12.csv', 'current_price': 0, 'stop_loss_price': None, 'stop_loss_percent': None,
            'quantity': 0
        },
        'GOLD': {
            'csv_file': 'data/gold.csv', 'current_price': 0, 'stop_loss_price': None, 'stop_loss_percent': None,
            'quantity': 0
        },
        # паи фонда российских акций УК "Тройка Диалог" (тикер выдуманный)
        'DOBR': {
            'csv_file': 'data/RU000A0EQ3R3.csv', 'current_price': 0, 'stop_loss_price': None, 'stop_loss_percent': None,
            'quantity': 0
        },
        # паи фонда облигаций "Илья Муромец" УК "Тройка Диалог" (тикер выдуманный)
        'MURM': {
            'csv_file': 'data/RU000A0EQ3Q5.csv', 'current_price': 0, 'stop_loss_price': None, 'stop_loss_percent': None,
            'quantity': 0
        },
        # выдуманный фонд - аналог LQDT который как будто бы существовал до создания LQDT
        'CBRT': {
            'csv_file': None, 'current_price': 0, 'stop_loss_price': None, 'stop_loss_percent': None,
            'quantity': 0
        }
    }

    def set_dates(self, date_start: str, date_finish: str) -> None:
        self.date_start = datetime.datetime.strptime(date_start, self.DEFAULT_DATE_FORMAT)
        self.date_finish = datetime.datetime.strptime(date_finish, self.DEFAULT_DATE_FORMAT)

    def load_trade_dates(self) -> None:
        with open('data/RU000A0EQ3R3.csv', 'r') as f:
            reader = csv.reader(f, dialect='excel')
            for row in reader:
                self.trade_data[row[0]] = {}
        self.fill_key_trade_dates()

    def get_closest_date(self, date: datetime.datetime, dates_list: list, date_type: str = 'min') -> str:
        return min(dates_list, key=lambda x: abs(x - date)).strftime(self.DEFAULT_DATE_FORMAT) if date_type == 'min' \
            else max(dates_list, key=lambda x: abs(x - date)).strftime(self.DEFAULT_DATE_FORMAT)

    def fill_key_trade_dates(self) -> None:
        quarter_months = [3, 6, 9, 12]
        self.key_trade_dates = dict()
        start_month_year, finish_month_year = self.date_start.strftime('%Y-%m'), self.date_finish.strftime('%Y-%m')
        start_end_data = {}

        for i, date in enumerate(self.trade_data):
            dt = datetime.datetime.strptime(date, self.DEFAULT_DATE_FORMAT)
            current_year = dt.year
            current_month = dt.month
            current_ym = f'{current_year}-{dt.strftime("%m")}'
            if current_ym == start_month_year:
                if 'start_dates' not in start_end_data:
                    start_end_data['start_dates'] = []
                start_end_data['start_dates'].append(dt)
            if current_ym == finish_month_year:
                if 'end_dates' not in start_end_data:
                    start_end_data['end_dates'] = []
                start_end_data['end_dates'].append(dt)
            if current_year not in self.key_trade_dates:
                self.key_trade_dates[current_year] = {}
            if 'all_dates' not in self.key_trade_dates[current_year]:
                self.key_trade_dates[current_year]['all_dates'] = []
            self.key_trade_dates[current_year]['all_dates'].append(dt)
            if 'all_quarters' not in self.key_trade_dates[current_year]:
                self.key_trade_dates[current_year]['all_quarters'] = {3: [], 6: [], 9: [], 12: []}
            if current_month in quarter_months:
                self.key_trade_dates[current_year]['all_quarters'][current_month].append(dt)
        for i, current_year in enumerate(self.key_trade_dates):
            max_trade_date = max(self.key_trade_dates[current_year]['all_dates']).strftime(self.DEFAULT_DATE_FORMAT)
            self.key_trade_dates[current_year]['max_date'] = max_trade_date
            for k, quarter in enumerate(self.key_trade_dates[current_year]['all_quarters']):
                if len(self.key_trade_dates[current_year]['all_quarters'][quarter]) < 1:
                    continue
                max_q_date = max(self.key_trade_dates[current_year]['all_quarters'][quarter]).strftime(
                    self.DEFAULT_DATE_FORMAT
                )
                self.key_trade_dates[current_year]['all_quarters'][quarter] = max_q_date
            del self.key_trade_dates[current_year]['all_dates']
        self.strategy_date_start = self.get_closest_date(self.date_start, start_end_data['start_dates'], 'min')
        self.strategy_date_finish = self.get_closest_date(self.date_finish, start_end_data['end_dates'], 'max')
        del start_end_data

    def load_asset_prices(self) -> None:
        self.load_trade_dates()
        tickers_to_ignore = ['CBRT', 'LQDT']
        all_tickers = []
        for k, ticker in enumerate(self.assets):
            if ticker in tickers_to_ignore:
                continue
            all_tickers.append(ticker)
            csv_filename = self.assets[ticker]['csv_file']
            with open(csv_filename, 'r') as f:
                reader = csv.reader(f, dialect='excel')
                for row in reader:
                    date, price = row[0], float(row[1])
                    if date in self.trade_data:
                        self.trade_data[date][ticker] = price

        # для некоторых дат у GOLD информации о цене, заполняем их ценой из прошлого
        for k, date in enumerate(self.trade_data):
            for ticker in all_tickers:
                if ticker not in self.trade_data[date]:
                    previous_price = None
                    price_backdate_limit = 30
                    current_price_backdate = 0
                    previous_date = date
                    while previous_price is None:
                        current_price_backdate += 1
                        if current_price_backdate >= price_backdate_limit:
                            break
                        if type(previous_date) != datetime.datetime:
                            previous_date = datetime.datetime.strptime(previous_date, self.DEFAULT_DATE_FORMAT)
                        previous_date = (previous_date - datetime.timedelta(days=1))
                        formatted_prev_date = previous_date.strftime('%Y-%m-%d')
                        if formatted_prev_date in self.trade_data:
                            if ticker in self.trade_data[formatted_prev_date]:
                                price = self.trade_data[formatted_prev_date][ticker]
                                self.trade_data[date][ticker] = price
                                break

    def buy_asset(self, asset_ticker: str, quantity: int, price: float) -> None:
        return self.buy_or_sell_asset('buy', asset_ticker, quantity, price)

    def sell_asset(self, asset_ticker: str, quantity: int, price: float) -> None:
        return self.buy_or_sell_asset('sell', asset_ticker, quantity, price)

    def buy_or_sell_asset(self, transaction_type: str, asset_ticker: str, quantity: int, price: float) -> None:
        self.assets[asset_ticker]['current_price'] = price
        if transaction_type not in ['sell', 'buy']:
            raise Exception(f'Invalid transaction type: {transaction_type}')
        if transaction_type == 'sell':
            self.assets[asset_ticker]['quantity'] -= quantity
            self.cash_balance += quantity * price
        elif transaction_type == 'buy':
            self.assets[asset_ticker]['quantity'] += quantity
            self.cash_balance -= quantity * price
        if self.current_date not in self.transactions_history[transaction_type]:
            self.transactions_history[transaction_type] = {self.current_date: {}}
        self.transactions_history[transaction_type][self.current_date][asset_ticker] = {
            'price': price, 'quantity': quantity
        }
        self.assets[asset_ticker]['price_total'] = price * self.assets[asset_ticker]['quantity']

    def distribute_cash_for_assets(self, cash_amount: float, asset_prices: dict) -> dict:
        """
        Распределяет наличные для равномерной закупки активов в примерно равных частях
        :param cash_amount:
        :param asset_prices:
        :return:
        """
        num_assets = len(asset_prices)
        equal_amount = cash_amount / num_assets

        assets_to_buy = {ticker: 0 for ticker in asset_prices}
        # Определяем количество акций, которые можно купить на выделенную сумму
        for ticker, price in asset_prices.items():
            assets_to_buy[ticker] = int(equal_amount // price)
        # Вычисляем оставшиеся деньги
        remaining_cash = cash_amount - sum(assets_to_buy[ticker] * asset_prices[ticker] for ticker in asset_prices)
        # Распределяем оставшие деньги
        for ticker, price in asset_prices.items():
            while remaining_cash >= price:
                assets_to_buy[ticker] += 1
                remaining_cash -= price
        return assets_to_buy

    def topup_cash_balance(self, amount: float):
        self.cash_balance += amount

    def update_history(self, ticker: str, date: str, price: float, qnty: int) -> None:
        if date not in self.historical_data:
            self.historical_data[date] = {}
        self.historical_data[date][ticker] = {'price_total': price * qnty, 'price_per_item': price, 'quantity': qnty}

    def update_assets_balance_for_date(self, date) -> None:
        total = 0.0
        for ticker, price_data in self.historical_data[date].items():
            total += price_data['price_total']
        self.historical_data[date]['all_assets_price'] = total
        self.cash_balance = total

    def get_asset_price_for_date(self, asset_ticker: str, date: str) -> float:
        return self.trade_data[date][asset_ticker]

    def make_initial_purchase(self, date: str) -> None:
        asset_prices = {}
        for asset_ticker in self.assets_for_strategy:
            current_asset_price = self.get_asset_price_for_date(asset_ticker, self.strategy_date_start)
            asset_prices[asset_ticker] = current_asset_price
        cash_allocation = self.distribute_cash_for_assets(self.cash_balance, asset_prices)
        for ticker, qnty in cash_allocation.items():
            price = asset_prices[ticker]
            self.buy_asset(ticker, qnty, price)
            self.update_history(ticker, date, price, qnty)

    def set_assets_for_strategy(self, tickers: list) -> None:
        self.assets_for_strategy = tickers

    def rebalancing(self, assets_data: dict) -> None:
        if self.current_date not in self.transactions_history['rebalancing']:
            self.transactions_history['rebalancing'].append(self.current_date)
        total_price = sum(info['price_total'] for info in assets_data.values())
        equal_price_total = total_price / len(assets_data)

        for ticker, info in assets_data.items():
            current_price_total = info['price_total']
            difference = equal_price_total - current_price_total

            quantity_to_buy_or_sell = int(abs(difference) // info['price_per_item'])
            if difference > 0:
                self.buy_asset(ticker, quantity_to_buy_or_sell, info['price_per_item'])
            else:
                self.sell_asset(ticker, quantity_to_buy_or_sell, info['price_per_item'])

    def start_backtest(self) -> None:
        self.current_date = self.date_start.strftime(self.DEFAULT_DATE_FORMAT)
        self.load_trade_dates()
        self.load_asset_prices()
        self.make_initial_purchase(self.date_start.strftime(self.DEFAULT_DATE_FORMAT))

    def backtest(self) -> None:
        self.start_backtest()
        allow_calculation = False
        for date, ticker_data in self.trade_data.items():
            self.current_date = date
            # уже посчитали в start_backtest, пропускаем первую дату
            if date == self.date_start.strftime(self.DEFAULT_DATE_FORMAT):
                allow_calculation = True
                continue
            if not allow_calculation:
                continue
            if date == self.date_finish.strftime(self.DEFAULT_DATE_FORMAT):
                allow_calculation = False

            for ticker, price in ticker_data.items():
                ticker_quantity = self.assets[ticker]['quantity']
                self.update_history(ticker, date, price, ticker_quantity)
            self.update_assets_balance_for_date(date)



# date_start = '2000-01-05'
# date_finish = '2024-08-02'
# initial_balance = 10000
# assets_to_buy = ['GOLD', 'DOBR', 'MURM']

# bt = BacktestStrategy()
# bt.set_dates(date_start, date_finish)
# bt.set_assets_for_strategy(assets_to_buy)
# bt.topup_cash_balance(initial_balance)
# print(bt.cash_balance)
# bt.backtest()
# print(bt.cash_balance)
# print(bt.historical_data)
# print(bt.assets)
# bt.load_asset_prices()
# print(bt.trade_data)
# print(bt.key_trade_dates)
# print(bt.strategy_date_start, bt.strategy_date_finish)