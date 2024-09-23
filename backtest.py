import csv
import pandas as pd
import matplotlib.pyplot as plt
import datetime


class BacktestStrategy:

    cash_balance = 0.0
    stop_losses = dict()
    stop_losses_enabled = False
    date_start = None
    strategy_date_start = None
    date_finish = None
    strategy_date_finish = None
    current_date: str = None
    rebalance_mode: str = None
    rebalance_enabled: bool = False
    rebalance_dates = []

    trade_data = dict()
    assets_for_strategy = []
    key_trade_dates = dict()
    historical_data = dict()
    trade_days_yearly = dict()
    trade_dates = []
    transactions_history = {"buy": {}, "sell": {}, "rebalancing": []}
    DEFAULT_DATE_FORMAT = "%Y-%m-%d"
    DEFAULT_STOP_LOSS_PERCENTAGE = 0.1
    EXPORT_FILENAME = 'export_data.csv'

    assets = {
        "LQDT": {
            "csv_file": "data/BBG00RPRPX12.csv",
            "current_price": 0,
            "stop_loss_price": None,
            "stop_loss_percent": None,
            "quantity": 0,
        },
        "GOLD": {
            "csv_file": "data/gold.csv",
            "current_price": 0,
            "stop_loss_price": None,
            "stop_loss_percent": None,
            "quantity": 0,
        },
        # паи фонда российских акций УК "Тройка Диалог" (тикер выдуманный)
        "DOBR": {
            "csv_file": "data/RU000A0EQ3R3.csv",
            "current_price": 0,
            "stop_loss_price": None,
            "stop_loss_percent": None,
            "quantity": 0,
        },
        # паи фонда облигаций "Илья Муромец" УК "Тройка Диалог" (тикер выдуманный)
        "MURM": {
            "csv_file": "data/RU000A0EQ3Q5.csv",
            "current_price": 0,
            "stop_loss_price": None,
            "stop_loss_percent": None,
            "quantity": 0,
        },
        # Выдуманный фонд - аналог LQDT который как будто бы существовал до создания LQDT.
        # Доходность берем как ставку рефинансирования ЦБ - 3%, делим ее на количество дней когда были сделки на бирже,
        # таким образом получаем изменение в процентах за день, на основании этого расчитывается
        # цена (изначально = 100 руб.)
        "CBRT": {
            "csv_file": "data/cbrt_history.csv",
            "current_price": 0,
            "stop_loss_price": None,
            "stop_loss_percent": None,
            "quantity": 0,
        },
    }

    def set_dates(self, date_start: str, date_finish: str) -> None:
        self.date_start = datetime.datetime.strptime(date_start, self.DEFAULT_DATE_FORMAT)
        self.date_finish = datetime.datetime.strptime(date_finish, self.DEFAULT_DATE_FORMAT)

    def load_trade_dates(self) -> None:
        for filename in ["data/RU000A0EQ3R3.csv", "data/RU000A0EQ3Q5.csv"]:
            with open(filename, "r") as f:
                reader = csv.reader(f, dialect="excel")
                for row in reader:
                    date = row[0]
                    if date not in self.trade_data:
                        self.trade_data[date] = {}

        # Считаем количество дней когда были торги на бирже по каждому году
        trade_dates = sorted(list(set([trade_date for trade_date, dt in self.trade_data.items()])))
        for trade_date in trade_dates:
            trade_year = datetime.datetime.strptime(trade_date, self.DEFAULT_DATE_FORMAT).strftime('%Y')
            if trade_year not in self.trade_days_yearly:
                self.trade_days_yearly[trade_year] = 0
            self.trade_days_yearly[trade_year] += 1
        self.fill_key_trade_dates()

    def get_closest_date(self, date: datetime.datetime, dates_list: list, date_type: str = "min") -> str:
        return (
            min(dates_list, key=lambda x: abs(x - date)).strftime(self.DEFAULT_DATE_FORMAT)
            if date_type == "min"
            else max(dates_list, key=lambda x: abs(x - date)).strftime(self.DEFAULT_DATE_FORMAT)
        )

    def fill_key_trade_dates(self) -> None:
        quarter_months = [3, 6, 9, 12]
        self.key_trade_dates = dict()
        start_month_year, finish_month_year = self.date_start.strftime("%Y-%m"), self.date_finish.strftime("%Y-%m")
        start_end_data = {}

        for i, date in enumerate(self.trade_data):
            dt = datetime.datetime.strptime(date, self.DEFAULT_DATE_FORMAT)
            current_year = dt.year
            current_month = dt.month
            current_ym = f'{current_year}-{dt.strftime("%m")}'
            if current_ym == start_month_year:
                if "start_dates" not in start_end_data:
                    start_end_data["start_dates"] = []
                start_end_data["start_dates"].append(dt)
            if current_ym == finish_month_year:
                if "end_dates" not in start_end_data:
                    start_end_data["end_dates"] = []
                start_end_data["end_dates"].append(dt)
            if current_year not in self.key_trade_dates:
                self.key_trade_dates[current_year] = {}
            if "all_dates" not in self.key_trade_dates[current_year]:
                self.key_trade_dates[current_year]["all_dates"] = []
            self.key_trade_dates[current_year]["all_dates"].append(dt)
            if "all_quarters" not in self.key_trade_dates[current_year]:
                self.key_trade_dates[current_year]["all_quarters"] = {3: [], 6: [], 9: [], 12: []}
            if current_month in quarter_months:
                self.key_trade_dates[current_year]["all_quarters"][current_month].append(dt)
        for i, current_year in enumerate(self.key_trade_dates):
            max_trade_date = max(self.key_trade_dates[current_year]["all_dates"]).strftime(self.DEFAULT_DATE_FORMAT)
            min_trade_date = min(self.key_trade_dates[current_year]["all_dates"]).strftime(self.DEFAULT_DATE_FORMAT)
            if self.rebalance_enabled and self.rebalance_mode == 'yearly':
                self.rebalance_dates.append(max_trade_date)
            self.key_trade_dates[current_year]["min_date"] = min_trade_date
            self.key_trade_dates[current_year]["max_date"] = max_trade_date
            for k, quarter in enumerate(self.key_trade_dates[current_year]["all_quarters"]):
                if len(self.key_trade_dates[current_year]["all_quarters"][quarter]) < 1:
                    continue
                max_q_date = max(self.key_trade_dates[current_year]["all_quarters"][quarter]).date()
                min_q_date = min(self.key_trade_dates[current_year]["all_quarters"][quarter]).date()
                self.key_trade_dates[current_year]["all_quarters"][quarter] = {
                    "max": max_q_date, "min": min_q_date
                }
                if self.rebalance_enabled and self.rebalance_mode == 'quarterly':
                    self.rebalance_dates.append(max_q_date)
            del self.key_trade_dates[current_year]["all_dates"]
        self.strategy_date_start = self.get_closest_date(self.date_start, start_end_data["start_dates"], "min")
        self.strategy_date_finish = self.get_closest_date(self.date_finish, start_end_data["end_dates"], "max")
        del start_end_data

    def load_asset_prices(self) -> None:
        self.load_trade_dates()
        tickers_to_ignore = []
        all_tickers = []
        for k, ticker in enumerate(self.assets):
            if ticker in tickers_to_ignore:
                continue
            all_tickers.append(ticker)
            csv_filename = self.assets[ticker]["csv_file"]
            with open(csv_filename, "r") as f:
                reader = csv.reader(f, dialect="excel")
                for row in reader:
                    date, price = row[0], float(row[1])
                    if date in self.trade_data:
                        self.trade_data[date][ticker] = price

        # для некоторых дат у GOLD информации о цене, заполняем их ценой из прошлого
        for k, date in enumerate(self.trade_data):
            for ticker in all_tickers:
                if ticker not in self.trade_data[date]:
                    previous_price = None
                    price_backdate_limit = 60
                    current_price_backdate = 0
                    previous_date = date
                    while previous_price is None:
                        current_price_backdate += 1
                        if current_price_backdate >= price_backdate_limit:
                            break
                        if not isinstance(previous_date, datetime.datetime):
                            previous_date = datetime.datetime.strptime(previous_date, self.DEFAULT_DATE_FORMAT)
                        previous_date = previous_date - datetime.timedelta(days=1)
                        formatted_prev_date = previous_date.strftime("%Y-%m-%d")
                        if formatted_prev_date in self.trade_data:
                            if ticker in self.trade_data[formatted_prev_date]:
                                price = self.trade_data[formatted_prev_date][ticker]
                                self.trade_data[date][ticker] = price
                                break
        self.trade_dates = [datetime.datetime.strptime(dt, self.DEFAULT_DATE_FORMAT) for dt in self.trade_data.keys()]

    def get_asset_stop_loss(self, asset: str, current_date: str) -> float:
        if asset not in self.stop_losses:
            self.stop_losses[asset] = {}
        dt_current = datetime.datetime.strptime(current_date, self.DEFAULT_DATE_FORMAT)
        current_year = dt_current.strftime('%Y')
        if current_year not in self.stop_losses[asset]:
            self.stop_losses[asset][current_year] = {1: None, 2: None, 3: None, 4: None}
        quarter = pd.Timestamp(dt_current.date()).quarter

        stop_loss_price = self.stop_losses[asset][current_year][quarter]
        if stop_loss_price:
            return stop_loss_price
        current_price = self.trade_data[current_date][asset]
        price_diff = current_price * self.DEFAULT_STOP_LOSS_PERCENTAGE
        stop_loss_price = current_price - price_diff
        self.stop_losses[asset][current_year][quarter] = stop_loss_price
        return stop_loss_price

    def buy_asset(self, asset_ticker: str, quantity: int, price: float) -> None:
        return self.buy_or_sell_asset("buy", asset_ticker, quantity, price)

    def sell_asset(self, asset_ticker: str, quantity: int, price: float) -> None:
        return self.buy_or_sell_asset("sell", asset_ticker, quantity, price)

    def buy_or_sell_asset(self, transaction_type: str, asset_ticker: str, quantity: int, price: float) -> None:
        print(f'buy_or_sell_asset: {transaction_type}\t{asset_ticker}: {quantity} * {price}')
        self.assets[asset_ticker]["current_price"] = price
        if transaction_type not in ["sell", "buy"]:
            raise Exception(f"Invalid transaction type: {transaction_type}")
        if transaction_type == "sell":
            self.assets[asset_ticker]["quantity"] -= quantity
            self.cash_balance += quantity * price
        elif transaction_type == "buy":
            final_balance = self.cash_balance - (quantity * price)
            if final_balance < 0:
                raise Exception('Cash balance can not be less than 0')
            self.assets[asset_ticker]["quantity"] += quantity
            self.cash_balance -= quantity * price
        if self.current_date not in self.transactions_history[transaction_type]:
            self.transactions_history[transaction_type][self.current_date] = {}
        if asset_ticker not in self.transactions_history[transaction_type][self.current_date]:
            self.transactions_history[transaction_type][self.current_date][asset_ticker] = {}

        self.transactions_history[transaction_type][self.current_date][asset_ticker] = {
            "price": price,
            "quantity": quantity,
        }
        self.assets[asset_ticker]["price_total"] = price * self.assets[asset_ticker]["quantity"]
        self.update_history(asset_ticker, self.current_date, price, quantity)

    def distribute_cash_for_assets(self, cash_amount: float, asset_prices: dict) -> dict:
        """
        Распределяет наличные для равномерной закупки активов в примерно равных частях
        """
        num_assets = len(asset_prices)
        cash_per_asset = cash_amount / num_assets
        distribution = {ticker: int(cash_per_asset // price) for ticker, price in asset_prices.items()}
        return distribution

    def topup_cash_balance(self, amount: float):
        self.cash_balance += amount

    def update_history(self, ticker: str, date: str, price: float, qnty: int) -> None:
        if date not in self.historical_data:
            self.historical_data[date] = {}
        self.historical_data[date][ticker] = {"price_total": price * qnty, "price_per_item": price, "quantity": qnty}

    def update_assets_balance_for_date(self, date) -> None:
        total = 0.0
        for ticker, price_data in self.historical_data[date].items():
            if ticker in self.assets_for_strategy:
                total += price_data["price_total"]
        self.historical_data[date]["all_assets_price"] = total

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
        # Вычисляем общее количество акций
        print('----> rebalancing')
        if self.current_date not in self.transactions_history["rebalancing"]:
            self.transactions_history["rebalancing"].append(self.current_date)
        assets_data_copy = dict(assets_data)
        if 'all_assets_price' in assets_data:
            del assets_data_copy['all_assets_price']
        for asset_ticker, a_data in assets_data_copy.items():
            if asset_ticker not in self.assets_for_strategy:
                del assets_data_copy[asset_ticker]
        total_value = self.cash_balance + sum(data['price_per_item'] * data['quantity'] for data in assets_data_copy.values())

        # Определяем идеальное распределение стоимости на каждую акцию
        equal_value_per_stock = total_value / len(assets_data_copy)

        # Определяем идеальное количество акций для каждого тикера
        ideal_quantities = {}
        for ticker, data in assets_data_copy.items():
            price_per_item = data['price_per_item']
            ideal_quantity = equal_value_per_stock // price_per_item
            ideal_quantities[ticker] = ideal_quantity

        # Сначала продаем излишек акций
        for ticker, data in assets_data_copy.items():
            current_quantity = data['quantity']
            price_per_item = data['price_per_item']
            ideal_quantity = ideal_quantities[ticker]

            if current_quantity > ideal_quantity:
                quantity_to_sell = current_quantity - ideal_quantity
                self.sell_asset(ticker, quantity_to_sell, price_per_item)

        # Затем покупаем недостающие акции
        for ticker, data in assets_data_copy.items():
            current_quantity = data['quantity']
            price_per_item = data['price_per_item']
            ideal_quantity = ideal_quantities[ticker]

            if current_quantity < ideal_quantity:
                quantity_to_buy = ideal_quantity - current_quantity
                cost = quantity_to_buy * price_per_item

                if self.cash_balance >= cost:
                    self.buy_asset(ticker, quantity_to_buy, price_per_item)
                else:
                    # Покупаем на весь оставшийся баланс
                    quantity_to_buy = self.cash_balance // price_per_item
                    if quantity_to_buy > 0:
                        self.buy_asset(ticker, quantity_to_buy, price_per_item)
        # Если на балансе что-то осталось и используется аналог LQDT, покупаем его на оставшийся кэш
        if self.cash_balance > 0 and 'CBRT' in self.assets_for_strategy:
            price_per_item = assets_data['CBRT']['price_per_item']
            quantity_to_buy = self.cash_balance // price_per_item
            if quantity_to_buy > 0:
                self.buy_asset('CBRT', quantity_to_buy, price_per_item)

    def set_rebalance_mode(self, mode: str) -> None:
        valid_modes = ['quarterly', 'yearly']
        if mode not in valid_modes:
            self.rebalance_mode = 'yearly'
        else:
            self.rebalance_mode = mode

    def draw_charts(self):
        asset_days = {}
        asset_prices = {}
        for asset in self.assets_for_strategy:
            asset_days[asset] = []
            asset_prices[asset] = []
        for date, asset_data in self.historical_data.items():
            for ticker in self.assets_for_strategy:
                asset_days[ticker].append(date)
                price = asset_data[ticker]['price_total']
                asset_prices[ticker].append(price)
        df = pd.DataFrame({
            'stock': self.assets_for_strategy,
            'price': [asset_prices[token] for token, price_data in asset_prices.items()],
            'date': [asset_days[token] for token, date_data in asset_days.items()],
        })
        df = df.set_index(['stock']).apply(pd.Series.explode).reset_index()
        # TODO дописать тут рисование графиков

    def export_history(self):
        with open(self.EXPORT_FILENAME, 'w') as f:
            writer = csv.writer(f, dialect='excel')
            header = ['Year', 'ALL'] + self.assets_for_strategy
            skip_cols = ['Year']
            writer.writerow(header)
            for date, history_data in self.historical_data.items():
                current_row = [date]
                for col in header:
                    if col in skip_cols:
                        continue
                    if col == 'ALL':
                        if 'all_assets_price' in history_data:
                            current_row += [history_data['all_assets_price']]
                        else:
                            current_row += [0]
                    else:
                        current_row += [history_data[col]['price_total']]
                writer.writerow(current_row)

    def start_backtest(self) -> None:
        self.current_date = self.date_start.strftime(self.DEFAULT_DATE_FORMAT)
        self.load_asset_prices()
        self.make_initial_purchase(self.date_start.strftime(self.DEFAULT_DATE_FORMAT))

    def apply_stop_loss(self, ticker: str, date: str, price: float, ticker_quantity: int):
        if 'CBRT' in self.assets_for_strategy:
            print(f'----> apply_stop_loss, cash start: {self.cash_balance}')
            asset_price = self.assets[ticker]['current_price']
            print(f'asset_price: {asset_price}, asset_quantity: {ticker_quantity}')
            self.sell_asset(ticker, ticker_quantity, asset_price)
            print(f'sold {ticker}, cash: {self.cash_balance}')
            cbrt_current_price = self.trade_data[date][ticker]
            cbrt_quantity_to_buy = self.cash_balance // cbrt_current_price
            self.buy_asset('CBRT', cbrt_quantity_to_buy, cbrt_current_price)
            print(f'bought CBRT ({cbrt_quantity_to_buy} * {cbrt_current_price} = '
                  f'{cbrt_quantity_to_buy * cbrt_current_price}), cash: {self.cash_balance}')

    def backtest(self) -> None:
        self.start_backtest()
        allow_calculation = False
        for date_str, ticker_data in self.trade_data.items():
            self.current_date = date_str
            dt_date = datetime.datetime.strptime(date_str, self.DEFAULT_DATE_FORMAT).date()
            # уже посчитали в start_backtest, пропускаем первую дату
            if date_str == self.date_start.strftime(self.DEFAULT_DATE_FORMAT):
                allow_calculation = True
                continue
            if not allow_calculation:
                continue
            if date_str == self.date_finish.strftime(self.DEFAULT_DATE_FORMAT):
                allow_calculation = False

            for ticker, price in ticker_data.items():
                if ticker not in self.assets_for_strategy:
                    continue
                ticker_quantity = self.assets[ticker]["quantity"]
                ticker_stop_loss_price = self.get_asset_stop_loss(ticker, date_str)
                if price <= ticker_stop_loss_price and ticker_quantity > 0 and self.stop_losses_enabled:
                    print('--> stop loss activated', date_str, ticker, price, ticker_stop_loss_price)
                    self.apply_stop_loss(ticker, date_str, price, ticker_quantity)
                self.update_history(ticker, date_str, price, ticker_quantity)
                self.update_assets_balance_for_date(date_str)
            if self.rebalance_enabled and dt_date in self.rebalance_dates:
                print(f'--> rebalancing at: {date_str}')
                print(f'--> cash before: {self.cash_balance}')
                self.rebalancing(self.historical_data[date_str])
                print(f'--> cash after: {self.cash_balance}')
            self.update_assets_balance_for_date(date_str)


if __name__ == '__main__':
    date_start = '2000-01-05'
    date_finish = '2024-08-15'
    initial_balance = 100000
    assets_to_buy = ['GOLD', 'DOBR', 'CBRT']

    bt = BacktestStrategy()
    bt.set_rebalance_mode('quarterly')
    bt.rebalance_enabled = False
    bt.stop_losses_enabled = False
    bt.set_dates(date_start, date_finish)
    bt.set_assets_for_strategy(assets_to_buy)
    bt.topup_cash_balance(initial_balance)
    bt.backtest()
    bt.export_history()
    print(bt.assets)
    print(bt.transactions_history)
    # print(bt.stop_losses)
    # print(bt.key_trade_dates)
    # assets = {
    #     'AKME': 164.68, 'GOLD': 1.8115, 'LQDT': 1.4617
    # }
    # print(bt.distribute_cash_for_assets(10000, assets))