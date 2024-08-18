import csv
import pandas as pd
import matplotlib.pyplot as plt
import datetime


class BacktestStrategy:

    cash_balance = 0.0
    stop_losses = dict()
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
    DEFAULT_STOP_LOSS_PERCENTAGE = 0.03
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
            if self.rebalance_enabled and self.rebalance_mode == 'yearly':
                self.rebalance_dates.append(max_trade_date)
            self.key_trade_dates[current_year]["max_date"] = max_trade_date
            for k, quarter in enumerate(self.key_trade_dates[current_year]["all_quarters"]):
                if len(self.key_trade_dates[current_year]["all_quarters"][quarter]) < 1:
                    continue
                max_q_date = max(self.key_trade_dates[current_year]["all_quarters"][quarter]).strftime(
                    self.DEFAULT_DATE_FORMAT
                )
                self.key_trade_dates[current_year]["all_quarters"][quarter] = max_q_date
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
        dt_current = datetime.datetime.strptime(current_date, self.DEFAULT_DATE_FORMAT)
        some_weeks_ago = dt_current - datetime.timedelta(weeks=5)
        one_week_ago = dt_current - datetime.timedelta(days=7)
        start = self.get_closest_date(some_weeks_ago, self.trade_dates, 'min')
        finish = self.get_closest_date(one_week_ago, self.trade_dates, 'min')
        asset_prices = []
        start_price_collection = False

        for date, trade_data in self.trade_data.items():
            if date == start:
                start_price_collection = True
            if date == finish:
                break
            if start_price_collection and asset in trade_data:
                asset_prices.append(trade_data[asset])
        max_price = max(asset_prices)
        current_price = self.trade_data[current_date][asset]
        price_diff = max_price * self.DEFAULT_STOP_LOSS_PERCENTAGE
        stop_loss_price = max_price - price_diff
        # print(f'asset: {asset}, {current_price}, {price_diff}, {stop_loss_price}')
        return stop_loss_price

    def buy_asset(self, asset_ticker: str, quantity: int, price: float) -> None:
        return self.buy_or_sell_asset("buy", asset_ticker, quantity, price)

    def sell_asset(self, asset_ticker: str, quantity: int, price: float) -> None:
        return self.buy_or_sell_asset("sell", asset_ticker, quantity, price)

    def buy_or_sell_asset(self, transaction_type: str, asset_ticker: str, quantity: int, price: float) -> None:
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
            self.transactions_history[transaction_type] = {self.current_date: {}}
        self.transactions_history[transaction_type][self.current_date][asset_ticker] = {
            "price": price,
            "quantity": quantity,
        }
        self.assets[asset_ticker]["price_total"] = price * self.assets[asset_ticker]["quantity"]

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
            total += price_data["price_total"]
        self.historical_data[date]["all_assets_price"] = total
        # self.cash_balance = total

    def get_asset_price_for_date(self, asset_ticker: str, date: str) -> float:
        return self.trade_data[date][asset_ticker]

    def make_initial_purchase(self, date: str) -> None:
        print('---make_initial_purchase')
        asset_prices = {}
        for asset_ticker in self.assets_for_strategy:
            current_asset_price = self.get_asset_price_for_date(asset_ticker, self.strategy_date_start)
            asset_prices[asset_ticker] = current_asset_price
        cash_allocation = self.distribute_cash_for_assets(self.cash_balance, asset_prices)
        print(f'cash_allocation: {cash_allocation}')
        for ticker, qnty in cash_allocation.items():
            price = asset_prices[ticker]
            self.buy_asset(ticker, qnty, price)
            self.update_history(ticker, date, price, qnty)
        print('initial cash:', self.cash_balance)

    def set_assets_for_strategy(self, tickers: list) -> None:
        self.assets_for_strategy = tickers

    def rebalancing(self, assets_data: dict) -> None:
        if self.current_date not in self.transactions_history["rebalancing"]:
            self.transactions_history["rebalancing"].append(self.current_date)
        total_price = sum(info["price_total"] for info in assets_data.values() if isinstance(info, dict))
        equal_price_total = total_price / len(assets_data)
        self.rebalance_init_transaction('sell', assets_data, equal_price_total)
        self.rebalance_init_transaction('buy', assets_data, equal_price_total)

    def rebalance_init_transaction(self, transation_type: str, assets_data, equal_price_total) -> None:
        for ticker, info in assets_data.items():
            if not isinstance(info, dict):
                continue
            current_price_total = info["price_total"]
            difference = equal_price_total - current_price_total
            quantity_to_buy_or_sell = int(abs(difference) // info["price_per_item"])
            if difference > 0 and transation_type == 'buy':
                self.buy_asset(ticker, quantity_to_buy_or_sell, info["price_per_item"])
            elif transation_type == 'sell':
                self.sell_asset(ticker, quantity_to_buy_or_sell, info["price_per_item"])

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
                ticker_quantity = self.assets[ticker]["quantity"]
                ticker_stop_loss_price = self.get_asset_stop_loss(ticker, date)
                # print(f'{ticker}, current: {price}, stop: {ticker_stop_loss_price}')
                if price <= ticker_stop_loss_price and ticker_quantity > 0:
                    if ticker not in self.stop_losses:
                        self.stop_losses[ticker] = []
                    self.stop_losses[ticker].append((date, price))
                    if 'CBRT' in self.assets_for_strategy:
                        asset_price = self.assets[ticker]['current_price']
                        print('--> stop loss activated', date, ticker, price, ticker_stop_loss_price)
                        print(f'asset_price: {asset_price}, asset_quantity: {ticker_quantity}, cash: {self.cash_balance}')
                        self.sell_asset(ticker, ticker_quantity, asset_price)
                        print(f'sold {ticker}, cash: {self.cash_balance}')
                        cbrt_current_price = self.trade_data[date][ticker]
                        cbrt_quantity_to_buy = self.cash_balance // cbrt_current_price
                        self.buy_asset('CBRT', cbrt_quantity_to_buy, cbrt_current_price)
                        print(f'bought CBRT ({cbrt_quantity_to_buy} * {cbrt_current_price} = '
                              f'{cbrt_quantity_to_buy * cbrt_current_price}), cash: {self.cash_balance}')

                self.update_history(ticker, date, price, ticker_quantity)

            if self.rebalance_enabled and date in self.rebalance_dates:
                print(f'--> rebalancing at: {date}')
                self.rebalancing(self.historical_data[date])
            self.update_assets_balance_for_date(date)


if __name__ == '__main__':
    date_start = '2000-01-05'
    date_finish = '2024-08-15'
    initial_balance = 100000
    assets_to_buy = ['GOLD', 'DOBR', 'CBRT']

    bt = BacktestStrategy()
    bt.set_rebalance_mode('quarterly')
    bt.rebalance_enabled = True
    bt.set_dates(date_start, date_finish)
    bt.set_assets_for_strategy(assets_to_buy)
    bt.topup_cash_balance(initial_balance)
    bt.backtest()
    bt.export_history()
    print(bt.stop_losses)