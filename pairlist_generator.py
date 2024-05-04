import concurrent.futures
import json
import os
from datetime import datetime
from pathlib import Path

import freqtrade.commands.data_commands
import nest_asyncio
import pandas as pd
from dateutil.relativedelta import *
from freqtrade.configuration import Configuration
from freqtrade.data.history.history_utils import (load_pair_history)
from freqtrade.enums import CandleType, MarginMode, TradingMode
from freqtrade.exchange import list_available_exchanges
from freqtrade.plugins.pairlistmanager import PairListManager
from freqtrade.resolvers import ExchangeResolver
from tqdm import tqdm

nest_asyncio.apply()
os.nice(19)


class Generator:
    STAKE_CURRENCY_NAME = ''
    EXCHANGE_NAME = ''
    TRADING_MODE_NAME = ''
    DATA_FORMAT = ''
    config = ''
    exchange = ''
    pairlists = ''
    pairs = ''
    data_location = ''
    DATE_FORMAT = '%Y%m%d'
    DATE_TIME_FORMAT = '%Y%m%d %H:%M:%S'
    TRADABLE_ONLY = ''
    ACTIVE_ONLY = ''
    DOWNLOAD_DATA = ''

    FUTURES_ONLY = False
    SPOT_ONLY = True

    def __init__(self):
        self.CANDLE_TYPE = None
        self.TRADING_MODES = None

    def set_config(self):
        self.config = Configuration.from_files([])
        self.config["dataformat_ohlcv"] = self.DATA_FORMAT
        self.config["timeframe"] = "1d"
        self.config['exchange']['name'] = self.EXCHANGE_NAME
        self.config['stake_currency'] = self.STAKE_CURRENCY_NAME
        self.config['exchange']['pair_whitelist'] = [
            f'.*/{self.STAKE_CURRENCY_NAME}',
        ]
        self.config['exchange']['pair_blacklist'] = []
        self.config['pairlists'] = [
            {
                "method": "StaticPairList",
            },
        ]

        if self.TRADING_MODE_NAME == "spot":
            self.config['trading_mode'] = TradingMode.SPOT
            self.config['margin_mode'] = MarginMode.ISOLATED
            self.CANDLE_TYPE = CandleType.SPOT
            self.FUTURES_ONLY = False
            self.SPOT_ONLY = True
            self.config['candle_type_def'] = CandleType.SPOT
        else:
            self.config['trading_mode'] = TradingMode.FUTURES
            self.config['margin_mode'] = MarginMode.ISOLATED
            self.CANDLE_TYPE = CandleType.FUTURES
            self.FUTURES_ONLY = True
            self.SPOT_ONLY = False
            self.config['candle_type_def'] = CandleType.FUTURES

        self.exchange = ExchangeResolver.load_exchange(self.config, validate=False)

        self.pairlists = PairListManager(self.exchange, self.config)
        # self.pairlists.refresh_pairlist()
        # self.pairs = self.pairlists.whitelist
        self.data_location = Path(self.config['user_data_dir'], 'data', self.config['exchange']['name'])

        self.pairs = list(self.get_pairs())
        self.config['exchange']['pair_whitelist'] = self.pairs

        #print(f"found {str(len(self.config['exchange']['pair_whitelist']))} "
        #      f"pairs on {self.config['exchange']['name']}"
        #      f", market:{str(self.config['trading_mode']).split('.')[1].lower()}"
        #      f", stake:{self.config['stake_currency']}")

    def get_pairs(self):
        try:
            pairs = self.exchange.get_markets(quote_currencies=[self.STAKE_CURRENCY_NAME],
                                              tradable_only=self.TRADABLE_ONLY,
                                              active_only=self.ACTIVE_ONLY,
                                              spot_only=self.SPOT_ONLY,
                                              margin_only=False,  # no margin atm, no need to set that in a variable.
                                              futures_only=self.FUTURES_ONLY)
            # Sort the pairs/markets by symbol
            pairs = dict(sorted(pairs.items()))
        except Exception as e:
            raise f"Cannot get markets. Reason: {e}" from e
        return pairs

    def get_data_slices_dates(self, start_date_str, end_date_str, interval):
        # df_start_date = df.date.min()
        # df_end_date = df.date.max()

        defined_start_date = datetime.strptime(start_date_str, self.DATE_TIME_FORMAT)
        defined_end_date = datetime.strptime(end_date_str, self.DATE_TIME_FORMAT)

        # start_date = df_start_date if defined_start_date < df_start_date else defined_start_date
        # end_date = df_end_date if defined_end_date > df_end_date else defined_end_date

        start_date = defined_start_date
        end_date = defined_end_date

        if interval == 'monthly':
            time_delta = relativedelta(months=+1)
        elif interval == 'weekly':
            time_delta = relativedelta(weeks=+1)
        elif interval == 'daily':
            time_delta = relativedelta(days=+1)
        else:
            time_delta = relativedelta(months=+1)

        slices = []

        run = True

        while run:
            # slice_start_time = end_date - time_delta
            slice_end_time = start_date + time_delta
            if slice_end_time <= end_date:
                slice_date = {
                    'start': start_date,
                    'end': slice_end_time
                }

                slices.append(slice_date)
                start_date = slice_end_time
            else:
                slice_date = {
                    'start': start_date,
                    'end': defined_end_date
                }

                slices.append(slice_date)
                run = False

        return slices

    def process_candles_data(self, filter_price):
        full_dataframe = pd.DataFrame()

        for pair in self.pairs:

            # print(self.data_location)
            # print(self.config["timeframe"])
            # print(pair)

            candles = load_pair_history(
                datadir=self.data_location,
                timeframe=self.config["timeframe"],
                pair=pair,
                data_format=self.DATA_FORMAT,
                candle_type=self.CANDLE_TYPE
            )

            if len(candles):
                # Not sure about AgeFilter
                # apply price filter make price 0 to ignore this pair after calculation of quoteVolume
                candles.loc[(candles.close < filter_price), 'close'] = 0
                column_name = pair
                candles[column_name] = candles['volume'] * candles['close']

                if full_dataframe.empty:
                    full_dataframe = candles[['date', column_name]].copy()
                else:
                    full_dataframe = pd.merge(full_dataframe, candles[['date', column_name]].copy(), on='date',
                                              how='outer')

        # print(full_dataframe.head())

        if "date" in full_dataframe:
            full_dataframe['date'] = full_dataframe['date'].dt.tz_localize(None)

        return full_dataframe

    def process_date_slices(self, df, date_slices, number_assets):
        result = {}
        for date_slice in date_slices:
            df_slice = df[(df.date >= date_slice['start']) & (df.date < date_slice['end'])].copy()
            summarised = df_slice.sum(numeric_only=True)
            summarised = summarised[summarised > 0]
            summarised = summarised.sort_values(ascending=False)

            if len(summarised) > number_assets:
                result_pairs_list = list(summarised.index[:number_assets])
            else:
                result_pairs_list = list(summarised.index)

            if len(result_pairs_list) > 0:
                result[
                    f'{date_slice["start"].strftime(self.DATE_FORMAT)}-{date_slice["end"].strftime(self.DATE_FORMAT)}'] \
                    = result_pairs_list

        return result

    def main(self, exchange):
        self.CANDLE_TYPE = CandleType.SPOT

        self.START_DATE_STR = '20171201 00:00:00'
        # in the next row if you want up to the current day.
        self.END_DATE_STR = datetime.today().replace(day=1).strftime('%Y%m%d') + ' 00:00:00'
        self.start_string = self.START_DATE_STR.split(' ')[0]
        self.end_string = self.END_DATE_STR.split(' ')[0]

        self.INTERVAL_ARR = ["monthly"]
        self.ASSET_FILTER_PRICE_ARR = [0, 0.01, 0.02, 0.05, 0.15, 0.5]
        self.NUMBER_ASSETS_ARR = [30, 45, 60, 75, 90, 105, 120, 200, 99999]

        # split_exchange = args.exchange.split(" ")
        self.DATA_FORMAT = "jsongz"

        self.TRADABLE_ONLY = False
        self.ACTIVE_ONLY = False
        self.DOWNLOAD_DATA = True
        self.TRADING_MODES = ["futures", "spot"]
        self.EXCHANGE_NAME = exchange
        for single_trading_mode in self.TRADING_MODES:
            self.TRADING_MODE_NAME = single_trading_mode

            for single_currency_name in ["USDT", "BUSD", "TUSD", "BTC", "EUR", "USD", "ETH"]:
                self.STAKE_CURRENCY_NAME = single_currency_name

                self.set_config()
                if len(self.config['exchange']['pair_whitelist']) == 0:
                    # print("-- Skipping this download/calculation part since there are no pairs here...")
                    pass
                else:
                    #print(f"Status: {self.exchange}|{single_trading_mode}, downloading data...")
                    download_args = {"pairs": self.pairs,
                                     "include_inactive": True,
                                     "timerange": self.start_string + "-" + self.end_string,
                                     "download_trades": False,
                                     "exchange": self.EXCHANGE_NAME,
                                     "timeframes": [self.config["timeframe"]],
                                     "trading_mode": self.config['trading_mode'],
                                     "dataformat_ohlcv": self.DATA_FORMAT,
                                     }

                    if self.DOWNLOAD_DATA:
                        freqtrade.commands.data_commands.start_download_data(download_args)

                    # print(f"Status: {self.exchange}|{single_trading_mode}, calculating pairlists...")
                    for asset_filter_price in self.ASSET_FILTER_PRICE_ARR:

                        volume_dataframe = self.process_candles_data(asset_filter_price)

                        if volume_dataframe.empty:
                            continue

                        for interval in self.INTERVAL_ARR:
                            date_slices = self.get_data_slices_dates(
                                self.START_DATE_STR,
                                self.END_DATE_STR,
                                interval)
                            path_prefix = os.path.join(
                                os.getcwd(), 'user_data', 'pairlists',
                                f'{self.EXCHANGE_NAME}_{self.TRADING_MODE_NAME}',
                                f'{self.STAKE_CURRENCY_NAME}',
                                f'{interval}'
                            )
                            if not os.path.exists(path_prefix):
                                os.makedirs(path_prefix)

                            for number_assets in self.NUMBER_ASSETS_ARR:
                                result_obj = self.process_date_slices(volume_dataframe,
                                                                      date_slices,
                                                                      number_assets)
                                for index, (timerange, current_slice) in enumerate(result_obj.items()):
                                    end_date_config_file = timerange.split("-")[1]
                                    whitelist = current_slice
                                    file_prefix = f'{interval}_{number_assets}_{self.STAKE_CURRENCY_NAME}_' \
                                                  f'{str(asset_filter_price).replace(".", ",")}' \
                                                  f'_minprice_'
                                    file_name = os.path.join(
                                        path_prefix, f'{file_prefix}{end_date_config_file}.json')
                                    last_slice_file_name = os.path.join(
                                        path_prefix, f'{file_prefix}_minprice_current.json')

                                    os.makedirs(os.path.dirname(file_name), exist_ok=True)

                                    # delete the single file if it exists instead of all files as previously
                                    # this makes updating the pairlists smoother,
                                    # enabling backtests to be running while it updates.

                                    data = {
                                        "pairlists": [
                                            {
                                                "method": "StaticPairList"
                                            }
                                        ],
                                        "trading_mode": self.TRADING_MODE_NAME.lower(),
                                        "margin_mode": "isolated",
                                        "stake_currency": self.STAKE_CURRENCY_NAME.upper(),
                                        "exchange": {
                                            "name": self.EXCHANGE_NAME.lower(),
                                            "key": "",
                                            "secret": "",
                                            "pair_whitelist": whitelist,
                                            "pair_blacklist": []
                                        }
                                    }

                                    # If this is the last slice, do something else
                                    if index == len(result_obj) - 1:
                                        if os.path.exists(last_slice_file_name):
                                            os.remove(last_slice_file_name)
                                        with open(last_slice_file_name, 'w') as f2:
                                            json.dump(data, f2, indent=4)

                                    if os.path.exists(file_name):
                                        os.remove(file_name)
                                    with open(file_name, 'w') as f2:
                                        json.dump(data, f2, indent=4)

                    # print(f'Done {self.exchange}|{single_trading_mode}|{self.STAKE_CURRENCY_NAME}')


exchanges = list_available_exchanges(True)
# remove any exchange that is named futures to remove duplicates
# remove bitfinex since bitfinex2 is the same as bitfinex2, but bitfinex2 has the newer api version
# same for hitbtc and hitbtc3
# remote gateio since gate is the same (rebranding)
exchanges_names = [
    exchange['name'] for exchange in exchanges
    if (exchange['name'] != 'gateio'
        and exchange['name'] != 'bitfinex'
        and exchange['name'] != 'hitbtc'
        and 'futures' not in exchange['name']
        )
]


# Define a function to process each exchange
def process_exchange(current_exchange):
    Generator().main(current_exchange)


# easier debugging, only with one thread
# for exchange_name in exchanges_names:
#    process_exchange(exchange_name)

with concurrent.futures.ProcessPoolExecutor(max_workers=len(exchanges_names)) as executor:
    futures = {executor.submit(process_exchange, exchange_name): exchange_name for exchange_name in exchanges_names}

    with tqdm(total=len(exchanges_names), desc="Processing exchanges") as pbar:
        for future in concurrent.futures.as_completed(futures):
            pbar.update(1)
            exchange_name = futures[future]
            del futures[future]
            remaining_tasks = len(futures)
            remaining_exchanges = ', '.join(list(futures.values()))
            pbar.set_description(f"Processing: {remaining_exchanges}")

print("DONE!")