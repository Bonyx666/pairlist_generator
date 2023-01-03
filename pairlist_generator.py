import fileinput
import shutil
from pathlib import Path

import freqtrade.commands.data_commands
from freqtrade.configuration import Configuration
from freqtrade.data.history import load_pair_history
from freqtrade.resolvers import ExchangeResolver
from freqtrade.plugins.pairlistmanager import PairListManager
import pandas as pd
from freqtrade.enums import CandleType, MarginMode, TradingMode
from datetime import datetime, timedelta
from freqtrade.data.history.history_utils import (_download_pair_history, _download_trades_history,
                                                  _load_cached_data_for_updating,
                                                  convert_trades_to_ohlcv, get_timerange, load_data,
                                                  load_pair_history, refresh_backtest_ohlcv_data,
                                                  refresh_backtest_trades_data, refresh_data,
                                                  validate_backtest_data)
import nest_asyncio
import re
from dateutil.relativedelta import *
import json
import os
import argparse
import numpy

nest_asyncio.apply()


class generator:
    CONFIG_TEMPLATE_PATH = 'pairlist_generator_config_template.json'
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

    def is_bool(s):
        bool_var = s.lower() in ['true', '1', 't', 'y', 'yes', 'yeah', 'yup', 'certainly']
        return bool_var

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
        '''# // Major coins
        "(BTC|ETH)/.*",
        # // BINANCE:
        # // Exchange
        "(BNB)/.*",
        # // Leverage
        ".*(_PREMIUM|BEAR|BULL|DOWN|HALF|HEDGE|UP|[1235][SL])/.*",
        # // Fiat
        "(AUD|BRZ|CAD|CHF|EUR|GBP|HKD|IDRT|JPY|NGN|RUB|SGD|TRY|UAH|USD|ZAR)/.*",
        # // Stable
        "(BUSD|CUSDT|DAI|PAXG|SUSD|TUSD|USDC|USDP|USDT|VAI)/.*",
        # // FAN
        "(ACM|AFA|ALA|ALL|APL|ASR|ATM|BAR|CAI|CITY|FOR|GAL|GOZ|IBFK|JUV|LAZIO|LEG|LOCK-1|NAVI|NMR|NOV|OG|PFL|PSG|ROUSH|STV|TH|TRA|UCH|UFC|YBO)/.*",
        # // Others
        "(CHZ|CTXC|HBAR|NMR|SHIB|SLP|XVS|ONG|ARDR)/.*",

        # // KUCOIN:
        # // Exchange Tokens
        "KCS/.*",
        # // Leverage tokens
        ".*(3|3L|3S|5L|5S)/.*",
        # // Fiat
        "(AUD|EUR|GBP|CHF|CAD|JPY)/.*",
        # // Stable tokens
        "(BUSD|USDT|TUSD|USDC|CUSDT|DAI|USDN|CUSD)/.*",
        # // FAN Tokens
        "(ACM|AFA|ALA|ALL|APL|ASR|ATM|BAR|CAI|CITY|FOR|GAL|GOZ|IBFK|JUV|LEG|LOCK-1|NAVI|NMR|NOV|OG|PFL|PORTO|PSG|ROUSH|STV|TH|TRA|UCH|UFC|YBO)/.*",
        # // Other Coins
        "(CHZ|SLP|XVS|MEM|AMPL|XYM|POLX|CARR|SKEY|MASK|KLV|TLOS)/.*"
        ]
        '''
        self.config['pairlists'] = [
            {
                "method": "StaticPairList",
            },
        ]

        if self.TRADING_MODE_NAME == "spot":
            self.config['trading_mode'] = TradingMode.SPOT
            self.config['margin_mode'] = MarginMode.NONE
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

        self.exchange = ExchangeResolver.load_exchange(self.config['exchange']['name'], self.config, validate=False)

        self.pairlists = PairListManager(self.exchange, self.config)
        # self.pairlists.refresh_pairlist()
        # self.pairs = self.pairlists.whitelist
        self.data_location = Path(self.config['user_data_dir'], 'data', self.config['exchange']['name'])

        self.pairs = list(self.get_pairs(self))
        self.config['exchange']['pair_whitelist'] = self.pairs

        print(f"found {str(len(self.config['exchange']['pair_whitelist']))} "
              f"pairs on {self.config['exchange']['name']}"
              f", market:{str(self.config['trading_mode']).split('.')[1].lower()}"
              f", stake:{self.config['stake_currency']}")

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
            raise (f"Cannot get markets. Reason: {e}") from e
        return pairs

    def get_data_slices_dates(df, start_date_str, end_date_str, interval, self):
        # df_start_date = df.date.min()
        # df_end_date = df.date.max()

        defined_start_date = datetime.strptime(start_date_str, self.DATE_TIME_FORMAT)
        defined_end_date = datetime.strptime(end_date_str, self.DATE_TIME_FORMAT)

        # start_date = df_start_date if defined_start_date < df_start_date else defined_start_date
        # end_date = df_end_date if defined_end_date > df_end_date else defined_end_date

        start_date = defined_start_date
        end_date = defined_end_date

        # time_delta = timedelta(hours=interval_hr)
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
                    # this row (as it was in the original) cut off the dataframe depending on the first (hence the how='left' candle of the the pair. Outer merges both including the new timerange of the 2ndary pairs.
                    # full_dataframe = pd.merge(full_dataframe, candles[['date', column_name]].copy(), on='date', how='left')
                    full_dataframe = pd.merge(full_dataframe, candles[['date', column_name]].copy(), on='date',
                                              how='outer')
                # print("Loaded " + str(len(candles)) + f" rows of data for {pair} from {data_location}")
                # print(full_dataframe.tail(1))

        # print(full_dataframe.head())

        if "date" in full_dataframe:
            full_dataframe['date'] = full_dataframe['date'].dt.tz_localize(None)

        return full_dataframe

    def process_date_slices(df, date_slices, number_assets, self):
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

    def main(self):
        if os.path.exists(self.CONFIG_TEMPLATE_PATH):

            self.CANDLE_TYPE = CandleType.SPOT
            parser = argparse.ArgumentParser()

            parser.add_argument("--exchange", default="gateio binance okex kucoin")
            parser.add_argument("--stake_currency", default="USDT BUSD BTC")
            parser.add_argument("--trading_mode", default="futures spot")
            parser.add_argument("--data_format", default="jsongz")
            parser.add_argument("--tradable_only", default="True")
            parser.add_argument("--active_only", default="True")
            parser.add_argument("--download_data", default="True")
            parser.add_argument("--intervals", default="monthly weekly daily")
            parser.add_argument("--asset_filter_prices", default="0 0.01 0.02 0.05 0.15 0.5")
            parser.add_argument("--number_assets", default="30 45 60 75 90 105 120 200")

            args = parser.parse_args()

            START_DATE_STR = '20171201 00:00:00'
            # wanted to have only monthly outputs, you can delete that replace thingy
            # in the next row if you want up to the current day.
            END_DATE_STR = datetime.today().replace(day=1).strftime('%Y%m%d') + ' 00:00:00'
            start_string = START_DATE_STR.split(' ')[0]
            end_string = END_DATE_STR.split(' ')[0]

            INTERVAL_ARR = args.intervals.split(' ')
            ASSET_FILTER_PRICE_ARR = [float(ele) for ele in args.asset_filter_prices.split(' ')]
            NUMBER_ASSETS_ARR = [int(ele) for ele in args.number_assets.split(' ')]

            # split_exchange = args.exchange.split(" ")
            self.DATA_FORMAT = args.data_format

            self.TRADABLE_ONLY = self.is_bool(args.tradable_only)
            self.ACTIVE_ONLY = self.is_bool(args.active_only)
            self.DOWNLOAD_DATA = self.is_bool(args.download_data)

            for single_exchange in args.exchange.split(" "):
                self.EXCHANGE_NAME = single_exchange
                for single_trading_mode in args.trading_mode.split(" "):
                    self.TRADING_MODE_NAME = single_trading_mode
                    del_root_path = f'user_data/pairlists/{self.EXCHANGE_NAME}_{self.TRADING_MODE_NAME}'
                    shutil.rmtree(del_root_path, True)

                    for single_currency_name in args.stake_currency.split(" "):
                        self.STAKE_CURRENCY_NAME = single_currency_name

                        self.set_config(self)

                        if len(self.config['exchange']['pair_whitelist']) == 0:
                            print("-- Skipping this download/calculation part since there are no pairs here...")
                        else:
                            print("Status: downloading data...")
                            download_args = {"pairs": self.pairs,
                                             "include_inactive": False,
                                             "timerange": start_string + "-" + end_string,
                                             "download_trades": False,
                                             "exchange": self.EXCHANGE_NAME,
                                             "timeframes": [self.config["timeframe"]],
                                             "trading_mode": self.config['trading_mode'],
                                             "dataformat_ohlcv": self.DATA_FORMAT,
                                             }
                            if self.DOWNLOAD_DATA:
                                freqtrade.commands.data_commands.start_download_data(download_args)

                            print("Status: calculating pairlists...")
                            for asset_filter_price in ASSET_FILTER_PRICE_ARR:

                                volume_dataframe = self.process_candles_data(self, asset_filter_price)

                                if volume_dataframe.empty:
                                    continue

                                for interval in INTERVAL_ARR:
                                    date_slices = self.get_data_slices_dates(
                                        volume_dataframe,
                                        START_DATE_STR,
                                        END_DATE_STR,
                                        interval,
                                        self)

                                    for number_assets in NUMBER_ASSETS_ARR:
                                        result_obj = self.process_date_slices(volume_dataframe,
                                                                              date_slices,
                                                                              number_assets,
                                                                              self)
                                        for index, (timerange, current_slice) in enumerate(result_obj.items()):
                                            end_date_config_file = timerange.split("-")[1]
                                            whitelist = current_slice

                                            file_name = f'user_data/pairlists/' \
                                                        f'{self.EXCHANGE_NAME}_{self.TRADING_MODE_NAME}/' \
                                                        f'{self.STAKE_CURRENCY_NAME}/' \
                                                        f'{interval}/' \
                                                        f'{interval}_{number_assets}_{self.STAKE_CURRENCY_NAME}_' \
                                                        f'{str(asset_filter_price).replace(".", ",")}' \
                                                        f'_minprice_{end_date_config_file}' \
                                                        f'.json'
                                            last_slice_file_name = f'user_data/pairlists/' \
                                                        f'{self.EXCHANGE_NAME}_{self.TRADING_MODE_NAME}/' \
                                                        f'{self.STAKE_CURRENCY_NAME}/' \
                                                        f'{interval}/' \
                                                        f'{interval}_{number_assets}_{self.STAKE_CURRENCY_NAME}_' \
                                                        f'{str(asset_filter_price).replace(".", ",")}' \
                                                        f'_minprice_current' \
                                                        f'.json'

                                            os.makedirs(os.path.dirname(file_name), exist_ok=True)

                                            shutil.copy(self.CONFIG_TEMPLATE_PATH,
                                                        file_name)
                                            with open(file_name, 'r') as f1:
                                                data = json.load(f1)

                                            data['trading_mode'] = self.TRADING_MODE_NAME.lower()
                                            data['stake_currency'] = self.STAKE_CURRENCY_NAME.upper()
                                            data['exchange']['name'] = self.EXCHANGE_NAME.lower()
                                            data['exchange']['pair_whitelist'].clear()
                                            # todo: make blacklist trigger (currently only whitelist is in effect)
                                            for pair in whitelist:
                                                data['exchange']['pair_whitelist'].append(pair)

                                            # If this is the last slice, do something else
                                            if index == len(result_obj) - 1:
                                                with open(last_slice_file_name, 'w') as f2:
                                                    json.dump(data, f2, indent=4)

                                            with open(file_name, 'w') as f2:
                                                json.dump(data, f2, indent=4)

                            # Save result object as json to --outfile location
                            print(f'Done {self.EXCHANGE_NAME}_{self.TRADING_MODE_NAME}_{self.STAKE_CURRENCY_NAME}')
        else:
            print(f'path {self.CONFIG_TEMPLATE_PATH} does not exist. shutting down!")')


generator.main(generator)
