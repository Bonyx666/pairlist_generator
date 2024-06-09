import argparse
import concurrent.futures
import copy
import fnmatch
import json
import logging
import os
import re
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

parser = argparse.ArgumentParser(description="Description of your script")
parser.add_argument("--jobs",
                    default=os.cpu_count(),
                    type=int,
                    help="Number of jobs, default=cores. -1 for unlimited")
parser.add_argument("--exchanges",
                    type=str,
                    help="Space separated list of exchanges",
                    default="")

# Parse the arguments
args = parser.parse_args()


class Generator:
    FIAT_currencies = ["USDT", "BUSD", "USDC", "DAI", "TUSD", "FDUSD", "PAX",
                       "USD", "EUR", "GBP", "TRY", "JPY", "NIS", "AUD", "KRW", "BRL"]
    STAKE_CURRENCY_NAME = ""
    EXCHANGE_NAME = ""
    TRADING_MODE_NAME = ""
    DATA_FORMAT = ""
    config = ""
    exchange = ""
    pairlists = ""
    pairs = ""
    data_location = ""
    DATE_FORMAT = "%Y%m%d"
    DATE_TIME_FORMAT = "%Y%m%d %H:%M:%S"
    TRADABLE_ONLY = ""
    ACTIVE_ONLY = ""
    DOWNLOAD_DATA = ""

    FUTURES_ONLY = False
    SPOT_ONLY = True

    def __init__(self):
        self.pairs_market_currency = []
        os.nice(19)
        self.INTERVAL_ARR = ["monthly"]
        self.ASSET_FILTER_PRICE_ARR = [0.0, 0.01, 0.02, 0.05, 0.15, 0.5]
        self.NUMBER_ASSETS_ARR = [30, 45, 60, 75, 90, 105, 120, 200, 99999]

        self.DATA_FORMAT = "jsongz"

        self.TRADABLE_ONLY = False
        self.ACTIVE_ONLY = False
        self.TRADING_MODES = ["futures", "spot"]
        self.CANDLE_TYPE = CandleType.SPOT

        self.START_DATE_STR = "20171201 00:00:00"
        self.END_DATE_STR = f"{datetime.today().replace(day=1).strftime('%Y%m%d')} 00:00:00"
        self.start_string = self.START_DATE_STR.split(" ")[0]
        self.end_string = self.END_DATE_STR.split(" ")[0]

        self.base_volume = None

    def set_config(self):
        self.config = Configuration.from_files([])
        self.config["dataformat_ohlcv"] = self.DATA_FORMAT
        self.config["timeframe"] = "1d"
        self.config["exchange"]["name"] = self.EXCHANGE_NAME
        self.config["stake_currency"] = self.STAKE_CURRENCY_NAME
        self.config["exchange"]["pair_whitelist"] = [
            f".*/{self.STAKE_CURRENCY_NAME}",
        ]
        self.config["exchange"]["pair_blacklist"] = []
        self.config["pairlists"] = [
            {
                "method": "StaticPairList",
            },
        ]

        if self.TRADING_MODE_NAME == "spot":
            self.config["trading_mode"] = TradingMode.SPOT
            self.config["margin_mode"] = MarginMode.ISOLATED
            self.CANDLE_TYPE = CandleType.SPOT
            self.FUTURES_ONLY = False
            self.SPOT_ONLY = True
            self.config["candle_type_def"] = CandleType.SPOT
        else:
            self.config["trading_mode"] = TradingMode.FUTURES
            self.config["margin_mode"] = MarginMode.ISOLATED
            self.CANDLE_TYPE = CandleType.FUTURES
            self.FUTURES_ONLY = True
            self.SPOT_ONLY = False
            self.config["candle_type_def"] = CandleType.FUTURES

        self.exchange = ExchangeResolver.load_exchange(self.config, validate=False)

        self.pairlists = PairListManager(self.exchange, self.config)
        self.data_location = Path(self.config["user_data_dir"], "data", self.config["exchange"]["name"])

        if self.STAKE_CURRENCY_NAME != "":
            self.pairs_market_currency = self.exchange.get_markets(
                quote_currencies=[self.STAKE_CURRENCY_NAME],
                tradable_only=self.TRADABLE_ONLY,
                active_only=self.ACTIVE_ONLY,
                spot_only=self.SPOT_ONLY,
                margin_only=False,
                futures_only=self.FUTURES_ONLY)
            self.config["exchange"]["pair_whitelist"] = [market_data["symbol"] for market_data in
                                                         self.pairs_market_currency.values()]
        else:
            self.pairs_market = self.exchange.get_markets(
                tradable_only=self.TRADABLE_ONLY,
                active_only=self.ACTIVE_ONLY,
                spot_only=self.SPOT_ONLY,
                margin_only=False,
                futures_only=self.FUTURES_ONLY)

    def get_data_slices_dates(self, start_date_str, end_date_str, interval):
        start_date = datetime.strptime(start_date_str, self.DATE_TIME_FORMAT)
        end_date = datetime.strptime(end_date_str, self.DATE_TIME_FORMAT)

        if interval == "monthly":
            time_delta = relativedelta(months=+1)
        elif interval == "weekly":
            time_delta = relativedelta(weeks=+1)
        elif interval == "daily":
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
                    "start": start_date,
                    "end": slice_end_time
                }

                slices.append(slice_date)
                start_date = slice_end_time
            else:
                slice_date = {
                    "start": start_date,
                    "end": end_date
                }

                slices.append(slice_date)
                run = False

        return slices

    # Exchanges mostly define volume of a candle by quote (USDT) but sometimes per base (BTC) on the example of BTC/USDT
    # Only returns proper values if both pairs that were compared could be done and at least 30 days overlap
    # Compared is against several pairs, since sometimes pairs don't exist on that exchange that are compared with.
    def calculate_is_base_volume(self):
        self.base_volume = None
        # Filter pairs that match the pattern "BTC/*"

        btc_pairs = [pair for pair in self.pairs_market_currency if fnmatch.fnmatch(pair, "BTC/*")]

        compared_pairs = [pair for pair in self.pairs_market_currency if
                          fnmatch.fnmatch(pair, "*SHIB/*") or
                          fnmatch.fnmatch(pair, "*PEPE/*") or
                          fnmatch.fnmatch(pair, "*FLOKI/*") or
                          fnmatch.fnmatch(pair, "*DOGE/*") or
                          fnmatch.fnmatch(pair, "*XRP/*") or
                          fnmatch.fnmatch(pair, "*ADA/*")]

        # Abort if those pairs don't exist
        if len(btc_pairs) == 0:
            return
        if len(compared_pairs) == 0:
            return

        candles_btc = load_pair_history(
            datadir=self.data_location,
            timeframe=self.config["timeframe"],
            pair=btc_pairs[0],
            data_format=self.DATA_FORMAT,
            candle_type=self.CANDLE_TYPE
        )

        candles_comparison = []
        # just in case those pairs didn't get overlap...
        for compared_pair in compared_pairs:

            candles_comparison = load_pair_history(
                datadir=self.data_location,
                timeframe=self.config["timeframe"],
                pair=compared_pair,
                data_format=self.DATA_FORMAT,
                candle_type=self.CANDLE_TYPE
            )
            if len(candles_comparison) > 0:
                break

        if len(candles_btc) == 0 or len(candles_comparison) == 0:
            return

        full_dataframe = pd.merge(candles_btc, candles_comparison, on="date", how="inner")

        # only continue if the overlap is > 30 candles
        if len(full_dataframe) < 30:
            return

        # Calculate the sum of volume columns for the last 30 rows of candles_BTC
        # adjust if you move off the daily timeframe!
        vol_btc = full_dataframe.tail(30)["volume_x"].sum()
        vol_comparison = full_dataframe.tail(30)["volume_y"].sum()
        returning_value = vol_btc < vol_comparison

        # This is for debugging to verify the calculations
        # print(f"vol_btc={vol_btc}, vol_{compared_pair}={vol_comparison}, "
        #      f"exchange:{self.EXCHANGE_NAME}, "
        #      f"market:{str(self.config['trading_mode'])} "
        #      f"Volume in BTC? {vol_btc<vol_comparison} !")

        return returning_value

    def process_candles_data(self, filter_price):
        full_dataframe = pd.DataFrame()

        for pair in self.pairs_market_currency:
            candles = load_pair_history(
                datadir=self.data_location,
                timeframe=self.config["timeframe"],
                pair=pair,
                data_format=self.DATA_FORMAT,
                candle_type=self.CANDLE_TYPE
            )

            if len(candles):
                # if volume is base volume (like BTC) then calculate it back to quote (USDT) example: BTC/USDT
                # by putting it back to USDT we can compare the pairs fairly
                if self.base_volume:
                    candles["volume"] = candles["volume"] * candles["close"]

                candles.loc[(candles.close < filter_price), "close"] = 0
                candles[pair] = candles["volume"] * candles["close"]

                if full_dataframe.empty:
                    full_dataframe = candles[["date", pair]].copy()
                else:
                    full_dataframe = pd.merge(full_dataframe, candles[["date", pair]].copy(), on="date",
                                              how="outer")

        # print(full_dataframe.head())

        if "date" in full_dataframe:
            full_dataframe["date"] = full_dataframe["date"].dt.tz_localize(None)

        return full_dataframe

    def process_date_slices(self, df, date_slices, number_assets):
        result = {}
        for date_slice in date_slices:
            df_slice = df[(df.date >= date_slice["start"]) & (df.date < date_slice["end"])].copy()

            summarised = df_slice.sum(numeric_only=True)
            summarised = summarised[summarised > 0]
            summarised = summarised.sort_values(ascending=False)

            if len(summarised) > number_assets:
                result_pairs_list = list(summarised.index[:number_assets])
            else:
                result_pairs_list = list(summarised.index)

            if len(result_pairs_list) > 0:
                result[(f"{date_slice['start'].strftime(self.DATE_FORMAT)}"
                        f"-{date_slice['end'].strftime(self.DATE_FORMAT)}")] \
                    = result_pairs_list

        return result

    # We now get a list of fiat currencies listed by occurencies
    def get_sorted_fiat_currencies(self):
        from collections import Counter

        # Get the quote currencies from the markets dictionary
        quote_currencies = [market_data["quote"] for market_data in self.pairs_market.values() if
                            any(market_data["quote"].startswith(fiat) for fiat in self.FIAT_currencies)]

        # Count the occurrences of each quote currency
        quote_currency_counts = Counter(quote_currencies)

        # Sort the quote currencies based on their occurrence counts (from most to least used)
        sorted_quote_currencies = sorted(quote_currency_counts.items(), key=lambda x: x[1], reverse=True)

        # If you want only the quote currencies without their counts
        sorted_quote_currencies = [currency for currency, _ in sorted_quote_currencies]

        # Get the list of markets for each sorted quote currency
        sorted_markets = {
            quote_currency: [
                market_symbol for market_symbol, market_data in self.exchange.markets.items()
                if quote_currency in market_data["quote"]]
            for quote_currency in sorted_quote_currencies}
        return sorted_markets

    def main(self, exchange):
        self.EXCHANGE_NAME = exchange

        for single_trading_mode in self.TRADING_MODES:
            # Only attempt to run futures exchanges that were actually implemented to not confuse users
            # (even though freqtrade allows downloading data of many more exchanges than those 4)
            # for spot on the other hand ... well, good luck - if they work, they work. If not, not.
            if (
                    single_trading_mode == "futures" and
                    self.EXCHANGE_NAME not in ["binance", "okx", "gate", "bybit", "binanceus"]
            ):
                continue

            self.TRADING_MODE_NAME = single_trading_mode
            self.set_config()  # initializes the exchange too, so we can get markets to begin with

            sorted_quote_currencies = self.get_sorted_fiat_currencies()

            for single_currency_name in sorted_quote_currencies.keys():
                self.STAKE_CURRENCY_NAME = single_currency_name

                self.set_config()

                if len(self.config["exchange"]["pair_whitelist"]) == 0:
                    continue

                if exchange != "kraken":
                    download_args = {"pairs": self.config["exchange"]["pair_whitelist"],
                                     "include_inactive": True,
                                     "timerange": self.start_string + "-" + self.end_string,
                                     "download_trades": False,
                                     "exchange": self.EXCHANGE_NAME,
                                     "timeframes": [self.config["timeframe"]],
                                     "trading_mode": self.config["trading_mode"],
                                     "dataformat_ohlcv": self.DATA_FORMAT,
                                     }
                    freqtrade.commands.data_commands.start_download_data(download_args)

                self.base_volume = self.calculate_is_base_volume()
                if self.base_volume is None:
                    continue

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
                            os.getcwd(), "user_data", "pairlists",
                            f"{self.EXCHANGE_NAME}_{self.TRADING_MODE_NAME}",
                            f"{self.STAKE_CURRENCY_NAME}",
                            f"{interval}"
                        )
                        if not os.path.exists(path_prefix):
                            os.makedirs(path_prefix)

                        for number_assets in self.NUMBER_ASSETS_ARR:
                            slices = self.process_date_slices(volume_dataframe, date_slices, number_assets)
                            for index, (timerange, current_slice) in enumerate(slices.items()):
                                end_date_config_file = timerange.split("-")[1]
                                whitelist = current_slice
                                file_prefix = (f"{interval}_{number_assets}_{self.STAKE_CURRENCY_NAME}_"
                                               f"{str(asset_filter_price).replace('.', ',')}"
                                               f"_minprice_")
                                file_name = os.path.join(
                                    path_prefix, f"{file_prefix}{end_date_config_file}.json")

                                os.makedirs(os.path.dirname(file_name), exist_ok=True)

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
                                        "name": self.EXCHANGE_NAME,
                                        "key": "",
                                        "secret": "",
                                        "pair_whitelist": [],
                                        "pair_blacklist": []
                                    }
                                }
                                data["exchange"]["pair_whitelist"] = whitelist

                                if os.path.exists(file_name):
                                    os.remove(file_name)
                                with open(file_name, "w") as f2:
                                    json.dump(data, f2, indent=4)

                                # If this is the last slice, additionally create a _current.json
                                if index == len(slices) - 1:
                                    last_slice_file_name = os.path.join(
                                        path_prefix, f"{file_prefix}_current.json")
                                    if os.path.exists(last_slice_file_name):
                                        os.remove(last_slice_file_name)
                                    with open(last_slice_file_name, "w") as f2:
                                        json.dump(data, f2, indent=4)

                # print(f"Done {self.exchange}|{single_trading_mode}|{self.STAKE_CURRENCY_NAME}")


if args.exchanges == "":
    exchanges = list_available_exchanges(True)
    # remove any exchange that is named futures to remove duplicates
    # remove bitfinex since bitfinex2 is the same as bitfinex2, but bitfinex2 has the newer api version
    # same for hitbtc and hitbtc3
    # remote gateio since gate is the same (rebranding)
    exchanges_names = [
        exchange["name"] for exchange in exchanges
        if (exchange["name"] != "gateio"
            and exchange["name"] != "bitfinex"
            and exchange["name"] != "hitbtc"
            and "futures" not in exchange["name"]
            )
    ]
else:
    exchanges_names = args.exchanges.split()


# Define a function to process each exchange
def process_exchange(current_exchange):
    Generator().main(current_exchange)


#process_exchange("binance")
# easier debugging, only with one thread
#for exchange_name in exchanges_names:
#    process_exchange(exchange_name)

jobs = args.jobs
if jobs == -1:
    jobs = len(exchanges_names)

if "kraken" in exchanges_names:
    print("We found the exchange kraken, it is assumed you downloaded AND "
                 "converted the csv-data to daily candles already as per "
                 "https://www.freqtrade.io/en/stable/exchanges/#historic-kraken-data . "
                 "it would otherwise take ages to do anything since we would have to "
                 "download all trade data from 2018 up to today with a query limit "
                 "of several seconds each.")

with concurrent.futures.ProcessPoolExecutor(max_workers=jobs) as executor:
    futures = {executor.submit(process_exchange, exchange_name): exchange_name for exchange_name in exchanges_names}

    with tqdm(total=len(exchanges_names), desc="Processing exchanges") as pbar:
        for future in concurrent.futures.as_completed(futures):
            pbar.update(1)
            exchange_name = futures[future]
            del futures[future]
            remaining_tasks = len(futures)
            remaining_exchanges = ", ".join(list(futures.values()))
            pbar.set_description(f"Processing: {remaining_exchanges}")

print("DONE!")
