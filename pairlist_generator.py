import argparse
import concurrent.futures
import copy
import fnmatch
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path

import pandas as pd
from dateutil.relativedelta import *
from freqtrade.data.history.history_utils import (load_pair_history)
from freqtrade.enums import CandleType, MarginMode, TradingMode
from freqtrade.exchange import list_available_exchanges
from tqdm import tqdm

my_env = os.environ.copy()
my_env["COLUMNS"] = str(200)

parser = argparse.ArgumentParser(description="Description of your script")
parser.add_argument(
    "--jobs",
    default=os.cpu_count(),
    type=int,
    help="Number of jobs, default=cores. -1 for unlimited"
)
parser.add_argument(
    "--exchanges",
    type=str,
    help="Space separated list of exchanges",
    default=""
)
parser.add_argument(
    "--download",
    action="store_true",
    help="If set, it just downloads data."
)

parser.add_argument(
    "--pairlist_generation",
    action="store_true",
    help="If set, just generates the pairlists from existing data."
)

# Parse the arguments
args = parser.parse_args()
args_download = args.download
args_pairlist_generation = args.pairlist_generation

class StaticVariables:
    FIAT_currencies = ["USDT", "BUSD", "USDC", "DAI", "TUSD", "FDUSD", "PAX",
                       "USD", "EUR", "GBP", "TRY", "JPY", "NIS", "AUD", "KRW", "BRL"]

    DATE_FORMAT = "%Y%m%d"
    USER_DATA_DIR: str = os.path.join(os.getcwd(), "freqtrade_itself", "user_data")
    pairlists_dir = os.path.join(
        USER_DATA_DIR, "pairlists")

    if not args_download and not args_pairlist_generation:
        logging.error("define one of --download or --pairlist_generation")
        exit()

    if args_pairlist_generation:
        if os.path.isdir(pairlists_dir):
            shutil.rmtree(pairlists_dir)
        os.makedirs(pairlists_dir)

    OUTPUTS_DIR: str = os.path.join(USER_DATA_DIR, "pairlists_output")
    if os.path.exists(OUTPUTS_DIR):
        shutil.rmtree(OUTPUTS_DIR)
    os.makedirs(OUTPUTS_DIR, exist_ok=False)

    DATA_FORMAT = ""

    START_DATE: datetime = datetime.strptime("20171201", DATE_FORMAT)
    END_DATE: datetime = datetime.today().replace(day=1)
    exchange_download_ccxt = {
        "binance": 75,
        "bybit": 100,
        "okx": 200,
        "htx": 100,
        "kucoin": 100,
        "gate": 100,
        "kraken": 750,
        "bitget": 100,
    }


def get_config_download_ccxt(current_exchange):
    ccxt_download_value = StaticVariables.exchange_download_ccxt.get(current_exchange, 200)
    result = os.path.join(StaticVariables.USER_DATA_DIR, "configs_backtest",
                          f"config_ccxt{ccxt_download_value}.json")
    return result


def count_files_in_directory(directory, include_subdirectories=False):
    if include_subdirectories:
        # Use os.walk() to count files recursively in subdirectories
        return sum([len(files) for _, _, files in os.walk(directory)])
    else:
        # List only files in the current directory
        return len([f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))])


class Generator:
    TRADABLE_ONLY = ""
    ACTIVE_ONLY = ""
    STAKE_CURRENCY_NAME = ""
    EXCHANGE_NAME = ""
    TRADING_MODE_NAME = ""

    exchange = ""
    pairlists = ""
    pairs = ""

    FUTURES_ONLY = False
    SPOT_ONLY = True

    def get_candle_type(self):
        if self.TRADING_MODE_NAME == "spot":
            return CandleType.SPOT
        elif self.TRADING_MODE_NAME == "futures":
            return CandleType.FUTURES
        else:
            logging.error("UH OH! WRONG CANDLE TYPE!!")
            exit()

    def __init__(self):
        self.data_location = None
        self.data_location_market = None

        self.pairs_market_currency = []
        os.nice(19) # this is a longer process, so it can't hurt to limit its priority.
        self.INTERVAL_ARR = ["monthly", "yearly", "lifetime"]
        self.ASSET_FILTER_PRICE_ARR = [0.0, 0.01, 0.02, 0.05, 0.15, 0.5]
        self.NUMBER_ASSETS_ARR = [5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 120, 200, 99999]

        self.DATA_FORMAT: str = "feather"

        self.TRADABLE_ONLY: bool = False
        self.ACTIVE_ONLY: bool = False
        self.TRADING_MODES = ["futures", "spot"]

        self.base_volume = None

    def get_data_slices_dates(self, interval):
        start_date = StaticVariables.START_DATE.replace()  # new instance
        end_date = StaticVariables.END_DATE.replace()  # new instance

        if interval == "lifetime":
            time_delta = relativedelta(months=+99)
        elif interval == "yearly":
            time_delta = relativedelta(months=+12)
            start_date = start_date.replace(month=1, day=1)
        elif interval == "monthly":
            time_delta = relativedelta(months=+1)
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

        btc_pairs = [pair for pair in
                     self.pairs_market_currency[self.STAKE_CURRENCY_NAME]
                     if fnmatch.fnmatch(pair, "BTC/*")]

        compared_pairs = [pair for pair in self.pairs_market_currency[self.STAKE_CURRENCY_NAME] if
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
            datadir=Path(self.data_location),
            timeframe="1d",
            pair=btc_pairs[0],
            data_format=self.DATA_FORMAT,
            candle_type=self.get_candle_type(),
        )

        candles_comparison = []
        # just in case those pairs didn't get overlap...
        for compared_pair in compared_pairs:

            candles_comparison = load_pair_history(
                datadir=Path(self.data_location),
                timeframe="1d",
                pair=compared_pair,
                data_format=self.DATA_FORMAT,
                candle_type=self.get_candle_type()
            )
            if len(candles_comparison) > 0:
                break

        if len(candles_btc) == 0 or len(candles_comparison) == 0:
            return

        full_dataframe = pd.merge(candles_btc, candles_comparison, on="date", how="inner")

        # bail if there is no data
        if len(full_dataframe) < 1:
            return

        # Calculate the sum of volume columns for the last 30 rows of candles_BTC
        # adjust if you move off the daily timeframe!
        vol_btc = full_dataframe.tail(30)["volume_x"].sum()
        vol_comparison = full_dataframe.tail(30)["volume_y"].sum()
        returning_value = vol_btc < vol_comparison

        return returning_value

    def process_candles_data(self, filter_price):
        full_dataframe = pd.DataFrame()

        for pair in self.pairs_market_currency[self.STAKE_CURRENCY_NAME]:
            candles = load_pair_history(
                datadir=Path(self.data_location),
                timeframe="1d",
                pair=pair,
                data_format=self.DATA_FORMAT,
                candle_type=self.get_candle_type(),
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
                result[(f"{date_slice['start'].strftime(StaticVariables.DATE_FORMAT)}"
                        f"-{date_slice['end'].strftime(StaticVariables.DATE_FORMAT)}")] \
                    = result_pairs_list

        return result

    def generate_pairs_market_currency(self):
        currency_dict = {}

        for fullpath in Path(self.data_location_market).iterdir():
            if not fullpath.is_file():
                continue  # Skip directories, etc.

            if "-1d" not in fullpath.stem:
                continue
            if "trades" in fullpath.stem:
                continue
            if self.DATA_FORMAT not in fullpath.suffix:
                continue

            base = fullpath.stem.split("-")[0]
            parts = base.split("_")

            currency = parts[1]
            if len(parts) >= 2:
                pair = parts[0] + "/" + parts[1]
                if len(parts) > 2:
                    pair += ":" + ":".join(parts[2:])
            else:
                logging.error("MALFORMED FILE! NOT ACCEPTABLE!")
                exit()

            # Add to dictionary
            currency_dict.setdefault(currency, []).append(pair)

        return currency_dict

    def get_new_data(self):
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
        return data

    def main(self, exchange):
        self.EXCHANGE_NAME = exchange

        for single_trading_mode in self.TRADING_MODES:
            self.TRADING_MODE_NAME = single_trading_mode
            exchange_market_base_folder = os.path.join(
                StaticVariables.pairlists_dir,
                f"{self.EXCHANGE_NAME}_{self.TRADING_MODE_NAME}")
            # Only attempt to run futures exchanges that were actually implemented to not confuse users
            # (even though freqtrade allows downloading data of many more exchanges than those 4)
            # for spot on the other hand ... well, good luck - if they work, they work. If not, not.
            if self.TRADING_MODE_NAME == "futures":
                if self.EXCHANGE_NAME not in [
                    "binance", "okx", "gate", "bybit", "binanceus", "hyperliquid", "bitget"]:
                    continue

            self.data_location: str = (
                str(os.path.join(StaticVariables.USER_DATA_DIR, "data_pairlist_generator",
                                 self.EXCHANGE_NAME)))
            self.data_location_market = self.data_location

            pair_results = [".*/" + currency for currency in StaticVariables.FIAT_currencies]
            if self.TRADING_MODE_NAME == "spot":
                pass
            elif self.TRADING_MODE_NAME == "futures":
                self.data_location_market = str(os.path.join(self.data_location, self.TRADING_MODE_NAME))
                pair_results = [f".*/{currency}:{currency}" for currency in StaticVariables.FIAT_currencies]

            command = [
                sys.executable, f"{os.path.join(os.getcwd(), "freqtrade_itself")}/freqtrade/main.py",
                "download-data",  # Replace this with your actual command
                f"--config={os.path.join(os.getcwd(), "freqtrade_itself", "user_data",
                                         "configs_backtest", "config_baseline.json")}",
                f"--pairs", *pair_results,
                f"--include-inactive-pairs",
                f"--new-pairs-days={days_since_20171101()}",
                f"--exchange={self.EXCHANGE_NAME}",
                f"--timeframes=1d",
                f"--trading-mode={self.TRADING_MODE_NAME}",
                f"--userdir={StaticVariables.USER_DATA_DIR}",
                f"--datadir={self.data_location}",
                f"--data-format-ohlcv={self.DATA_FORMAT}",
            ]
            if self.EXCHANGE_NAME.lower() == "kraken".lower():
                command.append(f"--dl-trades")
                command.append(f"--data-format-trades={self.DATA_FORMAT}")

            command.append(f"--config={get_config_download_ccxt(self.EXCHANGE_NAME)}")

            error_output_filepath = os.path.join(StaticVariables.OUTPUTS_DIR,
                                    f"{self.EXCHANGE_NAME}-{self.TRADING_MODE_NAME}.txt")
            if args_download:
                with (open(error_output_filepath, "w") as outputfile):
                    outputfile.write(" ".join(command) + "\n")
                    process = subprocess.Popen(
                        command,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        env=my_env
                    )

                    def stream_output(pipe, stream_outputfile):
                        for line in pipe:
                            stream_outputfile.write(line)
                            stream_outputfile.flush()

                    # Create and start threads for stdout and stderr
                    stdout_thread = threading.Thread(target=stream_output, args=(process.stdout, outputfile))
                    stderr_thread = threading.Thread(target=stream_output, args=(process.stderr, outputfile))

                    stdout_thread.start()
                    stderr_thread.start()

                    # Wait for both threads and the process to finish
                    stdout_thread.join()
                    stderr_thread.join()
                    process.wait()
                exit_code = process.returncode

                tail_of_logs = ""
                try:
                    with open(error_output_filepath, "rb") as f:
                        f.seek(-400, os.SEEK_END)
                        tail_of_logs = f.read().decode("utf-8", errors="ignore")
                except OSError:
                    raise RuntimeError("Failed to read output file for completion check.")

                if exit_code == 0:
                    pass
                    #logging.info(f"{self.EXCHANGE_NAME}-{self.TRADING_MODE_NAME}: process completed successfully.")
                elif exit_code < 0:
                    logging.error(f"{self.EXCHANGE_NAME}-{self.TRADING_MODE_NAME}: process killed by signal {-exit_code}.")
                    continue
                elif "ccxt library does not provide" in tail_of_logs:
                    # logging.info(f"{self.EXCHANGE_NAME}-{self.TRADING_MODE_NAME}: process completed "
                    #             f"without success since ccxt itself does not support this exchange + market combo.")
                    pass  # no need to spam this,
                elif "will not work with Freqtrade." in tail_of_logs:
                    #logging.info(f"{self.EXCHANGE_NAME}-{self.TRADING_MODE_NAME}: process completed "
                    #             f"without success since freqtrade does not support downloading there.")
                    pass # no need to spam this,
                else:
                    logging.error(f"{self.EXCHANGE_NAME}-{self.TRADING_MODE_NAME}: process exited with code {exit_code}. ")
                    continue
            '''
            datadir_count = count_files_in_directory(self.data_location, True)
            if datadir_count == 0:
                os.rmdir(self.data_location)
                continue
            datadir_count_direct = count_files_in_directory(self.data_location, False)
            if datadir_count_direct == 0:
                continue
            '''
            if args_pairlist_generation:
                logging.info(f"starting to create the pairlists of {self.EXCHANGE_NAME}-{self.TRADING_MODE_NAME}")
                self.pairs_market_currency = self.generate_pairs_market_currency()
                for single_currency_name in self.pairs_market_currency.keys():
                    self.STAKE_CURRENCY_NAME = single_currency_name

                    self.base_volume = self.calculate_is_base_volume()
                    if self.base_volume is None:
                        continue

                    for asset_filter_price in self.ASSET_FILTER_PRICE_ARR:

                        volume_dataframe = self.process_candles_data(asset_filter_price)

                        if volume_dataframe.empty:
                            continue
                        for number_assets in self.NUMBER_ASSETS_ARR:

                            for interval in self.INTERVAL_ARR:
                                date_slices = self.get_data_slices_dates(interval)
                                path_prefix = os.path.join(exchange_market_base_folder,
                                    f"{self.STAKE_CURRENCY_NAME}",
                                    f"{interval}"
                                )
                                all_slices_pairs_data = self.get_new_data()
                                slices = self.process_date_slices(volume_dataframe, date_slices, number_assets)
                                for index, (timerange, current_slice) in enumerate(slices.items()):
                                    end_date_config_file = timerange.split("-")[1]
                                    end_date_dt = datetime.strptime(end_date_config_file, "%Y%m%d")

                                    if interval == "yearly":
                                        if end_date_dt.month > 1:
                                            # skip if end is not the 1st of january and if is yearly
                                            continue

                                    whitelist = current_slice
                                    file_prefix = (f"{interval}_{number_assets}_{self.STAKE_CURRENCY_NAME}_"
                                                   f"{str(asset_filter_price).replace('.', ',')}"
                                                   f"_minprice_")
                                    file_name = os.path.join(
                                        path_prefix, f"{file_prefix}{end_date_config_file}.json")

                                    data = self.get_new_data()
                                    data["exchange"]["pair_whitelist"] = whitelist
                                    if len(whitelist) == 0:
                                        continue

                                    for pair in whitelist:

                                        if pair not in all_slices_pairs_data["exchange"]["pair_whitelist"]:
                                            all_slices_pairs_data["exchange"]["pair_whitelist"].append(pair)
                                    if os.path.exists(file_name):
                                        os.remove(file_name)
                                    # it would be useless to have a current date and lifetime file with the same content
                                    if interval != "lifetime":
                                        os.makedirs(os.path.dirname(file_name), exist_ok=True)
                                        with open(file_name, "w") as f2:
                                            json.dump(data, f2, indent=4)

                                    # If this is the last slice, additionally create a _current.json
                                    if index == len(slices) - 1:
                                        last_slice_file_name = os.path.join(
                                            path_prefix, f"{file_prefix}current.json")

                                        if os.path.exists(last_slice_file_name):
                                            os.remove(last_slice_file_name)

                                        os.makedirs(os.path.dirname(last_slice_file_name), exist_ok=True)
                                        with open(last_slice_file_name, "w") as f3:
                                            json.dump(data, f3, indent=4)

                                        all_slices_pairs_file_name = os.path.join(
                                            path_prefix, f"{file_prefix}all_slices_pairs.json")
                                        if os.path.exists(all_slices_pairs_file_name):
                                            os.remove(all_slices_pairs_file_name)

                                        os.makedirs(os.path.dirname(all_slices_pairs_file_name), exist_ok=True)
                                        with open(all_slices_pairs_file_name, "w") as f4:
                                            json.dump(all_slices_pairs_data, f4, indent=4)

                if os.path.isdir(exchange_market_base_folder):
                    # Count files recursively inside that folder
                    file_count = sum(len(files) for _, _, files in os.walk(exchange_market_base_folder))

                    # If there are no files, remove the directory tree
                    if file_count == 0:
                        shutil.rmtree(exchange_market_base_folder)
                        print(f"Removed empty folder: {exchange_market_base_folder}")

if args.exchanges == "":
    exchanges = list_available_exchanges(True)
    # remove any exchange that is named futures to remove duplicates
    # remove bitfinex since bitfinex2 is the same as bitfinex2, but bitfinex2 has the newer api version
    # same for hitbtc and hitbtc3
    # remote gateio since gate is the same (rebranding)
    exchanges_names = [
        exchange["classname"] for exchange in exchanges
        if (exchange["classname"] != "gateio"
            and exchange["classname"] != "bitfinex"
            and exchange["classname"] != "hitbtc"
            and "futures" not in exchange["classname"]
            )
    ]
else:
    exchanges_names = args.exchanges.split(" ")


# Define a function to process each exchange
def process_exchange(current_exchange):
    Generator().main(current_exchange)


def days_since_20171101():
    from datetime import datetime
    start_date = datetime(2017, 11, 1)
    today = datetime.today()
    result = (today - start_date).days
    return result


#process_exchange("binance")
# easier debugging, only with one thread
#for exchange_name in exchanges_names:
#    process_exchange(exchange_name)

jobs = args.jobs
if jobs == -1:
    jobs = len(exchanges_names)

if "kraken" in exchanges_names:
    print("We found the exchange kraken, it is assumed you downloaded AND "
          "converted the csv-data to trades already as per "
          "https://www.freqtrade.io/en/stable/exchanges/#historic-kraken-data . "
          "it would otherwise take AGES to do anything since we would have to "
          "download all trade data from 2018 up to today with a query limit.")
    # removed it since the cpu is overwhelmed, pushed back to the very last one.
    # Since kraken does translate from trades to candles, this takes GIANT amounts of RAM comparably to other exchanges.
    # By putting it in the back (not in the front) this should be mitigated greatly,
    # since it s assumed that by the point kraken translates USDT/USD from trades to candles,
    # the other exchanges are done already.
    # couldn't solve it with an sbc with 4GB RAM and 12GB ZRAMSwap and 10GB SWAP so ... let's just wait a little longer
    # ...
    exchanges_names.remove("kraken")
    exchanges_names.insert(0, "kraken")    # let's see if it s already in an OK place for the poor sbc,
    # this would just be the last ditch attempt to make it work on a low ram sbc.
    # exchanges_names.remove("kraken")
    # exchanges_names.append("kraken")

if jobs == 1:
    for exchange_name in exchanges_names:
        process_exchange(exchange_name)
else:
    with concurrent.futures.ProcessPoolExecutor(max_workers=jobs) as executor:
        futures = {executor.submit(process_exchange, exchange_name): exchange_name for exchange_name in exchanges_names}

        with tqdm(total=len(exchanges_names), desc="Processing exchanges") as pbar:
            for future in concurrent.futures.as_completed(futures):
                pbar.update(1)
                exchange_name = futures[future]
                del futures[future]
                remaining_tasks = len(futures)
                remaining_exchanges = ", ".join(list(futures.values()))
                logging.info(f"completed processing of the exchange {exchange_name}")
                pbar.set_description(f"Processing: {remaining_exchanges}")


print("DONE!")
