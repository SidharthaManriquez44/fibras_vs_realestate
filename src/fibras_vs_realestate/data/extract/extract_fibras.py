from typing import Any
import yfinance as yf
import pandas as pd
from datetime import datetime
import json
from pandas import DataFrame

from fibras_vs_realestate.config.logger_config import get_logger
from fibras_vs_realestate.config.settings import TICKERS, RAW_DIR, START_DATE

logger = get_logger(__name__)

class FibrasExtractor:

    def __init__(self, tickers: list, start_date: str):
        self.tickers = tickers
        self.start_date = start_date
        self.execution_date = datetime.now()

    # -------------------------
    # VALIDATE
    # -------------------------
    def validate_tickers(self) -> tuple[list[str], list[str]]:

        valid_tickers = []
        invalid_tickers = []

        for ticker in self.tickers:
            try:
                ticker_obj = yf.Ticker(ticker)
                hist = ticker_obj.history(period="1d")

                if hist.empty:
                    invalid_tickers.append(ticker)
                    continue

                valid_tickers.append(ticker)

            except Exception as e:
                invalid_tickers.append(ticker)
                logger.warning(f"Validation error with {ticker}: {e}")

        logger.info(f"Valid tickers: {valid_tickers}")
        logger.warning(f"Invalid tickers: {invalid_tickers}")

        return valid_tickers, invalid_tickers

    # -------------------------
    # PRICES
    # -------------------------
    def extract_prices(self) -> tuple[DataFrame, list[Any]]:

        data = yf.download(
            self.tickers,
            start=self.start_date,
            auto_adjust=True,
            group_by="ticker",
            progress=False
        )

        all_prices = []
        failed_tickers = []

        for ticker in self.tickers:
            try:
                if ticker not in data.columns.get_level_values(0):
                    failed_tickers.append(ticker)
                    continue

                df = data[ticker].reset_index()

                if (
                        df.empty
                        or "Close" not in df.columns
                        or df["Close"].isna().all()
                        or df["Volume"].isna().all()
                ):
                    failed_tickers.append(ticker)
                    continue

                df = df.rename(columns={
                    "Date": "date",
                    "Close": "close",
                    "Volume": "volume"
                })

                df["ticker"] = ticker
                df["source"] = "yahoo_finance"
                df["extraction_date"] = self.execution_date

                df = df[["date", "ticker", "close", "volume", "source", "extraction_date"]]

                all_prices.append(df)


                logger.info(f"{ticker} in data: {ticker in data.columns.get_level_values(0)}")

            except Exception as e:
                failed_tickers.append(ticker)
                logger.warning(f"Error with {ticker}: {e}")

        if failed_tickers:
            logger.warning(f"Error with (prices): {failed_tickers}")

        if not all_prices:
            return pd.DataFrame(), failed_tickers
        logger.info(f"Columns: {data.columns}")
        return pd.concat(all_prices, ignore_index=True), failed_tickers

    # -------------------------
    # DIVIDENDS
    # -------------------------
    def extract_dividends(self) -> tuple[DataFrame, list[Any]]:

        all_divs = []
        failed_divs = []

        for ticker in self.tickers:
            try:
                ticker_obj = yf.Ticker(ticker)
                divs = ticker_obj.dividends

                if divs.empty:
                    failed_divs.append(ticker)
                    continue
                divs = divs.reset_index()

                if divs is None or divs.empty:
                    continue

                divs = divs.rename(columns={
                    "Date": "date",
                    "Dividends": "dividend"
                })

                divs["ticker"] = ticker
                divs["source"] = "yahoo_finance"
                divs["extraction_date"] = self.execution_date

                divs = divs[["date", "ticker", "dividend", "source", "extraction_date"]]

                all_divs.append(divs)

            except Exception as e:
                failed_divs.append(ticker)
                logger.warning(f"Error with {ticker}: {e}")

        if failed_divs:
            logger.warning(f"Failed tickers (dividends): {failed_divs}")

        if not all_divs:
            return pd.DataFrame(), failed_divs

        return pd.concat(all_divs, ignore_index=True), failed_divs

    # -------------------------
    # SAVE (DATA LAKE)
    # -------------------------
    def save(self, df: pd.DataFrame, dataset_name: str, failed_tickers=None):

        year = self.execution_date.year
        month = self.execution_date.month

        path = RAW_DIR  / "fibras" / dataset_name / f"year={year}" / f"month={month}"
        path.mkdir(parents=True, exist_ok=True)

        log_path = RAW_DIR / "fibras" / "_logs"
        log_path.mkdir(parents=True, exist_ok=True)

        file_path = path / "data.parquet"

        df.to_parquet(file_path, index=False)
        logger.info(f"Saved {dataset_name} data to {file_path}")

        log_file = log_path / f"failed_{dataset_name}.json"

        with open(log_file, "w") as f:
            json.dump(failed_tickers or [], f, indent=2)

    def save_invalid_tickers(self, invalid_tickers):

        log_path = RAW_DIR / "fibras" / "_logs"
        log_path.mkdir(parents=True, exist_ok=True)

        with open(log_path / "invalid_tickers.json", "w") as f:
            json.dump(invalid_tickers, f, indent=2)

    # -------------------------
    # RUN
    # -------------------------
    def run(self):

        valid_tickers, invalid_tickers = self.validate_tickers()
        self.save_invalid_tickers(invalid_tickers)

        self.tickers = valid_tickers

        prices, failed_prices = self.extract_prices()
        self.save(prices, "prices", failed_prices)

        dividends, failed_divs = self.extract_dividends()
        self.save(dividends, "dividends", failed_divs)

        return prices, dividends

if __name__ == "__main__":
    extractor = FibrasExtractor(TICKERS, START_DATE)
    extractor.run()

