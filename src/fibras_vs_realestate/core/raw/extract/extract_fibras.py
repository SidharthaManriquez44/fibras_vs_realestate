import yfinance as yf
import pandas as pd
from datetime import datetime
from pandas import DataFrame
from fibras_vs_realestate.config.logger_config import get_logger

logger = get_logger(__name__)

class FibrasExtractor:

    def __init__(self, tickers: list[str], start_date, execution_date: datetime):
        self.tickers = tickers
        self.start_date = start_date
        self.execution_date = execution_date

    def extract_prices(self) -> tuple[DataFrame, list[str]]:

        data = yf.download(
            self.tickers,
            start=self.start_date,
            auto_adjust=True,
            group_by="ticker",
            progress=False
        )

        all_prices = []
        failed = []

        for ticker in self.tickers:
            try:
                if ticker not in data.columns.get_level_values(0):
                    failed.append(ticker)
                    logger.warning(f"Ticker failed append prices: {ticker}")
                    continue

                df = data[ticker].reset_index()

                if df.empty:
                    failed.append(ticker)
                    continue

                df["ticker"] = ticker
                df["extraction_date"] = self.execution_date

                all_prices.append(df)

            except Exception as e:
                error_info = {
                    "stage": "extract_all_prices",
                    "error": str(e),
                    "tickers": ticker
                }
                failed.append(error_info)
                logger.warning(
                    "Extract prices failed",
                    extra={
                        "error": str(e),
                        "tickers": ticker,
                        "failed": failed
                    }
                )

        if not all_prices:
            return pd.DataFrame(), failed

        return pd.concat(all_prices, ignore_index=True), failed


    def extract_dividends(self) -> tuple[DataFrame, list[str]]:

        all_divs = []
        failed = []

        for ticker in self.tickers:
            try:
                divs = yf.Ticker(ticker).dividends

                if divs.empty:
                    failed.append(ticker)
                    logger.warning(f"Ticker failed append dividends: {ticker}")
                    continue

                df = divs.reset_index()
                df["ticker"] = ticker
                df["extraction_date"] = self.execution_date

                all_divs.append(df)


            except Exception as e:
                error_info = {
                    "stage": "extract_all_dividends",
                    "error": str(e),
                    "tickers": ticker
                }
                failed.append(error_info)
                logger.warning(
                    "Extract dividends failed",
                    extra={
                        "error": str(e),
                       "tickers": ticker,
                        "failed": failed
                    }
                )
                logger.warning(f"Failed extract dividends: {failed}: {e}")

        if not all_divs:
            return pd.DataFrame(), failed

        return pd.concat(all_divs, ignore_index=True), failed
