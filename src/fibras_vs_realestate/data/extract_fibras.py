import yfinance as yf
import pandas as pd
from pathlib import Path

TICKERS = [
    "FUNO11.MX",
    "FMTY14.MX",
    "FIBRAPL14.MX",
    "TERRA13.MX",
    "DANHOS13.MX",
    "FIBRAHD15.MX",
    "FIHO12.MX",
    "STORAGE18.MX",
    "EDUCA18.MX",
    "SHOP13.MX",
    "NOVA13.MX"
]

START_DATE = "2011-01-01"

def extract_reit_prices():

    data = yf.download(
        TICKERS,
        start=START_DATE,
        auto_adjust=True,
        group_by="ticker"
    )

    prices = []

    for ticker in TICKERS:
        df = data[ticker].reset_index()
        df["ticker"] = ticker
        df = df[["Date", "ticker", "Close", "Volume"]]
        prices.append(df)

    prices = pd.concat(prices)

    output = Path("data/external/reit_prices.parquet")
    prices.to_parquet(output)

    return prices

def extract_dividends():

    all_divs = []

    for ticker in TICKERS:
        ticker_obj = yf.Ticker(ticker)
        divs = ticker_obj.dividends.reset_index()

        divs["ticker"] = ticker
        divs.columns = ["date", "dividend", "ticker"]

        all_divs.append(divs)

    dividends = pd.concat(all_divs)

    dividends.to_parquet("data/external/reit_dividends.parquet")

    return dividends