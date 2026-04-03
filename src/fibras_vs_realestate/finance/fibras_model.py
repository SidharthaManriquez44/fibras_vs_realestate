import pandas as pd
import numpy as np


def build_total_return_dataset():
    prices = pd.read_parquet("data/external/reit_prices.parquet")
    dividends = pd.read_parquet("data/external/reit_dividends.parquet")

    df = prices.merge(dividends, how="left", on=["date", "ticker"])

    df["dividend"] = df["dividend"].fillna(0)

    return df


def calculate_total_return(df):
    df = df.sort_values(["ticker", "date"])

    df["price_return"] = df.groupby("ticker")["Close"].pct_change()

    df["dividend_yield"] = df["dividend"] / df["Close"]

    df["total_return"] = df["price_return"] + df["dividend_yield"]

    return df


def calculate_log_returns(df):
    df["log_return"] = np.log(1 + df["total_return"])

    return df
