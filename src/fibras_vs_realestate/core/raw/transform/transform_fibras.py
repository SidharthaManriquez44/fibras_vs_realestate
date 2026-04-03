import pandas as pd
from fibras_vs_realestate.config.data_contracts.contracts import PRICE_CONTRACT, DIVIDEND_CONTRACT


class FibrasTransformer:
    @staticmethod
    def transform_prices(df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(columns={"Date": "date", "Close": "close", "Volume": "volume"})

        df["source"] = "yahoo_finance"

        return df[PRICE_CONTRACT.columns]

    @staticmethod
    def transform_dividends(df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(columns={"Date": "date", "Dividends": "dividend"})

        df["source"] = "yahoo_finance"

        return df[DIVIDEND_CONTRACT.columns]
