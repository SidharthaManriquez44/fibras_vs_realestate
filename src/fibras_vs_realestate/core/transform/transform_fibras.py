import pandas as pd
from fibras_vs_realestate.config.tables.columns import columns

class FibrasTransformer:

    @staticmethod
    def transform_prices(df: pd.DataFrame) -> pd.DataFrame:

        df = df.rename(columns={
            "Date": "date",
            "Close": "close",
            "Volume": "volume"
        })

        df["source"] = "yahoo_finance"

        return df[columns.price_columns]


    @staticmethod
    def transform_dividends(df: pd.DataFrame) -> pd.DataFrame:

        df = df.rename(columns={
            "Date": "date",
            "Dividends": "dividend"
        })

        df["source"] = "yahoo_finance"

        return df[columns.dividend_columns]
