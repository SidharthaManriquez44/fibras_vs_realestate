import pandas as pd
from fibras_vs_realestate.config.logger_config import get_logger

logger = get_logger(__name__)


class ReturnBuilder:
    @staticmethod
    def _standardize_dates(df: pd.DataFrame, col: str) -> pd.DataFrame:
        df = df.copy()
        df[col] = pd.to_datetime(df[col])

        if isinstance(df[col].dtype, pd.DatetimeTZDtype):
            df[col] = df[col].dt.tz_localize(None)

        return df

    @staticmethod
    def merge_prices_dividends(prices: pd.DataFrame, dividends: pd.DataFrame) -> pd.DataFrame:
        prices = ReturnBuilder._standardize_dates(prices, "date")
        dividends = ReturnBuilder._standardize_dates(dividends, "date")

        df = prices.merge(dividends, how="left", on=["date", "ticker"])

        df["dividend"] = df["dividend"].fillna(0)

        return df
