import pandas as pd
from fibras_vs_realestate.config.logger_config import get_logger

logger = get_logger(__name__)


class DataValidator:
    @staticmethod
    def validate_schema(df: pd.DataFrame, expected_columns: list[str]):
        missing = set(expected_columns) - set(df.columns)
        extra = set(df.columns) - set(expected_columns)

        if missing:
            raise ValueError(f"Missing columns: {missing}")

        if extra:
            logger.error(f"Extra columns: {extra}")

    @staticmethod
    def validate_not_null(df: pd.DataFrame, columns: list[str]):
        nulls = df[columns].isnull().sum()

        bad_cols = nulls[nulls > 0]

        if not bad_cols.empty:
            logger.error(f"Null columns: {bad_cols}")

    @staticmethod
    def validate_not_empty(df: pd.DataFrame):
        if df.empty:
            raise ValueError("DataFrame is empty")

    @staticmethod
    def validate_duplicates(df: pd.DataFrame, subset: list[str]):
        duplicates = df.duplicated(subset=subset).sum()

        if duplicates > 0:
            logger.error(f"Duplicate columns: {duplicates}")

    @staticmethod
    def business_price_validation(prices):
        if (prices["close"] < 0).any():
            logger.error("Negative prices detected")

        if (prices["volume"] < 0).any():
            logger.error("Negative volumes detected")

    @staticmethod
    def business_dividend_validation(divs):
        if (divs["dividend"] < 0).any():
            logger.error("Negative dividends detected")

    @staticmethod
    def validate_no_timezone(df, col):
        if "datetime64[ns," in str(df[col].dtype):
            raise ValueError(f"{col} has timezone — fix upstream")
