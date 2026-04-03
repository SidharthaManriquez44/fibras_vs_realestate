import pytest
from fibras_vs_realestate.core.quality.data_validator import DataValidator


def test_validate_not_empty(sample_prices_df):
    DataValidator.validate_not_empty(sample_prices_df)


def test_validate_empty_df():
    import pandas as pd

    with pytest.raises(ValueError):
        DataValidator.validate_not_empty(pd.DataFrame())


def test_validate_schema_ok(sample_prices_df):
    expected = ["date", "ticker", "close", "volume", "source", "extraction_date"]
    DataValidator.validate_schema(sample_prices_df, expected)


def test_validate_schema_missing_column(sample_prices_df):
    expected = ["date", "ticker", "close"]

    df = sample_prices_df.drop(columns=["close"])

    with pytest.raises(ValueError):
        DataValidator.validate_schema(df, expected)
