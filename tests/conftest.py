from pathlib import Path
import pytest
import pandas as pd
from datetime import datetime


@pytest.fixture
def sample_prices_df():
    return pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02"],
            "ticker": ["AAA", "AAA"],
            "close": [100.0, 101.0],
            "volume": [1000, 2000],
            "source": ["yahoo", "yahoo"],
            "extraction_date": [datetime.now(), datetime.now()],
        }
    )


@pytest.fixture
def sample_dividends_df():
    return pd.DataFrame(
        {
            "date": ["2024-01-01"],
            "ticker": ["AAA"],
            "dividend": [1.5],
            "source": ["yahoo"],
            "extraction_date": [datetime.now()],
        }
    )


@pytest.fixture
def sample_log():
    return dict(
        date="2024-01-01",
        ticker="AAA",
        extraction_date=datetime.now(),
    )


@pytest.fixture
def execution_date():
    return datetime(2024, 1, 1)


@pytest.fixture
def fake_path(
    layer="raw",
    domain="fibras",
    dataset=pd.DataFrame(
        {
            "date": ["2024-01-01"],
            "ticker": ["AAA"],
            "dividend": [1.5],
            "source": ["yahoo"],
            "extraction_date": [datetime.now()],
        }
    ),
    year="2026",
    month="3",
):
    return Path(__file__).parent / layer / domain / dataset / year / month
