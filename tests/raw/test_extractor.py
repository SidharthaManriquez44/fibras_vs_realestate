import pandas as pd
from unittest.mock import patch
from fibras_vs_realestate.core.raw.extract.extract_fibras import FibrasExtractor


@patch("yfinance.download")
def test_extract_prices(mock_download, execution_date):
    data = pd.DataFrame({("AAA", "Close"): [100], ("AAA", "Volume"): [1000]})

    data.columns = pd.MultiIndex.from_tuples(data.columns)

    mock_download.return_value = data

    extractor = FibrasExtractor(["AAA"], "2020-01-01", execution_date)

    df, failed = extractor.extract_prices()

    assert not df.empty
    assert failed == []
