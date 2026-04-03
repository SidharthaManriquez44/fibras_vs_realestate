from fibras_vs_realestate.core.raw.transform.transform_fibras import FibrasTransformer


def test_transform_prices(sample_prices_df):
    transformer = FibrasTransformer()

    df = transformer.transform_prices(sample_prices_df)

    assert "date" in df.columns
    assert "ticker" in df.columns
    assert "close" in df.columns
    assert "volume" in df.columns


def test_transform_dividends(sample_dividends_df):
    transformer = FibrasTransformer()

    df = transformer.transform_dividends(sample_dividends_df)

    assert "date" in df.columns
    assert "ticker" in df.columns
    assert "dividend" in df.columns
    assert "source" in df.columns
