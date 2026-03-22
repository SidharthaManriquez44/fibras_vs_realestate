import pandas as pd
from datetime import datetime

from fibras_vs_realestate.config.logger_config import get_logger
from fibras_vs_realestate.config.storage.paths import get_paths
from fibras_vs_realestate.config.settings import DatasetNames
from fibras_vs_realestate.config.storage.path_builder import DataLakePathBuilder


logger = get_logger(__name__)

PATHS = get_paths()
datalake = DataLakePathBuilder(PATHS)


def build_total_return_dataset():
    execution_date = datetime.today()
    year = execution_date.year
    month = execution_date.month

    path_price = datalake.get_processed_path(
        DatasetNames.domain,
        DatasetNames.prices,
        year,
        month
    )
    if not path_price.exists():
        raise FileNotFoundError(f"Missing prices data: {path_price}")

    path_div = datalake.get_processed_path(
        DatasetNames.domain,
        DatasetNames.dividends,
        year,
        month
    )
    if not path_div.exists():
        raise FileNotFoundError(f"Missing dividends data: {path_div}")

    try:
        prices = pd.read_parquet(path_price)
        dividends = pd.read_parquet(path_div)

        df = prices.merge(
            dividends,
            how="left",
            on=["date", "ticker"]
        )

        df["dividend"] = df["dividend"].fillna(0)

        return df

    except Exception as e:
        logger.exception(f"Error building total return dataset: {e}")
        raise
