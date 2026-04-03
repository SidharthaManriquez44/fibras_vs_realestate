from datetime import datetime
from pathlib import Path
from fibras_vs_realestate.config.settings import DATASETS
from tests.config.conf_data_save import DataSaver


class DummyDataLake:
    def __init__(self, base_path):
        self.base_path = base_path

    def build_path(self, layer, domain, dataset, year, month):
        return self.base_path / layer / domain / dataset / f"year={year}" / f"month={month}"


class DummyDatasets:
    domain = "test_domain"
    prices = "prices"
    dividends = "dividends"
    logs = "_logs"


def test_save_price_data(tmp_path, sample_prices_df, execution_date: datetime):
    execution_date = datetime.now()
    datalake = DummyDataLake(tmp_path)
    datasets = DummyDatasets()

    loader = DataSaver(datalake, execution_date)

    loader.save_parquet("raw", datasets.domain, datasets.prices, sample_prices_df)

    files = list(tmp_path.rglob("*.parquet"))

    assert len(files) == 1


def test_save_dividends_data(tmp_path, sample_dividends_df, execution_date: datetime):
    execution_date = datetime.now()
    datalake = DummyDataLake(tmp_path)
    datasets = DummyDatasets()

    loader = DataSaver(datalake, execution_date)

    loader.save_parquet("raw", datasets.domain, datasets.prices, sample_dividends_df)

    files = list(tmp_path.rglob("*.parquet"))

    assert len(files) == 1


def test_save_logs(tmp_path, sample_log, execution_date: datetime):
    execution_date = datetime.now()
    datalake = DummyDataLake(tmp_path)
    datasets = DummyDatasets()

    loader = DataSaver(datalake, execution_date)

    loader.save_json("raw", datasets.domain, datasets.prices, sample_log)

    files = list(tmp_path.rglob("*.json"))

    assert len(files) == 1


def test_build_partition_path(tmp_path):
    lake = DummyDataLake(tmp_path)
    lake.build_path("raw", DATASETS.domain, DATASETS.prices, "2206", "3")
    expected_path = Path("../../data/raw", DATASETS.domain, DATASETS.prices, "2206", "3")
    assert lake.base_path == expected_path
