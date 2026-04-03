from airflow.decorators import task, dag
import pandas as pd
from datetime import datetime
from fibras_vs_realestate.config.settings import START_DATE, DATASETS
from fibras_vs_realestate.config.financial.tickers import TICKERS
from fibras_vs_realestate.config.layers.layers import layer

from fibras_vs_realestate.core.raw.transform.transform_fibras import FibrasTransformer
from fibras_vs_realestate.core.raw.extract.extract_fibras import FibrasExtractor
from fibras_vs_realestate.config.data_pipeline.data_pipeline import DATA_PIPELINES
from fibras_vs_realestate.core.quality.raw_data_validator import validate_data
from fibras_vs_realestate.config.parquet.save_parquet import save_parquet


@task
def extract(dataset_name: str, **context) -> pd.DataFrame:
    execution_date = context["execution_date"]

    extractor = FibrasExtractor(TICKERS.fibras, START_DATE, execution_date)

    if dataset_name == "prices":
        df, _ = extractor.extract_prices()
    elif dataset_name == "dividends":
        df, _ = extractor.extract_dividends()
    else:
        raise ValueError(f"Unknown dataset {dataset_name}")

    return df


@task
def transform(dataset_name: str, df: pd.DataFrame) -> pd.DataFrame:
    transformer = FibrasTransformer()

    if dataset_name == "prices":
        return transformer.transform_prices(df)
    elif dataset_name == "dividends":
        return transformer.transform_dividends(df)
    else:
        raise ValueError(f"Unknown dataset: {dataset_name}")


@task
def load(df: pd.DataFrame, dataset, **context) -> str:
    execution_date = context["execution_date"]

    return save_parquet(layer.raw, DATASETS.domain, dataset, df, execution_date=execution_date)


@task
def validate(df: pd.DataFrame, contract, dataset):
    validate_data(df, contract, dataset)


# -------------------
# DAG AFTER
# -------------------


@dag(
    dag_id="raw_fibras_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bronze", "fibras"],
)
def raw_fibras_pipeline():
    def build_pipeline(pipeline):
        data = extract(pipeline["name"])
        data_t = transform(pipeline["name"], data)

        validate(data_t, pipeline["contract"], pipeline["dataset"])

        return load(data_t, pipeline["dataset"])

    for pipe in DATA_PIPELINES:
        build_pipeline(pipe)


dag = raw_fibras_pipeline()
