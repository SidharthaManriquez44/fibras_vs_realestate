import json
from datetime import datetime
import pandas as pd
from fibras_vs_realestate.config.logger_config import get_logger

logger = get_logger(__name__)

class ParquetLoader:

    def __init__(self, datalake, datasets):
        self.datalake = datalake
        self.datasets = datasets
        self.execution_date = datetime.now()

    def save_data(self, layer: str, df: pd.DataFrame, dataset_name: str):
        if df.empty:
            print(f"Skipping {dataset_name}, empty DataFrame")
            return

        year = self.execution_date.year
        month = self.execution_date.month

        path = self.datalake.build_path(
            layer=layer,
            domain=self.datasets.domain,
            dataset=dataset_name,
            year=year,
            month=month
        )

        path.mkdir(parents=True, exist_ok=True)

        file_path = path / f"data_{year}_{month}.parquet"
        df.to_parquet(file_path, index=False)

        logger.info(f"Saved {dataset_name} → {file_path}")

    def save_logs(self, layer: str, logs: dict):
        year = self.execution_date.year
        month = self.execution_date.month

        path = self.datalake.build_path(
            layer= layer,
            domain=self.datasets.domain,
            dataset=self.datasets.logs,
            year=year,
            month=month
        )

        path.mkdir(parents=True, exist_ok=True)

        with open(path / "failed.json", "w") as f:
            json.dump(logs, f, indent=2)

        logger.info(f"Saved logs → {path / 'failed.json'}")
