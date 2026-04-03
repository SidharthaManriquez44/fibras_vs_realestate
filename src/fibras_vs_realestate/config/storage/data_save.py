from datetime import datetime
import pandas as pd
from fibras_vs_realestate.config.logger_config import get_logger
from fibras_vs_realestate.config.storage.storage_backend import StorageBackend
from fibras_vs_realestate.config.path.path_builder import DataLakePathBuilder


logger = get_logger(__name__)


class DataSaver:
    def __init__(
        self,
        storage: StorageBackend,
        path_builder: DataLakePathBuilder,
        execution_date: datetime,
    ):
        self.storage = storage
        self.path_builder = path_builder
        self.execution_date = execution_date

    def _build_path(self, layer, domain, dataset):
        return self.path_builder.build_path(
            layer=layer,
            domain=domain,
            dataset=dataset,
            year=self.execution_date.year,
            month=self.execution_date.month,
        )

    def save_parquet(self, layer, domain, dataset, df: pd.DataFrame):
        if df.empty:
            logger.warning(
                "empty_dataframe_skipped",
                extra={"dataset": dataset, "layer": layer, "domain": domain},
            )
            return df

        path = self._build_path(layer, domain, dataset)

        try:
            self.storage.save_parquet(path, df)

            logger.info(
                "parquet_saved",
                extra={
                    "dataset": dataset,
                    "layer": layer,
                    "domain": domain,
                    "path": str(path),
                    "rows": len(df),
                },
            )

        except Exception:
            logger.exception("parquet_save_failed")
            raise

    def save_json(self, layer, domain, dataset, data: dict):
        if not data:
            logger.warning(
                "empty_dataframe_skipped",
                extra={"dataset": dataset, "layer": layer, "domain": domain},
            )
            return dict

        path = self._build_path(layer, domain, dataset)

        try:
            self.storage.save_json(path, data)

            logger.info(
                "json_saved",
                extra={
                    "dataset": dataset,
                    "layer": layer,
                    "domain": domain,
                    "path": str(path),
                    "rows": len(data),
                },
            )

        except Exception:
            logger.exception("json_save_failed")
            raise
