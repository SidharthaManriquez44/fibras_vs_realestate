import json
import pandas as pd
from datetime import datetime
from fibras_vs_realestate.config.logger_config import get_logger

logger = get_logger(__name__)


class DataSaver:

    def __init__(self, datalake):
        self.datalake = datalake

    def _build_partition_path(self, layer, domain, dataset, execution_date):
        year = execution_date.year
        month = execution_date.month

        path = self.datalake.build_path(
            layer=layer,
            domain=domain,
            dataset=dataset,
            year=year,
            month=month,
        )

        path.mkdir(parents=True, exist_ok=True)
        return path, year, month

    def save_parquet(
        self,
        layer: str,
        domain: str,
        dataset: str,
        df: pd.DataFrame,
        execution_date: datetime,
    ):
        if df.empty:
            logger.warning(
                "empty_dataframe_skipped",
                extra={"dataset": dataset, "layer": layer, "domain": domain},
            )
            return
        try:
            path, _, _ = self._build_partition_path(
                layer, domain, dataset, execution_date
            )

            file_path = path / f"data_{execution_date.strftime('%Y%m%d_%H%M%S')}.parquet"
            df.to_parquet(file_path, index=False)

            logger.info(
                "parquet_saved",
                extra={
                    "dataset": dataset,
                    "layer": layer,
                    "domain": domain,
                    "path": str(file_path),
                    "rows": len(df),
                    "columns": len(df.columns),
                },
            )


        except Exception:
            logger.exception(
                "parquet_save_failed",
                extra={"dataset": dataset, "layer": layer, "domain": domain},
            )
            raise

    def save_json(
            self,
            layer: str,
            domain: str,
            dataset: str,
            data: dict,
            execution_date: datetime,
            filename: str = None,
    ):
        try:
            path, _, _ = self._build_partition_path(
                layer, domain, dataset, execution_date
            )

            if not filename:
                filename = f"data_{execution_date.strftime('%Y%m%d_%H%M%S')}.json"

            file_path = path / filename

            with open(file_path, "w") as f:
                json.dump(data, f, indent=2, default=str)

            logger.info(
                "json_saved",
                extra={
                    "dataset": dataset,
                    "layer": layer,
                    "domain": domain,
                    "path": str(file_path),
                },
            )

        except Exception:
            logger.exception(
                "json_save_failed",
                extra={"dataset": dataset, "layer": layer, "domain": domain},
            )
            raise
