import json
import pandas as pd
from datetime import datetime
from fibras_vs_realestate.config.logger_config import get_logger

logger = get_logger(__name__)


class DataSaver:
    def __init__(self, datalake, execution_date:datetime):
        self.datalake = datalake
        self.execution_date = execution_date

    def save_parquet(
        self,
        layer,
        domain,
        dataset,
        df: pd.DataFrame

    ):
        if df.empty:
            logger.warning(
                "empty_dataframe_skipped",
                extra={"dataset": dataset, "layer": layer, "domain": domain},
            )
            return
        try:
            year = self.execution_date.year
            month = self.execution_date.month
            path = self.datalake.build_path(
                layer,
                domain,
                dataset,
                year,
                month
            )

            path.mkdir(parents=True, exist_ok=True)
            file_path = path/ f"data_{ self.execution_date.strftime('%Y%m%d_%H%M%S')}.parquet"
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
            layer,
            domain,
            dataset,
            data: dict
    ):
        try:
            year = self.execution_date.year
            month = self.execution_date.month
            path = self.datalake.build_path(
                layer,
                domain,
                dataset,
                year,
                month
            )

            path.mkdir(parents=True, exist_ok=True)
            filename =  f"data_{self.execution_date.strftime('%Y%m%d_%H%M%S')}.json"
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
