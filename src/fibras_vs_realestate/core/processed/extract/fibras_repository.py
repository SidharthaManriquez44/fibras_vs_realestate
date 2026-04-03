from typing import Any

import pandas as pd
from datetime import datetime
from pathlib import Path
from pandas import DataFrame

from fibras_vs_realestate.config.layers.layers import Layer
from fibras_vs_realestate.config.path.path_builder import DataLakePathBuilder
from fibras_vs_realestate.config.logger_config import get_logger

logger = get_logger(__name__)


class FibrasRepository:
    def __init__(self, path_builder: DataLakePathBuilder, datasets, execution_date: datetime):
        self.path_builder = path_builder
        self.datasets = datasets
        self.execution_date = execution_date

    def _build_dataset_path(self, dataset_name: str) -> Path:
        return self.path_builder.build_path(
            layer=Layer.raw,
            domain=self.datasets.domain,
            dataset=dataset_name,
            year=self.execution_date.year,
            month=self.execution_date.month,
        )

    @staticmethod
    def _read_parquet_folder(path: Path) -> DataFrame | tuple[DataFrame, list[Any]]:
        if not path.exists():
            logger.warning(f"Path does not exist: {path}")
            return pd.DataFrame()

        files = list(path.glob("*.parquet"))

        if not files:
            logger.warning(f"No parquet files found in: {path}")
            return pd.DataFrame()

        logger.info(f"Reading {len(files)} files from {path}")

        dfs = []

        for f in files:
            try:
                dfs.append(pd.read_parquet(f))
            except Exception(KeyError, FileExistsError) as e:
                logger.error(f"Error reading {f}: {e}")

        if not dfs:
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True)

    @staticmethod
    def _get_sorted_files(path: Path):
        files = list(path.glob("*.parquet"))

        if not files:
            return []

        files.sort(key=lambda x: x.stat().st_mtime)

        return files

    def _read_incremental(
        self, path: Path, last_file: str | None = None
    ) -> tuple[pd.DataFrame, str | None]:
        files = self._get_sorted_files(path)

        if not files:
            logger.warning(f"No files in {path}")
            return pd.DataFrame(), None

        if last_file:
            files = [f for f in files if f.name > last_file]

        if not files:
            logger.info("No new files to process")
            return pd.DataFrame(), None

        logger.info(f"Reading {len(files)} new files")

        df = pd.concat([pd.read_parquet(f) for f in files])
        last_file_read = files[-1].name

        return df, last_file_read

    def get_dataset(self, dataset_name: str) -> pd.DataFrame:
        path = self._build_dataset_path(dataset_name)
        return self._read_parquet_folder(path)

    def get_dataset_incremental(
        self, dataset_name: str, last_processed_file: str | None = None
    ) -> tuple[DataFrame, str | None]:
        path = self._build_dataset_path(dataset_name)
        return self._read_incremental(path, last_processed_file)
