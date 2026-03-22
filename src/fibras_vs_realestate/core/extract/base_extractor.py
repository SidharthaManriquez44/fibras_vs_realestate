from abc import ABC, abstractmethod
from zipfile import Path
import pandas as pd
from datetime import datetime

from fibras_vs_realestate.config.logger_config import get_logger

logger = get_logger(__name__)

class BaseExtractor(ABC):

    def __init__(self, source_name: str):
        self.path = None
        self.source_name = source_name
        self.execution_date = datetime.now()

    @abstractmethod
    def extract(self, path:Path, df: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"Start extracting raw data for {self.source_name}")
        if not path.exists():
            raise FileNotFoundError(f"File not found at {path}")

        return df

    def add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        df["source"] = self.source_name
        df["extraction_date"] = self.execution_date
        return df

    def save_raw(self, df: pd.DataFrame):
        return df.to_parquet(self.path, index=False)
