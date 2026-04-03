from abc import ABC, abstractmethod
import pandas as pd


class StorageBackend(ABC):
    @abstractmethod
    def save_parquet(self, path: str, df: pd.DataFrame):
        pass

    @abstractmethod
    def save_json(self, path: str, data: dict):
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        pass

    @abstractmethod
    def delete(self, path: str):
        pass

    @abstractmethod
    def move(self, src: str, dst: str):
        pass
