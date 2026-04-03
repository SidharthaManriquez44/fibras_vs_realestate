from abc import ABC, abstractmethod
import pandas as pd


class TableWriter(ABC):
    @abstractmethod
    def write(self, table: str, df: pd.DataFrame, mode: str):
        pass
