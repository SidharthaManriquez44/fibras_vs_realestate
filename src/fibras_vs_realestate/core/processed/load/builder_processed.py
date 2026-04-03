import pandas as pd
from datetime import datetime


class BuilderProcessed:
    def __init__(self, datalake, datasets):
        self.datalake = datalake
        self.datasets = datasets
        self.execution_date = datetime.now()

    def save_processed(self, layer: str, df: pd.DataFrame, dataset_name: str):
        pass
