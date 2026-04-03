import uuid
import pandas as pd
from fibras_vs_realestate.config.table.table_writer import TableWriter


class ParquetTableWriter(TableWriter):
    def __init__(self, storage, path_builder, execution_date):
        self.storage = storage
        self.path_builder = path_builder
        self.execution_date = execution_date

    def _overwrite(self, base_path, file_name, df):
        temp_path = f"{base_path}_tmp"

        temp_file = f"{temp_path}/{file_name}"
        self.storage.save_parquet(temp_file, df)

        if self.storage.exists(base_path):
            self.storage.delete(base_path)

        self.storage.move(temp_path, base_path)

    def write(self, table: str, df: pd.DataFrame, mode: str = "append"):
        layer, domain, dataset = table.split(".")

        base_path = self.path_builder.build_path(
            layer=layer,
            domain=domain,
            dataset=dataset,
            year=self.execution_date.year,
            month=self.execution_date.month,
        )

        timestamp = self.execution_date.strftime("%Y%m%d_%H%M%S")
        file_name = f"{timestamp}_{uuid.uuid4().hex}.parquet"

        if mode == "append":
            path = f"{base_path}/{file_name}"
            self.storage.save_parquet(path, df)

        elif mode == "overwrite":
            self._overwrite(base_path, file_name, df)

        else:
            raise ValueError(f"Invalid mode: {mode}")
