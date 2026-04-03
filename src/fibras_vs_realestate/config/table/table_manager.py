import pandas as pd
from fibras_vs_realestate.config.table.table_writer import TableWriter


class DuckDBTableManager(TableWriter):
    def __init__(self, duckdb_manager, parquet_writer):
        self.duckdb = duckdb_manager
        self.parquet_writer = parquet_writer

    def write(self, table: str, df: pd.DataFrame, mode: str):
        path = self._resolve_path(table)

        if mode == "append":
            self.parquet_writer.write(table, df, mode="append")

        elif mode == "overwrite":
            self.parquet_writer.write(table, df, mode="overwrite")

        elif mode == "upsert":
            self.duckdb.upsert(path, df, key="id")

    def read_table(self, path: str):
        return self.conn.execute(f"""
            SELECT * FROM '{path}/*.parquet'
        """).df()

    def create_view(self, table_name: str, path: str):
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW {table_name} AS
            SELECT * FROM '{path}/*.parquet'
        """)

    def upsert(self, target_path: str, df: pd.DataFrame, key: str):
        temp_table = "temp_data"

        self.conn.register(temp_table, df)

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE target AS
            SELECT * FROM '{target_path}/*.parquet'
        """)

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE merged AS
            SELECT * FROM target
            WHERE {key} NOT IN (SELECT {key} FROM {temp_table})

            UNION ALL

            SELECT * FROM {temp_table}
        """)

        self.conn.execute(f"""
            COPY merged TO '{target_path}' (FORMAT PARQUET)
        """)
