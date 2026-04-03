import os
import s3fs
import json
import pandas as pd


class MinIOStorage:
    def __init__(self):
        self.fs = s3fs.S3FileSystem(
            key=os.getenv("MINIO_USER"),
            secret=os.getenv("MINIO_PASSWORD"),
            client_kwargs={"endpoint_url": os.getenv("MINIO_ENDPOINT")},
        )

    def save_parquet(self, path: str, df: pd.DataFrame):
        object_path = path.replace("\\", "/")

        full_path = f"{object_path}/data.parquet"

        with self.fs.open(full_path, "wb") as f:
            df.to_parquet(f)

    def save_json(self, path: str, data: dict):
        full_path = f"{path}/data.json"

        with self.fs.open(full_path, "w") as f:
            json.dump(data, f, indent=2, default=str)

    def exists(self, path):
        return self.fs.exists(path)

    def delete(self, path):
        self.fs.rm(path, recursive=True)

    def move(self, src, dst):
        self.fs.mv(src, dst, recursive=True)
