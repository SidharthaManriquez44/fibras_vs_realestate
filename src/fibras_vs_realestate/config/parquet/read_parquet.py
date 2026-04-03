import os
import s3fs
import pandas as pd


def read_parquet_s3(path: str) -> pd.DataFrame:
    fs = s3fs.S3FileSystem(
        key=os.getenv("MINIO_USER"),
        secret=os.getenv("MINIO_PASSWORD"),
        client_kwargs={"endpoint_url": os.getenv("MINIO_ENDPOINT")},
    )
    return pd.read_parquet(path, filesystem=fs)
