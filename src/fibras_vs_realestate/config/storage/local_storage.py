import json
from pathlib import Path
import shutil
from fibras_vs_realestate.config.storage.storage_backend import StorageBackend


class LocalStorage(StorageBackend):
    def save_parquet(self, path: str, df):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)

    def save_json(self, path: str, data: dict):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)

    def exists(self, path):
        return Path(path).exists()

    def delete(self, path):
        shutil.rmtree(path, ignore_errors=True)

    def move(self, src, dst):
        shutil.move(src, dst)
