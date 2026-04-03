class DataLakePathBuilder:
    def __init__(self, paths):
        self.paths = paths

    def _get_base_path(self, layer: str):
        if layer == "raw":
            return self.paths.raw_dir
        elif layer == "processed":
            return self.paths.processed_dirD
        elif layer == "external":
            return self.paths.external_dir
        else:
            raise ValueError(f"Invalid layer: {layer}")

    def build_path(self, layer: str, domain: str, dataset: str, year: int, month: int):
        base = self._get_base_path(layer)

        return base / domain / dataset / f"year={year}" / f"month={month}"
