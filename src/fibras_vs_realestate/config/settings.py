from dataclasses import dataclass
from datetime import date

START_DATE = date(2011, 1, 1)
END_DATE = date(2026, 12, 31)


@dataclass(frozen=True)
class DatasetNames:
    prices: str = "prices"
    dividends: str = "dividends"
    domain: str = "fibras"
    logs: str = "_logs"
    failed: str = "failed_data.json"
    merged: str = "merged_data"


DATASETS = DatasetNames()
