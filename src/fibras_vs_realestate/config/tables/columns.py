from typing import List
from dataclasses import dataclass


@dataclass(frozen=True)
class DatasetColumns:
    price_columns: List[str]
    dividend_columns: List[str]

columns  = DatasetColumns(
    price_columns =  [
        "date",
        "ticker",
        "close",
        "volume",
        "source",
        "extraction_date"
    ],
    dividend_columns =  [
        "date",
        "ticker",
        "dividend",
        "source",
        "extraction_date"
    ])
