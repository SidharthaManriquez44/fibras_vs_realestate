from dataclasses import dataclass


@dataclass(frozen=True)
class DataContract:
    columns: list[str]
    not_null: list[str]
    unique: list[str]


PRICE_CONTRACT = DataContract(
    columns=["date", "ticker", "close", "volume", "source", "extraction_date"],
    not_null=["date", "ticker", "close"],
    unique=["date", "ticker"],
)

DIVIDEND_CONTRACT = DataContract(
    columns=["date", "ticker", "dividend", "source", "extraction_date"],
    not_null=["date", "ticker", "dividend"],
    unique=["date", "ticker"],
)

MERGED_CONTRACT = DataContract(
    columns=[
        "date",
        "ticker",
        "close",
        "volume",
        "dividend",
        "source_x",
        "source_y",
        "extraction_date_x",
        "extraction_date_y",
    ],
    not_null=["date", "ticker", "close"],
    unique=["date", "ticker"],
)
