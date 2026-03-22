from typing import List
from dataclasses import dataclass

@dataclass(frozen=True)
class TickersParams:
    fibras: List[str]

TICKERS = TickersParams(
    fibras=[
        "FUNO11.MX",
        "FMTY14.MX",
        "FIBRAPL14.MX",
        "TERRA13.MX",
        "DANHOS13.MX",
        "FIBRAHD15.MX",
        "FIHO12.MX",
        "STORAGE18.MX",
        "EDUCA18.MX",
        "SHOP13.MX",
        "NOVA13.MX"
    ]
)
