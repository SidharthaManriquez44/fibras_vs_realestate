from fibras_vs_realestate.config.settings import DATASETS
from fibras_vs_realestate.config.data_contracts.contracts import PRICE_CONTRACT, DIVIDEND_CONTRACT


DATA_PIPELINES = [
    {
        "name": "prices",
        "dataset": DATASETS.prices,
        "contract": PRICE_CONTRACT,
        "extract": "extract_prices",
        "transform": "transform_prices",
    },
    {
        "name": "dividends",
        "dataset": DATASETS.dividends,
        "contract": DIVIDEND_CONTRACT,
        "extract": "extract_dividends",
        "transform": "transform_dividends",
    },
]
