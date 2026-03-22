from fibras_vs_realestate.config.financial.tickers import TICKERS
from fibras_vs_realestate.config.settings import START_DATE, DATASETS
from fibras_vs_realestate.config.storage.paths import get_paths

from fibras_vs_realestate.core.extract.extract_fibras import FibrasExtractor
from fibras_vs_realestate.core.transform.transform_fibras import FibrasTransformer
from fibras_vs_realestate.core.load.load_fibras import ParquetLoader
from fibras_vs_realestate.config.storage.path_builder import DataLakePathBuilder
from fibras_vs_realestate.config.storage.layers import layer



def run_pipeline():

    logs = {}
    paths = get_paths()
    datalake = DataLakePathBuilder(paths)

    extractor = FibrasExtractor(TICKERS.fibras, START_DATE)
    transformer = FibrasTransformer()
    loader = ParquetLoader(datalake, DATASETS)

    # PRICES
    raw_prices, failed_prices = extractor.extract_prices()
    prices = transformer.transform_prices(raw_prices)

    loader.save_data(layer.raw, prices, DATASETS.prices)
    logs[DATASETS.prices] = failed_prices

    # DIVIDENDS
    raw_divs, failed_divs = extractor.extract_dividends()
    divs = transformer.transform_dividends(raw_divs)

    loader.save_data(layer.raw, divs, DATASETS.dividends)
    logs[DATASETS.dividends] = failed_divs

    # LOGS
    loader.save_logs(layer.raw, logs)


if __name__ == "__main__":
    run_pipeline()
