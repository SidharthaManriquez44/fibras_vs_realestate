from fibras_vs_realestate.config.financial.tickers import TICKERS
from fibras_vs_realestate.config.settings import START_DATE, DATASETS
from fibras_vs_realestate.config.storage.paths import get_paths
from datetime import datetime

from fibras_vs_realestate.core.raw.extract.extract_fibras import FibrasExtractor
from fibras_vs_realestate.core.raw.transform.transform_fibras import FibrasTransformer
from fibras_vs_realestate.config.storage.data_save import DataSaver
from fibras_vs_realestate.config.storage.path_builder import DataLakePathBuilder

from fibras_vs_realestate.config.storage.layers import layer
from fibras_vs_realestate.config.logger_config import get_logger
from fibras_vs_realestate.config.log_constructor.logs_raw_pipeline import logs_raw_pipeline
from fibras_vs_realestate.core.quality.raw_data_validator import validate_data

from fibras_vs_realestate.config.data_contracts.contracts import (
    PRICE_CONTRACT,
    DIVIDEND_CONTRACT
)

logger = get_logger(__name__)


def run_pipeline(execution_date: datetime):

    logs = {}
    paths = get_paths()
    datalake = DataLakePathBuilder(paths)
    saver = DataSaver(datalake)

    extractor = FibrasExtractor(TICKERS.fibras, START_DATE, execution_date)
    transformer = FibrasTransformer()

    try:
        # Extract
        raw_prices, failed_prices = extractor.extract_prices()
        logs[DATASETS.prices] = failed_prices
        raw_divs, failed_divs = extractor.extract_dividends()
        logs[DATASETS.dividends] = failed_divs

        # Transform
        prices = transformer.transform_prices(raw_prices)
        divs = transformer.transform_dividends(raw_divs)

        validation_errors_prices = validate_data(prices, PRICE_CONTRACT, DATASETS.prices)
        validation_errors_divs = validate_data(divs, DIVIDEND_CONTRACT, DATASETS.dividends)

        logs =  logs_raw_pipeline(
                    execution_date,
                    failed_prices,
                    failed_divs,
                    validation_errors_prices,
                    validation_errors_divs
        )

        # Load prices
        datasets_to_save = [
            (DATASETS.prices, prices),
            (DATASETS.dividends, divs)
        ]

        for dataset_name, df in datasets_to_save:
            saver.save_parquet(
                layer=layer.raw,
                domain=DATASETS.domain,
                dataset=dataset_name,
                df=df,
                execution_date=execution_date
            )


        saver.save_json(
            layer=layer.raw,
            domain=DATASETS.domain,
            dataset=DATASETS.logs,
            data=logs,
            execution_date=execution_date,
            filename=DATASETS.failed
        )

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}", exc_info=True)

if __name__ == "__main__":
    run_pipeline(datetime.now())
