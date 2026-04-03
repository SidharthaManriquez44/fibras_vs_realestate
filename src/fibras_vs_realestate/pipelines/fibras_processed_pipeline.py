from datetime import datetime
from fibras_vs_realestate.config.log_constructor.logs_processed_pipeline import (
    logs_processed_pipeline,
)
from fibras_vs_realestate.config.storage.data_save import DataSaver
from fibras_vs_realestate.config.layers.layers import layer
from fibras_vs_realestate.config.settings import DATASETS

from fibras_vs_realestate.config.path.path_builder import DataLakePathBuilder
from fibras_vs_realestate.config.path.paths import get_paths
from fibras_vs_realestate.core.quality.processed_data_validator import validate_data
from fibras_vs_realestate.config.data_contracts.contracts import MERGED_CONTRACT

from fibras_vs_realestate.core.processed.extract.fibras_repository import FibrasRepository
from fibras_vs_realestate.core.processed.transform.return_builder import ReturnBuilder
from fibras_vs_realestate.config.logger_config import get_logger


logger = get_logger(__name__)


def run_pipeline(execution_date: datetime):
    paths = get_paths()
    datalake = DataLakePathBuilder(paths)
    saver = DataSaver(datalake, execution_date)

    repository = FibrasRepository(
        path_builder=datalake, datasets=DATASETS, execution_date=execution_date
    )

    failed = []

    try:
        # Extract
        prices = repository.get_dataset(DATASETS.prices)
        dividends = repository.get_dataset(DATASETS.dividends)

        # Transform
        merge = None
        try:
            merge = ReturnBuilder.merge_prices_dividends(prices, dividends)
        except Exception as e:
            error_info = {
                "stage": "merge_prices_dividends",
                "error": str(e),
                "prices_shape": len(prices),
                "dividends_shape": len(dividends),
            }
            failed.append(error_info)

            logger.warning("Merge failed", extra=error_info)

        # Validate
        validation_errors = []
        if merge is not None:
            validation_errors = validate_data(merge, MERGED_CONTRACT, DATASETS.prices)

        # Logs
        logs = logs_processed_pipeline(execution_date, failed, validation_errors)

        if merge is not None and not failed and not validation_errors:
            saver.save_parquet(
                layer=layer.processed, domain=DATASETS.domain, dataset=DATASETS.merged, df=merge
            )
        else:
            logger.warning("Skipping load due to errors")

        saver.save_json(
            layer=layer.processed, domain=DATASETS.domain, dataset=DATASETS.logs, data=logs
        )

        logger.info(
            "Pipeline finished",
            extra={"failed_count": len(failed), "validation_errors": len(validation_errors)},
        )

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}", exc_info=True)


if __name__ == "__main__":
    run_pipeline(datetime.now())
