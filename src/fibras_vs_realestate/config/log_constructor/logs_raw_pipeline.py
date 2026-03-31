from fibras_vs_realestate.config.settings import DATASETS
from fibras_vs_realestate.config.log_constructor.log_constructor import build_log

def logs_raw_pipeline(
        execution_date,
        failed_prices,
        failed_divs,
        validation_errors_prices,
        validation_errors_divs,
    ):
    logs = build_log(
        execution_date=execution_date,
        dataset=DATASETS.logs,
        extract_errors={
            DATASETS.prices : failed_prices or [],
            DATASETS.dividends : failed_divs or [],
        },
        validation_errors={
            DATASETS.prices : validation_errors_prices or [],
            DATASETS.dividends : validation_errors_divs or [],
        },
    )
    return logs

