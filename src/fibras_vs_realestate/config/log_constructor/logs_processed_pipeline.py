from fibras_vs_realestate.config.settings import DATASETS
from fibras_vs_realestate.config.log_constructor.log_constructor import build_log


def logs_processed_pipeline(execution_date, failed, validation_errors):
    logs = build_log(
        execution_date=execution_date,
        dataset=DATASETS.logs,
        extract_errors={
            DATASETS.merged: failed or [],
        },
        validation_errors={
            DATASETS.merged: validation_errors or [],
        },
    )
    return logs
