from fibras_vs_realestate.core.quality.data_validator import DataValidator
from fibras_vs_realestate.config.logger_config import get_logger

logger = get_logger(__name__)

def validate_data(df, contract, dataset_name: str):
    try:
        DataValidator.validate_not_empty(df)
        DataValidator.validate_schema(df, contract.columns)
        DataValidator.validate_not_null(df, contract.not_null)
        DataValidator.validate_duplicates(df, contract.unique)

    except Exception as e:
        logger.error(f"Validation failed for {dataset_name}: {e}", exc_info=True)
