from datetime import datetime
from typing import Any, Dict


def build_log(
    execution_date: datetime,
    dataset: str,
    extract_errors: Dict[str, list] | None,
    validation_errors: Dict[str, list] | None,
) -> Dict[str, Any]:
    extract_errors = extract_errors or {}
    validation_errors = validation_errors or {}

    def count_errors(errors: Dict[str, list]) -> Dict[str, int]:
        return {f"{k}_count": len(v or []) for k, v in errors.items()}

    return {
        "metadata": {
            "dataset": dataset,
            "execution_date": execution_date.isoformat(),
        },
        "stages": {
            "extract": extract_errors,
            "validation": validation_errors,
        },
        "summary": {
            **{f"extract_{k}": v for k, v in count_errors(extract_errors).items()},
            **{f"validation_{k}": v for k, v in count_errors(validation_errors).items()},
        },
    }
