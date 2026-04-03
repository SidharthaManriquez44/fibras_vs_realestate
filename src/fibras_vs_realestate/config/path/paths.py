from pathlib import Path
from dataclasses import dataclass


@dataclass(frozen=True)
class PathParams:
    base_dir: Path
    raw_dir: Path
    processed_dir: Path
    external_dir: Path


def get_project_root() -> Path:
    current = Path(__file__).resolve()

    for parent in current.parents:
        if (parent / "data").exists():
            return parent

    raise RuntimeError("Project root not found")


def get_paths() -> PathParams:
    base_dir = get_project_root()

    return PathParams(
        base_dir=base_dir,
        raw_dir=base_dir / "data/raw",
        processed_dir=base_dir / "data/processed",
        external_dir=base_dir / "data/external",
    )
