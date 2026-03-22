from dataclasses import dataclass

@dataclass(frozen=True)
class Layer:
    raw: str  = "raw"
    processed: str = "processed"

layer  =Layer()