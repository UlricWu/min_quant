# src/data_system/engines/context.py
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class EngineContext:
    """
    EngineContext (FINAL)

    Used ONLY inside data engines.
    """

    input_file: Path
    output_file: Path
    mode: str = "full"
