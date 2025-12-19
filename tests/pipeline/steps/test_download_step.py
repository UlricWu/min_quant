from pathlib import Path
from unittest.mock import MagicMock

from src.dataloader.pipeline.steps.download_step import DownloadStep
from src.dataloader.pipeline.context import PipelineContext


def test_download_step_invokes_adapter():
    adapter = MagicMock()

    step = DownloadStep(adapter)

    ctx = PipelineContext(
        date="2025-01-01",
        raw_dir=Path("/tmp/raw"),
        parquet_dir=Path("/tmp/pq"),
        symbol_dir=Path("/tmp/sym"),
    )

    step.run(ctx)

    adapter.download_date.assert_called_once_with(
        "2025-01-01",
        Path("/tmp/raw"),
    )
