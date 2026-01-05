from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from src.pipeline.step import PipelineStep
from src.pipeline.pipeline import PipelineAbort
from src.training.context import TrainingContext
from src.config.training_config import FeatureLabelConfig
from src import logs


# -----------------------------------------------------------------------------
# DatasetDeclaration（冻结）
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class DatasetDeclaration:
    """
    Pure dataset semantics for ONE training step.
    """
    feature_columns: Optional[list[str]]
    label_column: str
    drop_na: bool


# -----------------------------------------------------------------------------
# Step
# -----------------------------------------------------------------------------
class DatasetResolveStep(PipelineStep):
    """
    DatasetResolveStep（FINAL / FROZEN）

    Responsibility:
    - Extract dataset semantics from TrainingConfig
    - Attach an immutable DatasetDeclaration to context

    Forbidden:
    - Reading files
    - Interpreting dates
    - Building data
    """

    stage = "dataset_resolve"
    output_slot = "dataset"

    def run(self, ctx: TrainingContext) -> TrainingContext:
        cfg_dataset: FeatureLabelConfig = ctx.cfg.dataset

        if cfg_dataset is None:
            raise PipelineAbort("TrainingConfig.dataset is required")

        dataset = DatasetDeclaration(
            feature_columns=cfg_dataset.feature_columns,
            label_column=cfg_dataset.label_column,
            drop_na=cfg_dataset.drop_na,
        )

        ctx.dataset = dataset

        logs.info(
            "[DatasetResolveStep] "
            f"features={dataset.feature_columns} "
            f"label={dataset.label_column} "
            f"drop_na={dataset.drop_na}"
        )

        return ctx
