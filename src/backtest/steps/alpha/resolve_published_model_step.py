#!filepath: src/backtest/steps/alpha/resolve_published_model_step.py
from __future__ import annotations

from dataclasses import dataclass

from src import logs, PathManager
from src.backtest.context import BacktestContext
from src.pipeline.model_artifact import resolve_model_artifact_from_dir
from src.pipeline.step import PipelineStep


@dataclass(frozen=True, slots=True)
class _ModelSelector:
    """
    _ModelSelector（FROZEN）

    Minimal selector for published namespace.
    """
    name: str
    selector: str  # published_latest | published_asof


class ResolvePublishedModelStep(PipelineStep):
    """
    ResolvePublishedModelStep（FINAL / FROZEN）

    Contract:
    - Resolve ModelArtifact ONLY from published namespace.
    - Never read training artifacts.
    - Never accept arbitrary file paths or run-scoped artifacts.
    - Output: ctx.model_artifact
    """

    stage = "resolve_model"

    def __init__(self, *, inst, pm: PathManager):
        super().__init__(inst=inst)
        self.pm = pm

    def _resolve_selector(self, ctx: BacktestContext) -> _ModelSelector:
        """
        Backward compatible:
        - Prefer cfg.model if present (recommended)
        - Fallback to legacy cfg.strategy.model.spec.artifact.run
        """
        # recommended new config layout
        model_spec = getattr(ctx.cfg, "model", None)
        if model_spec is not None:
            name = getattr(model_spec, "name", None)
            selector = getattr(model_spec, "selector", None)
            if name and selector:
                return _ModelSelector(name=name, selector=selector)

        # legacy fallback
        legacy = ctx.cfg.strategy["model"]["spec"]["artifact"]["run"]
        return _ModelSelector(name=str(legacy), selector="published_latest")

    def run(self, ctx: BacktestContext) -> BacktestContext:
        sel = self._resolve_selector(ctx)
        logs.info(
            f"[ResolvePublishedModelStep] model={sel.name} selector={sel.selector} date={ctx.today}"
        )

        if sel.selector == "published_latest":
            artifact_dir = self.pm.model_latest_dir(sel.name)
        elif sel.selector == "published_asof":
            # asof by date (string), you can tighten to trading calendar later
            artifact_dir = self.pm.model_asof_dir(sel.name, ctx.today)  # type: ignore[arg-type]
        else:
            raise ValueError(f"Unknown model selector: {sel.selector}")

        if not artifact_dir.exists():
            raise RuntimeError(
                f"[Backtest] No published model found: {artifact_dir}"
            )

        ctx.model_artifact = resolve_model_artifact_from_dir(artifact_dir)
        return ctx
