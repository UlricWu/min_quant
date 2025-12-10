#!filepath: src/observability/metrics.py
from dataclasses import dataclass, field
from typing import Dict, Any
from src import logs


@dataclass
class MetricRecorder:
    enabled: bool = True
    metrics: Dict[str, Any] = field(default_factory=dict)

    def record(self, name: str, value: Any):
        if not self.enabled:
            return
        self.metrics[name] = value
        logs.info(f"[Metric] {name} = {value}")
