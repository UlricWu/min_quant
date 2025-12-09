#!filepath: tests/observability/test_metrics.py

from src.observability.metrics import MetricRecorder


def test_metric_record():
    m = MetricRecorder(enabled=True)
    m.record("latency_ms", 123)

    assert "latency_ms" in m.metrics
    assert m.metrics["latency_ms"] == 123


def test_metric_disabled():
    m = MetricRecorder(enabled=False)
    m.record("x", 1)

    # Nothing should be recorded
    assert m.metrics == {}
