#!filepath: tests/observability/test_instrumentation.py

import time
from src.observability.instrumentation import Instrumentation

from loguru import logger
def test_instrumentation_timer():
    inst = Instrumentation(enabled=True)

    with inst.timer("step_A"):
        time.sleep(0.01)

    assert "step_A" in inst.timeline
    assert inst.timeline["step_A"] > 0


def test_instrumentation_metrics():
    inst = Instrumentation(enabled=True)
    inst.metrics.record("rows", 123)

    assert inst.metrics.metrics["rows"] == 123


def test_instrumentation_progress():
    inst = Instrumentation(enabled=True)

    inst.progress.start("Task", 10)
    inst.progress.update("Task", 5, 10)
    inst.progress.done("Task")


def test_generate_timeline_report(capsys):
    inst = Instrumentation(enabled=True)

    with inst.timer("phase_X"):
        time.sleep(0.005)

    inst.generate_timeline_report("2025-11-03")


    captured = []
    sink_id = logger.add(lambda msg: captured.append(str(msg)))

    inst.generate_timeline_report("2025-11-03")

    logger.remove(sink_id)

    output = "\n".join(captured)

    assert "phase_X" in output
    assert "2025-11-03" in output
    assert "Pipeline timeline" in output