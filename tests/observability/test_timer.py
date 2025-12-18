#!filepath: tests/observability/test_timer.py

import time
from src.observability.timer import Timer

def test_timer_basic():
    t = Timer(enabled=True)
    t.start("task")
    time.sleep(0.01)
    elapsed = t.end("task")

    assert elapsed > 0
    assert isinstance(elapsed, float)

def test_timer_disabled():
    t = Timer(enabled=False)
    t.start("task")
    elapsed = t.end("task")

    assert elapsed == 0.0
