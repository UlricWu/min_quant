#!filepath: tests/observability/test_progress.py

from src.observability.progress import ProgressReporter


def test_progress_no_crash():
    p = ProgressReporter(enabled=True)
    p.start("Download", 100, "bytes")
    p.update("Download", 20, 100, "bytes")
    p.done("Download")

def test_progress_disabled():
    p = ProgressReporter(enabled=False)
    # Should not crash, and should do nothing
    p.start("Task", 10)
    p.update("Task", 3, 10)
    p.done("Task")
