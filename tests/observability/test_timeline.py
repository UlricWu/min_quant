#!filepath: tests/observability/test_timeline.py

from src.observability.timeline_reporter import TimelineReporter
from src import logs  # your loguru wrapper
from loguru import logger

def test_timeline_log_output():
    tl = {
        "FTP": 1.23,
        "Convert": 2.34,
    }
    reporter = TimelineReporter(tl, "2025-11-03")

    captured = []

    # 临时添加一个 sink 捕获 Loguru 输出
    sink_id = logger.add(lambda msg: captured.append(str(msg)))

    reporter.print()

    logger.remove(sink_id)  # 恢复

    # 断言日志确实包含 timeline 内容
    output = "\n".join(captured)

    assert "Pipeline timeline for 2025-11-03" in output
    assert "FTP" in output
    assert "1.23" in output
    assert "Convert" in output
