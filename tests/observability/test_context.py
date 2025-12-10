#!filepath: tests/observability/test_context.py

from src.observability.context import InstrumentationContext


def test_context_set_get():
    ctx = InstrumentationContext()

    ctx.set("symbol", "600000")
    ctx.set("date", "2025-11-03")

    assert ctx.get("symbol") == "600000"
    assert ctx.get("date") == "2025-11-03"
    assert ctx.get("missing", "default") == "default"
