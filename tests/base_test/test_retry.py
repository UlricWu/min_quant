#!filepath: tests/test_retry.py
import pytest
import asyncio
from src import retry, async_retry


def test_retry_success_without_retry():
    """重试未触发情况：第一次运行成功"""
    call_count = {"n": 0}

    @retry.decorator(max_attempts=3)
    def func():
        call_count["n"] += 1
        return "ok"

    assert func() == "ok"
    assert call_count["n"] == 1


def test_retry_success_after_failures():
    """失败 2 次后成功，验证 retry 行为"""
    call_count = {"n": 0}

    @retry.decorator(max_attempts=5, delay=0.01, backoff=1)
    def func():
        call_count["n"] += 1
        if call_count["n"] < 3:
            raise ValueError("fail")
        return "success"

    assert func() == "success"
    assert call_count["n"] == 3  # 尝试了三次


def test_retry_raises_after_max_attempts():
    """达到最大重试次数仍失败，应抛异常"""
    call_count = {"n": 0}

    @retry.decorator(max_attempts=3, delay=0.01)
    def func():
        call_count["n"] += 1
        raise ValueError("fail")

    with pytest.raises(ValueError):
        func()

    assert call_count["n"] == 3


def test_retry_catches_specific_exception():
    """只捕获特定异常，其他异常直接抛出"""
    call_count = {"n": 0}

    @retry.decorator(exceptions=(KeyError,), max_attempts=3)
    def func():
        call_count["n"] += 1
        raise ValueError("this is not KeyError")

    # 非捕获异常，应该立即抛出，不尝试第二次
    with pytest.raises(ValueError):
        func()

    assert call_count["n"] == 1  # 只执行一次


def test_exponential_backoff_and_jitter(monkeypatch):
    """验证指数退避逻辑（不测试真实 sleep，只验证计算逻辑）"""
    sleep_calls = []

    # mock time.sleep，以记录延迟时间
    def fake_sleep(t):
        sleep_calls.append(t)

    monkeypatch.setattr("time.sleep", fake_sleep)

    call_count = {"n": 0}

    @retry.decorator(max_attempts=4, delay=1, backoff=2, jitter=False)
    def func():
        call_count["n"] += 1
        raise ValueError("fail")

    with pytest.raises(ValueError):
        func()

    # sleep 应该被调用 3 次（max_attempts=4 → 重试 3 次）
    assert len(sleep_calls) == 3

    # 指数退避: 1, 2, 4
    assert sleep_calls == [1, 2, 4]

# =============================
#   AsyncRetry 测试
# =============================
