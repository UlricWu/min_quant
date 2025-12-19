#!filepath: src/utils/retry.py
import time
import random
import asyncio
from functools import wraps
from typing import Callable, Tuple, Type

from src import logs


class Retry:
    """
    同步重试工具，支持指数退避、日志记录和 jitter。
    """

    @staticmethod
    def run(
        func: Callable,
        *args,
        exceptions: Tuple[Type[Exception], ...] = (Exception,),
        max_attempts: int = 3,
        delay: float = 1.0,
        backoff: float = 2.0,
        jitter: bool = True,
        **kwargs,
    ):
        """
        手动调用版本的重试机制
        """
        attempt = 1
        while attempt <= max_attempts:

            try:
                return func(*args, **kwargs)

            except exceptions as e:
                if attempt == max_attempts:
                    logs.error(f"[Retry] 函数 {func.__name__} 重试失败，已达最大次数")
                    raise

                wait = delay * (backoff ** (attempt - 1))
                if jitter:
                    wait = wait * random.uniform(0.8, 1.2)

                logs.warning(
                    f"[Retry] 第 {attempt}/{max_attempts-1} 次失败: {e}. "
                    f"{wait:.2f}s 后重试..."
                )
                time.sleep(wait)

                attempt += 1

    @staticmethod
    def decorator(
        exceptions: Tuple[Type[Exception], ...] = (Exception,),
        max_attempts: int = 2,
        delay: float = 1.0,
        backoff: float = 2.0,
        jitter: bool = True,
    ):
        """
        装饰器版本：同步函数使用。
        """

        def wrapper(func: Callable):
            @wraps(func)
            def inner(*args, **kwargs):
                return Retry.run(
                    func,
                    *args,
                    exceptions=exceptions,
                    max_attempts=max_attempts,
                    delay=delay,
                    backoff=backoff,
                    jitter=jitter,
                    **kwargs,
                )

            return inner

        return wrapper


class AsyncRetry:
    """
    异步 async 重试工具（用于 aioftp、aiohttp、async db 等）
    """

    @staticmethod
    async def run(
        func: Callable,
        *args,
        exceptions: Tuple[Type[Exception], ...] = (Exception,),
        max_attempts: int = 3,
        delay: float = 1.0,
        backoff: float = 2.0,
        jitter: bool = True,
        **kwargs,
    ):
        attempt = 1
        while attempt <= max_attempts:

            try:
                return await func(*args, **kwargs)

            except exceptions as e:
                if attempt == max_attempts:
                    logs.error(f"[AsyncRetry] 函数 {func.__name__} 达到最大重试次数！")
                    raise

                wait = delay * (backoff ** (attempt - 1))
                if jitter:
                    wait = wait * random.uniform(0.8, 1.2)

                logs.warning(
                    f"[AsyncRetry] 第 {attempt}/{max_attempts-1} 次失败: {e}. "
                    f"{wait:.2f}s 后重试..."
                )
                await asyncio.sleep(wait)

                attempt += 1

    @staticmethod
    def decorator(
        exceptions: Tuple[Type[Exception], ...] = (Exception,),
        max_attempts: int = 3,
        delay: float = 1.0,
        backoff: float = 2.0,
        jitter: bool = True,
    ):
        def wrapper(func: Callable):
            @wraps(func)
            async def inner(*args, **kwargs):
                return await AsyncRetry.run(
                    func,
                    *args,
                    exceptions=exceptions,
                    max_attempts=max_attempts,
                    delay=delay,
                    backoff=backoff,
                    jitter=jitter,
                    **kwargs,
                )

            return inner

        return wrapper
