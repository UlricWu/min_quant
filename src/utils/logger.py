#!filepath: src/utils/logger.py
import os
import json
from functools import wraps
from time import perf_counter
from datetime import datetime
from loguru import logger
from typing import Any, Callable, Optional


class Logging:
    """
    生产级日志模块
    ---------------------------------------
    - 支持按日期切割
    - 支持日志保留周期
    - 支持 JSON 格式扩展
    - 包含函数级日志装饰器
    ---------------------------------------
    """

    def __init__(
        self,
        log_dir: str = "logs",
        rotation: str = "1 day",
        retention: str = "30 days",
        log_level: str = "INFO",
    ):
        self.log_dir = log_dir
        self.rotation = rotation
        self.retention = retention
        self.level = log_level

        os.makedirs(self.log_dir, exist_ok=True)
        self._configure()

    def _configure(self) -> None:
        """
        配置全局 logger，只执行一次
        """

        logger.remove()

        # 基础文本日志
        logger.add(
            sink=f"{self.log_dir}/{{time:YYYY-MM-DD}}.log",
            rotation=self.rotation,
            retention=self.retention,
            level=self.level,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
            enqueue=True,  # 多进程安全
            backtrace=True,
            diagnose=True,
        )

        logger.info("\n-----------Logger initialized successfully.-----------")

    # ---------- 基础接口封装 ----------
    # ----------- 日志方法 -----------
    def debug(self, msg: str, *args, **kwargs):
        logger.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs):
        logger.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs):
        logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs):
        logger.error(msg, *args, **kwargs)

    def exception(self, msg: str, *args, **kwargs):
        logger.exception(msg, *args, **kwargs)
    # ---------- 日志装饰器 ----------
    def catch(
        self,
        msg: str = "Exception occurred",
        log_inputs: bool = False,
        log_outputs: bool = False,
        log_time: bool = True,
    ) -> Callable:

        def decorator(func: Callable):
            @wraps(func)
            def wrapper(*args, **kwargs):

                if log_inputs:
                    logger.info(
                        f"[CALL] {func.__name__} args={args}, kwargs={json.dumps(kwargs, ensure_ascii=False)}"
                    )

                start = perf_counter()

                try:
                    result = func(*args, **kwargs)
                except Exception:
                    logger.exception(f"[ERROR] {func.__name__}: {msg}")
                    raise  # 正式环境要让异常抛出，方便调试

                if log_outputs:
                    logger.info(f"[RETURN] {func.__name__} result={result}")

                if log_time:
                    cost = perf_counter() - start
                    logger.info(f"[TIME] {func.__name__} took {cost:.4f}s")

                return result

            return wrapper

        return decorator

# 默认全局 logs（可被 init_logging 替换）
logs = Logging()
