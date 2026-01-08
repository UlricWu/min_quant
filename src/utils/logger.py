#!filepath: src/utils/logger.py
import os
import json
from functools import wraps
from time import perf_counter
from pathlib import Path
from typing import Any, Callable, Optional
from loguru import logger

_LOGGER_CONFIGURED = False


class Logging:
    """
    全局日志单例（FROZEN）

    使用方式（唯一允许）：
        from src import logs
        logs.info(...)

    初始化（仅在进程入口）：
        logs.init(scope="api")
        logs.init(scope="backtest", run_id="2026-01-08")
    """

    def __init__(
            self,
            log_root: str = "logs",
            rotation: str = "1 day",
            retention: str = "30 days",
            log_level: str = "INFO",
    ):
        self.log_root = Path(log_root)
        self.rotation = rotation
        self.retention = retention
        self.level = log_level

        self._configured = False
        self.log_root.mkdir(exist_ok=True)

    # --------------------------------------------------
    # 🔑 唯一新增的公开方法
    # --------------------------------------------------
    def init(
            self,
            *,
            scope: str,
            run_id: Optional[str] = None,
    ) -> None:
        """
        Initialize logging destination ONCE.

        scope:
          - api
          - job
          - train
          - backtest
          - experiment
          - system
        """
        global _LOGGER_CONFIGURED
        if _LOGGER_CONFIGURED:
            return

        # -----------------------------
        # Resolve log file
        # -----------------------------
        if scope == "api":
            log_file = self.log_root / "api" / "api.current.log"

        elif scope == "job":
            job_id = os.getenv("MINQUANT_JOB_ID", "unknown")
            log_file = self.log_root / "jobs" / f"{job_id}.log"

        elif scope in {"train", "backtest", "experiment"}:
            if not run_id:
                raise ValueError(f"run_id required for scope={scope}")
            log_file = (
                    self.log_root / "runs" / scope / f"run_{run_id}.log"
            )

        elif scope == "system":
            log_file = self.log_root / "system" / "system.log"

        else:
            raise ValueError(f"Unknown log scope: {scope}")

        log_file.parent.mkdir(parents=True, exist_ok=True)

        # -----------------------------
        # Configure loguru (ONCE)
        # -----------------------------
        logger.remove()

        logger.add(
            sink=str(log_file),
            rotation=self.rotation,
            retention=self.retention,
            level=self.level,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
            enqueue=True,
            backtrace=True,
            diagnose=True,
        )

        logger.add(
            sink=lambda msg: print(msg, end=""),
            level=self.level,
        )

        logger.info(
            f"[Logger] initialized scope={scope} file={log_file}"
        )

        _LOGGER_CONFIGURED = True
        self._configured = True

    # --------------------------------------------------
    # 下面的 API 全部保持不变
    # --------------------------------------------------
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

    # --------------------------------------------------
    # 装饰器（完全保留）
    # --------------------------------------------------
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
                        f"[CALL] {func.__name__} "
                        f"args={args}, kwargs={json.dumps(kwargs, ensure_ascii=False)}"
                    )

                start = perf_counter()
                try:
                    result = func(*args, **kwargs)
                except Exception:
                    logger.exception(f"[ERROR] {func.__name__}: {msg}")
                    raise

                if log_outputs:
                    logger.info(f"[RETURN] {func.__name__} result={result}")

                if log_time:
                    cost = perf_counter() - start
                    logger.info(
                        f"[TIME] {func.__name__} took {cost:.4f}s"
                    )

                return result

            return wrapper

        return decorator

    # progress 原样保留（略，为简洁）


# --------------------------------------------------
# 全项目唯一实例（不变）
# --------------------------------------------------
logs = Logging()
