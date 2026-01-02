# src/steps/download_step.py
from __future__ import annotations

import ftplib
import json
import subprocess
from pathlib import Path
from typing import Optional

from src.pipeline.context import PipelineContext
from src.pipeline.step import PipelineStep
from src.pipeline.pipeline import PipelineAbort

from src.engines.ftp_download_engine import FtpDownloadEngine
from src.meta.base import BaseMeta, MetaOutput

from src.utils.filesystem import FileSystem
from src.utils.retry import Retry
from src.utils.logger import logs

from src.config.secret_config import SecretConfig
from src.config.pipeline_config import DownloadBackend


# TODO(stage-1):
#   在系统进入全自动跑后：
#   - 下载前获取远端 size（ftp.size）
#   - 下载后校验 local size == remote size
#   - 将 remote_size 写入 Meta


class DownloadStep(PipelineStep):
    """
    DownloadStep（Source Step / 冻结 · 串行）

    语义：
      - upstream：远端 FTP 文件（虚拟 Path，仅用于 Meta）
      - output  ：本地 raw/*.7z 文件
      - rows    ：0（Source Step，无表结构）

    Engine 约束：
      - 不做任何 I/O
      - 不依赖 ftplib / Path / OS
      - 只负责：日期、文件选择、远端路径规则

    冻结原则（与 MinuteTradeAggStep 完全一致）：
      1. Step 只做 orchestration
      2. 所有执行必须先通过 meta.upstream_changed()
      3. I/O 只在“获得执行资格”后发生
      4. 每个 logical output（filename）一个 Meta
      5. 成功后才 commit meta

    Source existence：
      - ftp.cwd(date) 是唯一 existence check
      - 550 Failed to change directory = 非致命
      - 必须立刻 PipelineAbort
      - 不得进入 meta / download / engine

    行为约定：
      - 非程序错误（无交易 / 无数据 / 权限） → PipelineAbort（跳过）
      - 程序错误（网络 / curl / bug）      → 原样抛异常
    """

    stage = "download"
    upstream_stage = "remote"

    # ------------------------------------------------------------------
    def __init__(
            self,
            engine: FtpDownloadEngine,
            secret: SecretConfig,
            backend: DownloadBackend = DownloadBackend.FTPLIB,
            inst=None,
            remote_root: str = "",
    ) -> None:
        super().__init__(inst)

        self.engine = engine
        self.backend = backend

        self._secret = secret
        self.host = secret.ftp_host
        self.user = secret.ftp_user
        self.password = secret.ftp_password
        self.port = secret.ftp_port

        self.remote_root = remote_root.rstrip("/")

    # ==================================================
    # Source Step: enumerate remote logical units
    # ==================================================
    def run(self, ctx: PipelineContext) -> PipelineContext:
        date_str = self.engine.resolve_date(ctx.today)

        # ==========================================================
        # 1. 列远端目录（Source 信息）
        #    ⚠️ 此处不等于“执行下载”
        # ==========================================================
        ftp = ftplib.FTP(timeout=1200)
        ftp.connect(self.host, self.port)
        ftp.login(self.user, self.password)

        # --------------------------------------------------
        # 2. 切到 remote_root + date 目录（Source existence check）
        # --------------------------------------------------
        try:
            ftp.cwd(self.remote_root)
            ftp.cwd(date_str)
        except ftplib.error_perm as e:
            # 550 Failed to change directory.
            msg = str(e)
            logs.warning(
                f"[{self.stage}][NO DATA] {date_str} directory not found | {msg}"
            )

            # 该 date 在 upstream 中不存在任何可消费的数据入口
            # ⛔️ 这是“无数据”，不是错误：
            #   - 不是某个文件坏了
            #   - 不是网络抖动
            #   - 不是权限错误（在你的白名单里）
            #   - 而是：这个交易日在该数据源中是“空”的
            raise PipelineAbort(msg)

        # ==========================================================
        # 3. 列文件（仅在 upstream 存在时）
        # ==========================================================
        filenames = ftp.nlst()

        plans = self.engine.plan_downloads(
            date=date_str,
            available_files=filenames,
        )

        # ==========================================================
        # 4. 文件级 meta-first 下载
        # ==========================================================
        for plan in plans:
            filename = plan["filename"]
            local_path = ctx.raw_dir / filename

            upstream = self._remote_upstream_path(
                date=date_str,
                filename=filename,
            )

            meta = BaseMeta(
                meta_dir=ctx.meta_dir,
                stage=self.stage,
                output_slot=filename,
            )

            # ⛔️ meta 不允许执行
            if not meta.upstream_changed():
                logs.info(f"[{self.stage}] meta hit → skip {filename}")
                continue

            # ✅ 第一次真正产生副作用，才创建目录
            FileSystem.ensure_dir(ctx.raw_dir)
            FileSystem.ensure_dir(ctx.meta_dir)

            with self.inst.timer(f"download_{filename.split('.')[0]}"):
                self._download_one(
                    ftp=ftp,
                    date=date_str,
                    plan=plan,
                    local_path=local_path,
                )

            meta.commit(
                MetaOutput(
                    input_file=upstream,
                    output_file=local_path,
                    rows=0,
                )
            )

            logs.info(f"[{self.stage}] committed {filename}")

        return ctx

    # ------------------------------------------------------------------
    def _remote_url(self, *, date: str, filename: str) -> str:
        """
        唯一的远端定位来源（冻结）：
        ftp://host:port/root/date/filename
        """
        return (
            f"ftp://{self.host}:{self.port}"
            f"/{self.remote_root}/{date}/{filename}"
        )

    def _remote_upstream_path(self, *, date: str, filename: str) -> Path:
        return Path(self._remote_url(date=date, filename=filename))

    # ------------------------------------------------------------------
    @logs.catch()
    def _download_one(
            self,
            *,
            ftp: ftplib.FTP,
            date: str,
            plan: dict,
            local_path: Path,
    ) -> None:
        if self.backend == DownloadBackend.CURL:
            self._download_by_curl(
                date=date,
                remote_file=plan["filename"],
                local_path=local_path,
            )
        else:
            self._download_by_ftplib(
                ftp=ftp,
                remote_file=plan["filename"],
                local_path=local_path,
            )

    # ------------------------------------------------------------------
    @Retry.decorator(
        exceptions=(ftplib.error_temp, ftplib.error_perm, OSError, TimeoutError),
        max_attempts=2,
        delay=60,
        backoff=2,
        jitter=True,
    )
    def _download_by_ftplib(
            self,
            *,
            ftp: ftplib.FTP,
            remote_file: str,
            local_path: Path,
    ) -> None:
        logs.info(f"[FTP] ftplib downloading {remote_file}")

        with open(local_path, "wb") as fh:
            ftp.retrbinary(f"RETR {remote_file}", fh.write)

    # ------------------------------------------------------------------
    @Retry.decorator(
        exceptions=(RuntimeError, OSError),
        max_attempts=3,
        delay=60,
        backoff=2,
        jitter=True,
    )
    def _download_by_curl(
            self,
            *,
            date: str,
            remote_file: str,
            local_path: Path,
    ) -> None:
        logs.info(f"[FTP] curl downloading {remote_file}")

        url = self._remote_url(date=date, filename=remote_file)

        cmd = [
            "curl",
            "--noproxy", "*",
            "--ftp-pasv",
            "-u", f"{self.user}:{self.password}",
            "--fail",
            "--connect-timeout", "30",
            "--retry", "5",
            "--retry-delay", "10",
            "--silent",
            "--show-error",
            "--stderr", "-",  # 错误仍可能进入 stdout
            "--write-out",
            (
                "{"
                "\"speed_bps\": %{speed_download}, "
                "\"size_bytes\": %{size_download}"
                "}\n"
            ),
            "-o", str(local_path),
            url,
        ]

        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=600,
            )
        except subprocess.TimeoutExpired as e:
            raise RuntimeError(
                f"[DownloadStep][curl timeout] {remote_file} | "
                f"timeout=600s | url={url}"
            ) from e

        # --------------------------------------------------
        # 1. curl 执行失败：立即抛异常（不解析 stdout）
        # --------------------------------------------------
        if result.returncode != 0:
            raise RuntimeError(
                f"[DownloadStep][curl failed] {remote_file} | "
                f"code={result.returncode} | "
                f"stdout={result.stdout.strip()} | "
                f"stderr={result.stderr.strip()}"
            )

        # --------------------------------------------------
        # 2. curl 成功：只解析 stdout 的最后一行
        # --------------------------------------------------
        stats_line = result.stdout.strip().splitlines()[-1]

        try:
            stats = json.loads(stats_line)
        except Exception as e:
            raise RuntimeError(
                f"[DownloadStep][curl stats parse failed] {remote_file} | "
                f"line={stats_line} | err={e}"
            )

        logs.info(
            f"[DownloadStats] {remote_file} | "
            f"speed={stats['speed_bps'] * 8 / 1_000_000:.2f} Mbps "
            f"size={stats['size_bytes'] / 1_000_000_000:.2f} GB"
        )

        logs.info(f"[Download] finished {remote_file} → {local_path}")
