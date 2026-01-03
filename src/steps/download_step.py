#!filepath: src/steps/download_step.py
from __future__ import annotations

import ftplib
import json
import subprocess
from pathlib import Path

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


class DownloadStep(PipelineStep):
    """
    DownloadStep（Source Step / 冻结 · FTP Connection Lifecycle）

    【核心定位】
    - DownloadStep 是 Source Step，仅负责将「远端逻辑文件」物化为本地 raw 文件
    - 所有执行必须遵循 meta-first 语义
    - 本 Step 不承载任何可复用的 I/O 连接状态

    ────────────────────────────────────────────────────────────────────────────
    FTP Connection Lifecycle（冻结级约束）
    ────────────────────────────────────────────────────────────────────────────

    ⚠️ 本 Step 对 FTP 连接的生命周期有严格且不可回退的设计约束：

    1. ftplib.FTP 连接是一次性、不可复用的 I/O 资源
    2. 任意 socket / timeout / I/O 异常后，该 FTP 对象被视为永久失效
    3. Retry 语义必须等价于「全新下载尝试」
    4. 因此：
       - 每一次文件下载 attempt 必须创建新的 FTP 连接
       - 失败的 FTP 实例不得进入下一次 retry
       - FTP 对象严禁跨文件、跨 retry、跨方法边界复用
    5. DownloadStep 实例本身不得持有任何 FTP 连接状态

    【强制约束】
    - FTP existence check 与 FTP download 禁止共享连接
    - Retry decorator 只能作用于“内部自行创建 FTP 连接”的函数
    - 任何试图缓存、复用、池化 ftplib.FTP 的修改，均视为破坏冻结契约

    ────────────────────────────────────────────────────────────────────────────
    Backend 策略（冻结）
    ────────────────────────────────────────────────────────────────────────────

    - curl 是默认且推荐的生产后端
    - ftplib 仅作为 fallback / debug backend 使用

    ────────────────────────────────────────────────────────────────────────────
    回退禁止声明（Hard Freeze）
    ────────────────────────────────────────────────────────────────────────────

    ❌ 禁止的修改包括但不限于：
    - 在 run() 中创建并复用单一 FTP 实例
    - 在 Step / Engine / Context 上缓存 FTP 对象
    - 在 Retry 中复用已失败的 FTP 连接
    - 通过“重置 socket”方式修补 FTP 实例

    任何违反上述规则的改动，必须引入新的 ADR。
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

        self.host = secret.ftp_host
        self.user = secret.ftp_user
        self.password = secret.ftp_password
        self.port = secret.ftp_port

        self.remote_root = remote_root.rstrip("/")

    # ==================================================================
    # Source Step entry
    # ==================================================================
    def run(self, ctx: PipelineContext) -> PipelineContext:
        date_str = self.engine.resolve_date(ctx.today)

        # --------------------------------------------------
        # 1. Existence check（独立 FTP 连接）
        # --------------------------------------------------
        self._check_remote_date_exists(date_str)

        # --------------------------------------------------
        # 2. 列文件名（独立 FTP 连接）
        # --------------------------------------------------
        filenames = self._list_remote_files(date_str)

        plans = self.engine.plan_downloads(
            date=date_str,
            available_files=filenames,
        )

        # --------------------------------------------------
        # 3. 文件级 meta-first 下载
        # --------------------------------------------------
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

            if not meta.upstream_changed():
                logs.info(f"[{self.stage}] meta hit → skip {filename}")
                continue

            with self.inst.timer(f"download_{filename.split('.')[0]}"):
                self._download_one(
                    date=date_str,
                    remote_file=filename,
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

    # ==================================================================
    # Remote helpers (FTP connections are NEVER shared)
    # ==================================================================
    def _check_remote_date_exists(self, date: str) -> None:
        ftp = ftplib.FTP(timeout=1200)
        try:
            ftp.connect(self.host, self.port)
            ftp.login(self.user, self.password)
            ftp.cwd(self.remote_root)
            ftp.cwd(date)
        except ftplib.error_perm as e:
            logs.warning(
                f"[{self.stage}][NO DATA] {date} not found | {e}"
            )
            raise PipelineAbort(str(e))
        finally:
            try:
                ftp.quit()
            except Exception:
                ftp.close()

    def _list_remote_files(self, date: str) -> list[str]:
        ftp = ftplib.FTP(timeout=1200)
        try:
            ftp.connect(self.host, self.port)
            ftp.login(self.user, self.password)
            ftp.cwd(self.remote_root)
            ftp.cwd(date)
            return ftp.nlst()
        finally:
            try:
                ftp.quit()
            except Exception:
                ftp.close()

    # ==================================================================
    # Download dispatch
    # ==================================================================
    def _download_one(
            self,
            *,
            date: str,
            remote_file: str,
            local_path: Path,
    ) -> None:
        if self.backend == DownloadBackend.CURL:
            self._download_by_curl(
                date=date,
                remote_file=remote_file,
                local_path=local_path,
            )
        else:
            self._download_by_ftplib(
                date=date,
                remote_file=remote_file,
                local_path=local_path,
            )

    # ==================================================================
    # ftplib backend (one connection per attempt)
    # ==================================================================
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
            date: str,
            remote_file: str,
            local_path: Path,
    ) -> None:
        logs.info(f"[FTP] ftplib downloading {remote_file}")

        ftp = ftplib.FTP(timeout=1200)
        try:
            ftp.connect(self.host, self.port)
            ftp.login(self.user, self.password)
            ftp.cwd(self.remote_root)
            ftp.cwd(date)

            with open(local_path, "wb") as fh:
                ftp.retrbinary(f"RETR {remote_file}", fh.write)

        finally:
            try:
                ftp.quit()
            except Exception:
                ftp.close()

    # ==================================================================
    # curl backend (preferred)
    # ==================================================================
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
            "--stderr", "-",
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

        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=600,
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"[DownloadStep][curl failed] {remote_file} | "
                f"code={result.returncode} | "
                f"stderr={result.stderr.strip()}"
            )

        stats = json.loads(result.stdout.strip().splitlines()[-1])

        logs.info(
            f"[DownloadStats] {remote_file} | "
            f"speed={stats['speed_bps'] * 8 / 1_000_000:.2f} Mbps "
            f"size={stats['size_bytes'] / 1_000_000_000:.2f} GB"
        )

    # ------------------------------------------------------------------
    def _remote_url(self, *, date: str, filename: str) -> str:
        return (
            f"ftp://{self.host}:{self.port}"
            f"/{self.remote_root}/{date}/{filename}"
        )

    def _remote_upstream_path(self, *, date: str, filename: str) -> Path:
        return Path(self._remote_url(date=date, filename=filename))
