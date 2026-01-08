# src/jobs/registry.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

JobStatus = Literal["PENDING", "RUNNING", "SUCCESS", "FAILED"]

JobType = Literal["l2", "train", "backtest", "experiment"]


@dataclass
class Job:
    job_id: str
    job_type: JobType  # ✅ 新增：冻结字段
    cmd: list[str]
    log_file: str
    status: JobStatus = "PENDING"
    pid: int | None = None
    created_at: str = ""
    started_at: str | None = None
    finished_at: str | None = None
    exit_code: int | None = None
    error: str | None = None

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.utcnow().isoformat()


class JobRegistry:
    def __init__(self):
        self._jobs: dict[str, Job] = {}

    def add(self, job: Job) -> None:
        self._jobs[job.job_id] = job

    def get(self, job_id: str) -> Job:
        return self._jobs[job_id]

    def list(self) -> list[Job]:
        """
        Return all jobs (read-only view).
        """
        return list(self._jobs.values())

    def clear(self) -> None:
        self._jobs.clear()
