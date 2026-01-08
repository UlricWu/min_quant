# src/api/app.py
from __future__ import annotations

import subprocess
from flask import Flask, jsonify, request
import threading
import uuid
from pathlib import Path
from datetime import datetime

from src import logs
from src.jobs.registry import Job, JobRegistry
import os
import signal
from flask import jsonify
from src.api.decorators import handle_job_not_found

app = Flask(__name__)

REGISTRY = JobRegistry()
LOG_DIR = Path("logs/jobs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

ALLOWED_STATUSES = {"PENDING", "RUNNING", "SUCCESS", "FAILED"}
ALLOWED_TYPES = {"l2", "train", "backtest", "experiment"}

logs.init(scope="api")


@app.before_request
def log_request():
    logs.info(f"[HTTP] {request.method} {request.path}")


def _tail_last_lines(path: Path, n: int = 50) -> str:
    if not path.exists():
        return ""
    lines = path.read_text(errors="ignore").splitlines()
    return "\n".join(lines[-n:])


def _run_job(job: Job) -> None:
    job.status = "RUNNING"
    job.started_at = datetime.utcnow().isoformat()

    log_path = Path(job.log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["MINQUANT_JOB_ID"] = job.job_id
    env["MINQUANT_JOB_TYPE"] = job.job_type

    with log_path.open("a") as f:
        try:
            proc = subprocess.Popen(
                job.cmd,
                stdout=f,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,  # 🔑
            )
            job.pid = proc.pid
            ret = proc.wait()
            job.exit_code = ret
            job.finished_at = datetime.utcnow().isoformat()
            job.status = "SUCCESS" if ret == 0 else "FAILED"
        except Exception as e:
            job.status = "FAILED"
            job.error = repr(e)
            job.finished_at = datetime.utcnow().isoformat()


@app.post("/jobs")
def create_job():
    payload = request.get_json(force=True)
    job_type = payload.get("type")

    if job_type == "l2":
        date = payload.get("date")
        if not date:
            return jsonify({"error": "missing date"}), 400
        cmd = ["python", "-m", "src.cli", "run", date]
    elif job_type == "train":
        cmd = ["python", "-m", "src.cli", "train"]
    elif job_type == "backtest":
        cmd = ["python", "-m", "src.cli", "backtest"]
    elif job_type == "experiment":
        cmd = ["python", "-m", "src.cli", "experiment"]
    else:
        return jsonify({"error": "unknown type"}), 400

    job_id = uuid.uuid4().hex[:10]
    log_file = str(LOG_DIR / f"{job_id}.log")

    job = Job(job_id=job_id, cmd=cmd, log_file=log_file, job_type=job_type)
    REGISTRY.add(job)

    t = threading.Thread(target=_run_job, args=(job,), daemon=True)
    t.start()

    logs.info(f"[API] create job type={job_type}")

    return jsonify({
        "job_id": job_id,
        "cmd": cmd,
        "log_file": log_file,
        "status_url": f"/jobs/{job_id}",
        "log_url": f"/jobs/{job_id}/log?offset=0",
    })


@app.get("/jobs/<job_id>")
@handle_job_not_found
def get_job(job_id: str):
    job = REGISTRY.get(job_id)

    log_path = Path(job.log_file)
    last_lines = _tail_last_lines(log_path, n=80) if job.status in ("FAILED", "SUCCESS") else ""
    return jsonify({
        "job_id": job.job_id,
        "cmd": job.cmd,
        "status": job.status,
        "pid": job.pid,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
        "exit_code": job.exit_code,
        "error": job.error,
        "last_lines": last_lines,
    })


@app.get("/jobs/<job_id>/log")
@handle_job_not_found
def get_job_log(job_id: str):
    job = REGISTRY.get(job_id)

    offset = int(request.args.get("offset", 0))
    log_path = Path(job.log_file)

    if not log_path.exists():
        return jsonify({"data": "", "offset": offset})

    with log_path.open("r", errors="ignore") as f:
        f.seek(offset)
        data = f.read()
        new_offset = f.tell()

    return jsonify({"data": data, "offset": new_offset})


@app.get("/health")
def health():
    return jsonify({"ok": True})


def _run_cmd_sync(cmd: list[str], timeout_sec: int = 3600) -> dict:
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout_sec,
    )
    return {
        "cmd": cmd,
        "exit_code": proc.returncode,
        "stdout": proc.stdout[-20000:],  # 防止太大，截断末尾
        "stderr": proc.stderr[-20000:],
    }


# @app.post("/run/l2")
# def run_l2():
#     payload = request.get_json(force=True)
#     date = payload.get("date")
#     if not date:
#         return jsonify({"error": "missing date"}), 400
#
#     cmd = ["python", "-m", "src.cli", "run", date]
#     out = _run_cmd_sync(cmd)
#     status = 200 if out["exit_code"] == 0 else 500
#     return jsonify(out), status
#
#
# @app.post("/run/train")
# def run_train():
#     cmd = ["python", "-m", "src.cli", "train"]
#     out = _run_cmd_sync(cmd)
#     status = 200 if out["exit_code"] == 0 else 500
#     return jsonify(out), status
#
#
# @app.post("/run/backtest")
# def run_backtest():
#     cmd = ["python", "-m", "src.cli", "backtest"]
#     out = _run_cmd_sync(cmd)
#     status = 200 if out["exit_code"] == 0 else 500
#     return jsonify(out), status


def _kill_pid(pid: int) -> None:
    """
    Kill a process by pid.
    Raises OSError if pid does not exist or permission denied.
    """
    os.kill(pid, signal.SIGTERM)


@app.post("/jobs/<job_id>/kill")
@handle_job_not_found
def kill_job(job_id: str):
    job = REGISTRY.get(job_id)

    # -------- 1. 状态校验（唯一前置条件） --------
    if job.status not in ("PENDING", "RUNNING"):
        return jsonify({
            "job_id": job.job_id,
            "status": job.status,
            "error": "job already finished",
        }), 400

    # -------- 2. Kill（仅当 pid 存在时） --------
    if job.pid is not None:
        try:
            _kill_pid(job.pid)
        except ProcessLookupError:
            job.status = "FAILED"
            job.error = "process not found"
            job.finished_at = datetime.utcnow().isoformat()
            return jsonify({
                "job_id": job.job_id,
                "status": job.status,
                "error": job.error,
            }), 410
        except PermissionError as e:
            return jsonify({
                "job_id": job.job_id,
                "status": job.status,
                "error": f"permission denied: {e}",
            }), 403

    # -------- 3. 更新状态 --------
    job.status = "FAILED"
    job.exit_code = -signal.SIGTERM
    job.finished_at = datetime.utcnow().isoformat()
    job.error = "killed by user"

    return jsonify({
        "job_id": job.job_id,
        "status": job.status,
        "pid": job.pid,
        "message": "job killed",
    })


@app.get("/jobs")
def list_jobs():
    jobs = REGISTRY.list()

    # # 按创建时间倒序（最新在前）
    # jobs = sorted(
    #     jobs,
    #     key=lambda j: j.created_at,
    #     reverse=True,
    # )
    # -------- status filter --------
    status = request.args.get("status")
    if status is not None:
        if status not in ALLOWED_STATUSES:
            return jsonify({
                "error": "invalid status",
                "allowed": sorted(ALLOWED_STATUSES),
            }), 400
        jobs = [j for j in jobs if j.status == status]

    # -------- type filter --------
    job_type = request.args.get("type")
    if job_type is not None:
        if job_type not in ALLOWED_TYPES:
            return jsonify({
                "error": "invalid type",
                "allowed": sorted(ALLOWED_TYPES),
            }), 400
        jobs = [j for j in jobs if j.job_type == job_type]

    # 最新的在前
    jobs = sorted(jobs, key=lambda j: j.created_at, reverse=True)

    return jsonify({
        "count": len(jobs),
        "jobs": [
            {
                "job_id": j.job_id,
                "type": j.job_type,
                "status": j.status,
                "pid": j.pid,
                "created_at": j.created_at,
                "started_at": j.started_at,
                "finished_at": j.finished_at,
                "error": j.error,
            }
            for j in jobs
        ]
    })


if __name__ == "__main__":
    # 关键：允许 python -m src.api.app 启动
    app.run(host="0.0.0.0", port=5000)
