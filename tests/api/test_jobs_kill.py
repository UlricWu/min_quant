from __future__ import annotations

import subprocess
import signal
import time


import threading
import time


class DummyProcess:
    def __init__(self):
        self.pid = 99999
        self._killed = threading.Event()

    def wait(self):
        # 模拟一个长期运行的进程
        # 直到被 kill
        while not self._killed.is_set():
            time.sleep(0.05)
        return -15  # 模拟 SIGTERM

    def terminate(self):
        self._killed.set()



def test_kill_job(client, monkeypatch):
    # 1. fake popen
    def fake_popen(*args, **kwargs):
        return DummyProcess()

    monkeypatch.setattr(subprocess, "Popen", fake_popen)

    # 2. fake os.kill
    killed = {}

    def fake_kill(pid, sig):
        killed["pid"] = pid
        killed["sig"] = sig

    monkeypatch.setattr("os.kill", fake_kill)

    # 3. create job
    resp = client.post("/jobs", json={"type": "train"})
    job_id = resp.json["job_id"]

    # 4. 等待 job 进入 RUNNING（最多 1s）
    for _ in range(10):
        status_resp = client.get(f"/jobs/{job_id}")
        if status_resp.json["pid"] is not None:
            break
        time.sleep(0.1)
    else:
        raise AssertionError("job did not enter RUNNING state")

    # 5. kill job
    kill_resp = client.post(f"/jobs/{job_id}/kill")
    assert kill_resp.status_code == 200
    assert killed["sig"] == signal.SIGTERM
def test_job_kill_not_found(client):
    resp = client.post("/jobs/non_exist/kill")
    assert resp.status_code == 404
def test_job_status_not_found(client):
    resp = client.get("/jobs/non_exist")
    assert resp.status_code == 404
