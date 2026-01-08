from __future__ import annotations

import subprocess
import signal
import time

import threading
import time


def test_kill_job(client, dummy_process):
    resp = client.post("/jobs", json={"type": "train"})
    job_id = resp.json["job_id"]

    kill_resp = client.post(f"/jobs/{job_id}/kill")
    assert kill_resp.status_code == 200



def test_job_kill_not_found(client):
    resp = client.post("/jobs/non_exist/kill")
    assert resp.status_code == 404


def test_job_status_not_found(client):
    resp = client.get("/jobs/non_exist")
    assert resp.status_code == 404
