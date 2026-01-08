from __future__ import annotations

import subprocess
from types import SimpleNamespace


class DummyProcess:
    def __init__(self):
        self.pid = 12345

    def wait(self):
        return 0


def test_create_job_l2(client, monkeypatch):
    def fake_popen(*args, **kwargs):
        return DummyProcess()

    monkeypatch.setattr(subprocess, "Popen", fake_popen)

    resp = client.post(
        "/jobs",
        json={"type": "l2", "date": "2026-01-07"},
    )

    assert resp.status_code == 200

    data = resp.json
    assert "job_id" in data
    assert "log_url" in data
