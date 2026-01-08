from __future__ import annotations

import subprocess
from types import SimpleNamespace


def test_create_job_l2(client):

    resp = client.post(
        "/jobs",
        json={"type": "l2", "date": "2026-01-07"},
    )

    assert resp.status_code == 200

    data = resp.json
    assert "job_id" in data
    assert "log_file" in data
