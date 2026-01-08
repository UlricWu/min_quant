def test_list_jobs_empty(client):
    resp = client.get("/jobs")
    assert resp.status_code == 200
    assert resp.json["count"] == 0
    assert resp.json["jobs"] == []


def test_list_jobs_with_one_job(client, monkeypatch):
    import subprocess

    class DummyProcess:
        def __init__(self):
            self.pid = 12345

        def wait(self):
            return 0

    monkeypatch.setattr(subprocess, "Popen", lambda *a, **k: DummyProcess())

    resp = client.post("/jobs", json={"type": "train"})
    assert resp.status_code == 200

    list_resp = client.get("/jobs")
    data = list_resp.json

    assert data["count"] == 1
    job = data["jobs"][0]

    assert job["type"] == "train"
    assert job["status"] in ("PENDING", "RUNNING", "SUCCESS")
    assert job["job_id"]


def test_list_jobs_empty_with_status(client):
    resp = client.get("/jobs?status=RUNNING")
    assert resp.status_code == 200
    assert resp.json["jobs"] == []


def test_list_jobs_filter_by_status(client):
    # create job
    client.post("/jobs", json={"type": "train"})

    # stable RUNNING state
    resp = client.get("/jobs?status=RUNNING")
    assert resp.status_code == 200
    assert resp.json["count"] == 1
    assert resp.json["jobs"][0]["type"] == "train"



def test_list_jobs_invalid_status(client):
    resp = client.get("/jobs?status=FOO")
    assert resp.status_code == 400
    assert resp.json["error"] == "invalid status"
