def test_job_log_non_exist(client):
    resp = client.get("/jobs/non_exist/log?offset=0")
    assert resp.status_code == 404
    assert resp.json["error"] == "job not found"
