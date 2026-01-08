from __future__ import annotations

import subprocess
import threading

import pytest

from src.api.app import app

from src.api.app import REGISTRY


@pytest.fixture
def client():
    """
    Flask test client (no real server).
    """
    # ⭐ 关键：每个 test 前清空 registry
    REGISTRY.clear()
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


# =============================================================================
# Blocking DummyProcess (shared by all API tests)
# =============================================================================

class DummyProcess:
    """
    A blocking dummy subprocess that simulates a long-running job.

    Contract:
    - wait() blocks until terminate() is called
    - pid is always available
    """

    def __init__(self):
        self.pid = 99999
        self._stop = threading.Event()

    def wait(self):
        while not self._stop.is_set():
            time.sleep(0.05)
        return 0

    def terminate(self):
        self._stop.set()


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def dummy_process(monkeypatch):
    """
    Patch subprocess.Popen to return a blocking DummyProcess.

    Returns:
        DummyProcess: the process instance created for the job
    """
    proc_holder = {}

    def fake_popen(*args, **kwargs):
        proc = DummyProcess()
        proc_holder["proc"] = proc
        return proc

    monkeypatch.setattr(subprocess, "Popen", fake_popen)
    yield proc_holder

    # safety cleanup
    proc = proc_holder.get("proc")
    if proc:
        proc.terminate()
