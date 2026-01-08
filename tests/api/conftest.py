from __future__ import annotations

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
