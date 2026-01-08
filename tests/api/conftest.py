from __future__ import annotations

import pytest

from src.api.app import app


@pytest.fixture
def client():
    """
    Flask test client (no real server).
    """
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client
