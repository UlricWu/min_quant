from __future__ import annotations

from functools import wraps
from flask import jsonify
from typing import Callable, Any


def handle_job_not_found(func: Callable[..., Any]):
    """
    Decorator: convert JobRegistry KeyError into HTTP 404.

    Contract (FROZEN):
    - Only catches KeyError
    - Assumes KeyError means job_id not found
    - Returns JSON {error, job_id}
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyError as e:
            # convention: job_id is always a path parameter
            job_id = kwargs.get("job_id")
            return jsonify({
                "error": "job not found",
                "job_id": job_id,
            }), 404

    return wrapper
