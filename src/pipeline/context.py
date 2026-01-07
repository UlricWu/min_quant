# src/training/context.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class BaseContext:
    """
    BaseContext (FINAL / FROZEN)

    Infrastructure marker base class.

    Design invariants:
    - Defines NO dataclass fields
    - Carries no business or runtime data
    - Exists only for type identity
    """
    pass
