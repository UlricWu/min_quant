from __future__ import annotations
from dataclasses import dataclass
from typing import Iterator
# src/backtest/core/time.py
US_PER_SECOND = 1_000_000
US_PER_MINUTE = 60_000_000


@dataclass(frozen=True)
class ReplayClock:
    """
    Deterministic replay clock.

    - Defines how historical time advances
    - Engine consumes it, never mutates it
    """
    start_us: int
    end_us: int
    step_us: int = US_PER_SECOND

    def __iter__(self) -> Iterator[int]:
        t = self.start_us
        while t <= self.end_us:
            yield t
            t += self.step_us


def is_minute_boundary(ts_us: int) -> bool:
    return ts_us % US_PER_MINUTE == 0
