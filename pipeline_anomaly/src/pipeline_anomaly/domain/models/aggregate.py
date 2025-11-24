from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Mapping


@dataclass(frozen=True, slots=True)
class Aggregate:
    metric: str
    value: float
    window_start: datetime
    window_end: datetime
    extra: Mapping[str, float] | None = None


@dataclass(frozen=True, slots=True)
class AggregateCollection:
    """Набор агрегатов, рассчитанных по батчу."""

    aggregates: tuple[Aggregate, ...]

    def as_dict(self) -> list[dict[str, object]]:
        payload: list[dict[str, object]] = []
        for aggregate in self.aggregates:
            row: dict[str, object] = {
                "metric": aggregate.metric,
                "value": aggregate.value,
                "window_start": aggregate.window_start.isoformat(),
                "window_end": aggregate.window_end.isoformat(),
                "extra": dict(aggregate.extra) if aggregate.extra else {},
            }
            payload.append(row)
        return payload
