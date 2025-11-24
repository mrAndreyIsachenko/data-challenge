from __future__ import annotations

from datetime import datetime

import pandas as pd

from pipeline_anomaly.domain.models.aggregate import Aggregate, AggregateCollection
from pipeline_anomaly.domain.services.interfaces import ClickHouseWriter


class ComputeAggregates:
    def __init__(self, writer: ClickHouseWriter, windows: tuple[str, ...]) -> None:
        self._writer = writer
        self._windows = windows

    def execute(self) -> AggregateCollection:
        dataframe = self._writer.read_latest_window()
        dataframe = dataframe.sort_values("event_time")
        window_start = dataframe["event_time"].min()
        window_end = dataframe["event_time"].max()

        aggregates = []
        aggregates.extend(self._base_metrics(dataframe, window_start, window_end))
        aggregates.extend(self._chain_metrics(dataframe, window_start, window_end))
        aggregates.extend(self._window_metrics(dataframe, window_end))

        collection = AggregateCollection(aggregates=tuple(aggregates))

        self._writer.persist_aggregates(collection)
        return collection

    def _base_metrics(self, dataframe: pd.DataFrame, window_start: datetime, window_end: datetime) -> list[Aggregate]:
        metrics = [
            Aggregate("count", float(len(dataframe)), window_start, window_end),
            Aggregate("mean_value", float(dataframe["value"].mean()), window_start, window_end),
            Aggregate("std_value", float(dataframe["value"].std()), window_start, window_end),
            Aggregate("p95_value", float(dataframe["value"].quantile(0.95)), window_start, window_end),
            Aggregate("p05_value", float(dataframe["value"].quantile(0.05)), window_start, window_end),
            Aggregate("mean_gas_used", float(dataframe["gas_used"].mean()), window_start, window_end),
            Aggregate("median_calldata", float(dataframe["calldata_size"].median()), window_start, window_end),
            Aggregate(
                "high_calldata_ratio",
                float((dataframe["calldata_size"] > 1024).sum() / len(dataframe)),
                window_start,
                window_end,
            ),
        ]
        return metrics

    def _chain_metrics(self, dataframe: pd.DataFrame, window_start: datetime, window_end: datetime) -> list[Aggregate]:
        metrics: list[Aggregate] = []
        grouped = dataframe.groupby("chain_id")
        for chain_id, frame in grouped:
            metrics.append(
                Aggregate(f"count_chain_{int(chain_id)}", float(len(frame)), window_start, window_end)
            )
            metrics.append(
                Aggregate(
                    f"mean_value_chain_{int(chain_id)}",
                    float(frame["value"].mean()),
                    window_start,
                    window_end,
                )
            )
        return metrics

    def _window_metrics(self, dataframe: pd.DataFrame, window_end: datetime) -> list[Aggregate]:
        metrics: list[Aggregate] = []
        if not self._windows:
            return metrics
        for window in self._windows:
            try:
                delta = pd.to_timedelta(window)
            except ValueError as exc:
                raise ValueError(f"invalid window {window}") from exc
            window_df = dataframe[dataframe["event_time"] >= (window_end - delta)]
            if window_df.empty:
                continue
            window_start = window_df["event_time"].min()
            metrics.append(
                Aggregate(f"count_last_{window}", float(len(window_df)), window_start, window_end)
            )
            metrics.append(
                Aggregate(
                    f"mean_value_last_{window}",
                    float(window_df["value"].mean()),
                    window_start,
                    window_end,
                )
            )
        return metrics
