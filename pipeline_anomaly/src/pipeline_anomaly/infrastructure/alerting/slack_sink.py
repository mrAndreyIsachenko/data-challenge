from __future__ import annotations

from typing import Any

import requests
from loguru import logger

from pipeline_anomaly.domain.models.anomaly import AnomalyReport
from pipeline_anomaly.domain.services.interfaces import AlertSink


class SlackAlertSink(AlertSink):
    """Отправляет алерты в Slack через webhook."""

    def __init__(self, webhook_url: str, channel: str | None = None, timeout: float = 5.0) -> None:
        if not webhook_url:
            raise ValueError("slack webhook url must be provided")
        self._webhook_url = webhook_url
        self._channel = channel
        self._timeout = timeout

    def send(self, report: AnomalyReport) -> None:
        text = self._build_message(report)
        payload: dict[str, Any] = {"text": text}
        if self._channel:
            payload["channel"] = self._channel
        response = requests.post(self._webhook_url, json=payload, timeout=self._timeout)
        response.raise_for_status()
        logger.info("slack alert delivered (status=%s)", response.status_code)

    def _build_message(self, report: AnomalyReport) -> str:
        lines = [
            f"*Anomaly alert* severity={report.highest_severity():.3f}",
            f"window: {report.window_start.isoformat()} — {report.window_end.isoformat()}",
        ]
        for anomaly in report.anomalies:
            lines.append(f"- `{anomaly.detector}` score={anomaly.score:.3f} severity={anomaly.severity:.3f}")
        return "\n".join(lines)
