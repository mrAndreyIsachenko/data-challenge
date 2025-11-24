from __future__ import annotations

from typing import Any

import requests
from loguru import logger

from pipeline_anomaly.domain.models.anomaly import AnomalyReport
from pipeline_anomaly.domain.services.interfaces import AlertSink


class HttpAlertSink(AlertSink):
    """Отправляет отчёт об аномалиях на произвольный webhook."""

    def __init__(self, url: str, timeout: float = 5.0) -> None:
        if not url:
            raise ValueError("webhook url is required for http sink")
        self._url = url
        self._timeout = timeout

    def send(self, report: AnomalyReport) -> None:
        payload = _serialize_report(report)
        response = requests.post(self._url, json=payload, timeout=self._timeout)
        response.raise_for_status()
        logger.info("http alert sink delivered payload (status=%s)", response.status_code)


def _serialize_report(report: AnomalyReport) -> dict[str, Any]:
    return {
        "generated_at": report.generated_at.isoformat(),
        "window_start": report.window_start.isoformat(),
        "window_end": report.window_end.isoformat(),
        "max_severity": report.highest_severity(),
        "anomalies": [
            {
                "detector": anomaly.detector,
                "score": anomaly.score,
                "severity": anomaly.severity,
                "description": anomaly.description,
            }
            for anomaly in report.anomalies
        ],
    }
