from __future__ import annotations

import logging
import time
from typing import Any

import requests

from .config import Settings

LOGGER = logging.getLogger(__name__)


class OpenMeteoClient:
    BASE_URL = "https://api.open-meteo.com/v1/forecast"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def fetch_hourly_weather(self) -> dict[str, Any]:
        params = {
            "latitude": self.settings.latitude,
            "longitude": self.settings.longitude,
            "hourly": ",".join(self.settings.hourly_fields),
            "timezone": "UTC",
            "past_days": 1,
            "forecast_days": 1,
        }

        last_error: Exception | None = None
        attempts = max(self.settings.retries, 1)

        for attempt in range(1, attempts + 1):
            try:
                response = requests.get(
                    self.BASE_URL,
                    params=params,
                    timeout=self.settings.timeout_seconds,
                )
                response.raise_for_status()
                payload = response.json()

                hourly = payload.get("hourly", {})
                if "time" not in hourly:
                    raise ValueError("Open-Meteo response missing hourly.time")

                LOGGER.info(
                    "extract_success",
                    extra={
                        "attempt": attempt,
                        "status_code": response.status_code,
                        "records": len(hourly.get("time", [])),
                    },
                )
                return payload
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                LOGGER.warning(
                    "extract_retry",
                    extra={"attempt": attempt, "error": str(exc)},
                )
                if attempt < attempts:
                    sleep_seconds = self.settings.backoff_seconds * (2 ** (attempt - 1))
                    time.sleep(sleep_seconds)

        raise RuntimeError(f"Failed to fetch Open-Meteo data: {last_error}")
