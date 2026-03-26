from unittest.mock import MagicMock, patch

import pytest
import requests

from src.api_client import OpenMeteoClient
from src.config import Settings


def _settings() -> Settings:
    return Settings(
        postgres_db="etl_db",
        postgres_user="postgres",
        postgres_password="postgres",
        postgres_host="localhost",
        postgres_port=5432,
        schedule_interval_hours=2,
        latitude=-6.2,
        longitude=106.8,
        hourly_fields=["temperature_2m"],
        timeout_seconds=5,
        retries=2,
        backoff_seconds=0,
        log_level="INFO",
    )


@patch("src.api_client.requests.get")
def test_fetch_hourly_weather_success(mock_get: MagicMock) -> None:
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "hourly": {"time": ["2026-03-26T00:00"], "temperature_2m": [30.0]}
    }
    response.raise_for_status.return_value = None
    mock_get.return_value = response

    payload = OpenMeteoClient(_settings()).fetch_hourly_weather()

    assert payload["hourly"]["time"] == ["2026-03-26T00:00"]
    assert mock_get.call_count == 1


@patch("src.api_client.requests.get")
def test_fetch_hourly_weather_raises_after_retries(mock_get: MagicMock) -> None:
    mock_get.side_effect = requests.RequestException("boom")

    with pytest.raises(RuntimeError):
        OpenMeteoClient(_settings()).fetch_hourly_weather()

    assert mock_get.call_count == 2
