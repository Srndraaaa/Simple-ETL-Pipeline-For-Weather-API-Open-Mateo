from datetime import timezone

from src.transform import transform_open_meteo


def test_transform_open_meteo_maps_and_deduplicates() -> None:
    payload = {
        "latitude": -6.2,
        "longitude": 106.8,
        "hourly": {
            "time": ["2026-03-26T00:00", "2026-03-26T00:00", "2026-03-26T01:00"],
            "temperature_2m": [29.1, 29.1, 28.7],
            "relative_humidity_2m": [80, 80, 82],
            "precipitation": [0.1, 0.1, 0.0],
            "wind_speed_10m": [12.0, 12.0, 11.5],
        },
    }

    rows = transform_open_meteo(payload)

    assert len(rows) == 2
    first = sorted(rows, key=lambda x: x["observed_at"])[0]
    assert first["source"] == "open-meteo"
    assert first["latitude"] == -6.2
    assert first["longitude"] == 106.8
    assert first["temperature_2m"] == 29.1
    assert first["observed_at"].tzinfo == timezone.utc
