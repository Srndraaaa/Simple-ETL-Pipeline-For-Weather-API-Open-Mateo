from datetime import timezone

from src.transform import evaluate_data_quality, transform_open_meteo


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


def test_evaluate_data_quality_pass_for_valid_records() -> None:
    rows = [
        {
            "source": "open-meteo",
            "latitude": -6.2,
            "longitude": 106.8,
            "observed_at": None,
            "temperature_2m": 30.0,
            "relative_humidity_2m": 70.0,
            "precipitation": 0.1,
            "wind_speed_10m": 12.0,
            "ingested_at": None,
        }
    ]

    checks = evaluate_data_quality(rows)
    by_name = {check["check_name"]: check for check in checks}

    assert by_name["null_rate_temperature_2m"]["status"] == "pass"
    assert by_name["temperature_range"]["status"] == "pass"
    assert by_name["coordinate_presence"]["status"] == "pass"


def test_evaluate_data_quality_flags_outliers_and_missing_coordinates() -> None:
    rows = [
        {
            "source": "open-meteo",
            "latitude": None,
            "longitude": 106.8,
            "observed_at": None,
            "temperature_2m": 99.0,
            "relative_humidity_2m": None,
            "precipitation": 0.0,
            "wind_speed_10m": 10.0,
            "ingested_at": None,
        }
    ]

    checks = evaluate_data_quality(rows)
    by_name = {check["check_name"]: check for check in checks}

    assert by_name["temperature_range"]["status"] == "fail"
    assert by_name["coordinate_presence"]["status"] == "fail"
    assert by_name["null_rate_relative_humidity_2m"]["status"] == "fail"
