from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def transform_open_meteo(payload: dict[str, Any], source: str = "open-meteo") -> list[dict[str, Any]]:
    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])
    latitude = payload.get("latitude")
    longitude = payload.get("longitude")

    records: list[dict[str, Any]] = []
    ingested_at = datetime.now(timezone.utc)

    for idx, ts in enumerate(times):
        observed_at = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
        record = {
            "source": source,
            "latitude": _to_float(latitude),
            "longitude": _to_float(longitude),
            "observed_at": observed_at,
            "temperature_2m": _to_float(_value_at(hourly, "temperature_2m", idx)),
            "relative_humidity_2m": _to_float(_value_at(hourly, "relative_humidity_2m", idx)),
            "precipitation": _to_float(_value_at(hourly, "precipitation", idx)),
            "wind_speed_10m": _to_float(_value_at(hourly, "wind_speed_10m", idx)),
            "ingested_at": ingested_at,
        }
        records.append(record)

    # Keep only unique rows by natural key to avoid redundant writes before upsert.
    deduped = {
        (r["source"], r["latitude"], r["longitude"], r["observed_at"]): r for r in records
    }
    return list(deduped.values())


def evaluate_data_quality(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    total = len(records)
    if total == 0:
        return [
            {
                "check_name": "non_empty_batch",
                "status": "warn",
                "severity": "warning",
                "details": {"records": 0},
            }
        ]

    checks: list[dict[str, Any]] = []
    fields = [
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "wind_speed_10m",
    ]
    for field in fields:
        null_count = sum(1 for row in records if row.get(field) is None)
        null_rate = null_count / total
        status = "pass"
        severity = "info"
        if null_rate > 0.05:
            status = "fail"
            severity = "critical"
        elif null_count > 0:
            status = "warn"
            severity = "warning"
        checks.append(
            {
                "check_name": f"null_rate_{field}",
                "status": status,
                "severity": severity,
                "details": {
                    "null_count": null_count,
                    "records": total,
                    "null_rate": round(null_rate, 4),
                    "threshold": 0.05,
                },
            }
        )

    out_of_range = 0
    for row in records:
        value = row.get("temperature_2m")
        if value is None:
            continue
        if value < -80 or value > 60:
            out_of_range += 1

    checks.append(
        {
            "check_name": "temperature_range",
            "status": "fail" if out_of_range > 0 else "pass",
            "severity": "critical" if out_of_range > 0 else "info",
            "details": {
                "out_of_range_count": out_of_range,
                "min_allowed": -80,
                "max_allowed": 60,
            },
        }
    )

    missing_coordinates = sum(
        1 for row in records if row.get("latitude") is None or row.get("longitude") is None
    )
    checks.append(
        {
            "check_name": "coordinate_presence",
            "status": "fail" if missing_coordinates > 0 else "pass",
            "severity": "critical" if missing_coordinates > 0 else "info",
            "details": {
                "missing_count": missing_coordinates,
                "records": total,
            },
        }
    )

    return checks


def _value_at(hourly: dict[str, Any], field: str, idx: int) -> Any:
    values = hourly.get(field)
    if not isinstance(values, list):
        return None
    if idx >= len(values):
        return None
    return values[idx]
