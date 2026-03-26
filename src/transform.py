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


def _value_at(hourly: dict[str, Any], field: str, idx: int) -> Any:
    values = hourly.get(field)
    if not isinstance(values, list):
        return None
    if idx >= len(values):
        return None
    return values[idx]
