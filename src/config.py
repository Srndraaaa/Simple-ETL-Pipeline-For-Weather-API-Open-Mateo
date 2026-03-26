from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    postgres_db: str
    postgres_user: str
    postgres_password: str
    postgres_host: str
    postgres_port: int
    schedule_interval_hours: int
    latitude: float
    longitude: float
    hourly_fields: list[str]
    timeout_seconds: int
    retries: int
    backoff_seconds: int
    log_level: str

    @property
    def dsn(self) -> str:
        return (
            f"dbname={self.postgres_db} "
            f"user={self.postgres_user} "
            f"password={self.postgres_password} "
            f"host={self.postgres_host} "
            f"port={self.postgres_port}"
        )


def load_settings() -> Settings:
    hourly = os.getenv(
        "OPEN_METEO_HOURLY_FIELDS",
        "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
    )
    hourly_fields = [x.strip() for x in hourly.split(",") if x.strip()]

    return Settings(
        postgres_db=os.getenv("POSTGRES_DB", "etl_db"),
        postgres_user=os.getenv("POSTGRES_USER", "postgres"),
        postgres_password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
        postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
        schedule_interval_hours=int(os.getenv("SCHEDULE_INTERVAL_HOURS", "2")),
        latitude=float(os.getenv("OPEN_METEO_LATITUDE", "-6.2088")),
        longitude=float(os.getenv("OPEN_METEO_LONGITUDE", "106.8456")),
        hourly_fields=hourly_fields,
        timeout_seconds=int(os.getenv("OPEN_METEO_TIMEOUT_SECONDS", "30")),
        retries=int(os.getenv("OPEN_METEO_RETRIES", "3")),
        backoff_seconds=int(os.getenv("OPEN_METEO_BACKOFF_SECONDS", "1")),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
    )
