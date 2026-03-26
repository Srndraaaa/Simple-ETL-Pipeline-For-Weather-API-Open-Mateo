from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import psycopg

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS weather_observations (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    temperature_2m DOUBLE PRECISION,
    relative_humidity_2m DOUBLE PRECISION,
    precipitation DOUBLE PRECISION,
    wind_speed_10m DOUBLE PRECISION,
    ingested_at TIMESTAMPTZ NOT NULL,
    UNIQUE (source, latitude, longitude, observed_at)
);

CREATE INDEX IF NOT EXISTS idx_weather_observations_observed_at
    ON weather_observations (observed_at DESC);
"""

UPSERT_SQL = """
INSERT INTO weather_observations (
    source,
    latitude,
    longitude,
    observed_at,
    temperature_2m,
    relative_humidity_2m,
    precipitation,
    wind_speed_10m,
    ingested_at
)
VALUES (
    %(source)s,
    %(latitude)s,
    %(longitude)s,
    %(observed_at)s,
    %(temperature_2m)s,
    %(relative_humidity_2m)s,
    %(precipitation)s,
    %(wind_speed_10m)s,
    %(ingested_at)s
)
ON CONFLICT (source, latitude, longitude, observed_at)
DO UPDATE SET
    temperature_2m = EXCLUDED.temperature_2m,
    relative_humidity_2m = EXCLUDED.relative_humidity_2m,
    precipitation = EXCLUDED.precipitation,
    wind_speed_10m = EXCLUDED.wind_speed_10m,
    ingested_at = EXCLUDED.ingested_at;
"""


def bootstrap_schema(dsn: str) -> None:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.commit()


def load_records(dsn: str, records: Iterable[dict[str, Any]]) -> int:
    records_list = list(records)
    if not records_list:
        return 0

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.executemany(UPSERT_SQL, records_list)
        conn.commit()

    return len(records_list)


def healthcheck_db(dsn: str) -> bool:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
            return bool(result and result[0] == 1)
