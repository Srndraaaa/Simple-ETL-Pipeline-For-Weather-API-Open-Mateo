from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json

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

CREATE TABLE IF NOT EXISTS etl_runs (
    id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    records_transformed INT,
    records_loaded INT,
    error_message TEXT,
    duration_ms INT
);

CREATE INDEX IF NOT EXISTS idx_etl_runs_started_at
    ON etl_runs (started_at DESC);

CREATE TABLE IF NOT EXISTS data_quality_checks (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES etl_runs(id) ON DELETE CASCADE,
    check_name TEXT NOT NULL,
    status TEXT NOT NULL,
    details JSONB,
    severity TEXT NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_data_quality_checks_run_id
    ON data_quality_checks (run_id);

CREATE TABLE IF NOT EXISTS etl_logs (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT REFERENCES etl_runs(id) ON DELETE CASCADE,
    level TEXT NOT NULL,
    logger TEXT,
    message TEXT NOT NULL,
    extra JSONB,
    exception TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_etl_logs_created_at
    ON etl_logs (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_etl_logs_run_id
    ON etl_logs (run_id);
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


def create_etl_run(dsn: str, started_at: Any) -> int:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO etl_runs (started_at, status)
                VALUES (%s, 'running')
                RETURNING id
                """,
                (started_at,),
            )
            row = cur.fetchone()
        conn.commit()

    if not row:
        raise RuntimeError("Failed to create etl run")
    return int(row[0])


def complete_etl_run(
    dsn: str,
    run_id: int,
    status: str,
    completed_at: Any,
    duration_ms: int,
    records_transformed: int | None,
    records_loaded: int | None,
    error_message: str | None,
) -> None:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE etl_runs
                SET completed_at = %s,
                    status = %s,
                    records_transformed = %s,
                    records_loaded = %s,
                    error_message = %s,
                    duration_ms = %s
                WHERE id = %s
                """,
                (
                    completed_at,
                    status,
                    records_transformed,
                    records_loaded,
                    error_message,
                    duration_ms,
                    run_id,
                ),
            )
        conn.commit()


def insert_data_quality_checks(dsn: str, run_id: int, checks: Iterable[dict[str, Any]]) -> None:
    rows = list(checks)
    if not rows:
        return

    query_rows = []
    for row in rows:
        query_rows.append(
            {
                "run_id": run_id,
                "check_name": row["check_name"],
                "status": row["status"],
                "details": Json(row.get("details", {})),
                "severity": row["severity"],
            }
        )

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO data_quality_checks (run_id, check_name, status, details, severity)
                VALUES (%(run_id)s, %(check_name)s, %(status)s, %(details)s, %(severity)s)
                """,
                query_rows,
            )
        conn.commit()


def get_dashboard_metrics(dsn: str, hours: int = 24) -> dict[str, Any]:
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE status = 'success') AS succeeded_runs,
                    COUNT(*) FILTER (WHERE status = 'failed') AS failed_runs,
                    COALESCE(AVG(duration_ms), 0) AS avg_duration_ms,
                    COALESCE(SUM(records_loaded), 0) AS records_loaded,
                    MAX(completed_at) FILTER (WHERE status = 'success') AS last_success_at,
                    MAX(completed_at) FILTER (WHERE status = 'failed') AS last_failure_at
                FROM etl_runs
                WHERE started_at >= NOW() - (%s * INTERVAL '1 hour')
                """,
                (hours,),
            )
            metrics = cur.fetchone() or {}

            cur.execute("SELECT MAX(observed_at) AS latest_observed_at FROM weather_observations")
            latest = cur.fetchone() or {}

    return {
        "window_hours": hours,
        "succeeded_runs": int(metrics.get("succeeded_runs") or 0),
        "failed_runs": int(metrics.get("failed_runs") or 0),
        "avg_duration_ms": float(metrics.get("avg_duration_ms") or 0),
        "records_loaded": int(metrics.get("records_loaded") or 0),
        "last_success_at": metrics.get("last_success_at"),
        "last_failure_at": metrics.get("last_failure_at"),
        "latest_observed_at": latest.get("latest_observed_at"),
    }


def get_run_history(dsn: str, limit: int = 50) -> list[dict[str, Any]]:
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    started_at,
                    completed_at,
                    status,
                    records_transformed,
                    records_loaded,
                    duration_ms,
                    error_message
                FROM etl_runs
                ORDER BY started_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()

    return list(rows)


def get_data_quality_summary(dsn: str, hours: int = 24) -> dict[str, Any]:
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    dq.check_name,
                    dq.status,
                    COUNT(*) AS total
                FROM data_quality_checks dq
                JOIN etl_runs er ON er.id = dq.run_id
                WHERE er.started_at >= NOW() - (%s * INTERVAL '1 hour')
                GROUP BY dq.check_name, dq.status
                ORDER BY dq.check_name, dq.status
                """,
                (hours,),
            )
            grouped = cur.fetchall()

            cur.execute(
                """
                SELECT
                    er.started_at,
                    dq.check_name,
                    dq.status,
                    dq.severity,
                    dq.details
                FROM data_quality_checks dq
                JOIN etl_runs er ON er.id = dq.run_id
                WHERE er.started_at >= NOW() - (%s * INTERVAL '1 hour')
                  AND dq.status <> 'pass'
                ORDER BY er.started_at DESC
                LIMIT 20
                """,
                (hours,),
            )
            latest_issues = cur.fetchall()

    summary: dict[str, dict[str, int]] = {}
    for item in grouped:
        check_name = str(item["check_name"])
        status = str(item["status"])
        total = int(item["total"])
        summary.setdefault(check_name, {})[status] = total

    return {
        "window_hours": hours,
        "checks": summary,
        "latest_issues": list(latest_issues),
    }


def insert_log(
    dsn: str,
    run_id: int | None,
    level: str,
    logger: str,
    message: str,
    extra: dict[str, Any] | None = None,
    exception: str | None = None,
) -> None:
    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO etl_logs (run_id, level, logger, message, extra, exception)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        run_id,
                        level,
                        logger,
                        message,
                        Json(extra) if extra else None,
                        exception,
                    ),
                )
            conn.commit()
    except Exception:
        pass


def get_run_trends(dsn: str, hours: int = 24) -> list[dict[str, Any]]:
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    started_at,
                    duration_ms,
                    records_loaded,
                    CASE WHEN status = 'success' THEN 1 ELSE 0 END AS success
                FROM etl_runs
                WHERE started_at >= NOW() - (%s * INTERVAL '1 hour')
                ORDER BY started_at ASC
                """,
                (hours,),
            )
            rows = cur.fetchall()

    return list(rows)


def get_top_errors(dsn: str, limit: int = 10) -> list[dict[str, Any]]:
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    error_message,
                    COUNT(*) AS count,
                    MAX(started_at) AS last_occurred
                FROM etl_runs
                WHERE error_message IS NOT NULL
                GROUP BY error_message
                ORDER BY count DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()

    return list(rows)


def get_service_health(dsn: str) -> dict[str, Any]:
    from datetime import datetime as dt, timezone as tz

    db_healthy = True
    last_run_at = None
    last_success_at = None
    records_loaded_24h = 0
    data_freshness_hours = None

    try:
        with psycopg.connect(dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                db_healthy = bool(cur.fetchone())

                cur.execute(
                    """
                    SELECT MAX(started_at) as last_run, MAX(CASE WHEN status='success' THEN completed_at END) as last_success
                    FROM etl_runs
                    """
                )
                result = cur.fetchone() or {}
                last_run_at = result.get("last_run")
                last_success_at = result.get("last_success")

                cur.execute(
                    """
                    SELECT COALESCE(SUM(records_loaded), 0) as total
                    FROM etl_runs
                    WHERE started_at >= NOW() - INTERVAL '24 hours' AND status = 'success'
                    """
                )
                loaded = cur.fetchone() or {}
                records_loaded_24h = int(loaded.get("total") or 0)

                cur.execute("SELECT MAX(observed_at) as latest FROM weather_observations")
                latest = cur.fetchone() or {}
                if latest.get("latest"):
                    diff = dt.now(tz.utc) - latest["latest"]
                    data_freshness_hours = round(diff.total_seconds() / 3600, 1)
    except Exception:
        db_healthy = False

    return {
        "db_healthy": db_healthy,
        "last_run_at": last_run_at,
        "last_success_at": last_success_at,
        "records_loaded_24h": records_loaded_24h,
        "data_freshness_hours": data_freshness_hours,
    }


def get_logs(dsn: str, level: str | None = None, search: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            query = "SELECT id, run_id, level, logger, message, extra, exception, created_at FROM etl_logs WHERE 1=1"
            params: list[Any] = []

            if level:
                query += " AND level = %s"
                params.append(level)

            if search:
                query += " AND (message ILIKE %s OR CAST(extra AS TEXT) ILIKE %s)"
                search_term = f"%{search}%"
                params.extend([search_term, search_term])

            query += " ORDER BY created_at DESC LIMIT %s"
            params.append(limit)

            cur.execute(query, params)
            rows = cur.fetchall()

    return list(rows)


def cleanup_old_logs(dsn: str, days: int = 2) -> int:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM etl_logs
                WHERE created_at < NOW() - (%s * INTERVAL '1 day')
                """,
                (days,),
            )
            deleted = cur.rowcount
        conn.commit()

    return deleted
