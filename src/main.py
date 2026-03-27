from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from threading import Event

from dotenv import load_dotenv

from .api_client import OpenMeteoClient
from .config import load_settings
from .db import (
    bootstrap_schema,
    cleanup_old_logs,
    complete_etl_run,
    create_etl_run,
    healthcheck_db,
    insert_data_quality_checks,
    load_records,
)
from .logger import setup_logging
from .transform import evaluate_data_quality, transform_open_meteo

LOGGER = logging.getLogger(__name__)


class EtlService:
    def __init__(self) -> None:
        load_dotenv()
        self.settings = load_settings()
        setup_logging(self.settings.log_level, self.settings.dsn)
        self.stop_event = Event()
        self.last_cleanup = datetime.now(timezone.utc)

    def run_once(self, run_id_context: int | None = None) -> int:
        started = datetime.now(timezone.utc)
        client = OpenMeteoClient(self.settings)
        run_id: int | None = None

        try:
            bootstrap_schema(self.settings.dsn)
            run_id = create_etl_run(self.settings.dsn, started)
            
            # Update logger context dengan run_id
            root_logger = logging.getLogger()
            for handler in root_logger.handlers:
                if hasattr(handler, "run_id"):
                    handler.run_id = run_id
            
            payload = client.fetch_hourly_weather()
            transformed = transform_open_meteo(payload)
            checks = evaluate_data_quality(transformed)
            insert_data_quality_checks(self.settings.dsn, run_id, checks)
            loaded = load_records(self.settings.dsn, transformed)
            completed = datetime.now(timezone.utc)
            duration_ms = int((completed - started).total_seconds() * 1000)
            complete_etl_run(
                self.settings.dsn,
                run_id,
                status="success",
                completed_at=completed,
                duration_ms=duration_ms,
                records_transformed=len(transformed),
                records_loaded=loaded,
                error_message=None,
            )
            LOGGER.info(
                "etl_success",
                extra={
                    "run_id": run_id,
                    "records_transformed": len(transformed),
                    "records_loaded": loaded,
                    "checks_total": len(checks),
                    "duration_ms": duration_ms,
                },
            )
            return 0
        except Exception as exc:  # noqa: BLE001
            completed = datetime.now(timezone.utc)
            duration_ms = int((completed - started).total_seconds() * 1000)
            if run_id is not None:
                try:
                    complete_etl_run(
                        self.settings.dsn,
                        run_id,
                        status="failed",
                        completed_at=completed,
                        duration_ms=duration_ms,
                        records_transformed=None,
                        records_loaded=None,
                        error_message=str(exc),
                    )
                except Exception:  # noqa: BLE001
                    LOGGER.exception(
                        "etl_finalize_failed",
                        extra={"run_id": run_id, "duration_ms": duration_ms},
                    )
            LOGGER.exception(
                "etl_failed",
                extra={"run_id": run_id, "error": str(exc), "duration_ms": duration_ms},
            )
            return 1

    def run_scheduled(self) -> int:
        interval_seconds = max(self.settings.schedule_interval_hours, 1) * 3600
        LOGGER.info(
            "scheduler_started",
            extra={"interval_hours": self.settings.schedule_interval_hours},
        )

        self.run_once()
        while not self.stop_event.wait(interval_seconds):
            self.run_once()
            
            # Cleanup old logs setiap jam
            now = datetime.now(timezone.utc)
            if (now - self.last_cleanup).total_seconds() >= 3600:
                try:
                    deleted = cleanup_old_logs(self.settings.dsn, days=2)
                    if deleted > 0:
                        LOGGER.info(
                            "cleanup_logs_completed",
                            extra={"deleted_logs": deleted},
                        )
                    self.last_cleanup = now
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning(
                        "cleanup_logs_failed",
                        extra={"error": str(exc)},
                    )

        LOGGER.info("scheduler_stopped")
        return 0

    def set_signal_handlers(self) -> None:
        def _stop_handler(signum: int, _frame: object) -> None:
            LOGGER.info("shutdown_signal", extra={"signal": signum})
            self.stop_event.set()

        signal.signal(signal.SIGINT, _stop_handler)
        signal.signal(signal.SIGTERM, _stop_handler)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simple ETL Open-Meteo to PostgreSQL")
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Execute ETL exactly once and exit",
    )
    parser.add_argument(
        "--healthcheck",
        action="store_true",
        help="Check PostgreSQL connectivity and exit",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    service = EtlService()

    if args.healthcheck:
        ok = healthcheck_db(service.settings.dsn)
        if ok:
            LOGGER.info("healthcheck_ok")
            return 0
        LOGGER.error("healthcheck_failed")
        return 1

    if args.run_once:
        return service.run_once()

    service.set_signal_handlers()
    return service.run_scheduled()


if __name__ == "__main__":
    raise SystemExit(main())
