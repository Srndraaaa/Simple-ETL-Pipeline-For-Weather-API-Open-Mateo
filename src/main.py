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
from .db import bootstrap_schema, healthcheck_db, load_records
from .logger import setup_logging
from .transform import transform_open_meteo

LOGGER = logging.getLogger(__name__)


class EtlService:
    def __init__(self) -> None:
        load_dotenv()
        self.settings = load_settings()
        setup_logging(self.settings.log_level)
        self.stop_event = Event()

    def run_once(self) -> int:
        started = datetime.now(timezone.utc)
        client = OpenMeteoClient(self.settings)

        try:
            bootstrap_schema(self.settings.dsn)
            payload = client.fetch_hourly_weather()
            transformed = transform_open_meteo(payload)
            loaded = load_records(self.settings.dsn, transformed)
            duration_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
            LOGGER.info(
                "etl_success",
                extra={
                    "records_transformed": len(transformed),
                    "records_loaded": loaded,
                    "duration_ms": duration_ms,
                },
            )
            return 0
        except Exception as exc:  # noqa: BLE001
            duration_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
            LOGGER.exception(
                "etl_failed",
                extra={"error": str(exc), "duration_ms": duration_ms},
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
