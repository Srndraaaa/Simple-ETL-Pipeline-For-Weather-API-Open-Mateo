from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key.startswith("_"):
                continue
            if key in {
                "name",
                "msg",
                "args",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "exc_info",
                "exc_text",
                "stack_info",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
            }:
                continue
            payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str)


class DbLogHandler(logging.Handler):
    """Handler untuk persist logs ke database"""
    def __init__(self, dsn: str, run_id: int | None = None):
        super().__init__()
        self.dsn = dsn
        self.run_id = run_id

    def emit(self, record: logging.LogRecord) -> None:
        try:
            from .db import insert_log

            extra: dict[str, Any] = {}
            for key, value in record.__dict__.items():
                if key.startswith("_") or key in {
                    "name", "msg", "args", "levelname", "levelno", "pathname",
                    "filename", "module", "exc_info", "exc_text", "stack_info",
                    "lineno", "funcName", "created", "msecs", "relativeCreated",
                    "thread", "threadName", "processName", "process", "getMessage",
                }:
                    continue
                extra[key] = value

            exception_str = None
            if record.exc_info:
                exception_str = self.format(record)

            insert_log(
                self.dsn,
                self.run_id,
                record.levelname,
                record.name,
                record.getMessage(),
                extra=extra if extra else None,
                exception=exception_str,
            )
        except Exception:
            pass


def setup_logging(level: str, dsn: str | None = None, run_id: int | None = None) -> None:
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)

    if dsn:
        db_handler = DbLogHandler(dsn, run_id)
        db_handler.setFormatter(JsonFormatter())
        root.addHandler(db_handler)
