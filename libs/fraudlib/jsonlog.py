"""Structured (JSON-lines) logging to stdout, one object per line."""
from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from decimal import Decimal

_STD = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime"}


def to_jsonable(o):
    if isinstance(o, datetime):
        return o.isoformat()
    if isinstance(o, Decimal):
        return float(o)
    if hasattr(o, "value"):
        return o.value
    return str(o)


def dumps(obj) -> str:
    return json.dumps(obj, default=to_jsonable, separators=(",", ":"))


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out = {"ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
               "level": record.levelname, "logger": record.name, "msg": record.getMessage()}
        out.update({k: v for k, v in record.__dict__.items() if k not in _STD})
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return dumps(out)


def setup(service: str, level: int = logging.INFO) -> logging.Logger:
    handler = logging.StreamHandler(sys.stdout)  # stdout, so shells don't paint normal logs as errors
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    return logging.getLogger(service)
