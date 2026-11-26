"""Structured logging utilities with secret redaction for DataPilot AI."""

from __future__ import annotations

import logging
import re
from pathlib import Path

_SECRET_PATTERNS = [
    re.compile(r"(AKIA[0-9A-Z]{16})"),
    re.compile(r"(ASIA[0-9A-Z]{16})"),
    re.compile(r"(AIza[0-9A-Za-z\-_]{20,})"),
    re.compile(r"(aws_secret_access_key\s*[:=]\s*)([^\s,;]+)", re.IGNORECASE),
    re.compile(r"(AWS_SECRET_ACCESS_KEY\s*[:=]\s*)([^\s,;]+)", re.IGNORECASE),
    re.compile(r"(AWS_SESSION_TOKEN\s*[:=]\s*)([^\s,;]+)", re.IGNORECASE),
    re.compile(r"(GEMINI_API_KEY\s*[:=]\s*)([^\s,;]+)", re.IGNORECASE),
    re.compile(r"(DATAPILOT_API_KEY\s*[:=]\s*)([^\s,;]+)", re.IGNORECASE),
    re.compile(r"((?:X-API-Key|Authorization)\s*[:=]\s*(?:Bearer\s+)?)([^\s,;]+)", re.IGNORECASE),
    re.compile(r"(postgresql(?:\+psycopg2)?://[^:]+:)([^@]+)(@)", re.IGNORECASE),
]


class _RedactingFormatter(logging.Formatter):
    """Formatter that scrubs secrets from messages and formatted exception tracebacks."""

    def format(self, record: logging.LogRecord) -> str:
        return redact_secrets(super().format(record))


class SecretRedactionFilter(logging.Filter):
    """Mask accidental credential patterns before emitting log lines."""

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        redacted = redact_secrets(msg)
        if redacted != msg:
            record.msg = redacted
            record.args = ()
        if record.exc_text:
            record.exc_text = redact_secrets(record.exc_text)
        return True


def redact_secrets(text: str) -> str:
    """Redact AWS keys, session tokens, API keys, and DB passwords from log text."""
    result = text
    result = _SECRET_PATTERNS[0].sub("[REDACTED_AWS_KEY]", result)
    result = _SECRET_PATTERNS[1].sub("[REDACTED_AWS_KEY]", result)
    result = _SECRET_PATTERNS[2].sub("[REDACTED_GEMINI_KEY]", result)
    result = _SECRET_PATTERNS[3].sub(r"\1[REDACTED]", result)
    result = _SECRET_PATTERNS[4].sub(r"\1[REDACTED]", result)
    result = _SECRET_PATTERNS[5].sub(r"\1[REDACTED_SESSION_TOKEN]", result)
    result = _SECRET_PATTERNS[6].sub(r"\1[REDACTED_GEMINI_KEY]", result)
    result = _SECRET_PATTERNS[7].sub(r"\1[REDACTED_API_KEY]", result)
    result = _SECRET_PATTERNS[8].sub(r"\1[REDACTED]", result)
    result = _SECRET_PATTERNS[9].sub(r"\1[REDACTED]\3", result)
    return result


def get_logger(
    name: str,
    level: str = "INFO",
    log_file: Path | None = None,
) -> logging.Logger:
    """Create or retrieve a configured logger with secret redaction."""
    logger = logging.getLogger(name)
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(numeric_level)

    formatter = _RedactingFormatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    if not any(isinstance(f, SecretRedactionFilter) for f in logger.filters):
        logger.addFilter(SecretRedactionFilter())

    if not logger.handlers:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(numeric_level)
        stream_handler.setFormatter(formatter)
        stream_handler.addFilter(SecretRedactionFilter())
        logger.addHandler(stream_handler)

    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        resolved = str(log_file.resolve())
        existing_paths = {
            str(Path(h.baseFilename).resolve())
            for h in logger.handlers
            if isinstance(h, logging.FileHandler)
        }
        if resolved not in existing_paths:
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setLevel(numeric_level)
            file_handler.setFormatter(formatter)
            file_handler.addFilter(SecretRedactionFilter())
            logger.addHandler(file_handler)

    logger.propagate = False
    return logger
