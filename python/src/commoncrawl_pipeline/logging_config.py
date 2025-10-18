"""Logging configuration with custom formatter for structured logging."""

import logging


class ContextFormatter(logging.Formatter):
    """Custom formatter that includes extra fields in log output.

    Any extra fields passed via logger.info("msg", extra={...}) will be
    automatically appended to the log message in key=value format.
    """

    def format(self, record):
        """Format log record, including extra fields if present."""
        # Add extra fields to the message if they exist
        extra_fields = {
            k: v
            for k, v in record.__dict__.items()
            if k
            not in [
                "name",
                "msg",
                "args",
                "created",
                "filename",
                "funcName",
                "levelname",
                "levelno",
                "lineno",
                "module",
                "msecs",
                "message",
                "pathname",
                "process",
                "processName",
                "relativeCreated",
                "thread",
                "threadName",
                "exc_info",
                "exc_text",
                "stack_info",
                "asctime",
                "taskName",
            ]
        }
        if extra_fields:
            record.msg = f"{record.msg} | {' '.join(f'{k}={v}' for k, v in extra_fields.items())}"
        return super().format(record)


def setup_logging(level: int = logging.INFO) -> None:
    """Configure logging with custom formatter.

    Args:
        level: Logging level (default: logging.INFO)
    """
    handler = logging.StreamHandler()
    handler.setFormatter(
        ContextFormatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    logging.root.addHandler(handler)
    logging.root.setLevel(level)
