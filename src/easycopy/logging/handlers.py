"""Log handler helpers for EasyCopy."""

import logging


def add_console_handler(logger: logging.Logger) -> None:
    """Attach a standard stream handler to the logger."""
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)
