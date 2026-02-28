"""Structured logger configuration (simple logger for stubbing).

Provides a lightweight logger used during orchestration.
"""

import logging
from pathlib import Path
from typing import Optional


def configure_structured_logger(logs_dir: Optional[Path] = None) -> logging.Logger:
    logger = logging.getLogger("easycopy")
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    # Optionally add a file handler if logs_dir is provided
    try:
        if logs_dir:
            Path(logs_dir).mkdir(parents=True, exist_ok=True)
            fh = logging.FileHandler(Path(logs_dir) / "easycopy.log")
            fh.setFormatter(fmt)
            logger.addHandler(fh)
    except Exception:
        # never fail logger creation
        pass

    return logger
