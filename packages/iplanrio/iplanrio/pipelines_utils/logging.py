# -*- coding: utf-8 -*-
import logging
from typing import Any

import prefect


def log(msg: Any, level: str = "info") -> None:
    """
    Logs a message to prefect's logger.
    """
    levels = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }

    blank_spaces = 4 * " "
    msg = blank_spaces + "----\n" + str(msg)
    msg = "\n".join([blank_spaces + line for line in msg.split("\n")]) + "\n\n"

    if level not in levels:
        raise ValueError(f"Invalid log level: {level}")
    logger = prefect.get_run_logger()
    logger.log(level=levels[level], msg=msg)


def log_mod(msg: Any, level: str = "info", index: int = 0, mod: int = 1):
    """
    Only logs a message if the index is a multiple of mod.
    """
    if index % mod == 0 or index == 0:
        log(msg=f"iteration {index}:\n {msg}", level=level)
