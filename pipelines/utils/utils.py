# -*- coding: utf-8 -*-
from os import getenv
import logging

from typing import Union, Any

from loguru import logger
import prefect


def getenv_or_action(
    key: str, default: str = None, action: str = "raise"
) -> Union[str, None]:
    """
    Gets an environment variable or executes an action.

    Args:
        key (str): The environment variable key.
        default (str, optional): The default value. Defaults to `None`.
        action (str, optional): The action to execute. Must be one of `'raise'`,
            `'warn'` or `'ignore'`. Defaults to `'raise'`.

    Returns:
        Union[str, None]: The environment variable value or the default value.
    """
    if action not in ("raise", "warn", "ignore"):
        raise ValueError(
            f"Invalid action '{action}'. Must be one of 'raise', 'warn' or 'ignore'."
        )
    value = getenv(key, default)
    if value is None:
        if action == "raise":
            raise ValueError(f"Environment variable '{key}' not found.")
        elif action == "warn":
            logger.warning(f"Environment variable '{key}' not found.")
    return value


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

    blank_spaces = 8 * " "
    msg = blank_spaces + "----\n" + str(msg)
    msg = "\n".join([blank_spaces + line for line in msg.split("\n")]) + "\n\n"

    if level not in levels:
        raise ValueError(f"Invalid log level: {level}")
    logger = prefect.get_run_logger()
    logger.log(level=levels[level], msg=msg)
