"""Tests for :mod:`prefect_rj_iplanrio.logging`.

Logging configuration is process-wide state, so every scenario runs in a fresh
Python interpreter and inspects what reaches its standard streams — the same
streams the pipeline pod's log collector reads.
"""

import os
import subprocess
import sys
from collections.abc import Mapping

import pytest

MESSAGE = "dev758-mensagem-info"


def run_snippet(code: str, extra_env: Mapping[str, str] | None = None) -> str:
    """Run ``code`` in a new interpreter and return its combined stdout and stderr.

    :param code: Python source to execute.
    :param extra_env: Environment variables added to (or overriding) the current ones.
    :returns: Everything the process wrote to stdout and stderr.
    """
    env = {key: value for key, value in os.environ.items() if not key.startswith("PREFECT_LOGGING_")}
    env.update(extra_env or {})
    completed = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        check=True,
        env=env,
        text=True,
        timeout=120,
    )
    return completed.stdout + completed.stderr


def test_info_is_emitted_without_any_prior_configuration() -> None:
    output = run_snippet(
        "from prefect_rj_iplanrio.logging import get_logger\n"
        "logger = get_logger('pipelines.rj_demo__exemplo.tasks')\n"
        f"logger.info('{MESSAGE}')\n"
    )

    assert output.count(MESSAGE) == 1
    assert "INFO" in output
    assert "pipelines.rj_demo__exemplo.tasks" in output


def test_info_is_emitted_once_when_prefect_is_already_configured() -> None:
    output = run_snippet(
        "from prefect import flow  # noqa: F401 — configures Prefect logging, as every flow module does\n"
        "from prefect_rj_iplanrio.logging import get_logger\n"
        "logger = get_logger('pipelines.rj_demo__exemplo.tasks')\n"
        f"logger.info('{MESSAGE}')\n"
    )

    assert output.count(MESSAGE) == 1


def test_info_is_emitted_once_for_a_logger_listed_in_prefect_extra_loggers() -> None:
    output = run_snippet(
        "from prefect import flow  # noqa: F401\n"
        "from prefect_rj_iplanrio.logging import get_logger\n"
        "logger = get_logger('pipelines.rj_demo__exemplo.tasks')\n"
        f"logger.info('{MESSAGE}')\n",
        {"PREFECT_LOGGING_EXTRA_LOGGERS": "pipelines.rj_demo__exemplo", "PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW": "ignore"},
    )

    assert output.count(MESSAGE) == 1


def test_info_is_emitted_from_inside_a_flow_run() -> None:
    output = run_snippet(
        "from prefect import flow\n"
        "from prefect_rj_iplanrio.logging import get_logger\n"
        "logger = get_logger('pipelines.rj_demo__exemplo.flow')\n"
        "@flow\n"
        "def rj_demo__exemplo() -> None:\n"
        f"    logger.info('{MESSAGE}')\n"
        "rj_demo__exemplo()\n",
        {"PREFECT_SERVER_EPHEMERAL_STARTUP_TIMEOUT_SECONDS": "90"},
    )

    assert output.count(MESSAGE) == 1


def test_root_logger_level_set_by_a_dependency_does_not_hide_info() -> None:
    output = run_snippet(
        "import logging\n"
        "from prefect_rj_iplanrio.logging import get_logger\n"
        "logger = get_logger('pipelines.rj_demo__exemplo.tasks')\n"
        "logging.getLogger().setLevel(logging.ERROR)\n"
        f"logger.info('{MESSAGE}')\n"
    )

    assert output.count(MESSAGE) == 1


@pytest.mark.parametrize("level", ["WARNING", "ERROR"])
def test_prefect_logging_level_raises_the_threshold(level: str) -> None:
    output = run_snippet(
        "from prefect_rj_iplanrio.logging import get_logger\n"
        "logger = get_logger('pipelines.rj_demo__exemplo.tasks')\n"
        f"logger.info('{MESSAGE}')\n"
        "logger.critical('dev758-critico')\n",
        {"PREFECT_LOGGING_LEVEL": level},
    )

    assert MESSAGE not in output
    assert "dev758-critico" in output


def test_level_set_explicitly_by_the_pipeline_is_kept() -> None:
    output = run_snippet(
        "import logging\n"
        "from prefect_rj_iplanrio.logging import get_logger\n"
        "logging.getLogger('pipelines.rj_demo__exemplo.tasks').setLevel(logging.DEBUG)\n"
        "logger = get_logger('pipelines.rj_demo__exemplo.tasks')\n"
        "logger.debug('dev758-debug')\n"
    )

    assert output.count("dev758-debug") == 1
