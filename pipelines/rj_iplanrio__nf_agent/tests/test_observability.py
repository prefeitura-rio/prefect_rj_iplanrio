"""Tests for the run-aware logger wrapper."""

from unittest.mock import MagicMock, patch

from pipelines.rj_iplanrio__nf_agent.utils import observability


def test_get_logger_falls_back_outside_run_context():
    logger = observability.get_logger("some.module")
    with patch.object(observability, "get_run_logger", side_effect=observability.MissingContextError()):
        with patch.object(logger, "_fallback") as fallback:
            logger.warning("oi %s", "mundo")
    fallback.warning.assert_called_once_with("oi %s", "mundo")


def test_get_logger_routes_to_run_logger_inside_a_run():
    run_logger = MagicMock()
    logger = observability.get_logger("some.module")
    with patch.object(observability, "get_run_logger", return_value=run_logger):
        logger.info("processando %d", 3)
    run_logger.info.assert_called_once_with("processando %d", 3)


def test_get_logger_reresolves_on_every_call():
    logger = observability.get_logger("some.module")
    first_run_logger = MagicMock()
    with patch.object(observability, "get_run_logger", return_value=first_run_logger):
        logger.info("dentro de um run")
    with patch.object(observability, "get_run_logger", side_effect=observability.MissingContextError()):
        with patch.object(logger, "_fallback") as fallback:
            logger.info("fora de um run")
    first_run_logger.info.assert_called_once_with("dentro de um run")
    fallback.info.assert_called_once_with("fora de um run")
