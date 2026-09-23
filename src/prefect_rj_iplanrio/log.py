"""Logging unificado para todos os pipelines do workspace.

Todo módulo (flow, task ou utilitário) deve obter seu logger via :func:`get_logger`
em vez de chamar diretamente ``logging.getLogger`` ou ``prefect.get_run_logger``.

Quando executado dentro de um flow ou task do Prefect, o logger retornado integra-se
automaticamente com o sistema de logging do Prefect (exibe na UI, propaga para o
``PREFECT_LOGGING_EXTRA_LOGGERS``, etc.). Quando chamado fora de qualquer contexto
de execução (ex: testes unitários, scripts avulsos), cai de volta para o logger
padrão do Python sem lançar exceções.

Uso::

    from prefect_rj_iplanrio.log import get_logger

    logger = get_logger(__name__)

    logger.info("Processando %d registros", count)
    logger.warning("Tentando novamente após erro transitório")
    logger.error("Falha no upload: %s", error)
"""

import logging
from logging import Logger

from prefect.context import FlowRunContext, TaskRunContext


def get_logger(name: str) -> Logger:
    """Retorna um logger pré-configurado para o módulo chamador.

    Dentro de um flow ou task do Prefect, delega para ``prefect.get_run_logger``
    a fim de integrar com a UI e o sistema de observabilidade do Prefect.
    Fora de qualquer contexto de execução (testes, scripts, imports em tempo de
    definição), retorna um ``logging.Logger`` padrão do Python para evitar
    ``MissingContextError``.

    :param name: Nome do módulo — passe ``__name__`` a partir do módulo chamador.
    :returns: Uma instância de :class:`logging.Logger` com configuração
        aplicada ao workspace.
    """
    in_prefect_context = (
        FlowRunContext.get() is not None or TaskRunContext.get() is not None
    )

    if in_prefect_context:
        from prefect import get_run_logger  # importação lazy para evitar overhead

        return get_run_logger()

    logger = logging.getLogger(name)
    if not logger.handlers and not logging.root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s | %(levelname)-8s | %(name)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(handler)
        logger.propagate = False

    logger.setLevel(logging.DEBUG)
    return logger
