"""Logging unificado para todos os pipelines do workspace.

Todo módulo (flow, task ou utilitário) deve obter seu logger via :func:`get_logger`
em vez de chamar diretamente ``logging.getLogger`` ou ``prefect.get_run_logger``.

## Como os logs chegam à UI do Prefect

O Prefect captura logs de loggers externos via a variável de ambiente
``PREFECT_LOGGING_EXTRA_LOGGERS``. Quando esse valor está definido, o Prefect
adiciona seus handlers (que enviam logs para a API) em cada logger listado.
Todo logger filho de um logger registrado propaga automaticamente para cima.

Portanto, o mecanismo correto é:

1. Cada pipeline declara ``PREFECT_LOGGING_EXTRA_LOGGERS`` no ``prefect.yaml``
   apontando para o nome-raiz dos seus módulos (ex: ``flow,tasks,utils``).
2. ``get_logger(__name__)`` retorna um ``logging.Logger`` padrão com
   ``propagate=True`` (padrão do Python), permitindo que o handler do Prefect
   capture todos os logs via hierarquia.

Uso::

    from prefect_rj_iplanrio.log import get_logger

    logger = get_logger(__name__)

    logger.info("Processando %d registros", count)
    logger.warning("Tentando novamente após erro transitório")
    logger.error("Falha no upload: %s", error)
"""

import logging
from logging import Logger


def get_logger(name: str) -> Logger:
    """Retorna um logger pré-configurado para o módulo chamador.

    Retorna um :class:`logging.Logger` padrão do Python com nível ``DEBUG``
    e ``propagate=True``. Quando ``PREFECT_LOGGING_EXTRA_LOGGERS`` está
    configurado no deployment, o Prefect instala seus handlers no logger-raiz
    do módulo e todos os logs passam a aparecer na UI automaticamente.

    Fora de contexto Prefect (testes, scripts), adiciona um
    :class:`logging.StreamHandler` com formato legível apenas se não houver
    nenhum handler configurado na hierarquia — evitando duplicação.

    :param name: Nome do módulo — passe ``__name__`` a partir do módulo chamador.
    :returns: Uma instância de :class:`logging.Logger`.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Adiciona handler de fallback somente quando não há nenhum handler
    # configurado na hierarquia inteira (ex: fora de um deployment Prefect).
    root = logging.getLogger()
    if not root.handlers and not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s | %(levelname)-8s | %(name)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(handler)
        logger.propagate = False

    return logger
