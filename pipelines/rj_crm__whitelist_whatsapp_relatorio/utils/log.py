"""Logger da pipeline, com nível ajustado.

O `get_logger` do workspace apenas devolve `logging.getLogger(nome)`, sem configurar
nível. E o Prefect, ao processar `PREFECT_LOGGING_EXTRA_LOGGERS`, copia só os *handlers*
do logger `prefect.extra` — nunca o nível. Sem ajuste explícito, o nível efetivo é o
padrão do Python (`WARNING`) e todo `logger.info` é descartado antes de chegar ao
handler.

Os dois mecanismos são complementares: a variável de ambiente entrega os handlers, esta
função entrega o nível.
"""

import logging
from logging import Logger

from prefect_rj_iplanrio.logging import get_logger

NIVEL = logging.INFO


def logger_da_pipeline(nome: str) -> Logger:
    """Devolve o logger do módulo já no nível de informação.

    :param nome: Nome do módulo — passe ``__name__``.
    :returns: Logger pronto para uso, com nível ``INFO``.
    """
    logger = get_logger(nome)
    logger.setLevel(NIVEL)
    return logger
