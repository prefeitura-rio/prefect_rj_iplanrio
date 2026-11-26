"""Inicialização dos helpers da pipeline SMTR e nível padrão de logging."""

from logging import INFO

from prefect_rj_iplanrio.logging import get_logger

PACKAGE_LOGGER_NAME = "pipelines.rj_smtr__triagem_recursos"


def garantir_nivel_logs_pipeline(nivel: int = INFO) -> None:
    """Garante que o logger raiz da pipeline emita INFO+ mesmo com PREFECT_LOGGING_EXTRA_LOGGERS.

    O Prefect anexa `APILogHandler` ao logger listado em `PREFECT_LOGGING_EXTRA_LOGGERS`,
    mas mantém o nível em `NOTSET` (herdando `WARNING` do root). Ajustar o logger do
    pacote para `INFO` habilita todos os submódulos na UI sem violar o STYLEGUIDE.
    """
    pkg_logger = get_logger(PACKAGE_LOGGER_NAME)
    if pkg_logger.level == 0 or pkg_logger.level > nivel:
        pkg_logger.setLevel(nivel)


garantir_nivel_logs_pipeline()
