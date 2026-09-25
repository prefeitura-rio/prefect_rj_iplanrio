"""Carrega o champion (versão do modelo em produção) do GCS — compartilhado por score
(exige um champion) e retreino (tolera não haver nenhum ainda, na 1ª execução)."""

from google.cloud import storage
from iplanrio.pipelines_utils.env import get_bd_credentials_from_env
from prefect import task
from prefect_rj_iplanrio.logging import get_logger

from pipelines.rj_crm__modelo_qualidade_telefone import constants
from pipelines.rj_crm__modelo_qualidade_telefone.utils import modelo_store

logger = get_logger(__name__)


def carrega_champion_opcional(
    environment: str, raiz: str = constants.RAIZ_MODELOS
) -> modelo_store.ModeloCarregado | None:
    """Autentica no GCS com o secret do work pool e carrega o champion, se existir.

    :param environment: ``"prod"`` ou ``"staging"`` — decide qual credencial usar
        (``get_bd_credentials_from_env``, escopo ``cloud-platform``).
    :param raiz: Raiz das versões do modelo no GCS (ver ``utils/modelo_store.py``).
    :returns: O champion, ou ``None`` se ainda não houver ``champion.json`` (1ª execução
        do retreino, antes de qualquer versão ter sido promovida).
    """
    credentials = get_bd_credentials_from_env(mode=environment)
    client = storage.Client(credentials=credentials, project=constants.PROJECT_ID)
    champion = modelo_store.carrega_champion(raiz, client=client)
    if champion:
        logger.info("Champion carregado: versão %s", champion.versao)
    else:
        logger.info("Nenhum champion publicado em %s ainda.", raiz)
    return champion


def carrega_champion(environment: str, raiz: str = constants.RAIZ_MODELOS) -> modelo_store.ModeloCarregado:
    """Como :func:`carrega_champion_opcional`, mas exige que exista — uso do scoring
    diário, que não tem como rodar sem nenhum modelo publicado.

    :raises RuntimeError: Se ainda não houver ``champion.json`` publicado em ``raiz``.
    """
    champion = carrega_champion_opcional(environment, raiz)
    if champion is None:
        raise RuntimeError(f"nenhum champion publicado em {raiz} — publicar um modelo antes do 1º scoring.")
    return champion


@task
def carrega_champion_task(environment: str, raiz: str = constants.RAIZ_MODELOS) -> modelo_store.ModeloCarregado:
    """Task-wrapper fina de :func:`carrega_champion`."""
    return carrega_champion(environment, raiz)


@task
def carrega_champion_opcional_task(
    environment: str, raiz: str = constants.RAIZ_MODELOS
) -> modelo_store.ModeloCarregado | None:
    """Task-wrapper fina de :func:`carrega_champion_opcional`."""
    return carrega_champion_opcional(environment, raiz)
