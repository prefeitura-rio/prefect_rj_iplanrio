"""Montagem das portas reais (ADN, GCS, BigQuery, Discord) de um run; é a única parte que fala com sistemas de fora."""

from dataclasses import dataclass
from urllib.parse import urlsplit

from google.cloud import bigquery, storage

from pipelines.rj_smfp__nota_carioca_adn.constants import AMBIENTES
from pipelines.rj_smfp__nota_carioca_adn.utils.adn_client import AdnClient, AdnConfig
from pipelines.rj_smfp__nota_carioca_adn.utils.adn_errors import ConfiguracaoAdnInvalida
from pipelines.rj_smfp__nota_carioca_adn.utils.armazenamento import GcsArmazenamento
from pipelines.rj_smfp__nota_carioca_adn.utils.backfill import ServicosBackfill
from pipelines.rj_smfp__nota_carioca_adn.utils.discord import Environment
from pipelines.rj_smfp__nota_carioca_adn.utils.extrator import Servicos
from pipelines.rj_smfp__nota_carioca_adn.utils.indice import BigQueryIndice
from pipelines.rj_smfp__nota_carioca_adn.utils.notify import ExtracaoNotificador
from pipelines.rj_smfp__nota_carioca_adn.utils.oracle import LeitorOracle, OracleConfig
from pipelines.rj_smfp__nota_carioca_adn.utils.relogio import RelogioReal

HOST_PRODUCAO = "adn.nfse.gov.br"
MARCA_PRODUCAO_RESTRITA = "producaorestrita"


@dataclass(frozen=True, slots=True)
class ParametrosServicos:
    """Parâmetros do flow que escolhem as portas reais.

    :param ambiente: ``staging`` (produção restrita do ADN) ou ``prod``.
    :param projeto: Projeto do GCP.
    :param bucket: Bucket do raw.
    :param dataset_id: Dataset do índice.
    :param cursor_inclusivo: Variante do cursor do ADN (padrão da app: exclusivo).
    :param tamanho_lote_esperado: Tamanho do lote do ADN (50 no manual, 100 no swagger).
    :param discord_notifications: ``False`` desliga o Discord.
    :param run_url: Link do flow run, para a mensagem do Discord.
    """

    ambiente: str
    projeto: str
    bucket: str
    dataset_id: str
    cursor_inclusivo: bool
    tamanho_lote_esperado: int
    discord_notifications: bool
    run_url: str | None


def validar_url_do_ambiente(ambiente: str, base_url: str) -> None:
    """Recusa o ambiente apontado para o ADN errado (staging na produção, ou prod na produção restrita).

    :raises ConfiguracaoAdnInvalida: Se o ambiente não existe ou a URL é a do outro ambiente.
    """
    if ambiente not in AMBIENTES:
        raise ConfiguracaoAdnInvalida(f"Ambiente {ambiente!r} inválido; use um de {AMBIENTES}")
    host = urlsplit(base_url).hostname or ""
    if ambiente == "staging" and host == HOST_PRODUCAO:
        raise ConfiguracaoAdnInvalida("O ambiente staging não pode usar o ADN de produção (ADN_BASE_URL)")
    if ambiente == "prod" and MARCA_PRODUCAO_RESTRITA in host:
        raise ConfiguracaoAdnInvalida("O ambiente prod não pode usar a produção restrita do ADN (ADN_BASE_URL)")


def construir_servicos(parametros: ParametrosServicos) -> Servicos:
    """Cria as portas reais do run: ADN por mTLS, GCS, BigQuery e Discord do ambiente.

    :raises ConfiguracaoAdnInvalida: Se faltar variável do ADN ou a URL for a do ambiente errado.
    """
    configuracao = AdnConfig.do_ambiente(
        cursor_inclusivo=parametros.cursor_inclusivo, tamanho_lote_esperado=parametros.tamanho_lote_esperado
    )
    validar_url_do_ambiente(parametros.ambiente, configuracao.base_url)
    return Servicos(
        cliente=AdnClient(configuracao),
        armazenamento=GcsArmazenamento(storage.Client(project=parametros.projeto), parametros.bucket),
        indice=BigQueryIndice(bigquery.Client(project=parametros.projeto), parametros.projeto, parametros.dataset_id),
        notificador=ExtracaoNotificador.criar(
            Environment(parametros.ambiente), parametros.discord_notifications, run_url=parametros.run_url
        ),
        relogio=RelogioReal(),
    )


@dataclass(frozen=True, slots=True)
class ParametrosBackfill:
    """Parâmetros do flow de backfill que escolhem as portas reais.

    :param projeto: Projeto do GCP.
    :param bucket: Bucket do raw.
    :param dataset_id: Dataset do índice.
    """

    projeto: str
    bucket: str
    dataset_id: str


def construir_servicos_backfill(parametros: ParametrosBackfill) -> ServicosBackfill:
    """Cria as portas reais do backfill: Oracle somente leitura (``DB_ORACLE_NC_LEITURA__*``), GCS e BigQuery.

    A sessão do Oracle só abre na primeira leitura, depois da checagem da janela horária.
    """
    return ServicosBackfill(
        fonte=LeitorOracle(OracleConfig.do_ambiente()),
        armazenamento=GcsArmazenamento(storage.Client(project=parametros.projeto), parametros.bucket),
        indice=BigQueryIndice(bigquery.Client(project=parametros.projeto), parametros.projeto, parametros.dataset_id),
        relogio=RelogioReal(),
    )
