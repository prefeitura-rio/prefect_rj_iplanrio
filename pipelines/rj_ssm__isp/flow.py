"""Flow para extrair ocorrências criminais do ISP-GEO (ISP/RJ) e enviar para BigQuery."""

from typing import Literal

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from constants import MUNICIPIO_RIO_DE_JANEIRO, MAX_CONCURRENT_REQUESTS
from task import fetch_ocorrencias_task, upload_ocorrencias_task, resolve_dates


@flow(log_prints=True)
def rj_ssm__isp(
    dataset_id: str = "brutos_ispgeo",
    table_id: str = "ocorrencias",
    dump_mode: str = "append",
    fase: Literal["consolidados", "errata", "parcial"] = "parcial",
    data_inicio: str | None = None,
    data_fim: str | None = None,
    todos: bool = True,
    municipio: int = MUNICIPIO_RIO_DE_JANEIRO,
    max_concurrent_requests: int = MAX_CONCURRENT_REQUESTS,
) -> None:
    """Extrai ocorrências da camada de microdados do ISP-GEO e carrega no BigQuery.

    :param dataset_id: Dataset do BigQuery.
    :param table_id: Tabela do BigQuery.
    :param dump_mode: ``"append"`` para acumular dados, ``"overwrite"`` para substituir.
    :param fase: Fase de disponibilidade dos dados.
        - ``"parcial"`` (Fase 1): dados do dia anterior; janela padrão de 1 dia (D-1 a D-1).
        - ``"consolidados"`` (Fase 2): dados do mês anterior; janela padrão de 30 dias (D-30 a D-1).
        - ``"errata"`` (Fase 3): dados definitivos; janela padrão do último trimestre completo.
    :param data_inicio: Data de início (``YYYY-MM-DD``). Sobrescreve o default da fase.
    :param data_fim: Data de fim (``YYYY-MM-DD``). Sobrescreve o default da fase.
    :param todos: Se ``True``, ignora o filtro de tipos de delito e traz tudo.
    :param municipio: Código IBGE do município do fato.
    :param max_concurrent_requests: Número máximo de requisições assíncronas simultâneas
        ao buscar páginas. Default: ``MAX_CONCURRENT_REQUESTS``.
    """
    data_inicio, data_fim = resolve_dates(fase, data_inicio, data_fim)

    rename_current_flow_run_task(new_name=f"{dataset_id}.{table_id} [{fase}]")
    inject_bd_credentials_task(environment="prod")
    dataframe = fetch_ocorrencias_task(
        data_inicio=data_inicio,
        data_fim=data_fim,
        fase=fase,
        todos=todos,
        municipio=municipio,
        max_concurrent_requests=max_concurrent_requests,
    )

    upload_ocorrencias_task(
        dataframe=dataframe,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
    )