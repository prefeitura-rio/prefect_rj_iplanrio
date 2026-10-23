"""Flow for rj_smfp__nota_carioca_adn_extracao.

Extração própria do ADN (Ambiente de Dados Nacional da NFS-e) para a Nota Carioca: um consumidor sequencial do
``GET /municipios/DFe/{nsu}`` com o certificado e-CNPJ do município (mTLS), que grava cada lote como objeto imutável
no GCS (``gs://rj-iplanrio-dia-nfse-nacional-raw/adn/{ambiente}/dt={AAAA-MM-DD}/lote_{ini}_{fim}.ndjson.gz``, uma linha
por documento com o XML original), indexa os documentos em ``documentos`` do dataset do ambiente (load job) e só então
avança o cursor em ``adn_cursor``. Depois de esgotar o ADN ("sem documentos"), busca uma a uma as
lacunas de NSU do índice, até ``max_lacunas_por_run``, e encerra: a agenda do deployment roda de novo em 60 minutos,
que é o tempo mínimo que o manual pede depois de "nenhum documento localizado".

``ambiente=staging`` usa a produção restrita do ADN e ``prod`` a produção; a URL vem de ``ADN_BASE_URL`` e o flow
recusa o ambiente apontado para o ADN errado. O primeiro run de um ambiente exige ``nsu_inicial`` (o cursor fica gravado
e os runs seguintes o ignoram). A extração não pode prejudicar a da app: concorrência 1 (``concurrency_limit: 1`` no
deployment mais uma trava por objeto do GCS com geração), uma chamada por vez com ``intervalo_entre_chamadas_s``, retry
só em 429/503 e circuit breaker (3 respostas 429/503 seguidas pausam 15 minutos e falham o run).

KILL SWITCH: pausar o deployment ``rj-smfp--nota-carioca-adn-extracao--<ambiente>`` no Prefect (ou desativar a agenda)
impede novos runs; um run em andamento precisa ser cancelado no Prefect, o que solta a trava no ``finally`` (se o
processo morrer, a trava vence sozinha em 30 minutos). O cursor só avança depois de objeto e índice gravados, então
retomar é reativar a agenda: o run seguinte continua do último lote completo. As agendas nascem inativas até o gate 38
do plano.
"""

from typing import Literal

from prefect import flow
from prefect.runtime import flow_run
from prefect.settings import PREFECT_UI_URL

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from pipelines.rj_smfp__nota_carioca_adn.constants import (
    BUCKET_RAW,
    DATASET_RAW,
    INTERVALO_ENTRE_CHAMADAS_S,
    LACUNAS_POR_OBJETO,
    MAX_LACUNAS_POR_RUN,
    PROJETO_GCP,
)
from pipelines.rj_smfp__nota_carioca_adn.tasks import extrair_task
from pipelines.rj_smfp__nota_carioca_adn.utils.modelos import ConfigExtracao
from pipelines.rj_smfp__nota_carioca_adn.utils.servicos import ParametrosServicos


def _link_do_run() -> str | None:
    base = PREFECT_UI_URL.value()
    return f"{base.rstrip('/')}/runs/flow-run/{flow_run.id}" if base and flow_run.id else None


@flow(log_prints=True)
def rj_smfp__nota_carioca_adn_extracao(  # noqa: PLR0913, PLR0917
    ambiente: Literal["staging", "prod"] = "staging",
    intervalo_entre_chamadas_s: float = INTERVALO_ENTRE_CHAMADAS_S,
    max_lacunas_por_run: int = MAX_LACUNAS_POR_RUN,
    nsu_inicial: int | None = None,
    max_lotes_por_run: int | None = None,
    cursor_inclusivo: bool = False,
    tamanho_lote_esperado: int = 50,
    project: str = PROJETO_GCP,
    bucket: str = BUCKET_RAW,
    dataset_id: str = DATASET_RAW,
    discord_notifications: bool = True,
) -> None:
    rename_current_flow_run_task(new_name=f"adn-{ambiente}")
    inject_bd_credentials_task(environment="prod")
    extrair_task(
        ConfigExtracao(
            ambiente=ambiente,
            intervalo_entre_chamadas_s=intervalo_entre_chamadas_s,
            max_lacunas_por_run=max_lacunas_por_run,
            nsu_inicial=nsu_inicial,
            max_lotes_por_run=max_lotes_por_run,
            run_id=str(flow_run.id),
            lacunas_por_objeto=LACUNAS_POR_OBJETO,
        ),
        ParametrosServicos(
            ambiente=ambiente,
            projeto=project,
            bucket=bucket,
            dataset_id=dataset_id,
            cursor_inclusivo=cursor_inclusivo,
            tamanho_lote_esperado=tamanho_lote_esperado,
            discord_notifications=discord_notifications,
            run_url=_link_do_run(),
        ),
    )
