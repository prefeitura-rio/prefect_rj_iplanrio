"""Flow for rj_smfp__nota_carioca_adn_backfill.

Backfill do histórico do ADN a partir do XML que a app já gravou no Oracle (``NFSE_RPE.MV_DOCUMENTOSFISCAISNACIONAIS``
e a ``_QUARENTENA``), por faixa de NSU, e conferência byte a byte contra o ADN direto. Grava no mesmo formato do
extrator (``gs://rj-iplanrio-dia-nfse-nacional-raw/adn/{ambiente}/dt=.../lote_{ini}_{fim}.ndjson.gz``, com
``origem='oracle_backfill'`` e ``quarentena_oracle`` em cada linha) e indexa em ``raw_nfse_nacional.documentos``.

Cuidados com o Oracle da app: só o usuário somente leitura ``NC_LEITURA`` (``DB_ORACLE_NC_LEITURA__*``; qualquer usuário
com privilégio de escrita é recusado), uma sessão, uma transação ``SET TRANSACTION READ ONLY`` por leitura, no máximo
``linhas_por_minuto`` (20.000) e só dentro de ``janela_horaria`` (São Paulo, ``20:00-06:00``): fora dela o flow termina
sem ler e registra ``fora_da_janela``; se a janela fechar no meio, para entre duas faixas. É idempotente por NSU: faixas
já indexadas não são relidas, então o mesmo comando pode rodar todas as noites até cobrir ``nsu_inicio..nsu_fim``.
``modo=conferir`` compara o XML de origem ``adn`` do índice (sanitizado como a app o faz) com o do Oracle e devolve
``iguais``, ``diferentes``, ``ausentes_oracle`` e ``ausentes_nuvem``, também só na janela.

KILL SWITCH: pausar o deployment ``rj-smfp--nota-carioca-adn-backfill--prod`` e cancelar o run em andamento; a DBA pode
ainda bloquear o ``NC_LEITURA`` (``ALTER USER NC_LEITURA ACCOUNT LOCK``), e o run termina com o erro do Oracle, sem
insistir. A agenda nasce inativa até o gate 38 do plano.
"""

from typing import Literal

from prefect import flow

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from pipelines.rj_smfp__nota_carioca_adn.constants import BUCKET_RAW, DATASET_RAW, PROJETO_GCP
from pipelines.rj_smfp__nota_carioca_adn.tasks import backfill_task
from pipelines.rj_smfp__nota_carioca_adn.utils.backfill import ConfigBackfill
from pipelines.rj_smfp__nota_carioca_adn.utils.servicos import ParametrosBackfill


@flow(log_prints=True)
def rj_smfp__nota_carioca_adn_backfill(  # noqa: PLR0913, PLR0917
    nsu_inicio: int,
    nsu_fim: int,
    ambiente: Literal["staging", "prod"] = "prod",
    modo: Literal["backfill", "conferir"] = "backfill",
    linhas_por_minuto: int = 20_000,
    janela_horaria: str = "20:00-06:00",
    faixa_nsus: int = 1000,
    linhas_por_objeto: int = 1000,
    project: str = PROJETO_GCP,
    bucket: str = BUCKET_RAW,
    dataset_id: str = DATASET_RAW,
) -> None:
    rename_current_flow_run_task(new_name=f"adn-{modo}-{ambiente}-{nsu_inicio}-{nsu_fim}")
    inject_bd_credentials_task(environment="prod")
    resumo = backfill_task(
        ConfigBackfill(
            ambiente=ambiente,
            nsu_inicio=nsu_inicio,
            nsu_fim=nsu_fim,
            linhas_por_minuto=linhas_por_minuto,
            janela_horaria=janela_horaria,
            faixa_nsus=faixa_nsus,
            linhas_por_objeto=linhas_por_objeto,
        ),
        modo,
        ParametrosBackfill(projeto=project, bucket=bucket, dataset_id=dataset_id),
    )
    log(f"Resumo do {modo}: {resumo}")
