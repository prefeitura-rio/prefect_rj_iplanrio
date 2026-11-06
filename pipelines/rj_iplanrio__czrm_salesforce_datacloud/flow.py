"""Flow CZRM (Salesforce) Data Cloud -> BigQuery.

Dois deployments, mesmo código:

- ``--staging``: sem schedule e com ``dry_run=True`` por padrão. Serve para
  validar extração/transformação contra a org escolhida sem escrever nada.
- ``--prod``: schedules de 15min (janela de 1h), diário (dia anterior) e
  semanal (7 dias). Escreve em ``brutos_czrm_salesforce_staging``.

A primeira carga é manual (``modo=carga_inicial``): lê a tabela inteira sem
filtro de data. Não é schedule de propósito — é um botão que se aperta uma vez.

O ``origem`` escolhe o jogo de credenciais (``SF_DC_<ORIGEM>_*``) **e** a DLO
de cada tabela (``tabelas.yaml``). O destino é sempre o mesmo dataset; um
guard-rail impede que origem não-prod grave na zona de pouso de produção.
"""

from __future__ import annotations

import time
from datetime import date

from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_iplanrio__czrm_salesforce_datacloud.constants import DATASET_ID, PROJECT_ID
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.tasks import (
    Destino,
    Modo,
    descobrir_schemas,
    montar_janelas,
    montar_rotulo,
    processar_janelas,
    selecionar,
    validar_destino,
)
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.auth import get_session
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.bigquery import garantir_tabelas
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.notify import notify_falha_flow
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.log import logger_da_pipeline
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.tabelas import NomeTabela, Origem

logger = logger_da_pipeline(__name__)


@flow(name="czrm-salesforce-datacloud", log_prints=True,)
def rj_iplanrio__czrm_salesforce_datacloud(  # noqa: PLR0913
    origem: Origem = "prod",
    project_id: str = PROJECT_ID,
    dataset_id: str | None = None,
    tabelas: list[NomeTabela] | None = None,
    modo: Modo = "hora",
    dia: date | None = None,
    reprocessar: bool = False,
    reprocessar_de: date | None = None,
    reprocessar_ate: date | None = None,
    dry_run: bool = False,
) -> dict[str, dict[str, int]]:
    """Sincroniza as tabelas de ``tabelas.yaml`` do Data Cloud para o BigQuery.

    :param origem: org do Salesforce de onde ler (``qa``, ``staging`` ou ``prod``).
    :param project_id: projeto GCP de destino.
    :param dataset_id: dataset de destino. Padrão: ``brutos_czrm_salesforce_staging``.
    :param tabelas: subconjunto de tabelas a processar. Vazio/None = todas.
    :param modo: ``hora`` (janela de 1h), ``dia`` (dia inteiro),
        ``reconciliacao`` (últimos 7 dias) ou ``carga_inicial`` (tabela inteira).
    :param dia: dia a processar no modo ``dia``. Padrão: ontem.
    :param reprocessar: reprocessa dia a dia de ``reprocessar_de`` a ``reprocessar_ate``.
    :param reprocessar_de: primeiro dia do reprocessamento (inclusivo).
    :param reprocessar_ate: último dia (inclusivo). Padrão: igual a ``de``.
    :param dry_run: extrai e transforma sem escrever no BigQuery.
    :returns: dict por grupo com ``{tabela: linhas inseridas + atualizadas}``.
    :raises ValueError: Se a origem tentar gravar na zona de pouso de produção
        ou se a seleção de tabelas for inválida.
    :raises RuntimeError: Se todas as tabelas falharem.
    """
    dataset_id = dataset_id or DATASET_ID
    validar_destino(origem, dataset_id, dry_run)

    selecionadas = selecionar(tabelas)
    janelas = montar_janelas(modo, dia, reprocessar, reprocessar_de, reprocessar_ate)
    rotulo = montar_rotulo(modo, reprocessar, reprocessar_de, reprocessar_ate)

    rename_current_flow_run_task(new_name=f"czrm-salesforce-{origem}-{modo}" + ("-dry-run" if dry_run else ""))

    session = get_session(origem)

    logger.info(
        "[FLOW] %s | origem=%s | dataset=%s.%s | tabelas=%s%s",
        rotulo,
        origem,
        project_id,
        dataset_id,
        [t.nome for t in selecionadas],
        " | DRY-RUN" if dry_run else "",
    )

    schemas = descobrir_schemas(session, selecionadas, origem)

    destino = Destino(project_id=project_id, dataset_id=dataset_id, dry_run=dry_run)
    processar_janelas(janelas, selecionadas, schemas, session, origem, destino)


if __name__ == "__main__":
    rj_iplanrio__czrm_salesforce_datacloud(
        modo="carga_inicial",
        origem="prod",
        dataset_id="brutos_czrm_salesforce",
        tabelas=["work_order_relationship"],
        dry_run=False,
    )
