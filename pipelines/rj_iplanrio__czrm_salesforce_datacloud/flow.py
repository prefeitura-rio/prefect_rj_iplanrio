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
from dataclasses import dataclass
from datetime import date
from typing import Literal

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_iplanrio__czrm_salesforce_datacloud.constants import DATASET_ID, PROJECT_ID
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.tasks.auth import get_session
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.tasks.bigquery import (
    carregar_staging,
    garantir_tabelas,
    merge_staging,
)
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.tasks.extract import (
    descobrir_colunas,
    extrair,
)
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.tasks.notify import (
    notify_falha_flow,
    notify_falha_tabela,
    notify_resumo,
)
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.janela import (
    filtro_sql,
    fmt_janela,
    janelas_do_run,
)
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.log import logger_da_pipeline
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.schema import SchemaTabela, montar
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.tabelas import (
    TABELAS,
    NomeTabela,
    Origem,
    Tabela,
)
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.transform import transformar

logger = logger_da_pipeline(__name__)

Modo = Literal["hora", "dia", "reconciliacao", "carga_inicial"]


@dataclass(frozen=True)
class Destino:
    """Para onde e como o run escreve (agrupado para não inflar assinaturas)."""

    project_id: str
    dataset_id: str
    dry_run: bool


def processar_tabela(  # noqa: PLR0913 - parametros sao o contexto do run
    tabela: Tabela,
    schema: SchemaTabela,
    session: dict,
    origem: str,
    destino: Destino,
    filtro: str,
) -> dict[str, int]:
    """Extrai, transforma e grava UMA tabela em UMA janela.

    :param tabela: configuração da tabela.
    :param schema: schema descoberto da DLO.
    :param session: sessão autenticada no Salesforce.
    :param origem: org de origem (só para log).
    :param destino: projeto, dataset e se é dry-run.
    :param filtro: cláusula WHERE da janela (vazia na carga inicial).
    :returns: ``{"extraidas", "inseridas", "atualizadas"}``.
    """
    dlo = tabela.dlo_de(origem)
    df = extrair(session=session, dlo=dlo, filtro=filtro, order_by=tabela.order_by, tabela=tabela.nome)
    if df.empty:
        return {"extraidas": 0, "inseridas": 0, "atualizadas": 0}

    extraidas = len(df)
    df = transformar(df, tabela=tabela, schema=schema)
    if destino.dry_run:
        return {"extraidas": extraidas, "inseridas": 0, "atualizadas": 0}

    carregar_staging(
        df=df,
        project_id=destino.project_id,
        dataset_id=destino.dataset_id,
        staging_id=tabela.staging,
        campos=list(schema.campos),
    )
    resultado = merge_staging(
        project_id=destino.project_id,
        dataset_id=destino.dataset_id,
        staging_id=tabela.staging,
        target_id=tabela.nome,
        primary_key=tabela.primary_key,
    )
    return {"extraidas": extraidas, **resultado}


def _validar_destino(origem: str, dataset_id: str, dry_run: bool) -> None:
    """Impede que origem não-prod grave na zona de pouso de produção.

    :param origem: org de origem.
    :param dataset_id: dataset de destino do run.
    :param dry_run: se o run não escreve.
    :raises ValueError: Se a combinação for insegura.
    """
    if origem != "prod" and dataset_id == DATASET_ID and not dry_run:
        raise ValueError(
            f"origem '{origem}' não pode gravar em '{DATASET_ID}' (zona de pouso de produção). "
            "Use dry_run=True ou um dataset_id explícito de validação."
        )


def _selecionar(tabelas: list[str] | None) -> list[Tabela]:
    """Valida a seleção e devolve as tabelas na ordem do YAML.

    :param tabelas: nomes escolhidos no run. Vazio/None = todas.
    :returns: tabelas a processar.
    :raises ValueError: Se algum nome não existir em ``tabelas.yaml``.
    """
    desconhecidas = set(tabelas or []) - {t.nome for t in TABELAS}
    if desconhecidas:
        raise ValueError(f"Tabelas desconhecidas: {sorted(desconhecidas)}. Ver tabelas.yaml.")
    return [t for t in TABELAS if not tabelas or t.nome in tabelas]


def _janelas(
    modo: str,
    dia: date | None,
    reprocessar: bool,
    reprocessar_de: date | None,
    reprocessar_ate: date | None,
) -> list[tuple[str | None, str | None]]:
    """Janelas do run. ``carga_inicial`` é uma janela aberta (sem filtro)."""
    if modo == "carga_inicial":
        return [(None, None)]
    return list(janelas_do_run(modo, dia, reprocessar, reprocessar_de, reprocessar_ate))


def _rotulo(
    modo: str,
    reprocessar: bool,
    reprocessar_de: date | None,
    reprocessar_ate: date | None,
) -> str:
    """Rótulo do run para log e notificação."""
    if reprocessar:
        return f"reprocesso {reprocessar_de} a {reprocessar_ate or reprocessar_de}"
    if modo == "carga_inicial":
        return "carga inicial"
    return f"modo={modo}"


def _descobrir_schemas(session: dict, selecionadas: list[Tabela], origem: str) -> dict[str, SchemaTabela]:
    """Descobre o schema de cada tabela selecionada a partir da DLO."""
    schemas: dict[str, SchemaTabela] = {}
    for tabela in selecionadas:
        colunas = descobrir_colunas(session=session, dlo=tabela.dlo_de(origem), tabela=tabela.nome)
        schemas[tabela.nome] = montar(colunas, tabela.renomear, tabela.booleanas)
    return schemas


def _processar_janelas(  # noqa: PLR0913 - parametros sao o contexto do run
    janelas: list[tuple[str | None, str | None]],
    selecionadas: list[Tabela],
    schemas: dict[str, SchemaTabela],
    session: dict,
    origem: str,
    destino: Destino,
) -> tuple[dict[str, dict[str, int]], list[str]]:
    """Roda todas as janelas x tabelas, isolando falha por tabela."""
    resultados: dict[str, dict[str, int]] = {}
    falhas: list[str] = []

    for data_inicio, data_fim in janelas:
        if data_inicio and data_fim:
            logger.info("[FLOW] janela %s", fmt_janela(data_inicio, data_fim))

        for tabela in selecionadas:
            filtro = filtro_sql(tabela.coluna_modificacao, data_inicio or "", data_fim or "")
            try:
                resultado = processar_tabela(
                    tabela=tabela,
                    schema=schemas[tabela.nome],
                    session=session,
                    origem=origem,
                    destino=destino,
                    filtro=filtro,
                )
                logger.info(
                    "[FLOW] %s: %d extraídas, %d inseridas, %d atualizadas",
                    tabela.nome,
                    resultado["extraidas"],
                    resultado["inseridas"],
                    resultado["atualizadas"],
                )
                linhas = (
                    resultado["inseridas"] + resultado["atualizadas"] if not destino.dry_run else resultado["extraidas"]
                )
            except Exception as exc:
                logger.warning("[FLOW] '%s' falhou: %s", tabela.nome, exc)
                notify_falha_tabela(
                    tabela=tabela.nome,
                    grupo=tabela.grupo,
                    janela=fmt_janela(data_inicio, data_fim) if data_inicio and data_fim else "carga inicial",
                    erro=str(exc),
                )
                if tabela.nome not in falhas:
                    falhas.append(tabela.nome)
                linhas = 0

            grupo = resultados.setdefault(tabela.grupo, {})
            grupo[tabela.nome] = grupo.get(tabela.nome, 0) + linhas

    return resultados, falhas


@flow(name="czrm-salesforce-datacloud", log_prints=True, on_failure=[notify_falha_flow])
def rj_iplanrio__czrm_salesforce_datacloud(  # noqa: PLR0913 - assinatura do flow é a interface do deployment
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
    _validar_destino(origem, dataset_id, dry_run)
    inicio_run = time.time()

    selecionadas = _selecionar(tabelas)
    janelas = _janelas(modo, dia, reprocessar, reprocessar_de, reprocessar_ate)
    rotulo = _rotulo(modo, reprocessar, reprocessar_de, reprocessar_ate)

    rename_current_flow_run_task(new_name=f"czrm-salesforce-{origem}-{modo}" + ("-dry-run" if dry_run else ""))
    if not dry_run:
        inject_bd_credentials_task(environment="prod")

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

    schemas = _descobrir_schemas(session, selecionadas, origem)

    if not dry_run:
        garantir_tabelas(
            project_id=project_id,
            dataset_id=dataset_id,
            schemas=schemas,
            clusters={t.nome: t.cluster for t in selecionadas},
        )

    destino = Destino(project_id=project_id, dataset_id=dataset_id, dry_run=dry_run)
    resultados, falhas = _processar_janelas(janelas, selecionadas, schemas, session, origem, destino)

    if falhas and len(falhas) == len(selecionadas):
        raise RuntimeError(f"todas as tabelas falharam: {', '.join(falhas)}")

    if not dry_run and (reprocessar or modo in ("dia", "reconciliacao", "carga_inicial")):
        notify_resumo(
            rotulo=rotulo,
            resultados=resultados,
            falhas=falhas,
            minutos=(time.time() - inicio_run) / 60,
        )

    total = sum(quantidade for grupo in resultados.values() for quantidade in grupo.values())
    logger.info(
        "[FLOW] concluído — %d linha(s) em %d tabela(s)%s",
        total,
        len(selecionadas),
        f"; falharam: {', '.join(falhas)}" if falhas else "",
    )
    return resultados
