# -*- coding: utf-8 -*-
"""
Flow orquestrador — Salesforce Data Cloud → BigQuery.

Duas partes:
  - salesforce_datacloud (o @flow): decide O QUE roda — janelas (hora, dia ou
    reprocessamento), quais tabelas, e o que fazer se der errado. Tabela
    crítica (as do Agentforce) que falhar aborta tudo; não-crítica avisa no
    Discord e segue.
  - processar_tabela: o COMO de uma tabela numa janela — extrair → transformar
    → staging + MERGE → validar. Não sabe de modo, reprocessamento nem Discord.

Até 2026-09-29 isso era dividido em "fases" (F1 como subflow separado,
F2a/F3/F5 inline aqui) e a rotina de uma tabela ficava em flows/template.py
(depois sf_to_bq.py) — a divisão em fases só servia pra dizer o que é crítico
e filtrar o que roda, então virou atributo de cada tabela (tabelas.yaml).

Modo janela (desde 2026-09-08, ver utils/janela.py): substitui o watermark
forward-only (nunca revisita o passado, não se recupera sozinho de um tick
perdido — foi a causa raiz de um buraco de quase 100% num dia inteiro) por
duas passadas com sobreposição intencional — `modo='hora'` roda de 15/15min
com janela rolante de 1h, `modo='dia'` roda 1x/dia reconciliando o dia
inteiro. As duas escrevem via staging + MERGE, nunca watermark. Ver
prefect.yaml pros dois schedules e o concurrency_limit que impede as duas
passadas de escreverem na mesma tabela ao mesmo tempo.
"""

from __future__ import annotations

import time
from datetime import date

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_crm__salesforce_datacloud.constants import DataCloudConstants
from pipelines.rj_crm__salesforce_datacloud.tasks.auth import get_data_cloud_session
from pipelines.rj_crm__salesforce_datacloud.tasks.bigquery import (
    ensure_bq_tables,
    load_chunk_to_staging,
    merge_staging_to_target,
    validate_row_count,
)
from pipelines.rj_crm__salesforce_datacloud.tasks.extract import (
    extract_from_crm_rest,
    extract_from_data_cloud,
)
from pipelines.rj_crm__salesforce_datacloud.tasks.notify import (
    notify_falha_flow,
    notify_falha_tabela,
    notify_resumo,
)
from pipelines.rj_crm__salesforce_datacloud.tasks.transform import transform_dataframe
from pipelines.rj_crm__salesforce_datacloud.utils.janela import fmt_janela, janela_utc, janelas_do_run
from pipelines.rj_crm__salesforce_datacloud.utils.queries import ler_query
from pipelines.rj_crm__salesforce_datacloud.utils.tabelas import TABELAS, NomeTabela, Tabela


def processar_tabela(
    tabela: Tabela,
    data_inicio: str,
    data_fim: str,
    partition_date: date,
    project_id: str,
    dataset_id: str,
    session: dict,
) -> dict[str, int]:
    """
    Extrai → transforma → carrega (staging + MERGE) → valida UMA tabela numa
    janela.

    Args:
        tabela         : Configuração da tabela (tabelas.yaml).
        data_inicio    : Início da janela (hora-parede de SP, ver utils/janela.py).
        data_fim       : Fim da janela (exclusivo).
        partition_date : data_particao carimbada nas linhas (dia de SP).
        project_id     : Projeto GCP.
        dataset_id     : Dataset BQ de destino.
        session        : Sessão Salesforce (get_data_cloud_session) — serve pro
                         Data Cloud e pro CRM REST.

    Returns:
        {"extraidas": n, "inseridas": n, "atualizadas": n} — extraídas da fonte
        e o resultado do MERGE (ver merge_staging_to_target).
    """
    # Fonte que grava UTC de verdade (ex.: DLL de eventos WhatsApp) recebe a
    # janela convertida + os limites de partição UTC; as DMOs usam direto.
    if tabela.janela_utc:
        (inicio_q, fim_q), extras = janela_utc(data_inicio, data_fim)
    else:
        (inicio_q, fim_q), extras = (data_inicio, data_fim), {}
    query = ler_query(tabela.query).format(data_inicio=inicio_q, data_fim=fim_q, **extras)

    # --- Extrair (única etapa que muda por source) ---
    if tabela.source == "data_cloud":
        df = extract_from_data_cloud(
            dc_session=session,
            query=query,
            table_name=tabela.nome,
            order_by_col=tabela.order_by_col,
        )
    else:  # crm_rest (source já validado em utils/tabelas.py)
        df = extract_from_crm_rest(crm_session=session, soql=query, table_name=tabela.nome)

    if df.empty:
        print(f"[FLOW] '{tabela.nome}': sem dados na janela — pulando carga.")
        return {"extraidas": 0, "inseridas": 0, "atualizadas": 0}
    extraidas = len(df)

    # --- Transformar ---
    df = transform_dataframe(
        df=df,
        table_name=tabela.nome,
        is_data_cloud=tabela.source == "data_cloud",
        date_columns=tabela.date_columns,
        partition_date=partition_date,
        output_value_text_action_step_only=tabela.output_value_text_action_step_only,
    )

    # --- Carregar: staging + MERGE ---
    load_chunk_to_staging(
        df_chunk=df,
        project_id=project_id,
        dataset_id=dataset_id,
        staging_table_id=tabela.staging,
    )
    merge = merge_staging_to_target(
        project_id=project_id,
        dataset_id=dataset_id,
        staging_table_id=tabela.staging,
        target_table_id=tabela.nome,
        primary_key=tabela.primary_key,
        partition_field="data_particao",
    )

    # --- Validar ---
    validate_row_count(
        source_count=merge["inseridas"] + merge["atualizadas"],
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=tabela.nome,
        partition_date=str(partition_date),
        write_mode="merge",
    )

    return {"extraidas": extraidas, **merge}


@flow(name="salesforce-datacloud", log_prints=True, on_failure=[notify_falha_flow])
def salesforce_datacloud(
    project_id: str | None = None,
    dataset_id: str | None = None,
    partition_date: date | None = None,
    modo: str = "hora",
    tabelas: list[NomeTabela] | None = None,
    reprocessar: bool = False,
    reprocessar_de: date | None = None,
    reprocessar_ate: date | None = None,
    environment: str = "prod",
) -> dict[str, dict[str, int]]:
    """
    Extrai as tabelas de tabelas.yaml do Salesforce pro BigQuery.

    Args:
        project_id      : ID do projeto GCP.
        dataset_id      : Dataset BQ de destino.
        partition_date  : Só relevante em modo='dia' — qual dia reconciliar.
                          Padrão: ontem.
        modo            : 'hora' (padrão, janela rolante de 1h — schedule de
                          15/15min) ou 'dia' (dia inteiro — schedule 1x/dia,
                          reconciliação funda). Ver utils/janela.py. Ignorado
                          se reprocessar=True.
        tabelas         : Quais tabelas atualizar — uma ou mais (seleção
                          múltipla na UI do Prefect). Vazio/None = todas.
        reprocessar     : Toggle de reprocessamento — refaz as `tabelas`
                          selecionadas dia a dia, de reprocessar_de até
                          reprocessar_ate. Via MERGE (igual ao normal), então
                          rodar de novo não duplica: id existente é
                          atualizado, id novo é inserido.
        reprocessar_de  : 1º dia do reprocessamento (inclusivo). Obrigatório
                          se reprocessar=True.
        reprocessar_ate : Último dia (inclusivo). Padrão: = reprocessar_de.
        environment     : Ambiente de execução ('prod' ou 'staging').

    Returns:
        Dict por grupo com {tabela: linhas inseridas + atualizadas no MERGE}
        (somando todos os dias, se reprocessamento).
    """
    project_id = project_id or DataCloudConstants.BQ_PROJECT_ID.value
    dataset_id = dataset_id or DataCloudConstants.DATASET_ID.value

    # Vazio ([] na UI) = todas, igual a None. Nome inválido já é barrado pelo
    # Prefect (NomeTabela é enum), a checagem abaixo cobre chamada direta.
    desconhecidas = set(tabelas or []) - {t.nome for t in TABELAS}
    if desconhecidas:
        raise ValueError(f"Tabelas desconhecidas: {sorted(desconhecidas)}. Ver tabelas.yaml.")
    selecionadas = [t for t in TABELAS if not tabelas or t.nome in tabelas]

    # Valida antes de qualquer efeito colateral (credencial, ensure_tables)
    janelas = janelas_do_run(modo, partition_date, reprocessar, reprocessar_de, reprocessar_ate)

    if reprocessar:
        rotulo = f"reprocessamento {janelas[0][2]} a {janelas[-1][2]} ({len(janelas)} dia(s))"
        rename_current_flow_run_task(
            new_name=f"salesforce-datacloud-reprocesso-{janelas[0][2]}_{janelas[-1][2]}"
        )
    else:
        rotulo = f"modo='{modo}'"
        rename_current_flow_run_task(new_name="salesforce-datacloud")
    inject_bd_credentials_task(environment=environment)

    ensure_bq_tables(project_id=project_id, dataset_id=dataset_id)

    t_pipeline_start = time.time()
    resultados: dict[str, dict[str, int]] = {}
    falhas: list[str] = []

    print(f"[FLOW] {rotulo} | tabelas: {[t.nome for t in selecionadas]}")

    # Uma sessão pra tudo — CRM REST usa a mesma credencial do Data Cloud.
    # Sem pre-flight (removido 2026-09-29): tabela/credencial com problema
    # falha na própria extração, com o erro real do Salesforce — crítica
    # aborta (on_failure avisa no Discord), não-crítica avisa e segue.
    session = get_data_cloud_session()

    for data_inicio, data_fim, partition_date_efetivo in janelas:
        if reprocessar:
            print(f"[FLOW] === Reprocessando {partition_date_efetivo} ===")

        for tabela in selecionadas:
            t0 = time.time()
            try:
                r = processar_tabela(
                    tabela=tabela,
                    data_inicio=data_inicio,
                    data_fim=data_fim,
                    partition_date=partition_date_efetivo,
                    project_id=project_id,
                    dataset_id=dataset_id,
                    session=session,
                )
                print(
                    f"[FLOW] Tabela {tabela.nome} completa"
                    f"{' (reprocessamento)' if reprocessar else ''} — "
                    f"janela {fmt_janela(data_inicio, data_fim)}: "
                    f"{r['extraidas']} extraídas, {r['inseridas']} linhas inseridas e "
                    f"{r['atualizadas']} atualizadas após o MERGE ({time.time() - t0:.0f}s)"
                )
                linhas = r["inseridas"] + r["atualizadas"]
            except Exception as exc:
                if tabela.critica:
                    raise  # aborta o flow — o hook on_failure avisa no Discord
                print(f"[FLOW] WARN: '{tabela.nome}' falhou — {exc}. Continuando...")
                notify_falha_tabela(
                    tabela=tabela.nome,
                    grupo=tabela.grupo,
                    janela=fmt_janela(data_inicio, data_fim),
                    erro=str(exc),
                )
                if tabela.nome not in falhas:
                    falhas.append(tabela.nome)
                linhas = 0

            grupo = resultados.setdefault(tabela.grupo, {})
            grupo[tabela.nome] = grupo.get(tabela.nome, 0) + linhas

    total_elapsed = time.time() - t_pipeline_start
    # Resumo no Discord só no diário e no reprocessamento — o de hora roda
    # 96x/dia; falha dele já avisa sozinha (notify_falha_tabela/on_failure).
    if reprocessar or modo == "dia":
        notify_resumo(
            rotulo=rotulo,
            resultados=resultados,
            falhas=falhas,
            minutos=total_elapsed / 60,
        )

    print(f"[FLOW] Pipeline concluido em {total_elapsed:.0f}s | {resultados}")
    return resultados
