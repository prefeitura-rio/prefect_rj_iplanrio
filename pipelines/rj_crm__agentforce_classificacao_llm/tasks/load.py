# -*- coding: utf-8 -*-
"""
Criação e carga da tabela de classificação no BigQuery.

Tabela particionada por dia + clusterizada, tmp + MERGE — mesmo padrão de
pipelines/rj_crm__salesforce_agentforce_api/tasks/{ensure_tables,load_bigquery}.py,
adaptado pra 1 tabela só e pra MERGE por id_sessao (sem filtro de partição — aqui
reclassificar uma sessão pode mudar sua data_particao, então não dá pra usar partição
no ON como a pipeline irmã faz). Nome da tabela intermediária (`_tmp`, não `_staging`)
segue pipelines/rj_crm__get_history_data em vez do agentforce_api.
"""

from __future__ import annotations

import pandas as pd
from google.api_core.exceptions import Forbidden, NotFound
from google.cloud import bigquery
from prefect import task

from pipelines.rj_crm__agentforce_classificacao_llm.utils.bigquery import get_bq_client

_TIMESTAMP = bigquery.enums.SqlTypeNames.TIMESTAMP
_STRING = bigquery.enums.SqlTypeNames.STRING
_DATE = bigquery.enums.SqlTypeNames.DATE
_BOOL = bigquery.enums.SqlTypeNames.BOOLEAN
_INT64 = bigquery.enums.SqlTypeNames.INT64

SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("id_sessao", _STRING, mode="REQUIRED"),
    bigquery.SchemaField("telefone", _STRING),
    bigquery.SchemaField("cpf", _STRING),
    bigquery.SchemaField("nome_cidadao", _STRING),
    bigquery.SchemaField("sessao_inicio_datahora", _TIMESTAMP),
    bigquery.SchemaField("sessao_fim_datahora", _TIMESTAMP),
    bigquery.SchemaField("jornada_nome", _STRING),
    bigquery.SchemaField("id_jornada", _STRING),
    bigquery.SchemaField("id_disparo_hsm", _STRING),
    bigquery.SchemaField("hsm_envio_datahora", _TIMESTAMP),
    # Conversa/HSM originais — pra permitir validar resumo/motivo/tema/classificacao
    # lendo direto daqui, sem precisar reconstruir a partir de prompt_enviado (que é
    # null nas sessões RESPOSTA_ATRASADA_BTN, decididas por regra sem chamar a LLM).
    bigquery.SchemaField("mensagens_usuario_concatenadas", _STRING),
    bigquery.SchemaField("hsm_texto", _STRING),
    # com_hsm | sem_hsm | null (resposta_atrasada_btn não passa por prompt nenhum —
    # ver coluna `modelo` pra saber que foi decidida por regra)
    bigquery.SchemaField("tipo_prompt", _STRING),
    # DENTRO_DO_ESCOPO | FORA_DO_ESCOPO | MISTO (classificado pela LLM, tem HSM) |
    # SEM_HSM_ASSOCIADO (LLM roda, não avalia escopo) | RESPOSTA_ATRASADA_BTN
    # (decidido por regra, sem LLM). Não deveria ficar null na prática (todo caminho
    # do código força um destes 5 valores), mas o campo continua NULLABLE de
    # propósito: um JSON incompleto da LLM (sem a chave "classificacao", apesar do
    # prompt pedir) não pode travar a carga do lote inteiro por violar NOT NULL.
    bigquery.SchemaField("classificacao", _STRING),
    bigquery.SchemaField("conteudo_relevante", _BOOL),
    bigquery.SchemaField("resumo", _STRING),
    bigquery.SchemaField("secretaria_relacionada", _STRING),
    # rótulos de natureza da manifestação (multi-label, separados por vírgula):
    # Dúvida | Reclamação | Elogio | Solicitação — apesar do nome, não é sentimento/polaridade
    bigquery.SchemaField("sentimento", _STRING),
    bigquery.SchemaField("motivo", _STRING),
    bigquery.SchemaField("justificativa", _STRING),
    bigquery.SchemaField("resposta_llm_bruta", _STRING),
    bigquery.SchemaField("modelo", _STRING),
    # Auditoria de custo/depuração — null nas sessões RESPOSTA_ATRASADA_BTN (não
    # chamam a LLM). tokens_* vêm de usageMetadata da própria resposta do Gemini,
    # sem custo extra de chamada.
    bigquery.SchemaField("prompt_enviado", _STRING),
    bigquery.SchemaField("tokens_entrada", _INT64),
    bigquery.SchemaField("tokens_saida", _INT64),
    bigquery.SchemaField("tokens_total", _INT64),
    bigquery.SchemaField("prompt_versao", _STRING),
    bigquery.SchemaField("classificado_em", _TIMESTAMP, mode="REQUIRED"),
    # Etapas 2 e 3 do pipeline de clusterização (ver
    # clustering/docs/pipeline_clusterizacao_agentforce.md) — tema (indução por
    # secretaria) e causa sistêmica (indução de motivo dentro do tema). Reservadas
    # aqui: esta pipeline só escreve a etapa 1 e sempre grava as duas como array
    # vazio ([]); quando a pipeline de tema/motivo existir, ela faz MERGE nesta
    # mesma linha (por id_sessao) e preenche de verdade, sem recriar linha. Array
    # (REPEATED), não string única: a avaliação é por função Python por categoria
    # (ver clustering/modules/rules_sandbox.py) e pode bater em mais de uma ao
    # mesmo tempo — "bateu em mais de uma" é só ARRAY_LENGTH(coluna) > 1, sem
    # precisar de um indicador booleano separado. Array REPEATED nunca é NULL no
    # BigQuery (vazio é o "sem valor" natural), por isso sem NOT NULL explícito.
    bigquery.SchemaField("tema_nome", _STRING, mode="REPEATED"),
    bigquery.SchemaField("causa_nome", _STRING, mode="REPEATED"),
    bigquery.SchemaField("data_particao", _DATE, mode="REQUIRED"),
]

_COLUNAS = [f.name for f in SCHEMA]
_PRIMARY_KEY = "id_sessao"


def _full_table_id(project_id: str, dataset_id: str, table_id: str) -> str:
    return f"{project_id}.{dataset_id}.{table_id}"


def _ensure_uma_tabela(
    client: bigquery.Client,
    dataset_ref: bigquery.DatasetReference,
    project_id: str,
    dataset_id: str,
    table_id: str,
    descricao: str,
    particionada: bool,
) -> None:
    """Só tenta CREATE se a tabela realmente não existir. get_table pede uma permissão
    bem mais comum (bigquery.tables.get) que create_table (bigquery.tables.create) — sem
    esse check, client.create_table(..., exists_ok=True) tentaria criar TODA execução
    mesmo pra tabela que já existe (exists_ok só engole o erro "já existe" depois de
    tentar, não evita a tentativa) e quebraria com 403 se a service account não tiver
    permissão de DDL nesse dataset — mesmo a tabela já existindo e não precisando ser
    tocada. Por isso as tabelas (destino e tmp) precisam já existir de antemão,
    criadas manualmente uma vez (DDL enviado à parte) — esta função só confirma."""
    full_id = f"{project_id}.{dataset_id}.{table_id}"
    try:
        client.get_table(full_id)
        return
    except NotFound:
        pass

    table = bigquery.Table(dataset_ref.table(table_id), schema=SCHEMA)
    table.description = descricao
    if particionada:
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="data_particao"
        )
        table.clustering_fields = [_PRIMARY_KEY]
    try:
        client.create_table(table, exists_ok=True)
    except Forbidden as e:
        raise PermissionError(
            f"'{table_id}' não existe em '{dataset_id}' e esta service account não tem "
            f"permissão bigquery.tables.create pra criá-la. Rode o CREATE TABLE manualmente "
            f"(DDL enviado à parte) com uma credencial com BigQuery Data Editor+ nesse dataset."
        ) from e


@task(log_prints=True)
def ensure_destino_table(project_id: str, dataset_id: str, table_id: str, tmp_table_id: str) -> None:
    """Confirma que a tabela destino (particionada por data_particao, clusterizada por
    id_sessao) e a tmp existem — cria só se realmente faltar (ver _ensure_uma_tabela).
    Idempotente, seguro rodar toda execução."""
    client = get_bq_client(project_id)
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)

    _ensure_uma_tabela(
        client, dataset_ref, project_id, dataset_id, table_id,
        descricao=(
            "Classificação inicial por LLM das sessões do Agentforce (WhatsApp) — 1 linha por "
            "id_sessao, sobrescrita via MERGE se a sessão for reclassificada. Alimentada pela "
            "pipeline rj_crm__agentforce_classificacao_llm, diariamente às 15h."
        ),
        particionada=True,
    )
    _ensure_uma_tabela(
        client, dataset_ref, project_id, dataset_id, tmp_table_id,
        descricao="Tmp da pipeline rj_crm__agentforce_classificacao_llm — truncada a cada execução.",
        particionada=False,
    )

    print(f"[LOAD] Tabelas OK: '{table_id}' (destino) e '{tmp_table_id}' (tmp).")


@task(log_prints=True, retries=3, retry_delay_seconds=[30, 60, 120])
def carrega_classificacoes(
    df_final: pd.DataFrame,
    project_id: str,
    dataset_id: str,
    table_id: str,
    tmp_table_id: str,
) -> int:
    """Carrega df_final na tabela tmp e faz MERGE (upsert por id_sessao) pro destino.
    Se df_final estiver vazio, não faz nada (não trunca a tmp à toa).

    ATENÇÃO pra quem construir a pipeline de tema/motivo (etapas 2/3): este MERGE
    sobrescreve TODAS as colunas com o valor da tmp, inclusive as desta pipeline
    (classificacao, resumo etc.). Uma pipeline de etapa 2/3 que faça MERGE nesta mesma
    tabela só pode reaproveitar este set_clause se a tmp dela trouxer a linha inteira
    (reextraída), e não só as colunas de tema/motivo — senão apaga a classificação da
    etapa 1 com NULL. Rever esta função (UPDATE parcial, coluna a coluna) antes de
    escrever a pipeline de tema/motivo."""
    if df_final.empty:
        print("[LOAD] Nada a carregar — nenhuma sessão classificada com sucesso nesta execução.")
        return 0

    client = get_bq_client(project_id)
    tmp_full = _full_table_id(project_id, dataset_id, tmp_table_id)
    destino_full = _full_table_id(project_id, dataset_id, table_id)

    df_carga = df_final.reindex(columns=_COLUNAS)

    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND, schema=SCHEMA)
    job = client.load_table_from_dataframe(df_carga, tmp_full, job_config=job_config)
    job.result()
    if job.errors:
        raise RuntimeError(f"[LOAD] Erro ao carregar tmp: {job.errors}")

    set_clause = ", ".join(f"t.{c} = s.{c}" for c in _COLUNAS if c != _PRIMARY_KEY)
    insert_cols = ", ".join(_COLUNAS)
    insert_vals = ", ".join(f"s.{c}" for c in _COLUNAS)

    merge_sql = f"""
        MERGE `{destino_full}` AS t
        USING `{tmp_full}` AS s
        ON t.{_PRIMARY_KEY} = s.{_PRIMARY_KEY}
        WHEN MATCHED THEN
            UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals})
    """
    merge_job = client.query(merge_sql)
    merge_job.result()
    linhas_afetadas = merge_job.num_dml_affected_rows or 0

    client.query(f"TRUNCATE TABLE `{tmp_full}`").result()

    print(f"[LOAD] MERGE concluído: {linhas_afetadas} linha(s) afetada(s) em '{table_id}'. Tmp limpa.")
    return linhas_afetadas
