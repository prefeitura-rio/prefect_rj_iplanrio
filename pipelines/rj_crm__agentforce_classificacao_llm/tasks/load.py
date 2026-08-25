# -*- coding: utf-8 -*-
"""
Criação e carga da tabela de classificação no BigQuery.

Diferente de pipelines/rj_crm__salesforce_agentforce_api/tasks/{ensure_tables,load_bigquery}.py
(tabela particionada por dia + clusterizada, staging + MERGE): aqui não tem tabela de
staging. O MERGE usa `USING (SELECT * FROM UNNEST(@linhas))` — o "source" é montado
inline via parâmetro STRUCT array, não uma tabela física. Dois motivos: (1) a service
account desta pipeline tem permissão de escrever dado, não de criar tabela
(bigquery.tables.create) — staging exigiria DDL toda vez que precisasse recriar; (2)
sem filtro de partição no ON (reclassificar uma sessão pode mudar sua data_particao,
então não dá pra usar partição no MERGE como a pipeline irmã faz).
"""

from __future__ import annotations

import pandas as pd
from google.api_core.exceptions import Forbidden, NotFound
from google.cloud import bigquery
from prefect import task

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
_COLUNAS_ARRAY = {"tema_nome", "causa_nome"}
_PRIMARY_KEY = "id_sessao"

# Tipo de parâmetro (nome padrão SQL) por coluna — não reaproveita os enums do SCHEMA
# acima porque a nomenclatura diverge em alguns tipos: SchemaField usa "BOOLEAN"
# (schema físico), ScalarQueryParameter espera "BOOL" (SQL padrão); idem INTEGER x INT64.
_TIPO_PARAM = {
    "id_sessao": "STRING", "telefone": "STRING", "cpf": "STRING", "nome_cidadao": "STRING",
    "sessao_inicio_datahora": "TIMESTAMP", "sessao_fim_datahora": "TIMESTAMP",
    "jornada_nome": "STRING", "id_jornada": "STRING", "id_disparo_hsm": "STRING",
    "hsm_envio_datahora": "TIMESTAMP", "tipo_prompt": "STRING", "classificacao": "STRING",
    "conteudo_relevante": "BOOL", "resumo": "STRING", "secretaria_relacionada": "STRING",
    "sentimento": "STRING", "motivo": "STRING", "justificativa": "STRING",
    "resposta_llm_bruta": "STRING", "modelo": "STRING", "prompt_enviado": "STRING",
    "tokens_entrada": "INT64", "tokens_saida": "INT64", "tokens_total": "INT64",
    "prompt_versao": "STRING", "classificado_em": "TIMESTAMP", "data_particao": "DATE",
}


def _full_table_id(project_id: str, dataset_id: str, table_id: str) -> str:
    return f"{project_id}.{dataset_id}.{table_id}"


@task(log_prints=True)
def ensure_destino_table(project_id: str, dataset_id: str, table_id: str) -> None:
    """Cria a tabela destino (particionada por data_particao, clusterizada por id_sessao)
    se ainda não existir. Idempotente — seguro rodar toda execução.

    get_table primeiro, só tenta CREATE se realmente não existir: get_table pede uma
    permissão bem mais comum (bigquery.tables.get) que create_table
    (bigquery.tables.create) — sem esse check, client.create_table(..., exists_ok=True)
    tentaria criar TODA execução mesmo pra tabela que já existe (exists_ok só engole o
    erro "já existe" depois de tentar, não evita a tentativa) e quebraria com 403 se a
    service account não tiver permissão de DDL nesse dataset — mesmo a tabela já
    existindo e não precisando ser tocada."""
    client = bigquery.Client(project=project_id)
    full_id = _full_table_id(project_id, dataset_id, table_id)

    try:
        client.get_table(full_id)
        print(f"[LOAD] Tabela '{table_id}' já existe.")
        return
    except NotFound:
        pass

    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    destino = bigquery.Table(dataset_ref.table(table_id), schema=SCHEMA)
    destino.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="data_particao"
    )
    destino.clustering_fields = [_PRIMARY_KEY]
    destino.description = (
        "Classificação inicial por LLM das sessões do Agentforce (WhatsApp) — 1 linha por "
        "id_sessao, sobrescrita via MERGE se a sessão for reclassificada. Alimentada pela "
        "pipeline rj_crm__agentforce_classificacao_llm, diariamente às 15h."
    )
    try:
        client.create_table(destino, exists_ok=True)
    except Forbidden as e:
        raise PermissionError(
            f"'{table_id}' não existe em '{dataset_id}' e esta service account não tem "
            f"permissão bigquery.tables.create pra criá-la. Rode o CREATE TABLE manualmente "
            f"(DDL já enviado) com uma credencial com BigQuery Data Editor+ nesse dataset."
        ) from e
    print(f"[LOAD] Tabela '{table_id}' criada.")


def _valor_seguro(v):
    """None/NaN/NaT/pd.NA -> None; escalar numpy/pandas -> tipo Python nativo. O cliente
    de ScalarQueryParameter do BigQuery não aceita numpy.bool_/numpy.int64/pd.Timestamp
    com NaT/pd.NA diretamente."""
    if isinstance(v, list):  # tema_nome/causa_nome — tratados à parte, nunca None
        return v
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    if hasattr(v, "item"):  # escalar numpy (bool_, int64, float64...)
        return v.item()
    return v


def _linha_para_struct(registro: dict) -> bigquery.StructQueryParameter:
    campos = []
    for col in _COLUNAS:
        valor = _valor_seguro(registro.get(col))
        if col in _COLUNAS_ARRAY:
            campos.append(bigquery.ArrayQueryParameter(col, "STRING", valor or []))
        else:
            campos.append(bigquery.ScalarQueryParameter(col, _TIPO_PARAM[col], valor))
    return bigquery.StructQueryParameter(None, *campos)


@task(log_prints=True, retries=3, retry_delay_seconds=[30, 60, 120])
def carrega_classificacoes(
    df_final: pd.DataFrame,
    project_id: str,
    dataset_id: str,
    table_id: str,
    tamanho_lote: int = 200,
) -> int:
    """MERGE direto na tabela destino via `USING (SELECT * FROM UNNEST(@linhas))` — sem
    tabela de staging (ver docstring do módulo). Processa em lotes de tamanho_lote linhas
    por chamada de MERGE, pra não estourar limite de tamanho de requisição do BigQuery com
    resposta_llm_bruta/prompt_enviado (texto grande, pode somar MB em lotes maiores — ex.
    primeira carga, até 14 dias de backlog). Cada lote é seu próprio commit: uma falha no
    meio não perde os lotes já persistidos (só refaz o resto no próximo run, idempotente).

    ATENÇÃO pra quem construir a pipeline de tema/motivo (etapas 2/3): este MERGE
    sobrescreve TODAS as colunas com o valor do lote, inclusive as desta pipeline
    (classificacao, resumo etc.). Uma pipeline de etapa 2/3 que faça MERGE nesta mesma
    tabela só pode reaproveitar esse padrão se o lote dela trouxer a linha inteira
    (reextraída), e não só as colunas de tema/motivo — senão apaga a classificação da
    etapa 1 com NULL. Rever esta função (UPDATE parcial, coluna a coluna) antes de
    escrever a pipeline de tema/motivo."""
    if df_final.empty:
        print("[LOAD] Nada a carregar — nenhuma sessão classificada com sucesso nesta execução.")
        return 0

    client = bigquery.Client(project=project_id)
    destino_full = _full_table_id(project_id, dataset_id, table_id)

    df_carga = df_final.reindex(columns=_COLUNAS)
    registros = df_carga.to_dict("records")

    set_clause = ", ".join(f"t.{c} = s.{c}" for c in _COLUNAS if c != _PRIMARY_KEY)
    insert_cols = ", ".join(_COLUNAS)
    insert_vals = ", ".join(f"s.{c}" for c in _COLUNAS)
    merge_sql = f"""
        MERGE `{destino_full}` AS t
        USING (SELECT * FROM UNNEST(@linhas)) AS s
        ON t.{_PRIMARY_KEY} = s.{_PRIMARY_KEY}
        WHEN MATCHED THEN
            UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals})
    """

    linhas_afetadas = 0
    total_lotes = (len(registros) + tamanho_lote - 1) // tamanho_lote
    for i, inicio in enumerate(range(0, len(registros), tamanho_lote), start=1):
        lote = registros[inicio : inicio + tamanho_lote]
        linhas_param = bigquery.ArrayQueryParameter(
            "linhas", "STRUCT", [_linha_para_struct(r) for r in lote]
        )
        job_config = bigquery.QueryJobConfig(query_parameters=[linhas_param])
        merge_job = client.query(merge_sql, job_config=job_config)
        merge_job.result()
        linhas_afetadas += merge_job.num_dml_affected_rows or 0
        print(f"[LOAD] Lote {i}/{total_lotes}: {len(lote)} linha(s) processada(s).")

    print(f"[LOAD] MERGE concluído: {linhas_afetadas} linha(s) afetada(s) em '{table_id}'.")
    return linhas_afetadas
