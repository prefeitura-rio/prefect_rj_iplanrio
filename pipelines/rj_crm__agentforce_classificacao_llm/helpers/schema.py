"""The schema of the table of the classifications."""

from typing import Final, cast

from google.cloud import bigquery

TIMESTAMP: Final = bigquery.enums.SqlTypeNames.TIMESTAMP
STRING: Final = bigquery.enums.SqlTypeNames.STRING
DATE: Final = bigquery.enums.SqlTypeNames.DATE
BOOL: Final = bigquery.enums.SqlTypeNames.BOOLEAN
INT64: Final = bigquery.enums.SqlTypeNames.INT64

SCHEMA: Final = [
    bigquery.SchemaField("id_sessao", STRING, mode="REQUIRED"),
    bigquery.SchemaField("telefone", STRING),
    bigquery.SchemaField("cpf", STRING),
    bigquery.SchemaField("nome_cidadao", STRING),
    bigquery.SchemaField("sessao_inicio_datahora", TIMESTAMP),
    bigquery.SchemaField("sessao_fim_datahora", TIMESTAMP),
    bigquery.SchemaField("jornada_nome", STRING),
    bigquery.SchemaField("id_jornada", STRING),
    bigquery.SchemaField("id_disparo_hsm", STRING),
    bigquery.SchemaField("hsm_envio_datahora", TIMESTAMP),
    bigquery.SchemaField("hsm_texto", STRING),
    bigquery.SchemaField("conversa_completa", STRING),
    bigquery.SchemaField("tipo_prompt", STRING),
    bigquery.SchemaField("relacao_hsm", STRING),
    bigquery.SchemaField("conteudo_relevante", BOOL),
    bigquery.SchemaField("resumo", STRING),
    bigquery.SchemaField("secretarias_relacionadas", STRING),
    bigquery.SchemaField("secretaria_principal", STRING),
    bigquery.SchemaField("natureza", STRING),
    bigquery.SchemaField("sentimento", STRING),
    bigquery.SchemaField("motivo", STRING),
    bigquery.SchemaField("justificativa", STRING),
    bigquery.SchemaField("resposta_llm_bruta", STRING),
    bigquery.SchemaField("modelo", STRING),
    bigquery.SchemaField("prompt_enviado", STRING),
    bigquery.SchemaField("tokens_entrada", INT64),
    bigquery.SchemaField("tokens_saida", INT64),
    bigquery.SchemaField("tokens_total", INT64),
    bigquery.SchemaField("prompt_versao", STRING),
    bigquery.SchemaField("classificado_em", TIMESTAMP, mode="REQUIRED"),
    bigquery.SchemaField("tema_nome", STRING, mode="REPEATED"),
    bigquery.SchemaField("causa_nome", STRING, mode="REPEATED"),
    bigquery.SchemaField("data_particao", DATE, mode="REQUIRED"),
    bigquery.SchemaField("telefone_confirmacao_flag", STRING),
    bigquery.SchemaField("telefone_confirmacao_datahora", TIMESTAMP),
]
"""The columns, in the physical order of the table.

`relacao_hsm` should never be empty, but it stays nullable so an answer of the model that lacks the key does not
fail the load of the whole batch. `tema_nome` and `causa_nome` are arrays, so a category that matches many rules
is `ARRAY_LENGTH > 1`. The two last columns were added after the table existed."""
COLUMNS: Final = [cast("str", field.name) for field in SCHEMA]
PRIMARY_KEY: Final = "id_sessao"
