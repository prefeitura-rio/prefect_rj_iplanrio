"""Constants of rj_crm__agentforce_classificacao_llm."""

from typing import Final, cast

from google.cloud import bigquery
from pydantic import TypeAdapter

from pipelines.rj_crm__agentforce_classificacao_llm.types import MacrothemeCatalog, SecretariatCatalog

DEFAULT_PROJECT_ID: Final = "rj-crm-registry"
DEFAULT_SOURCE_TABLE: Final = "rj-crm-registry.rmi_conversas.v2_chatbot_conversas"
"""The consolidated view of the Agentforce sessions.

A session with `fim_datahora` filled is ready to classify.
"""
DEFAULT_HSM_CATALOG_TABLE: Final = "rj-crm-registry.intermediario_crm_whatsapp.v2_atividades_enriquecidas_hsm"
"""The catalog of HSMs by journey, that gives the buttons of an HSM."""
DEFAULT_DATASET_ID: Final = "brutos_salesforce"
DEFAULT_TABLE_ID: Final = "ai_agent_session_classificacao"
DEFAULT_TMP_TABLE_ID: Final = "ai_agent_session_classificacao_tmp"
"""The table that each load fills, merges and clears.

It must exist, as the service account may not create it.
"""
DEFAULT_RULES_TABLE_ID: Final = "taxonomia_regras"
CHATBOT_DATASET_ID: Final = "rmi_conversas"
CHATBOT_V1_TABLE_ID: Final = "chatbot"
CHATBOT_V2_TABLE_ID: Final = "v2_chatbot_conversas"
THEME_STAGE: Final = "tema"
REASON_STAGE: Final = "motivo"
DEFAULT_LOOKBACK_DAYS: Final = 7
"""How many days of sessions each run looks at.

A session left unclassified for longer is not picked up again.
"""
DEFAULT_HSM_MAX_DAYS_BEFORE: Final = 7
"""How many days before a session its HSM can have been sent."""
DEFAULT_MAX_WORKERS: Final = 8
DEFAULT_LOAD_BATCH: Final = 500
"""The sessions that a run classifies before it loads them.

A crash then loses at most one batch."""
LLM_TIMEOUT: Final = 60
LLM_MAX_OUTPUT_TOKENS: Final = 2048
LCS_RATIO: Final = 0.7
"""The share of a placeholder that the best key must cover.

The key is the one with the longest common subsequence in the variables."""
WITH_HSM: Final = "com_hsm"
WITHOUT_HSM: Final = "sem_hsm"
CLASS_WITHOUT_HSM: Final = "SEM_HSM_ASSOCIADO"
CLASS_LATE_BUTTON: Final = "RESPOSTA_ATRASADA_BTN"
LATE_BUTTON_NOTE: Final = " ".join(
    [
        "Resposta do usuário é idêntica ao texto de um botão do HSM,",
        "mas chegou fora da janela de 24h (não foi capturada como clique de botão).",
    ]
)
PROMPT_VERSION: Final = "v4"
MACROTHEME_PROMPT_VERSION: Final = "v2"
SECRETARIAT_PROMPT_VERSION: Final = "v3"
"""Goes up by hand when the prompts change what a classification means.

It is for audit only.
"""
VALID_RELATIONS: Final = ("DENTRO_DO_ESCOPO", "FORA_DO_ESCOPO", "MISTO")
VALID_NATURES: Final = ("Dúvida", "Problema", "Reclamação", "Elogio", "Solicitação", "Informação")
VALID_SECRETARIATS: Final = ("SMTR", "Comlurb", "RioLuz", "Seconserva", "SMS", "SMAS", "SME")
VALID_PHONE_FLAGS: Final = ("PERTENCE", "INDEFINIDO", "NAO_PERTENCE")
NO_SECRETARIAT: Final = "Sem secretaria identificada"
"""The secretariat that the rules use for a session that the model gave none."""
SAFE_IMPORTS: Final = frozenset({"re", "unicodedata"})
FORBIDDEN_NAMES: Final = frozenset(
    {
        "eval",
        "exec",
        "open",
        "__import__",
        "compile",
        "globals",
        "locals",
        "vars",
        "getattr",
        "setattr",
        "delattr",
        "input",
        "os",
        "sys",
        "subprocess",
    }
)


TIMESTAMP: Final = bigquery.enums.SqlTypeNames.TIMESTAMP


STRING: Final = bigquery.enums.SqlTypeNames.STRING


DATE: Final = bigquery.enums.SqlTypeNames.DATE


BOOL: Final = bigquery.enums.SqlTypeNames.BOOLEAN


INT64: Final = bigquery.enums.SqlTypeNames.INT64


SCHEMA: Final = [
    bigquery.SchemaField("id_sessao", STRING, mode="REQUIRED"),
    bigquery.SchemaField("telefone", STRING),
    bigquery.SchemaField("telefone_original", STRING),
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
    bigquery.SchemaField("macrotema_principal", STRING),
    bigquery.SchemaField("macrotemas_relacionados", STRING),
    bigquery.SchemaField("justificativa_macrotema", STRING),
    bigquery.SchemaField("resposta_llm_bruta_macrotema", STRING),
    bigquery.SchemaField("prompt_versao_macrotema", STRING),
    bigquery.SchemaField("justificativa_secretaria", STRING),
    bigquery.SchemaField("resposta_llm_bruta_secretaria", STRING),
    bigquery.SchemaField("prompt_versao_secretaria", STRING),
    bigquery.SchemaField("tokens_cache", INT64),
]
"""The columns, in the physical order of the table.

`relacao_hsm` should never be empty, but it stays nullable so an answer of the model
that lacks the key does not fail the load of the whole batch. `tema_nome` and
`causa_nome` are arrays, so a category that matches many rules is
`ARRAY_LENGTH > 1`. The two last columns were added after the table existed."""


COLUMNS: Final = [cast("str", field.name) for field in SCHEMA]


PRIMARY_KEY: Final = "id_sessao"


MACROTHEME_CATALOG = TypeAdapter(list[MacrothemeCatalog])


SECRETARIAT_CATALOG = TypeAdapter(list[SecretariatCatalog])


OUTSIDE = "OUTRA"
