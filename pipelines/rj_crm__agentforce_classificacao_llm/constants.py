"""Constants of rj_crm__agentforce_classificacao_llm."""

from typing import Final

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
PROMPT_VERSION: Final = "v3"
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
