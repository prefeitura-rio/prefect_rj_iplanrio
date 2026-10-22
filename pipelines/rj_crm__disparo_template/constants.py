"""Constants of rj_crm__disparo_template."""

from typing import Final

from utils.types import Table

BILLING_PROJECT: Final = "rj-crm-registry"
DEFAULT_CAMPAIGN: Final = "template"
DEFAULT_TABLE: Final = Table(dataset_id="brutos_wetalkie", table_id="disparos_efetuados")
TIMEZONE: Final = "America/Sao_Paulo"
REPOSITORY: Final = "https://GITHUB_TOKEN@github.com/prefeitura-rio/queries-rj-crm-registry.git"
STATUS_MODEL: Final = "+int_crm_status_disparo"
"""The dbt model that has the status of each dispatch, with the models that it needs."""
REQUIRED_COLUMNS: Final = ("cpf", "telefone")
CPF_ALIASES: Final = ("SubscriberKey",)
"""The old names of the column `cpf`."""
SUCCESS_SHARE: Final = 0.2
"""The share of the sends that must have a status for a campaign to count as sent."""
VALIDATION_SAMPLE: Final = 5
SFTP_TIMEOUT: Final = 30
SFTP_KEEPALIVE: Final = 15
RETRY_PAUSE_MINUTES: Final = 3
TEST_INITIAL_WAIT: Final = 2
TEST_CHECK_INTERVAL: Final = 2
TEST_MAX_WAIT: Final = 8
TEST_MATERIALIZATION_WAIT: Final = 5
DEBTS_URL: Final = "https://services.pref.rio/mcp/consulta_debitos"
DEBTS_TIMEOUT: Final = 15.0
DEBTS_TRIES: Final = 3
ESCAPED_FIELDS: Final = ("lista_cdas", "lista_guias", "dicionario_itens", "itens_informados")
"""The debt fields that SFMC reads as JSON with the inner quotes escaped."""
DEBTS_FIELDS: Final = (
    "api_resposta_sucesso",
    "lista_cdas",
    "lista_guias",
    "dicionario_itens",
    "total_itens_pagamento",
    "mensagem_divida_contribuinte",
    "guias_quantidade_total",
    "efs_cdas_quantidade_total",
    "total_nao_parcelado",
    "total_parcelado",
    "debitos_msg",
)
DEBTS_OUTPUT: Final = ("cpfCnpj", *DEBTS_FIELDS, "itens_informados")
