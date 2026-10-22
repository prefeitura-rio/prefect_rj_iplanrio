"""Constants of rj_sms__sisreg_disparo_lembretes."""

from typing import Final

from utils.types import Table

DEFAULT_TABLE: Final = Table(dataset_id="disparos", table_id="disparos_efetuados")
BILLING_PROJECT: Final = "rj-crm-registry"
HSM_ID: Final = 18
"""The template of the HSM message of the appointments."""
CAMPAIGN_NAME: Final = "SISREG_AGENDAMENTOS"
COST_CENTER_ID: Final = 2
CHUNK_SIZE: Final = 1000
LOGIN_ROUTE: Final = "users/login"
SEND_PATH: Final = "/callcenter/hsm/send/{id_hsm}"
