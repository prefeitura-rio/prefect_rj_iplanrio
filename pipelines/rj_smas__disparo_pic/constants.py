"""Constants of rj_smas__disparo_pic."""

from typing import Final

from utils.types import Table

DEFAULT_TABLE: Final = Table(dataset_id="brutos_wetalkie", table_id="disparos_efetuados")
BILLING_PROJECT: Final = "rj-smas"
HSM_ID: Final = 184
CAMPAIGN_NAME: Final = "smas-cartaopic-disparoevento"
COST_CENTER_ID: Final = 71
CHUNK_SIZE: Final = 1000
SLEEP_MINUTES: Final = 5
RESULTS_MINUTES: Final = 15
WHITELIST_PERCENTAGE: Final = 30
APPROVAL_COLUMN: Final = "APROVACAO_DISPARO_AVISO"
EVENT_DATE_COLUMN: Final = "DATA_ENTREGA"
APPROVED: Final = "aprovado"
