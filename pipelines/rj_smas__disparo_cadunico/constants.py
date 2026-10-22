"""Constants of rj_smas__disparo_cadunico."""

from typing import Final

from utils.types import Table

DEFAULT_TABLE: Final = Table(dataset_id="brutos_wetalkie", table_id="disparos_efetuados")
BILLING_PROJECT: Final = "rj-crm-registry"
HSM_ID: Final = 101
CAMPAIGN_NAME: Final = "smas-cadunico-prod"
COST_CENTER_ID: Final = 71
CHUNK_SIZE: Final = 1000
PROCESSOR: Final = "skip_weekends"
DAYS_AHEAD: Final = 2
SLEEP_MINUTES: Final = 5
RESULTS_MINUTES: Final = 15
WHITELIST_PERCENTAGE: Final = 30
TEST_PREFIX: Final = "teste-"
