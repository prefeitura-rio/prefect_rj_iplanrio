"""Constants shared across two or more modules of this pipeline."""

PROJECT_ID = "rj-iplanrio"
DATASET_ID = "brutos_czrm_salesforce_staging"
DATASPACE = "default"
API_VERSION = "v67.0"
PAGE_SIZE = 1000
MAX_ROWS = 5_000_000
TIMEOUT = 120
BQ_LOCATION = "US"
BQ_LABELS: dict[str, str] = {"pipeline": "rj_iplanrio__czrm_salesforce_datacloud"}
DIAS_RECONCILIACAO = 7
