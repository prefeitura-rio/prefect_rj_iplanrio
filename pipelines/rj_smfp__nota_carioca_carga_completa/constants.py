"""Constants of rj_smfp__nota_carioca_carga_completa."""

from typing import Final

EXTRACTION_DEPLOYMENT: Final = "rj-smfp--nota-carioca-extracao-xml/rj-smfp--nota-carioca-extracao-xml--prod"
"""The `<flow>/<deployment>` of the XML extraction in production, before the loads."""

ORACLE_TO_BQ_DEPLOYMENT: Final = "rj-smfp--nota-carioca-oracle-to-bq/rj-smfp--nota-carioca-oracle-to-bq--prod"
"""The `<flow>/<deployment>` of the load from Oracle to BigQuery, in production."""

BQ_TO_ORACLE_DEPLOYMENT: Final = "rj-smfp--nota-carioca-bq-to-oracle/rj-smfp--nota-carioca-bq-to-oracle--prod"
"""The `<flow>/<deployment>` of the load from BigQuery to Oracle, in production."""

EXTRACTION_LABEL: Final = "Extração XML"
ORACLE_TO_BQ_LABEL: Final = "Oracle → BigQuery"
BQ_TO_ORACLE_LABEL: Final = "BigQuery → Oracle"

SECONDS_PER_HOUR: Final = 3600

POLL_SECONDS: Final = 30.0
"""The pod only waits, so reading the state of the child runs every 30 s is enough."""

COMPLETED_STATE: Final = "COMPLETED"
FINISHED_STATES: Final = frozenset({"COMPLETED", "FAILED", "CRASHED", "CANCELLED"})
BUSY_STATES: Final = frozenset({"RUNNING", "PENDING", "CANCELLING"})
SCHEDULED_STATE: Final = "SCHEDULED"

SCHEDULED_WINDOW_MINUTES: Final = 10
"""A run scheduled to start within this time counts as busy."""

MAX_LISTED_RUNS: Final = 3

DISPATCH_TAG: Final = "disparo:carga-completa"
ITEM_TAG_PREFIX: Final = "carga-completa-item:"
"""The tag with the position of a child run among the sets of parameters."""
INHERITED_TAG_PREFIXES: Final = ("disparo:", "discord-user:")
