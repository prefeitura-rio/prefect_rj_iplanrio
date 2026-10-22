"""The names of the tables of a run."""

from pipelines.rj_crm__agentforce_classificacao_llm.constants import (
    CHATBOT_DATASET_ID,
    CHATBOT_V1_TABLE_ID,
    CHATBOT_V2_TABLE_ID,
    DEFAULT_DATASET_ID,
    DEFAULT_HSM_CATALOG_TABLE,
    DEFAULT_RULES_TABLE_ID,
    DEFAULT_SOURCE_TABLE,
    DEFAULT_TABLE_ID,
    DEFAULT_TMP_TABLE_ID,
)
from pipelines.rj_crm__agentforce_classificacao_llm.types import Names, Tables


def resolve_names(project: str, tables: Tables) -> Names:
    """Return the full names of the tables of a run, with the defaults for what `tables` does not set."""
    dataset = tables.get("dataset_id", DEFAULT_DATASET_ID)

    return Names(
        project=project,
        destination=f"{project}.{dataset}.{tables.get('table_id', DEFAULT_TABLE_ID)}",
        tmp=f"{project}.{dataset}.{tables.get('tmp_table_id', DEFAULT_TMP_TABLE_ID)}",
        rules=f"{project}.{dataset}.{tables.get('rules_table_id', DEFAULT_RULES_TABLE_ID)}",
        source=tables.get("source_table", DEFAULT_SOURCE_TABLE),
        hsm_catalog=tables.get("hsm_catalog_table", DEFAULT_HSM_CATALOG_TABLE),
        chatbot_v1=f"{project}.{CHATBOT_DATASET_ID}.{CHATBOT_V1_TABLE_ID}",
        chatbot_v2=f"{project}.{CHATBOT_DATASET_ID}.{CHATBOT_V2_TABLE_ID}",
    )
