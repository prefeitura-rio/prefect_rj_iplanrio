"""Constantes compartilhadas entre os módulos da pipeline."""

# load_query resolve queries/ ao lado do arquivo recebido; esta constante aponta para a raiz da pipeline.
QUERIES_ANCHOR = __file__

GCS_PREFIX = "oracle_to_bq"
TEMP_TABLE_SUFFIX = "__oracle_to_bq_tmp"
DEFAULT_TABLES = ("DPS", "NOTAS_NACIONAIS", "PESSOAS_NACIONAIS")

AIRBYTE_RAW_ID = "_airbyte_raw_id"
AIRBYTE_EXTRACTED_AT = "_airbyte_extracted_at"
AIRBYTE_META = "_airbyte_meta"
AIRBYTE_GENERATION_ID = "_airbyte_generation_id"

# Coluna de cluster de cada tabela, igual à do destino mantido pelo Airbyte; ``_airbyte_extracted_at`` vem depois.
CLUSTER_KEYS = {
    "DPS": "DPS",
    "NOTAS_NACIONAIS": "NOTA_NACIONAL",
    "PESSOAS_NACIONAIS": "PESSOA_NACIONAL",
}
