"""Constantes compartilhadas entre os módulos da pipeline."""

# load_query resolve queries/ ao lado do arquivo recebido; esta constante aponta para a raiz da pipeline.
QUERIES_ANCHOR = __file__

DEFAULT_TABLES = ("DPS", "NOTAS_NACIONAIS", "PESSOAS_NACIONAIS")
DEFAULT_WORKER_COUNTS = (1, 2, 4)
DEFAULT_COMPRESSIONS = ("zstd", "snappy", "none")
SUPPORTED_COMPRESSIONS = ("zstd", "snappy", "none", "gzip", "lz4", "brotli")

# Tudo que a sonda grava no GCS fica sob <GCS_PREFIX>/<flow_run_id>/ e é apagado ao final.
GCS_PREFIX = "oracle_probe"
# Prefixo da tarefa do DBMS_PARALLEL_EXECUTE, a única coisa que a sonda cria no Oracle.
CHUNK_TASK_PREFIX = "O2BQPROBE_"

AIRBYTE_RAW_ID = "_airbyte_raw_id"
AIRBYTE_EXTRACTED_AT = "_airbyte_extracted_at"
AIRBYTE_GENERATION_ID = "_airbyte_generation_id"

BYTES_PER_MB = 1_000_000
