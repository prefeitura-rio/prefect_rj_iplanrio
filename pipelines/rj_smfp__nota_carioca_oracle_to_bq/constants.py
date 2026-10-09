"""Constantes compartilhadas entre os módulos da pipeline."""

# load_query resolve queries/ ao lado do arquivo recebido; esta constante aponta para a raiz da pipeline.
QUERIES_ANCHOR = __file__

GCS_PREFIX = "oracle_to_bq"
# JSONs de progresso dos filhos, lidos pelo pai para o Discord. Irmão de GCS_PREFIX, nunca dentro dele: o load lista
# ``oracle_to_bq/<tabela>/<run id>/`` e não pode enxergá-los.
PROGRESS_PREFIX = "oracle_to_bq_progress"
TEMP_TABLE_SUFFIX = "__oracle_to_bq_tmp"
DEFAULT_TABLES = ("DPS", "NOTAS_NACIONAIS", "PESSOAS_NACIONAIS")

AIRBYTE_RAW_ID = "_airbyte_raw_id"
AIRBYTE_EXTRACTED_AT = "_airbyte_extracted_at"
AIRBYTE_META = "_airbyte_meta"
AIRBYTE_GENERATION_ID = "_airbyte_generation_id"
# Colunas do destino do Airbyte que a pipeline deixou de gravar de propósito; a remoção delas não aborta a carga.
# _airbyte_raw_id: UUID aleatório por linha, sem uso no dbt, ~13-16% dos bytes enviados ao GCS.
RETIRED_COLUMNS = frozenset({AIRBYTE_RAW_ID})

# Coluna de cluster usada só quando a tabela final ainda não existe; com a final existente, a temporária espelha o
# particionamento e o cluster dela (o PESSOAS_NACIONAIS do Airbyte tem só ``_airbyte_extracted_at``, sem chave).
# ``_airbyte_extracted_at`` vem depois da chave.
CLUSTER_KEYS = {
    "DPS": "DPS",
    "NOTAS_NACIONAIS": "NOTA_NACIONAL",
    "PESSOAS_NACIONAIS": "PESSOA_NACIONAL",
}

# Colunas NUMBER de cada tabela cuja contagem de não nulos e soma exata são comparadas entre o que foi extraído do
# Oracle e a tabela temporária no BigQuery (conferência de conteúdo, além da contagem de linhas). Todas existem
# no schema atual de brutos_nota_fiscal; o plano confere que existem no Oracle e são NUMBER antes de extrair.
CHECKSUM_COLUMNS = {
    "DPS": ("VALOR_SERVICO", "NUMERO_DPS"),
    "NOTAS_NACIONAIS": ("NSU", "VALOR_ISSQN"),
    "PESSOAS_NACIONAIS": ("TELEFONE", "CAEPF"),
}

# Tag dos flow runs filhos (um por tabela); o pai a usa para distingui-los dos pais na exclusão mútua.
CHILD_TAG = "o2bq-table"
# Labels da tabela temporária validada pelo filho; o pai só publica se baterem com o seu run id, o SCN e a contagem.
LABEL_RUN_ID = "o2bq_run_id"
LABEL_SCN = "o2bq_scn"
LABEL_ROWS = "o2bq_rows"
# Limite do BigQuery para o valor de um label.
LABEL_VALUE_MAX_LENGTH = 63
