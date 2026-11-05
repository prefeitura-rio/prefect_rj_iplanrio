"""Constantes do passo opcional de dbt antes da carga."""

from copy import deepcopy
from re import Pattern
from re import compile as compile_pattern
from zoneinfo import ZoneInfo

DBT_FLOW_NAME = "rj-iplanrio--run-dbt"
"""The name of the flow in Prefect, `rj_iplanrio__run_dbt` with `_` as `-`."""

DBT_DEPLOYMENT = f"{DBT_FLOW_NAME}/rj-iplanrio--run-dbt--prod"
"""Deployment do flow de dbt, no formato ``<flow>/<deployment>``."""

DBT_POLL_SECONDS = 30.0
"""Intervalo entre as consultas ao estado do run de dbt."""

DBT_DEFAULT_PARAMETERS: dict[str, object] = {
    "command": "build",
    "select": "tag:nota_carioca",
    "send_discord_report": False,
    "github_repo": "https://github.com/prefeitura-rio/queries-rj-iplanrio.git",
    "bigquery_project": "rj-iplanrio",
    "target": "prod",
    "gcs_buckets": {"prod": "rj-iplanrio_dbt", "dev": "rj-iplanrio-dev_dbt"},
}


def default_dbt_parameters() -> dict[str, object]:
    """Return a copy of the default parameters of the dbt run.

    They are the parameters of the flow `rj_iplanrio__run_dbt`, and the caller may
    change them.
    """
    return deepcopy(DBT_DEFAULT_PARAMETERS)


BRASILIA = ZoneInfo("America/Sao_Paulo")
"""The time zone of the In-Memory window."""

DDL_LOCK_TIMEOUT_SECONDS = 8
"""How long a `DDL` waits for the lock of an object in one attempt."""

DDL_ATTEMPTS = 26
"""The attempts of a `DDL` on an object that is busy."""

DDL_RETRY_SECONDS = 30.0
"""The wait between two attempts of a `DDL` on a busy object."""

RESOURCE_BUSY = 54
"""The number of `ORA-00054`, the error of an object that is busy."""

UPPER_BOUND_PATTERN: Pattern[str] = compile_pattern(r"TO_DATE\(\s*'\s*(\d{4})-(\d{2})-(\d{2})")
"""The upper bound of a `RANGE` partition by date, as in `TO_DATE(' 2026-08-01 ...`."""


INDEX_NAME_PATTERN = compile_pattern(r"^[A-Z][A-Z0-9_$#]{0,127}$")


# O Oracle cria I_SNAP$_<mview> sozinho para o fast refresh de views materializadas;
# numa tabela comum não tem uso.
SNAPSHOT_INDEX_PREFIX = "I_SNAP$"


SUPPORTED_PARTITIONING = ("RANGE", "LIST")


SUPPORTED_INDEX_TYPES = ("NORMAL", "BITMAP", "FUNCTION-BASED NORMAL")


FUNCTION_BASED_INDEX_TYPE = "FUNCTION-BASED NORMAL"


# MAXEXTENTS UNLIMITED, as the dictionary gives it.
UNLIMITED_EXTENTS = 2147483645


# Valores de all_tables e all_part_tables aceitos no INMEMORY; o texto entra no DDL como
# está.
INMEMORY_PRIORITIES = ("NONE", "LOW", "MEDIUM", "HIGH", "CRITICAL")


INMEMORY_COMPRESSIONS = (
    "NO MEMCOMPRESS",
    "FOR DML",
    "FOR QUERY LOW",
    "FOR QUERY HIGH",
    "FOR CAPACITY LOW",
    "FOR CAPACITY HIGH",
    "AUTO",
)


INMEMORY_DISTRIBUTES = ("AUTO", "BY ROWID RANGE", "BY PARTITION", "BY SUBPARTITION")


INMEMORY_DUPLICATES = ("NO DUPLICATE", "DUPLICATE", "DUPLICATE ALL")


# A tabela carregada nasce NO INMEMORY. Depois da carga, só as partições da janela
# recebem esta cláusula, com as opções das MVT_ originais (helpers/inmemory.py), e a
# tabela sai do In-Memory inteira quando deixa de ser a tabela em uso.
LOADED_TABLE_INMEMORY = "INMEMORY PRIORITY HIGH MEMCOMPRESS FOR QUERY HIGH DISTRIBUTE AUTO NO DUPLICATE"


COLOR_RUNNING = 3447003


COLOR_SUCCESS = 3066993


COLOR_FAILURE = 15158332


COLOR_CANCELLED = 9807270


TITLE_LIMIT = 256


DESCRIPTION_LIMIT = 4096


FIELD_NAME_LIMIT = 256


FIELD_VALUE_LIMIT = 1024


FOOTER_LIMIT = 2048


MAX_FIELDS = 25


# Limite do Discord é 6000 para a soma de título, descrição, campos e rodapé; sobra
# margem para contagem por pontos de código diferente da do Discord.
TOTAL_BUDGET = 5800


MIN_DESCRIPTION = 300


CONTENT_LIMIT = 2000


SHORT_ERROR_LIMIT = 200


VALIDATION_LINE_LIMIT = 300


MISSING_COUNT = "—"


NO_MENTIONS = {"parse": []}


TABLE_PREFIX = "BQLOAD_"


MANAGED_TABLE_MARKER = "rj_smfp__nota_carioca_bq_to_oracle"


CONSUMER_SYNONYM_OWNERS = ("NFSE_SIGA", "NFSE_USER", "NFSE_OWNER")


CONSUMER_GRANTS = (
    ("RL_NFSE", "SELECT"),
    ("RL_NFSE_SIGA", "SELECT"),
    ("RL_NFSEOWNER_DRL", "SELECT"),
    ("NFSE_OWNER", "SELECT, ALTER, DELETE"),
)


# Tabela particionada aparece em all_objects também como TABLE PARTITION/SUBPARTITION,
# com o nome da tabela.
REPLACEABLE_OBJECT_TYPES = {"TABLE", "TABLE PARTITION", "TABLE SUBPARTITION", "SYNONYM"}


CHARACTER_TYPES = ("VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR")


TIMESTAMP_TYPE_PATTERN = compile_pattern(r"^TIMESTAMP\(\d\)( WITH TIME ZONE)?$")


COMPLETED = "COMPLETED"


FINAL_STATE_TYPES = frozenset({COMPLETED, "FAILED", "CRASHED", "CANCELLED"})


DEFAULT_HEARTBEAT_SECONDS = 60.0
