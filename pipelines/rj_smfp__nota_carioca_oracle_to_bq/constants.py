"""Constants of rj_smfp__nota_carioca_oracle_to_bq."""

from re import Pattern
from re import compile as compile_pattern
from typing import Final

TASK_EXTRACTION: Final = 46
"""The type of the task that extracts the national tax documents to the DFEN schema."""

TASK_POST_PROCESSING: Final = 48
"""The type of the task that post-processes the national notes, after the extraction."""

TASK_RUNNING: Final = 1
"""The `STATUS` of a task that is running."""

TASK_KINDS: Final = {
    TASK_EXTRACTION: "extração de documentos fiscais nacionais",
    TASK_POST_PROCESSING: "pós-processamento das notas nacionais",
}
"""The name of each type of task, for the messages."""

DEFAULT_STALE_HOURS: Final = 6
"""The hours of a running task after which it counts as stuck."""

SECONDS_PER_HOUR: Final = 3600

NO_ACCESS: Final[Pattern[str]] = compile_pattern(r"ORA-(00942|01031)\b")
"""`ORA-00942`, a table that is missing or not visible, and `ORA-01031`, a privilege."""

ACCESS_HINT: Final = "Peça à DBA: GRANT SELECT ON NFSE_OWNER.TAREFAS ao usuário do segredo do Oracle"
