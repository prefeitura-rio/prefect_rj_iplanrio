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
